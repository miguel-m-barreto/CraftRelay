#![forbid(unsafe_code)]

use craftrelay_domain::{Checksum, DeliveryStatus};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

const SCHEMA_VERSION: i64 = 1;
const MAX_ATTEMPTS: usize = 16;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptRequest {
    pub installation_id: String,
    pub event_id: String,
    pub producer_id: String,
    pub producer_instance_id: String,
    pub producer_operation_sequence: i64,
    pub namespace: String,
    pub logical_stream_type: String,
    pub stream_key: String,
    pub event_type: String,
    pub schema_version: i32,
    pub payload: Vec<u8>,
    pub request_fingerprint: Checksum,
    pub routing_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptResult {
    Accepted(AcceptedEvent),
    ExistingDuplicate(AcceptedEvent),
    Corrupted { event_id: String, detail: String },
    FingerprintConflict { event_id: String },
    SequenceConflict { detail: String },
    DiskUnsafe,
    ShuttingDown,
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedEvent {
    pub event_id: String,
    pub journal_sequence: i64,
    pub stream_sequence: i64,
    pub lifecycle_revision: i64,
    pub envelope_checksum: Checksum,
    pub delivery_status: DeliveryStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusResult {
    Found(PersistedStatus),
    NotFound,
    Corrupted { event_id: String, detail: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedStatus {
    pub event_id: String,
    pub journal_sequence: i64,
    pub stream_sequence: i64,
    pub lifecycle_revision: i64,
    pub lifecycle_checksum: Checksum,
    pub delivery_status: String,
    pub retention_status: String,
    pub payload_digest: Checksum,
    pub envelope_checksum: Checksum,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasResult {
    Updated,
    StaleRevision,
    NotFound,
    InvalidTransition,
    Corrupted { event_id: String, detail: String },
    Error(String),
}

/// Delivery statuses allowed in this sprint (no Kafka delivery).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalDeliveryStatus {
    LocalAccepted,
    DeliveryPending,
    DeliveryBlocked,
}

impl LocalDeliveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalAccepted => "LOCAL_ACCEPTED",
            Self::DeliveryPending => "DELIVERY_PENDING",
            Self::DeliveryBlocked => "DELIVERY_BLOCKED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalRetentionStatus {
    Present,
}

impl LocalRetentionStatus {
    pub fn as_str(self) -> &'static str {
        "PRESENT"
    }
}

pub trait DiskGuard: Send + Sync {
    fn is_safe_to_write(&self) -> bool;
}

#[derive(Debug)]
pub struct AlwaysSafeDiskGuard;
impl DiskGuard for AlwaysSafeDiskGuard {
    fn is_safe_to_write(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct AlwaysUnsafeDiskGuard;
impl DiskGuard for AlwaysUnsafeDiskGuard {
    fn is_safe_to_write(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyResult {
    Ok,
    NotFound,
    DigestMismatch,
    Error(String),
}

// ---------------------------------------------------------------------------
// Journal config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub busy_timeout_ms: u32,
    pub wal_autocheckpoint: u32,
    pub max_payload_bytes: usize,
    pub max_metadata_entries: usize,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            busy_timeout_ms: 5_000,
            wal_autocheckpoint: 1_000,
            max_payload_bytes: 1_048_576,
            max_metadata_entries: 32,
        }
    }
}

// ---------------------------------------------------------------------------
// Schema SQL — Blocker 3: journal_meta created first, checked before rest
// ---------------------------------------------------------------------------

const META_TABLE_SQL: &str = "\
CREATE TABLE IF NOT EXISTS journal_meta (\
    key   TEXT PRIMARY KEY,\
    value TEXT NOT NULL\
);";

// Blocker 4: real foreign keys on payload_blob, lifecycle_state, dedup_fingerprint.
// write_attempt does NOT use FK because conflict/reject attempts are recorded
// before the envelope exists.
const SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS stored_envelope (
    journal_sequence    INTEGER PRIMARY KEY,
    event_id            TEXT    NOT NULL UNIQUE,
    installation_id     TEXT    NOT NULL,
    producer_id         TEXT    NOT NULL,
    producer_instance_id TEXT   NOT NULL,
    producer_op_seq     INTEGER NOT NULL,
    namespace           TEXT    NOT NULL,
    logical_stream_type TEXT    NOT NULL,
    stream_key          TEXT    NOT NULL,
    event_type          TEXT    NOT NULL,
    schema_version      INTEGER NOT NULL,
    stream_sequence     INTEGER NOT NULL,
    payload_digest      BLOB    NOT NULL,
    request_fingerprint BLOB    NOT NULL,
    envelope_checksum   BLOB    NOT NULL,
    payload_ref_id      TEXT    NOT NULL,
    payload_length      INTEGER NOT NULL,
    routing_version     INTEGER NOT NULL,
    received_at_ms      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS payload_blob (
    event_id       TEXT PRIMARY KEY
        REFERENCES stored_envelope(event_id),
    payload        BLOB NOT NULL,
    payload_digest BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS lifecycle_state (
    event_id           TEXT    PRIMARY KEY
        REFERENCES stored_envelope(event_id),
    revision           INTEGER NOT NULL,
    lifecycle_checksum BLOB    NOT NULL,
    delivery_status    TEXT    NOT NULL,
    retention_status   TEXT    NOT NULL,
    updated_at_ms      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dedup_fingerprint (
    event_id            TEXT PRIMARY KEY
        REFERENCES stored_envelope(event_id),
    request_fingerprint BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS producer_sequence (
    producer_key   TEXT PRIMARY KEY,
    highest_seq    INTEGER NOT NULL,
    last_event_id  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stream_head (
    stream_key     TEXT PRIMARY KEY,
    head_sequence  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS write_attempt (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id   TEXT    NOT NULL,
    outcome    TEXT    NOT NULL,
    detail     TEXT,
    attempt_at_ms INTEGER NOT NULL
);
";

// ---------------------------------------------------------------------------
// Journal implementation
// ---------------------------------------------------------------------------

pub struct LocalJournal {
    conn: Mutex<Connection>,
    config: JournalConfig,
    disk_guard: Box<dyn DiskGuard>,
    shutdown: AtomicBool,
}

impl LocalJournal {
    pub fn open(
        path: &Path,
        config: JournalConfig,
        disk_guard: Box<dyn DiskGuard>,
    ) -> Result<Self, String> {
        let conn = Connection::open(path).map_err(|e| e.to_string())?;
        let journal = Self {
            conn: Mutex::new(conn),
            config,
            disk_guard,
            shutdown: AtomicBool::new(false),
        };
        journal.init_schema()?;
        Ok(journal)
    }

    pub fn open_in_memory(
        config: JournalConfig,
        disk_guard: Box<dyn DiskGuard>,
    ) -> Result<Self, String> {
        let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
        let journal = Self {
            conn: Mutex::new(conn),
            config,
            disk_guard,
            shutdown: AtomicBool::new(false),
        };
        journal.init_schema()?;
        Ok(journal)
    }

    // Blocker 3: create meta table first, check version BEFORE
    // applying the rest of the schema.
    fn init_schema(&self) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        apply_pragmas(&conn, &self.config);

        conn.execute_batch(META_TABLE_SQL)
            .map_err(|e| e.to_string())?;

        let existing: Option<String> = conn
            .query_row(
                "SELECT value FROM journal_meta \
                 WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;

        if let Some(v) = &existing {
            let ver: i64 = v.parse().map_err(|_| "corrupt schema version")?;
            if ver > SCHEMA_VERSION {
                return Err(format!(
                    "unknown future schema version {ver}, \
                     max supported is {SCHEMA_VERSION}"
                ));
            }
        }

        conn.execute_batch(SCHEMA_SQL).map_err(|e| e.to_string())?;

        if existing.is_none() {
            conn.execute(
                "INSERT INTO journal_meta (key, value) \
                 VALUES ('schema_version', ?1)",
                params![SCHEMA_VERSION.to_string()],
            )
            .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn schema_version(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT value FROM journal_meta \
             WHERE key = 'schema_version'",
            [],
            |row| {
                let v: String = row.get(0)?;
                Ok(v.parse::<i64>().unwrap_or(0))
            },
        )
        .unwrap_or(0)
    }

    pub fn journal_mode(&self) -> String {
        let conn = self.conn.lock().unwrap();
        conn.query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .unwrap_or_default()
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    // --- Accept ---

    pub fn accept(&self, req: &AcceptRequest) -> AcceptResult {
        if self.is_shutdown() {
            return AcceptResult::ShuttingDown;
        }
        if !self.disk_guard.is_safe_to_write() {
            return AcceptResult::DiskUnsafe;
        }
        if req.payload.len() > self.config.max_payload_bytes {
            return AcceptResult::Error("payload exceeds max size".into());
        }
        if req.event_id.is_empty() || req.installation_id.is_empty() {
            return AcceptResult::Error("missing required field".into());
        }

        let mut conn = self.conn.lock().unwrap();
        let now_ms = now_epoch_ms();
        let payload_digest = compute_sha256(&req.payload);

        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(e) => return AcceptResult::Error(e.to_string()),
        };

        let dedup = match check_dedup(&tx, &req.event_id, &req.request_fingerprint) {
            Ok(d) => d,
            Err(detail) => {
                return AcceptResult::Corrupted {
                    event_id: req.event_id.clone(),
                    detail,
                };
            }
        };

        match dedup {
            DedupCheck::New => {}
            DedupCheck::Duplicate => match get_status_impl(&tx, &req.event_id) {
                StatusResult::Found(status) => {
                    if let Err(detail) = verify_payload_digest(&tx, &req.event_id) {
                        return AcceptResult::Corrupted {
                            event_id: req.event_id.clone(),
                            detail,
                        };
                    }
                    let delivery = match status.delivery_status.as_str() {
                        "LOCAL_ACCEPTED" => DeliveryStatus::LocalAccepted,
                        "DELIVERY_PENDING" => DeliveryStatus::DeliveryPending,
                        _ => DeliveryStatus::DeliveryBlocked,
                    };
                    record_attempt(&tx, &req.event_id, "EXISTING_DUPLICATE", None, now_ms);
                    let _ = tx.commit();
                    return AcceptResult::ExistingDuplicate(AcceptedEvent {
                        event_id: status.event_id,
                        journal_sequence: status.journal_sequence,
                        stream_sequence: status.stream_sequence,
                        lifecycle_revision: status.lifecycle_revision,
                        envelope_checksum: status.envelope_checksum,
                        delivery_status: delivery,
                    });
                }
                StatusResult::Corrupted { event_id, detail } => {
                    return AcceptResult::Corrupted { event_id, detail };
                }
                StatusResult::NotFound => {
                    return AcceptResult::Error("dedup entry without envelope".into());
                }
            },
            DedupCheck::Conflict => {
                record_attempt(&tx, &req.event_id, "FINGERPRINT_CONFLICT", None, now_ms);
                let _ = tx.commit();
                return AcceptResult::FingerprintConflict {
                    event_id: req.event_id.clone(),
                };
            }
        }

        let producer_key = format!(
            "{}\0{}\0{}",
            req.installation_id, req.producer_id, req.producer_instance_id
        );
        match check_producer_sequence(
            &tx,
            &producer_key,
            req.producer_operation_sequence,
            &req.event_id,
        ) {
            Err(detail) => {
                return AcceptResult::Corrupted {
                    event_id: req.event_id.clone(),
                    detail,
                };
            }
            Ok(Some(conflict)) => {
                record_attempt(
                    &tx,
                    &req.event_id,
                    "SEQUENCE_CONFLICT",
                    Some(&conflict),
                    now_ms,
                );
                let _ = tx.commit();
                return AcceptResult::SequenceConflict { detail: conflict };
            }
            Ok(None) => {}
        }

        let journal_sequence = allocate_journal_sequence(&tx);
        let stream_key = format!(
            "{}\0{}\0{}\0{}",
            req.installation_id, req.namespace, req.logical_stream_type, req.stream_key
        );
        let stream_sequence = match allocate_stream_sequence(&tx, &stream_key) {
            Ok(s) => s,
            Err(detail) => return AcceptResult::Error(detail),
        };
        let payload_ref_id = format!("{}/{}", req.installation_id, req.event_id);

        // Blocker 2: full canonical envelope checksum
        let envelope_checksum = compute_envelope_checksum(
            journal_sequence,
            &req.event_id,
            &req.installation_id,
            &req.producer_id,
            &req.producer_instance_id,
            req.producer_operation_sequence,
            &req.namespace,
            &req.logical_stream_type,
            &req.stream_key,
            &req.event_type,
            req.schema_version,
            stream_sequence,
            &payload_digest,
            &req.request_fingerprint,
            &payload_ref_id,
            req.payload.len() as i64,
            req.routing_version,
        );

        if let Err(e) = tx.execute(
            "INSERT INTO stored_envelope (\
                journal_sequence, event_id, installation_id, \
                producer_id, producer_instance_id, producer_op_seq, \
                namespace, logical_stream_type, stream_key, \
                event_type, schema_version, stream_sequence, \
                payload_digest, request_fingerprint, \
                envelope_checksum, payload_ref_id, payload_length, \
                routing_version, received_at_ms\
             ) VALUES \
             (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,\
              ?13,?14,?15,?16,?17,?18,?19)",
            params![
                journal_sequence,
                req.event_id,
                req.installation_id,
                req.producer_id,
                req.producer_instance_id,
                req.producer_operation_sequence,
                req.namespace,
                req.logical_stream_type,
                req.stream_key,
                req.event_type,
                req.schema_version,
                stream_sequence,
                payload_digest.as_slice(),
                req.request_fingerprint.as_slice(),
                envelope_checksum.as_slice(),
                payload_ref_id,
                req.payload.len() as i64,
                req.routing_version,
                now_ms,
            ],
        ) {
            return AcceptResult::Error(e.to_string());
        }

        if let Err(e) = tx.execute(
            "INSERT INTO payload_blob \
             (event_id, payload, payload_digest) \
             VALUES (?1, ?2, ?3)",
            params![
                req.event_id,
                req.payload.as_slice(),
                payload_digest.as_slice(),
            ],
        ) {
            return AcceptResult::Error(e.to_string());
        }

        let lifecycle_checksum =
            compute_lifecycle_checksum(&req.event_id, 1, "LOCAL_ACCEPTED", "PRESENT");
        if let Err(e) = tx.execute(
            "INSERT INTO lifecycle_state (\
                event_id, revision, lifecycle_checksum, \
                delivery_status, retention_status, updated_at_ms\
             ) VALUES (?1, 1, ?2, 'LOCAL_ACCEPTED', 'PRESENT', ?3)",
            params![req.event_id, lifecycle_checksum.as_slice(), now_ms,],
        ) {
            return AcceptResult::Error(e.to_string());
        }

        if let Err(e) = tx.execute(
            "INSERT INTO dedup_fingerprint \
             (event_id, request_fingerprint) VALUES (?1, ?2)",
            params![req.event_id, req.request_fingerprint.as_slice(),],
        ) {
            return AcceptResult::Error(e.to_string());
        }

        if let Err(e) = tx.execute(
            "INSERT INTO producer_sequence \
             (producer_key, highest_seq, last_event_id) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT(producer_key) DO UPDATE SET \
             highest_seq = ?2, last_event_id = ?3",
            params![producer_key, req.producer_operation_sequence, req.event_id,],
        ) {
            return AcceptResult::Error(e.to_string());
        }

        record_attempt(&tx, &req.event_id, "NEWLY_ACCEPTED", None, now_ms);

        if let Err(e) = tx.commit() {
            return AcceptResult::Error(e.to_string());
        }

        AcceptResult::Accepted(AcceptedEvent {
            event_id: req.event_id.clone(),
            journal_sequence,
            stream_sequence,
            lifecycle_revision: 1,
            envelope_checksum,
            delivery_status: DeliveryStatus::LocalAccepted,
        })
    }

    // --- Status lookup ---

    pub fn get_status(&self, event_id: &str) -> StatusResult {
        let conn = self.conn.lock().unwrap();
        get_status_impl(&conn, event_id)
    }

    pub fn get_status_by_journal_seq(&self, journal_seq: i64) -> StatusResult {
        let conn = self.conn.lock().unwrap();
        let event_id: String = match conn
            .query_row(
                "SELECT event_id FROM stored_envelope \
                 WHERE journal_sequence = ?1",
                params![journal_seq],
                |row| row.get(0),
            )
            .optional()
        {
            Ok(Some(eid)) => eid,
            Ok(None) => return StatusResult::NotFound,
            Err(e) => {
                return StatusResult::Corrupted {
                    event_id: format!("journal_seq={journal_seq}"),
                    detail: format!("sequence lookup error: {e}"),
                };
            }
        };
        get_status_impl(&conn, &event_id)
    }

    // --- CAS lifecycle update (Blocker 1: typed, rejects replicated) ---

    pub fn update_lifecycle(
        &self,
        event_id: &str,
        expected_revision: i64,
        new_delivery: LocalDeliveryStatus,
        new_retention: LocalRetentionStatus,
    ) -> CasResult {
        let conn = self.conn.lock().unwrap();

        match get_status_impl(&conn, event_id) {
            StatusResult::NotFound => CasResult::NotFound,
            StatusResult::Corrupted { event_id, detail } => {
                CasResult::Corrupted { event_id, detail }
            }
            StatusResult::Found(status) => {
                if status.lifecycle_revision != expected_revision {
                    return CasResult::StaleRevision;
                }
                let new_rev = expected_revision + 1;
                let ds = new_delivery.as_str();
                let rs = new_retention.as_str();
                let checksum = compute_lifecycle_checksum(event_id, new_rev, ds, rs);
                let now_ms = now_epoch_ms();
                let affected = match conn.execute(
                    "UPDATE lifecycle_state SET \
                     revision = ?1, lifecycle_checksum = ?2, \
                     delivery_status = ?3, retention_status = ?4, \
                     updated_at_ms = ?5 \
                     WHERE event_id = ?6 AND revision = ?7",
                    params![
                        new_rev,
                        checksum.as_slice(),
                        ds,
                        rs,
                        now_ms,
                        event_id,
                        expected_revision,
                    ],
                ) {
                    Ok(n) => n,
                    Err(e) => return CasResult::Error(e.to_string()),
                };
                match affected {
                    1 => CasResult::Updated,
                    0 => CasResult::StaleRevision,
                    n => {
                        CasResult::Error(format!("lifecycle update affected {n} rows, expected 1"))
                    }
                }
            }
        }
    }

    // --- Payload verification ---

    pub fn verify_payload(&self, event_id: &str) -> VerifyResult {
        let conn = self.conn.lock().unwrap();
        let row = match conn
            .query_row(
                "SELECT payload, payload_digest FROM payload_blob \
                 WHERE event_id = ?1",
                params![event_id],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
        {
            Ok(r) => r,
            Err(e) => return VerifyResult::Error(format!("payload query error: {e}")),
        };
        match row {
            None => VerifyResult::NotFound,
            Some((payload, stored_digest)) => {
                let actual = compute_sha256(&payload);
                if actual.as_slice() == stored_digest.as_slice() {
                    VerifyResult::Ok
                } else {
                    VerifyResult::DigestMismatch
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn apply_pragmas(conn: &Connection, config: &JournalConfig) {
    conn.execute_batch("PRAGMA journal_mode = WAL;")
        .expect("journal_mode");
    conn.execute_batch("PRAGMA synchronous = FULL;")
        .expect("synchronous");
    conn.execute_batch(&format!(
        "PRAGMA busy_timeout = {};",
        config.busy_timeout_ms
    ))
    .expect("busy_timeout");
    conn.execute_batch(&format!(
        "PRAGMA wal_autocheckpoint = {};",
        config.wal_autocheckpoint
    ))
    .expect("wal_autocheckpoint");
    conn.execute_batch("PRAGMA foreign_keys = ON;")
        .expect("foreign_keys");
}

enum DedupCheck {
    New,
    Duplicate,
    Conflict,
}

fn check_dedup(
    tx: &Transaction<'_>,
    event_id: &str,
    fingerprint: &Checksum,
) -> Result<DedupCheck, String> {
    let stored: Option<Vec<u8>> = match tx
        .query_row(
            "SELECT request_fingerprint FROM dedup_fingerprint \
             WHERE event_id = ?1",
            params![event_id],
            |row| row.get(0),
        )
        .optional()
    {
        Ok(r) => r,
        Err(e) => return Err(format!("dedup query error: {e}")),
    };
    Ok(match stored {
        None => DedupCheck::New,
        Some(fp) if fp.as_slice() == fingerprint.as_slice() => DedupCheck::Duplicate,
        Some(_) => DedupCheck::Conflict,
    })
}

fn check_producer_sequence(
    tx: &Transaction<'_>,
    producer_key: &str,
    seq: i64,
    event_id: &str,
) -> Result<Option<String>, String> {
    let row: Option<(i64, String)> = match tx
        .query_row(
            "SELECT highest_seq, last_event_id \
             FROM producer_sequence WHERE producer_key = ?1",
            params![producer_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
    {
        Ok(r) => r,
        Err(e) => return Err(format!("producer sequence query error: {e}")),
    };
    Ok(match row {
        None => None,
        Some((highest, last_eid)) => {
            if seq <= highest && last_eid != event_id {
                Some(format!(
                    "producer sequence {seq} conflicts with \
                     existing highest {highest} \
                     (last event_id={last_eid})"
                ))
            } else {
                None
            }
        }
    })
}

fn allocate_journal_sequence(tx: &Transaction<'_>) -> i64 {
    tx.query_row(
        "SELECT COALESCE(MAX(journal_sequence), 0) + 1 \
         FROM stored_envelope",
        [],
        |row| row.get(0),
    )
    .expect("journal_sequence allocation")
}

fn allocate_stream_sequence(tx: &Transaction<'_>, stream_key: &str) -> Result<i64, String> {
    let existing: Option<i64> = match tx
        .query_row(
            "SELECT head_sequence FROM stream_head \
             WHERE stream_key = ?1",
            params![stream_key],
            |row| row.get(0),
        )
        .optional()
    {
        Ok(r) => r,
        Err(e) => return Err(format!("stream head query error: {e}")),
    };
    let next = existing.unwrap_or(0) + 1;
    tx.execute(
        "INSERT INTO stream_head (stream_key, head_sequence) \
         VALUES (?1, ?2) \
         ON CONFLICT(stream_key) DO UPDATE SET head_sequence = ?2",
        params![stream_key, next],
    )
    .map_err(|e| format!("stream head upsert error: {e}"))?;
    Ok(next)
}

fn compute_sha256(data: &[u8]) -> Checksum {
    Sha256::digest(data).into()
}

// Blocker 2: canonical envelope checksum covering all immutable fields.
#[allow(clippy::too_many_arguments)]
fn compute_envelope_checksum(
    journal_seq: i64,
    event_id: &str,
    installation_id: &str,
    producer_id: &str,
    producer_instance_id: &str,
    producer_op_seq: i64,
    namespace: &str,
    logical_stream_type: &str,
    stream_key: &str,
    event_type: &str,
    schema_version: i32,
    stream_seq: i64,
    payload_digest: &Checksum,
    request_fingerprint: &Checksum,
    payload_ref_id: &str,
    payload_length: i64,
    routing_version: i64,
) -> Checksum {
    let mut h = Sha256::new();
    h.update(journal_seq.to_be_bytes());
    h.update(b"\0");
    h.update(event_id.as_bytes());
    h.update(b"\0");
    h.update(installation_id.as_bytes());
    h.update(b"\0");
    h.update(producer_id.as_bytes());
    h.update(b"\0");
    h.update(producer_instance_id.as_bytes());
    h.update(b"\0");
    h.update(producer_op_seq.to_be_bytes());
    h.update(b"\0");
    h.update(namespace.as_bytes());
    h.update(b"\0");
    h.update(logical_stream_type.as_bytes());
    h.update(b"\0");
    h.update(stream_key.as_bytes());
    h.update(b"\0");
    h.update(event_type.as_bytes());
    h.update(b"\0");
    h.update(schema_version.to_be_bytes());
    h.update(b"\0");
    h.update(stream_seq.to_be_bytes());
    h.update(b"\0");
    h.update(payload_digest);
    h.update(b"\0");
    h.update(request_fingerprint);
    h.update(b"\0");
    h.update(payload_ref_id.as_bytes());
    h.update(b"\0");
    h.update(payload_length.to_be_bytes());
    h.update(b"\0");
    h.update(routing_version.to_be_bytes());
    h.finalize().into()
}

fn recompute_envelope_checksum_from_row(
    conn: &Connection,
    event_id: &str,
) -> Result<Checksum, String> {
    let row = match conn
        .query_row(
            "SELECT journal_sequence, installation_id, producer_id, \
                    producer_instance_id, producer_op_seq, namespace, \
                    logical_stream_type, stream_key, event_type, \
                    schema_version, stream_sequence, payload_digest, \
                    request_fingerprint, payload_ref_id, \
                    payload_length, routing_version \
             FROM stored_envelope WHERE event_id = ?1",
            params![event_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, i32>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, Vec<u8>>(11)?,
                    row.get::<_, Vec<u8>>(12)?,
                    row.get::<_, String>(13)?,
                    row.get::<_, i64>(14)?,
                    row.get::<_, i64>(15)?,
                ))
            },
        )
        .optional()
        .map_err(|e| format!("envelope recompute query error: {e}"))?
    {
        Some(r) => r,
        None => return Err("stored envelope row not found for recompute".into()),
    };
    let (js, inst, prod, pinst, opseq, ns, lst, sk, et, sv, ss, pd, rf, pref, pl, rv) = row;
    if pd.len() != 32 {
        return Err(format!("payload_digest length {}, expected 32", pd.len()));
    }
    if rf.len() != 32 {
        return Err(format!(
            "request_fingerprint length {}, expected 32",
            rf.len()
        ));
    }
    let mut pd_arr = [0u8; 32];
    pd_arr.copy_from_slice(&pd);
    let mut rf_arr = [0u8; 32];
    rf_arr.copy_from_slice(&rf);
    Ok(compute_envelope_checksum(
        js, event_id, &inst, &prod, &pinst, opseq, &ns, &lst, &sk, &et, sv, ss, &pd_arr, &rf_arr,
        &pref, pl, rv,
    ))
}

fn verify_payload_digest(conn: &Connection, event_id: &str) -> Result<(), String> {
    let (payload, stored_digest) = match conn
        .query_row(
            "SELECT payload, payload_digest FROM payload_blob \
             WHERE event_id = ?1",
            params![event_id],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
    {
        Ok(Some(r)) => r,
        Ok(None) => return Err("payload blob not found".into()),
        Err(e) => return Err(format!("payload query error: {e}")),
    };
    if stored_digest.len() != 32 {
        return Err(format!(
            "payload_digest length {}, expected 32",
            stored_digest.len()
        ));
    }
    let actual = compute_sha256(&payload);
    if actual.as_slice() != stored_digest.as_slice() {
        return Err("payload digest mismatch".into());
    }
    Ok(())
}

fn compute_lifecycle_checksum(
    event_id: &str,
    revision: i64,
    delivery_status: &str,
    retention_status: &str,
) -> Checksum {
    craftrelay_domain::sha256(
        format!(
            "{event_id}\0{revision}\0\
             {delivery_status}\0{retention_status}"
        )
        .as_bytes(),
    )
}

fn record_attempt(
    tx: &Transaction<'_>,
    event_id: &str,
    outcome: &str,
    detail: Option<&str>,
    now_ms: i64,
) {
    let _ = tx.execute(
        "INSERT INTO write_attempt \
         (event_id, outcome, detail, attempt_at_ms) \
         VALUES (?1, ?2, ?3, ?4)",
        params![event_id, outcome, detail, now_ms],
    );
    let _ = tx.execute(
        "DELETE FROM write_attempt WHERE id IN (\
            SELECT id FROM write_attempt \
            WHERE event_id = ?1 \
            ORDER BY id DESC \
            LIMIT -1 OFFSET ?2\
         )",
        params![event_id, MAX_ATTEMPTS as i64],
    );
}

fn is_allowed_delivery_status(status: &str) -> bool {
    matches!(
        status,
        "LOCAL_ACCEPTED" | "DELIVERY_PENDING" | "DELIVERY_BLOCKED"
    )
}

fn is_allowed_retention_status(status: &str) -> bool {
    matches!(status, "PRESENT")
}

fn get_status_impl(conn: &Connection, event_id: &str) -> StatusResult {
    let row = match conn
        .query_row(
            "SELECT e.journal_sequence, e.stream_sequence, \
                    e.payload_digest, e.envelope_checksum, \
                    l.revision, l.lifecycle_checksum, \
                    l.delivery_status, l.retention_status \
             FROM stored_envelope e \
             JOIN lifecycle_state l \
               ON e.event_id = l.event_id \
             WHERE e.event_id = ?1",
            params![event_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .optional()
    {
        Ok(r) => r,
        Err(e) => {
            return StatusResult::Corrupted {
                event_id: event_id.to_string(),
                detail: format!("status query error: {e}"),
            };
        }
    };

    match row {
        None => StatusResult::NotFound,
        Some((journal_seq, stream_seq, pd, ec, rev, lc, delivery, retention)) => {
            if pd.len() != 32 || ec.len() != 32 || lc.len() != 32 {
                return StatusResult::Corrupted {
                    event_id: event_id.to_string(),
                    detail: "checksum field length invalid".into(),
                };
            }
            let mut payload_digest = [0u8; 32];
            payload_digest.copy_from_slice(&pd);
            let mut envelope_checksum = [0u8; 32];
            envelope_checksum.copy_from_slice(&ec);
            let mut lifecycle_checksum = [0u8; 32];
            lifecycle_checksum.copy_from_slice(&lc);

            // Verify lifecycle checksum
            let expected_lc = compute_lifecycle_checksum(event_id, rev, &delivery, &retention);
            if lifecycle_checksum != expected_lc {
                return StatusResult::Corrupted {
                    event_id: event_id.to_string(),
                    detail: "lifecycle checksum mismatch".into(),
                };
            }

            if !is_allowed_delivery_status(&delivery) {
                return StatusResult::Corrupted {
                    event_id: event_id.to_string(),
                    detail: format!("unsupported delivery status: {delivery}"),
                };
            }
            if !is_allowed_retention_status(&retention) {
                return StatusResult::Corrupted {
                    event_id: event_id.to_string(),
                    detail: format!("unsupported retention status: {retention}"),
                };
            }

            // Verify envelope checksum — errors are corruption, not skip
            match recompute_envelope_checksum_from_row(conn, event_id) {
                Ok(expected_ec) => {
                    if envelope_checksum != expected_ec {
                        return StatusResult::Corrupted {
                            event_id: event_id.to_string(),
                            detail: "envelope checksum mismatch".into(),
                        };
                    }
                }
                Err(detail) => {
                    return StatusResult::Corrupted {
                        event_id: event_id.to_string(),
                        detail,
                    };
                }
            }

            StatusResult::Found(PersistedStatus {
                event_id: event_id.to_string(),
                journal_sequence: journal_seq,
                stream_sequence: stream_seq,
                lifecycle_revision: rev,
                lifecycle_checksum,
                delivery_status: delivery,
                retention_status: retention,
                payload_digest,
                envelope_checksum,
            })
        }
    }
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> JournalConfig {
        JournalConfig::default()
    }

    fn req(event_id: &str, seq: i64) -> AcceptRequest {
        let payload = b"test-payload";
        AcceptRequest {
            installation_id: "inst-a".into(),
            event_id: event_id.into(),
            producer_id: "producer-a".into(),
            producer_instance_id: "instance-1".into(),
            producer_operation_sequence: seq,
            namespace: "economy".into(),
            logical_stream_type: "account".into(),
            stream_key: "player-1".into(),
            event_type: "transfer".into(),
            schema_version: 1,
            payload: payload.to_vec(),
            request_fingerprint: craftrelay_domain::sha256(payload),
            routing_version: 1,
        }
    }

    fn mem() -> LocalJournal {
        LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap()
    }

    fn tmpdir(name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.db");
        (dir, path)
    }

    // --- Schema ---

    #[test]
    fn schema_created_on_open() {
        assert_eq!(mem().schema_version(), SCHEMA_VERSION);
    }

    #[test]
    fn repeated_open_is_idempotent() {
        let (dir, path) = tmpdir("cr-j-idem");
        let j = LocalJournal::open(&path, cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        assert_eq!(j.schema_version(), SCHEMA_VERSION);
        drop(j);
        let j = LocalJournal::open(&path, cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        assert_eq!(j.schema_version(), SCHEMA_VERSION);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn future_schema_rejected_before_mutation() {
        let (dir, path) = tmpdir("cr-j-future");
        let j = LocalJournal::open(&path, cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        {
            let c = j.conn.lock().unwrap();
            c.execute(
                "UPDATE journal_meta SET value='999' WHERE key='schema_version'",
                [],
            )
            .unwrap();
        }
        drop(j);
        match LocalJournal::open(&path, cfg(), Box::new(AlwaysSafeDiskGuard)) {
            Err(e) => assert!(e.contains("unknown future schema")),
            Ok(_) => panic!("should reject"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn wal_mode_enabled() {
        let mode = mem().journal_mode();
        assert!(mode == "wal" || mode == "memory");
    }

    // --- Accept ---

    #[test]
    fn local_acceptance_writes_atomically() {
        let j = mem();
        match j.accept(&req("e1", 1)) {
            AcceptResult::Accepted(a) => {
                assert_eq!(a.journal_sequence, 1);
                assert_eq!(a.stream_sequence, 1);
                assert_eq!(a.lifecycle_revision, 1);
                assert_eq!(a.delivery_status, DeliveryStatus::LocalAccepted);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn journal_seq_monotonic() {
        let j = mem();
        let s: Vec<i64> = (1..=3)
            .map(|i| match j.accept(&req(&format!("e{i}"), i)) {
                AcceptResult::Accepted(a) => a.journal_sequence,
                _ => panic!(),
            })
            .collect();
        assert_eq!(s, vec![1, 2, 3]);
    }

    #[test]
    fn stream_seq_per_stream() {
        let j = mem();
        let mut r1 = req("e1", 1);
        r1.stream_key = "a".into();
        let mut r2 = req("e2", 2);
        r2.stream_key = "a".into();
        let mut r3 = req("e3", 3);
        r3.stream_key = "b".into();
        let s: Vec<i64> = [j.accept(&r1), j.accept(&r2), j.accept(&r3)]
            .iter()
            .map(|r| match r {
                AcceptResult::Accepted(a) => a.stream_sequence,
                _ => panic!(),
            })
            .collect();
        assert_eq!(s, vec![1, 2, 1]);
    }

    #[test]
    fn failed_tx_no_partial() {
        let j = mem();
        let mut r = req("e1", 1);
        r.payload = vec![0u8; 2_000_000];
        assert!(matches!(j.accept(&r), AcceptResult::Error(_)));
        assert!(matches!(j.get_status("e1"), StatusResult::NotFound));
    }

    // --- Dedup/conflict ---

    #[test]
    fn dup_same_fp() {
        let j = mem();
        let r = req("e1", 1);
        assert!(matches!(j.accept(&r), AcceptResult::Accepted(_)));
        assert!(matches!(j.accept(&r), AcceptResult::ExistingDuplicate(_)));
    }

    #[test]
    fn dup_diff_fp() {
        let j = mem();
        j.accept(&req("e1", 1));
        let mut r2 = req("e1", 1);
        r2.request_fingerprint = [0xFF; 32];
        assert!(matches!(
            j.accept(&r2),
            AcceptResult::FingerprintConflict { .. }
        ));
    }

    #[test]
    fn producer_seq_conflict() {
        let j = mem();
        j.accept(&req("e1", 5));
        let mut r2 = req("e2", 3);
        r2.request_fingerprint = [0xAA; 32];
        assert!(matches!(
            j.accept(&r2),
            AcceptResult::SequenceConflict { .. }
        ));
    }

    // --- Status ---

    #[test]
    fn status_by_event_id() {
        let j = mem();
        j.accept(&req("e1", 1));
        match j.get_status("e1") {
            StatusResult::Found(s) => {
                assert_eq!(s.delivery_status, "LOCAL_ACCEPTED");
                assert_eq!(s.retention_status, "PRESENT");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn status_by_journal_seq() {
        let j = mem();
        j.accept(&req("e1", 1));
        assert!(matches!(
            j.get_status_by_journal_seq(1),
            StatusResult::Found(_)
        ));
        assert!(matches!(
            j.get_status_by_journal_seq(99),
            StatusResult::NotFound
        ));
    }

    #[test]
    fn status_after_reopen() {
        let (dir, path) = tmpdir("cr-j-reopen");
        let j = LocalJournal::open(&path, cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&req("e1", 1));
        drop(j);
        let j = LocalJournal::open(&path, cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        assert!(matches!(j.get_status("e1"), StatusResult::Found(_)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_not_found() {
        assert!(matches!(mem().get_status("nope"), StatusResult::NotFound));
    }

    // --- Lifecycle CAS (Blocker 1) ---

    #[test]
    fn cas_update_succeeds() {
        let j = mem();
        j.accept(&req("e1", 1));
        assert_eq!(
            j.update_lifecycle(
                "e1",
                1,
                LocalDeliveryStatus::DeliveryPending,
                LocalRetentionStatus::Present
            ),
            CasResult::Updated
        );
        match j.get_status("e1") {
            StatusResult::Found(s) => {
                assert_eq!(s.lifecycle_revision, 2);
                assert_eq!(s.delivery_status, "DELIVERY_PENDING");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn cas_rejects_stale() {
        let j = mem();
        j.accept(&req("e1", 1));
        j.update_lifecycle(
            "e1",
            1,
            LocalDeliveryStatus::DeliveryPending,
            LocalRetentionStatus::Present,
        );
        assert_eq!(
            j.update_lifecycle(
                "e1",
                1,
                LocalDeliveryStatus::DeliveryBlocked,
                LocalRetentionStatus::Present
            ),
            CasResult::StaleRevision
        );
    }

    #[test]
    fn replicated_status_not_possible() {
        // LocalDeliveryStatus has no Replicated variant — this is a compile-time guarantee.
        // Verify the allowed variants don't include it.
        let allowed = [
            LocalDeliveryStatus::LocalAccepted,
            LocalDeliveryStatus::DeliveryPending,
            LocalDeliveryStatus::DeliveryBlocked,
        ];
        for s in &allowed {
            assert_ne!(s.as_str(), "REPLICATED");
        }
    }

    #[test]
    fn status_remains_local_after_cas() {
        let j = mem();
        j.accept(&req("e1", 1));
        j.update_lifecycle(
            "e1",
            1,
            LocalDeliveryStatus::DeliveryPending,
            LocalRetentionStatus::Present,
        );
        match j.get_status("e1") {
            StatusResult::Found(s) => {
                assert_ne!(s.delivery_status, "REPLICATED");
            }
            _ => panic!(),
        }
    }

    // --- Envelope checksum verification (Blocker 2) ---

    #[test]
    fn envelope_checksum_verified_on_read() {
        let j = mem();
        j.accept(&req("e1", 1));
        assert!(matches!(j.get_status("e1"), StatusResult::Found(_)));
    }

    #[test]
    fn tampered_namespace_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE stored_envelope SET namespace='HACKED' WHERE event_id='e1'",
        );
        assert!(matches!(j.get_status("e1"), StatusResult::Corrupted { .. }));
    }

    #[test]
    fn tampered_producer_id_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE stored_envelope SET producer_id='evil' WHERE event_id='e1'",
        );
        assert!(matches!(j.get_status("e1"), StatusResult::Corrupted { .. }));
    }

    #[test]
    fn tampered_event_type_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE stored_envelope SET event_type='bad' WHERE event_id='e1'",
        );
        assert!(matches!(j.get_status("e1"), StatusResult::Corrupted { .. }));
    }

    #[test]
    fn tampered_routing_version_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE stored_envelope SET routing_version=999 WHERE event_id='e1'",
        );
        assert!(matches!(j.get_status("e1"), StatusResult::Corrupted { .. }));
    }

    #[test]
    fn tampered_payload_length_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE stored_envelope SET payload_length=0 WHERE event_id='e1'",
        );
        assert!(matches!(j.get_status("e1"), StatusResult::Corrupted { .. }));
    }

    #[test]
    fn tampered_schema_version_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE stored_envelope SET schema_version=99 WHERE event_id='e1'",
        );
        assert!(matches!(j.get_status("e1"), StatusResult::Corrupted { .. }));
    }

    #[test]
    fn immutable_envelope_stable_across_lifecycle() {
        let j = mem();
        j.accept(&req("e1", 1));
        let s1 = j.get_status("e1");
        j.update_lifecycle(
            "e1",
            1,
            LocalDeliveryStatus::DeliveryPending,
            LocalRetentionStatus::Present,
        );
        let s2 = j.get_status("e1");
        match (s1, s2) {
            (StatusResult::Found(a), StatusResult::Found(b)) => {
                assert_eq!(a.envelope_checksum, b.envelope_checksum);
                assert_eq!(a.payload_digest, b.payload_digest);
                assert_ne!(a.lifecycle_revision, b.lifecycle_revision);
            }
            _ => panic!(),
        }
    }

    // --- Corruption ---

    #[test]
    fn corrupted_payload_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE payload_blob SET payload=X'DEAD' WHERE event_id='e1'",
        );
        assert_eq!(j.verify_payload("e1"), VerifyResult::DigestMismatch);
    }

    #[test]
    fn corrupted_lifecycle_checksum_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE lifecycle_state SET lifecycle_checksum=X'00' WHERE event_id='e1'",
        );
        assert!(matches!(j.get_status("e1"), StatusResult::Corrupted { .. }));
    }

    // --- Duplicate retry integrity (Blocker: corruption verification) ---

    #[test]
    fn dup_valid_state_returns_existing_duplicate() {
        let j = mem();
        let r = req("e1", 1);
        assert!(matches!(j.accept(&r), AcceptResult::Accepted(_)));
        assert!(matches!(j.accept(&r), AcceptResult::ExistingDuplicate(_)));
    }

    #[test]
    fn dup_after_envelope_tamper_returns_corrupted() {
        let j = mem();
        let r = req("e1", 1);
        assert!(matches!(j.accept(&r), AcceptResult::Accepted(_)));
        tamper(
            &j,
            "UPDATE stored_envelope SET namespace='HACKED' WHERE event_id='e1'",
        );
        match j.accept(&r) {
            AcceptResult::Corrupted { event_id, .. } => assert_eq!(event_id, "e1"),
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn dup_after_lifecycle_checksum_tamper_returns_corrupted() {
        let j = mem();
        let r = req("e1", 1);
        assert!(matches!(j.accept(&r), AcceptResult::Accepted(_)));
        tamper(
            &j,
            "UPDATE lifecycle_state SET lifecycle_checksum=X'00' WHERE event_id='e1'",
        );
        match j.accept(&r) {
            AcceptResult::Corrupted { event_id, .. } => assert_eq!(event_id, "e1"),
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn dup_after_payload_digest_tamper_returns_corrupted() {
        let j = mem();
        let r = req("e1", 1);
        assert!(matches!(j.accept(&r), AcceptResult::Accepted(_)));
        tamper(
            &j,
            "UPDATE stored_envelope \
             SET payload_digest=X'DEADBEEF00000000000000000000000000000000000000000000000000000000' \
             WHERE event_id='e1'",
        );
        match j.accept(&r) {
            AcceptResult::Corrupted { event_id, .. } => assert_eq!(event_id, "e1"),
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    // --- Lifecycle status validation (Blocker: unsupported status strings) ---

    #[test]
    fn replicated_status_with_valid_checksum_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper_lifecycle(&j, "e1", "REPLICATED", "PRESENT");
        match j.get_status("e1") {
            StatusResult::Corrupted { detail, .. } => {
                assert!(detail.contains("unsupported delivery status"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn unknown_retention_with_valid_checksum_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper_lifecycle(&j, "e1", "LOCAL_ACCEPTED", "ARCHIVED");
        match j.get_status("e1") {
            StatusResult::Corrupted { detail, .. } => {
                assert!(detail.contains("unsupported retention status"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn dup_after_unsupported_lifecycle_returns_corrupted() {
        let j = mem();
        let r = req("e1", 1);
        assert!(matches!(j.accept(&r), AcceptResult::Accepted(_)));
        tamper_lifecycle(&j, "e1", "REPLICATED", "PRESENT");
        match j.accept(&r) {
            AcceptResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("unsupported delivery status"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    // --- SQL/read error handling (Blocker: do not swallow errors) ---

    #[test]
    fn short_request_fingerprint_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE stored_envelope SET request_fingerprint=X'AABB' WHERE event_id='e1'",
        );
        match j.get_status("e1") {
            StatusResult::Corrupted { detail, .. } => {
                assert!(detail.contains("request_fingerprint"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn malformed_blob_type_not_found() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE stored_envelope SET payload_digest='not-a-blob' WHERE event_id='e1'",
        );
        if let StatusResult::Found(_) = j.get_status("e1") {
            panic!("malformed type should not be Found");
        }
    }

    #[test]
    fn recompute_envelope_failure_is_corrupted() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE stored_envelope SET request_fingerprint=X'FF' WHERE event_id='e1'",
        );
        match j.get_status("e1") {
            StatusResult::Corrupted { .. } => {}
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn status_by_journal_seq_corrupt_not_hidden() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE lifecycle_state SET lifecycle_checksum=X'00' WHERE event_id='e1'",
        );
        match j.get_status_by_journal_seq(1) {
            StatusResult::Corrupted { .. } => {}
            StatusResult::NotFound => panic!("corrupt row should not be hidden as NotFound"),
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    // --- Duplicate retry payload verification (Blocker: payload blob integrity) ---

    #[test]
    fn dup_after_payload_blob_tamper_returns_corrupted() {
        let j = mem();
        let r = req("e1", 1);
        assert!(matches!(j.accept(&r), AcceptResult::Accepted(_)));
        tamper(
            &j,
            "UPDATE payload_blob SET payload=X'DEAD' WHERE event_id='e1'",
        );
        match j.accept(&r) {
            AcceptResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("digest mismatch"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn dup_after_payload_blob_digest_tamper_returns_corrupted() {
        let j = mem();
        let r = req("e1", 1);
        assert!(matches!(j.accept(&r), AcceptResult::Accepted(_)));
        tamper(
            &j,
            "UPDATE payload_blob \
             SET payload_digest=X'DEADBEEF00000000000000000000000000000000000000000000000000000000' \
             WHERE event_id='e1'",
        );
        match j.accept(&r) {
            AcceptResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("digest mismatch"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn dup_missing_payload_blob_returns_corrupted() {
        let j = mem();
        let r = req("e1", 1);
        assert!(matches!(j.accept(&r), AcceptResult::Accepted(_)));
        tamper(&j, "DELETE FROM payload_blob WHERE event_id='e1'");
        match j.accept(&r) {
            AcceptResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("not found"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    // --- Lifecycle CAS corruption guard (Blocker: no update over corrupted state) ---

    #[test]
    fn cas_after_lifecycle_checksum_tamper_returns_corrupted() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE lifecycle_state SET lifecycle_checksum=X'00' WHERE event_id='e1'",
        );
        match j.update_lifecycle(
            "e1",
            1,
            LocalDeliveryStatus::DeliveryPending,
            LocalRetentionStatus::Present,
        ) {
            CasResult::Corrupted { event_id, .. } => assert_eq!(event_id, "e1"),
            other => panic!("expected Corrupted, got {other:?}"),
        }
        // Verify lifecycle was NOT updated — still corrupted
        assert!(matches!(j.get_status("e1"), StatusResult::Corrupted { .. }));
    }

    #[test]
    fn cas_after_unsupported_delivery_returns_corrupted() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper_lifecycle(&j, "e1", "REPLICATED", "PRESENT");
        match j.update_lifecycle(
            "e1",
            1,
            LocalDeliveryStatus::DeliveryPending,
            LocalRetentionStatus::Present,
        ) {
            CasResult::Corrupted { event_id, .. } => assert_eq!(event_id, "e1"),
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn cas_after_envelope_tamper_returns_corrupted() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE stored_envelope SET namespace='HACKED' WHERE event_id='e1'",
        );
        match j.update_lifecycle(
            "e1",
            1,
            LocalDeliveryStatus::DeliveryPending,
            LocalRetentionStatus::Present,
        ) {
            CasResult::Corrupted { event_id, .. } => assert_eq!(event_id, "e1"),
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    // --- Write-path metadata error handling (Blocker: do not swallow errors) ---

    #[test]
    fn corrupt_dedup_fingerprint_rejects_accept() {
        let j = mem();
        let r = req("e1", 1);
        assert!(matches!(j.accept(&r), AcceptResult::Accepted(_)));
        // Set fingerprint to integer — causes FromSql type conversion error
        tamper(
            &j,
            "UPDATE dedup_fingerprint SET request_fingerprint=42 WHERE event_id='e1'",
        );
        match j.accept(&r) {
            AcceptResult::Corrupted { .. } => {}
            AcceptResult::Accepted(_) | AcceptResult::ExistingDuplicate(_) => {
                panic!("corrupt dedup must not allow acceptance or duplicate success")
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn corrupt_producer_sequence_rejects_accept() {
        let j = mem();
        j.accept(&req("e1", 5));
        // Corrupt highest_seq to text — causes FromSql i64 conversion error
        tamper(
            &j,
            "UPDATE producer_sequence SET highest_seq='not_a_number'",
        );
        let mut r2 = req("e2", 3);
        r2.request_fingerprint = [0xAA; 32];
        match j.accept(&r2) {
            AcceptResult::Corrupted { .. } => {}
            AcceptResult::Accepted(_) => {
                panic!("corrupt producer_sequence must not allow acceptance")
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn corrupt_stream_head_does_not_reset_sequence() {
        let j = mem();
        j.accept(&req("e1", 1));
        // Corrupt head_sequence to text — causes FromSql i64 conversion error
        tamper(&j, "UPDATE stream_head SET head_sequence='corrupt'");
        let mut r2 = req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        match j.accept(&r2) {
            AcceptResult::Error(_) => {}
            AcceptResult::Accepted(a) => {
                assert_ne!(a.stream_sequence, 1, "stream sequence must not reset to 1");
                panic!("corrupt stream_head should not accept");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    // --- Lifecycle CAS affected-row handling (Blocker: no unwrap, check row count) ---

    #[test]
    fn cas_update_nonexistent_returns_not_found() {
        let j = mem();
        assert_eq!(
            j.update_lifecycle(
                "nonexistent",
                1,
                LocalDeliveryStatus::DeliveryPending,
                LocalRetentionStatus::Present,
            ),
            CasResult::NotFound,
        );
    }

    #[test]
    fn cas_stale_revision_still_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        j.update_lifecycle(
            "e1",
            1,
            LocalDeliveryStatus::DeliveryPending,
            LocalRetentionStatus::Present,
        );
        assert_eq!(
            j.update_lifecycle(
                "e1",
                1,
                LocalDeliveryStatus::DeliveryBlocked,
                LocalRetentionStatus::Present,
            ),
            CasResult::StaleRevision,
        );
    }

    #[test]
    fn cas_valid_update_returns_updated() {
        let j = mem();
        j.accept(&req("e1", 1));
        assert_eq!(
            j.update_lifecycle(
                "e1",
                1,
                LocalDeliveryStatus::DeliveryPending,
                LocalRetentionStatus::Present,
            ),
            CasResult::Updated,
        );
        match j.get_status("e1") {
            StatusResult::Found(s) => {
                assert_eq!(s.lifecycle_revision, 2);
                assert_eq!(s.delivery_status, "DELIVERY_PENDING");
            }
            other => panic!("{other:?}"),
        }
    }

    // --- FK enforcement (Blocker 4) ---

    #[test]
    fn fk_payload_requires_envelope() {
        let j = mem();
        let conn = j.conn.lock().unwrap();
        let r = conn.execute(
            "INSERT INTO payload_blob (event_id, payload, payload_digest) VALUES ('orphan', X'AA', X'BB')",
            [],
        );
        assert!(r.is_err(), "FK should reject orphan payload_blob");
    }

    #[test]
    fn fk_lifecycle_requires_envelope() {
        let j = mem();
        let conn = j.conn.lock().unwrap();
        let r = conn.execute(
            "INSERT INTO lifecycle_state (event_id, revision, lifecycle_checksum, delivery_status, retention_status, updated_at_ms) VALUES ('orphan', 1, X'CC', 'X', 'Y', 0)",
            [],
        );
        assert!(r.is_err(), "FK should reject orphan lifecycle_state");
    }

    #[test]
    fn fk_dedup_requires_envelope() {
        let j = mem();
        let conn = j.conn.lock().unwrap();
        let r = conn.execute(
            "INSERT INTO dedup_fingerprint (event_id, request_fingerprint) VALUES ('orphan', X'DD')",
            [],
        );
        assert!(r.is_err(), "FK should reject orphan dedup_fingerprint");
    }

    // --- Guards ---

    #[test]
    fn disk_guard_rejects() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysUnsafeDiskGuard)).unwrap();
        assert_eq!(j.accept(&req("e1", 1)), AcceptResult::DiskUnsafe);
    }

    #[test]
    fn shutdown_rejects() {
        let j = mem();
        j.shutdown();
        assert_eq!(j.accept(&req("e1", 1)), AcceptResult::ShuttingDown);
    }

    #[test]
    fn never_claims_replicated() {
        let j = mem();
        match j.accept(&req("e1", 1)) {
            AcceptResult::Accepted(a) => {
                assert_eq!(a.delivery_status, DeliveryStatus::LocalAccepted);
                assert_ne!(a.delivery_status, DeliveryStatus::Replicated);
            }
            _ => panic!(),
        }
    }

    // --- Helpers ---

    fn tamper(j: &LocalJournal, sql: &str) {
        let conn = j.conn.lock().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
        conn.execute(sql, []).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    }

    fn tamper_lifecycle(j: &LocalJournal, event_id: &str, delivery: &str, retention: &str) {
        let lc = compute_lifecycle_checksum(event_id, 1, delivery, retention);
        let conn = j.conn.lock().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
        conn.execute(
            "UPDATE lifecycle_state SET delivery_status=?1, \
             retention_status=?2, lifecycle_checksum=?3 \
             WHERE event_id=?4",
            params![delivery, retention, lc.as_slice(), event_id],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    }
}
