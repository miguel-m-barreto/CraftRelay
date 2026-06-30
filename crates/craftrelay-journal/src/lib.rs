#![forbid(unsafe_code)]

use craftrelay_domain::{Checksum, DeliveryStatus};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

const SCHEMA_VERSION: i64 = 2;
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
    AlreadyConfirmed,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadReadResult {
    Found(Vec<u8>),
    NotFound,
    Corrupted { event_id: String, detail: String },
    Error(String),
}

// ---------------------------------------------------------------------------
// Sprint 5: Delivery state types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryLifecycleStatus {
    DeliveryPending,
    DeliveryRetrying,
    Replicated,
    DeliveryBlocked,
}

impl DeliveryLifecycleStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeliveryPending => "DELIVERY_PENDING",
            Self::DeliveryRetrying => "DELIVERY_RETRYING",
            Self::Replicated => "REPLICATED",
            Self::DeliveryBlocked => "DELIVERY_BLOCKED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryStateRecord {
    pub event_id: String,
    pub delivery_status: String,
    pub attempt_count: i32,
    pub next_retry_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub blocked_reason: Option<String>,
    pub kafka_topic: Option<String>,
    pub kafka_partition: Option<i32>,
    pub kafka_offset: Option<i64>,
    pub routing_fingerprint: Option<Checksum>,
    pub profile_id: Option<String>,
    pub profile_version: Option<i32>,
    pub delivery_revision: i64,
    pub delivery_checksum: Checksum,
    pub confirmed_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryAttemptRecord {
    pub event_id: String,
    pub attempt_number: i32,
    pub outcome: String,
    pub error_code: Option<String>,
    pub kafka_topic: Option<String>,
    pub kafka_partition: Option<i32>,
    pub kafka_offset: Option<i64>,
    pub profile_id: Option<String>,
    pub profile_version: Option<i32>,
    pub attempted_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicatedDeliveryConfirmation<'a> {
    pub expected_routing_fingerprint: &'a Checksum,
    pub expected_profile_id: &'a str,
    pub expected_profile_version: i32,
    pub kafka_topic: &'a str,
    pub kafka_partition: i32,
    pub kafka_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryStateResult {
    Found(Box<DeliveryStateRecord>),
    NotFound,
    Corrupted { event_id: String, detail: String },
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryCandidate {
    pub event_id: String,
    pub installation_id: String,
    pub namespace: String,
    pub logical_stream_type: String,
    pub stream_key: String,
    pub stream_sequence: i64,
    pub event_type: String,
    pub routing_version: i64,
    pub delivery_state: DeliveryStateRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryCandidateResult {
    Found(Box<DeliveryCandidate>),
    NotFound,
    Corrupted { event_id: String, detail: String },
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDelivery {
    pub event_id: String,
    pub journal_sequence: i64,
    pub stream_key: String,
    pub stream_sequence: i64,
    pub delivery_status: String,
    pub attempt_count: i32,
    pub next_retry_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryScanResult {
    Ok(Vec<PendingDelivery>),
    Corrupted {
        event_id: Option<String>,
        detail: String,
    },
    Error(String),
}

struct DeliveryScanRow {
    event_id: String,
    journal_sequence: Option<i64>,
    stream_key: Option<String>,
    stream_sequence: Option<i64>,
    envelope_checksum: Option<Vec<u8>>,
    delivery_status: String,
    attempt_count: i32,
    next_retry_at_ms: Option<i64>,
}

const MAX_DELIVERY_ATTEMPTS: usize = 16;

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

const SCHEMA_V2_SQL: &str = r"
CREATE TABLE IF NOT EXISTS delivery_state (
    event_id            TEXT    PRIMARY KEY
        REFERENCES stored_envelope(event_id),
    delivery_status     TEXT    NOT NULL,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    next_retry_at_ms    INTEGER,
    last_error          TEXT,
    blocked_reason      TEXT,
    kafka_topic         TEXT,
    kafka_partition     INTEGER,
    kafka_offset        INTEGER,
    routing_fingerprint BLOB,
    profile_id          TEXT,
    profile_version     INTEGER,
    delivery_revision   INTEGER NOT NULL DEFAULT 1,
    delivery_checksum   BLOB    NOT NULL,
    confirmed_at_ms     INTEGER,
    created_at_ms       INTEGER NOT NULL,
    updated_at_ms       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_attempt (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT    NOT NULL
        REFERENCES stored_envelope(event_id),
    attempt_number  INTEGER NOT NULL,
    outcome         TEXT    NOT NULL,
    error_code      TEXT,
    kafka_topic     TEXT,
    kafka_partition INTEGER,
    kafka_offset    INTEGER,
    profile_id      TEXT,
    profile_version INTEGER,
    attempted_at_ms INTEGER NOT NULL
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

        let existing_ver = existing
            .as_ref()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);

        if existing_ver < 2 {
            conn.execute_batch(SCHEMA_V2_SQL)
                .map_err(|e| e.to_string())?;
        }

        conn.execute(
            "INSERT OR REPLACE INTO journal_meta (key, value) \
             VALUES ('schema_version', ?1)",
            params![SCHEMA_VERSION.to_string()],
        )
        .map_err(|e| e.to_string())?;

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

    pub fn read_verified_payload(&self, event_id: &str) -> PayloadReadResult {
        let conn = self.conn.lock().unwrap();
        read_verified_payload_impl(&conn, event_id)
    }

    // --- Sprint 5: Delivery state methods ---

    pub fn create_delivery_pending(
        &self,
        event_id: &str,
        routing_fingerprint: &Checksum,
        profile_id: &str,
        profile_version: i32,
        kafka_topic: &str,
    ) -> CasResult {
        let conn = self.conn.lock().unwrap();
        let exists: bool = match conn
            .query_row(
                "SELECT 1 FROM stored_envelope WHERE event_id = ?1",
                params![event_id],
                |_| Ok(true),
            )
            .optional()
        {
            Ok(Some(exists)) => exists,
            Ok(None) => false,
            Err(e) => return CasResult::Error(e.to_string()),
        };
        if !exists {
            return CasResult::NotFound;
        }
        let now_ms = now_epoch_ms();
        let pending = DeliveryStateRecord {
            event_id: event_id.to_string(),
            delivery_status: "DELIVERY_PENDING".to_string(),
            attempt_count: 0,
            next_retry_at_ms: None,
            last_error: None,
            blocked_reason: None,
            kafka_topic: Some(kafka_topic.to_string()),
            kafka_partition: None,
            kafka_offset: None,
            routing_fingerprint: Some(*routing_fingerprint),
            profile_id: Some(profile_id.to_string()),
            profile_version: Some(profile_version),
            delivery_revision: 1,
            delivery_checksum: [0u8; 32],
            confirmed_at_ms: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        };
        let checksum = compute_delivery_checksum(&pending);
        match conn.execute(
            "INSERT INTO delivery_state (\
                event_id, delivery_status, attempt_count, \
                kafka_topic, routing_fingerprint, profile_id, profile_version, \
                delivery_revision, delivery_checksum, \
                created_at_ms, updated_at_ms\
             ) VALUES (?1, 'DELIVERY_PENDING', 0, ?2, ?3, ?4, ?5, 1, ?6, ?7, ?7)",
            params![
                event_id,
                kafka_topic,
                routing_fingerprint.as_slice(),
                profile_id,
                profile_version,
                checksum.as_slice(),
                now_ms,
            ],
        ) {
            Ok(_) => CasResult::Updated,
            Err(e) => CasResult::Error(e.to_string()),
        }
    }

    pub fn record_delivery_attempt(&self, attempt: &DeliveryAttemptRecord) -> CasResult {
        let mut conn = self.conn.lock().unwrap();
        let tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(e) => return CasResult::Error(e.to_string()),
        };
        let now_ms = now_epoch_ms();
        let mut state = match load_delivery_state(&tx, &attempt.event_id) {
            DeliveryStateResult::Found(state) => *state,
            DeliveryStateResult::NotFound => return CasResult::NotFound,
            DeliveryStateResult::Corrupted { event_id, detail } => {
                return CasResult::Corrupted { event_id, detail };
            }
            DeliveryStateResult::Error(e) => return CasResult::Error(e),
        };
        if let Err(e) = tx.execute(
            "INSERT INTO delivery_attempt (\
                event_id, attempt_number, outcome, error_code, \
                kafka_topic, kafka_partition, kafka_offset, \
                profile_id, profile_version, attempted_at_ms\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                attempt.event_id,
                attempt.attempt_number,
                attempt.outcome,
                attempt.error_code,
                attempt.kafka_topic,
                attempt.kafka_partition,
                attempt.kafka_offset,
                attempt.profile_id,
                attempt.profile_version,
                attempt.attempted_at_ms,
            ],
        ) {
            return CasResult::Error(e.to_string());
        }
        if let Err(e) = tx.execute(
            "DELETE FROM delivery_attempt WHERE id IN (\
                SELECT id FROM delivery_attempt \
                WHERE event_id = ?1 \
                ORDER BY id DESC \
                LIMIT -1 OFFSET ?2\
             )",
            params![attempt.event_id, MAX_DELIVERY_ATTEMPTS as i64],
        ) {
            return CasResult::Error(e.to_string());
        }
        state.attempt_count += 1;
        state.last_error = attempt.error_code.clone();
        state.updated_at_ms = now_ms;
        let checksum = compute_delivery_checksum(&state);
        let affected = match tx.execute(
            "UPDATE delivery_state SET \
             attempt_count = attempt_count + 1, \
             last_error = ?1, delivery_checksum = ?2, updated_at_ms = ?3 \
             WHERE event_id = ?4 AND delivery_revision = ?5",
            params![
                attempt.error_code,
                checksum.as_slice(),
                now_ms,
                attempt.event_id,
                state.delivery_revision,
            ],
        ) {
            Ok(n) => n,
            Err(e) => return CasResult::Error(e.to_string()),
        };
        match affected {
            1 => match tx.commit() {
                Ok(()) => CasResult::Updated,
                Err(e) => CasResult::Error(e.to_string()),
            },
            0 => CasResult::StaleRevision,
            n => CasResult::Error(format!(
                "delivery attempt state update affected {n} rows, expected 1"
            )),
        }
    }

    pub fn confirm_replicated_delivery(
        &self,
        event_id: &str,
        expected_delivery_revision: i64,
        confirmation: ReplicatedDeliveryConfirmation<'_>,
    ) -> CasResult {
        let mut conn = self.conn.lock().unwrap();
        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(e) => return CasResult::Error(e.to_string()),
        };
        let current = match load_delivery_state(&tx, event_id) {
            DeliveryStateResult::Found(state) => *state,
            DeliveryStateResult::NotFound => return CasResult::NotFound,
            DeliveryStateResult::Corrupted { event_id, detail } => {
                return CasResult::Corrupted { event_id, detail };
            }
            DeliveryStateResult::Error(e) => return CasResult::Error(e),
        };
        if current.delivery_status == "REPLICATED" {
            let lifecycle = match load_verified_lifecycle_for_transition(&tx, event_id) {
                Ok(lifecycle) => lifecycle,
                Err(result) => return result,
            };
            if lifecycle.delivery_status != "REPLICATED" {
                return CasResult::Corrupted {
                    event_id: event_id.to_string(),
                    detail: "replicated delivery state without replicated lifecycle".into(),
                };
            }
            let same_confirmation = current.kafka_topic.as_deref()
                == Some(confirmation.kafka_topic)
                && current.kafka_partition == Some(confirmation.kafka_partition)
                && current.kafka_offset == Some(confirmation.kafka_offset)
                && current.routing_fingerprint == Some(*confirmation.expected_routing_fingerprint)
                && current.profile_id.as_deref() == Some(confirmation.expected_profile_id)
                && current.profile_version == Some(confirmation.expected_profile_version);
            return if same_confirmation {
                CasResult::AlreadyConfirmed
            } else {
                CasResult::Corrupted {
                    event_id: event_id.to_string(),
                    detail: "conflicting replicated confirmation metadata".into(),
                }
            };
        }
        let lifecycle = match load_verified_lifecycle_for_transition(&tx, event_id) {
            Ok(lifecycle) => lifecycle,
            Err(result) => return result,
        };
        if current.delivery_revision != expected_delivery_revision {
            return CasResult::StaleRevision;
        }
        if current.routing_fingerprint != Some(*confirmation.expected_routing_fingerprint) {
            return CasResult::Corrupted {
                event_id: event_id.to_string(),
                detail: "routing fingerprint mismatch".into(),
            };
        }
        if current.profile_id.as_deref() != Some(confirmation.expected_profile_id)
            || current.profile_version != Some(confirmation.expected_profile_version)
        {
            return CasResult::Corrupted {
                event_id: event_id.to_string(),
                detail: "profile mismatch".into(),
            };
        }
        if current.kafka_topic.as_deref() != Some(confirmation.kafka_topic) {
            return CasResult::Corrupted {
                event_id: event_id.to_string(),
                detail: "kafka topic mismatch".into(),
            };
        }
        let now_ms = now_epoch_ms();
        let new_rev = expected_delivery_revision + 1;
        let mut next = current;
        next.delivery_status = "REPLICATED".to_string();
        next.next_retry_at_ms = None;
        next.last_error = None;
        next.blocked_reason = None;
        next.kafka_topic = Some(confirmation.kafka_topic.to_string());
        next.kafka_partition = Some(confirmation.kafka_partition);
        next.kafka_offset = Some(confirmation.kafka_offset);
        next.delivery_revision = new_rev;
        next.confirmed_at_ms = Some(now_ms);
        next.updated_at_ms = now_ms;
        let checksum = compute_delivery_checksum(&next);
        let affected = match tx.execute(
            "UPDATE delivery_state SET \
             delivery_status = 'REPLICATED', \
             kafka_topic = ?1, kafka_partition = ?2, kafka_offset = ?3, \
             delivery_revision = ?4, delivery_checksum = ?5, \
             confirmed_at_ms = ?6, updated_at_ms = ?6, \
             next_retry_at_ms = NULL, last_error = NULL, blocked_reason = NULL \
             WHERE event_id = ?7 AND delivery_revision = ?8",
            params![
                confirmation.kafka_topic,
                confirmation.kafka_partition,
                confirmation.kafka_offset,
                new_rev,
                checksum.as_slice(),
                now_ms,
                event_id,
                expected_delivery_revision,
            ],
        ) {
            Ok(n) => n,
            Err(e) => return CasResult::Error(e.to_string()),
        };
        if affected != 1 {
            return CasResult::StaleRevision;
        }
        match update_lifecycle_delivery_status(&tx, event_id, &lifecycle, "REPLICATED", now_ms) {
            CasResult::Updated => {}
            other => return other,
        }
        match tx.commit() {
            Ok(()) => CasResult::Updated,
            Err(e) => CasResult::Error(e.to_string()),
        }
    }

    pub fn block_delivery(
        &self,
        event_id: &str,
        expected_delivery_revision: i64,
        reason: &str,
    ) -> CasResult {
        let mut conn = self.conn.lock().unwrap();
        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(e) => return CasResult::Error(e.to_string()),
        };
        let current = match load_delivery_state(&tx, event_id) {
            DeliveryStateResult::Found(state) => *state,
            DeliveryStateResult::NotFound => return CasResult::NotFound,
            DeliveryStateResult::Corrupted { event_id, detail } => {
                return CasResult::Corrupted { event_id, detail };
            }
            DeliveryStateResult::Error(e) => return CasResult::Error(e),
        };
        let lifecycle = match load_verified_lifecycle_for_transition(&tx, event_id) {
            Ok(lifecycle) => lifecycle,
            Err(result) => return result,
        };
        if current.delivery_revision != expected_delivery_revision {
            return CasResult::StaleRevision;
        }
        if current.delivery_status == "REPLICATED" {
            return CasResult::InvalidTransition;
        }
        let now_ms = now_epoch_ms();
        let new_rev = expected_delivery_revision + 1;
        let mut next = current;
        next.delivery_status = "DELIVERY_BLOCKED".to_string();
        next.next_retry_at_ms = None;
        next.blocked_reason = Some(reason.to_string());
        next.delivery_revision = new_rev;
        next.updated_at_ms = now_ms;
        let checksum = compute_delivery_checksum(&next);
        let affected = match tx.execute(
            "UPDATE delivery_state SET \
             delivery_status = 'DELIVERY_BLOCKED', \
             blocked_reason = ?1, \
             delivery_revision = ?2, delivery_checksum = ?3, \
             updated_at_ms = ?4, \
             next_retry_at_ms = NULL \
             WHERE event_id = ?5 AND delivery_revision = ?6",
            params![
                reason,
                new_rev,
                checksum.as_slice(),
                now_ms,
                event_id,
                expected_delivery_revision,
            ],
        ) {
            Ok(n) => n,
            Err(e) => return CasResult::Error(e.to_string()),
        };
        if affected != 1 {
            return CasResult::StaleRevision;
        }
        match update_lifecycle_delivery_status(
            &tx,
            event_id,
            &lifecycle,
            "DELIVERY_BLOCKED",
            now_ms,
        ) {
            CasResult::Updated => {}
            other => return other,
        }
        match tx.commit() {
            Ok(()) => CasResult::Updated,
            Err(e) => CasResult::Error(e.to_string()),
        }
    }

    pub fn update_delivery_retry(
        &self,
        event_id: &str,
        expected_delivery_revision: i64,
        next_retry_at_ms: i64,
    ) -> CasResult {
        let conn = self.conn.lock().unwrap();
        let current = match load_delivery_state(&conn, event_id) {
            DeliveryStateResult::Found(state) => *state,
            DeliveryStateResult::NotFound => return CasResult::NotFound,
            DeliveryStateResult::Corrupted { event_id, detail } => {
                return CasResult::Corrupted { event_id, detail };
            }
            DeliveryStateResult::Error(e) => return CasResult::Error(e),
        };
        if current.delivery_revision != expected_delivery_revision {
            return CasResult::StaleRevision;
        }
        if current.delivery_status == "REPLICATED" {
            return CasResult::InvalidTransition;
        }
        let now_ms = now_epoch_ms();
        let new_rev = expected_delivery_revision + 1;
        let mut next = current;
        next.delivery_status = "DELIVERY_RETRYING".to_string();
        next.next_retry_at_ms = Some(next_retry_at_ms);
        next.delivery_revision = new_rev;
        next.updated_at_ms = now_ms;
        let checksum = compute_delivery_checksum(&next);
        let affected = match conn.execute(
            "UPDATE delivery_state SET \
             delivery_status = 'DELIVERY_RETRYING', \
             next_retry_at_ms = ?1, \
             delivery_revision = ?2, delivery_checksum = ?3, \
             updated_at_ms = ?4 \
             WHERE event_id = ?5 AND delivery_revision = ?6",
            params![
                next_retry_at_ms,
                new_rev,
                checksum.as_slice(),
                now_ms,
                event_id,
                expected_delivery_revision,
            ],
        ) {
            Ok(n) => n,
            Err(e) => return CasResult::Error(e.to_string()),
        };
        match affected {
            1 => CasResult::Updated,
            0 => CasResult::StaleRevision,
            n => CasResult::Error(format!(
                "delivery retry update affected {n} rows, expected 1"
            )),
        }
    }

    pub fn scan_pending_deliveries(&self, limit: i32) -> DeliveryScanResult {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT d.event_id, e.journal_sequence, \
                    e.stream_key, e.stream_sequence, \
                    e.envelope_checksum, \
                    d.delivery_status, d.attempt_count, d.next_retry_at_ms \
             FROM delivery_state d \
             LEFT JOIN stored_envelope e ON d.event_id = e.event_id \
             WHERE d.delivery_status = 'DELIVERY_PENDING' \
             ORDER BY e.journal_sequence ASC \
             LIMIT ?1",
        ) {
            Ok(s) => s,
            Err(e) => return DeliveryScanResult::Error(e.to_string()),
        };
        let rows = stmt
            .query_map(params![limit], |row| {
                Ok(DeliveryScanRow {
                    event_id: row.get(0)?,
                    journal_sequence: row.get(1)?,
                    stream_key: row.get(2)?,
                    stream_sequence: row.get(3)?,
                    envelope_checksum: row.get(4)?,
                    delivery_status: row.get(5)?,
                    attempt_count: row.get(6)?,
                    next_retry_at_ms: row.get(7)?,
                })
            })
            .map_err(|e| e.to_string());
        collect_delivery_scan(&conn, rows)
    }

    pub fn scan_retrying_deliveries(&self, now_ms: i64, limit: i32) -> DeliveryScanResult {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT d.event_id, e.journal_sequence, \
                    e.stream_key, e.stream_sequence, \
                    e.envelope_checksum, \
                    d.delivery_status, d.attempt_count, d.next_retry_at_ms \
             FROM delivery_state d \
             LEFT JOIN stored_envelope e ON d.event_id = e.event_id \
             WHERE d.delivery_status = 'DELIVERY_RETRYING' \
               AND d.next_retry_at_ms <= ?1 \
             ORDER BY d.next_retry_at_ms ASC \
             LIMIT ?2",
        ) {
            Ok(s) => s,
            Err(e) => return DeliveryScanResult::Error(e.to_string()),
        };
        let rows = stmt
            .query_map(params![now_ms, limit], |row| {
                Ok(DeliveryScanRow {
                    event_id: row.get(0)?,
                    journal_sequence: row.get(1)?,
                    stream_key: row.get(2)?,
                    stream_sequence: row.get(3)?,
                    envelope_checksum: row.get(4)?,
                    delivery_status: row.get(5)?,
                    attempt_count: row.get(6)?,
                    next_retry_at_ms: row.get(7)?,
                })
            })
            .map_err(|e| e.to_string());
        collect_delivery_scan(&conn, rows)
    }

    pub fn scan_delivery_gate_states(&self, limit: i32) -> DeliveryScanResult {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT d.event_id, e.journal_sequence, \
                    e.stream_key, e.stream_sequence, \
                    e.envelope_checksum, \
                    d.delivery_status, d.attempt_count, d.next_retry_at_ms \
             FROM delivery_state d \
             LEFT JOIN stored_envelope e ON d.event_id = e.event_id \
             ORDER BY e.journal_sequence ASC \
             LIMIT ?1",
        ) {
            Ok(s) => s,
            Err(e) => return DeliveryScanResult::Error(e.to_string()),
        };
        let rows = stmt
            .query_map(params![limit], |row| {
                Ok(DeliveryScanRow {
                    event_id: row.get(0)?,
                    journal_sequence: row.get(1)?,
                    stream_key: row.get(2)?,
                    stream_sequence: row.get(3)?,
                    envelope_checksum: row.get(4)?,
                    delivery_status: row.get(5)?,
                    attempt_count: row.get(6)?,
                    next_retry_at_ms: row.get(7)?,
                })
            })
            .map_err(|e| e.to_string());
        collect_delivery_scan(&conn, rows)
    }

    pub fn get_delivery_state(&self, event_id: &str) -> DeliveryStateResult {
        let conn = self.conn.lock().unwrap();
        load_delivery_state(&conn, event_id)
    }

    pub fn get_delivery_candidate(&self, event_id: &str) -> DeliveryCandidateResult {
        let conn = self.conn.lock().unwrap();
        load_delivery_candidate(&conn, event_id)
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

fn read_verified_payload_impl(conn: &Connection, event_id: &str) -> PayloadReadResult {
    let envelope_digest = match conn
        .query_row(
            "SELECT payload_digest FROM stored_envelope WHERE event_id = ?1",
            params![event_id],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
    {
        Ok(Some(digest)) => digest,
        Ok(None) => return PayloadReadResult::NotFound,
        Err(e) => {
            return PayloadReadResult::Error(format!(
                "stored envelope payload digest query error: {e}"
            ));
        }
    };
    if envelope_digest.len() != 32 {
        return PayloadReadResult::Corrupted {
            event_id: event_id.to_string(),
            detail: format!(
                "stored envelope payload_digest length {}, expected 32",
                envelope_digest.len()
            ),
        };
    }

    let (payload, blob_digest) = match conn
        .query_row(
            "SELECT payload, payload_digest FROM payload_blob \
             WHERE event_id = ?1",
            params![event_id],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
    {
        Ok(Some(row)) => row,
        Ok(None) => {
            return PayloadReadResult::Corrupted {
                event_id: event_id.to_string(),
                detail: "payload blob not found".into(),
            };
        }
        Err(e) => return PayloadReadResult::Error(format!("payload blob query error: {e}")),
    };
    if blob_digest.len() != 32 {
        return PayloadReadResult::Corrupted {
            event_id: event_id.to_string(),
            detail: format!(
                "payload blob payload_digest length {}, expected 32",
                blob_digest.len()
            ),
        };
    }
    if blob_digest.as_slice() != envelope_digest.as_slice() {
        return PayloadReadResult::Corrupted {
            event_id: event_id.to_string(),
            detail: "payload blob digest differs from stored envelope digest".into(),
        };
    }

    let actual = compute_sha256(&payload);
    if actual.as_slice() != envelope_digest.as_slice() {
        return PayloadReadResult::Corrupted {
            event_id: event_id.to_string(),
            detail: "payload digest mismatch".into(),
        };
    }

    PayloadReadResult::Found(payload)
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

fn compute_delivery_checksum(state: &DeliveryStateRecord) -> Checksum {
    // `updated_at_ms` is intentionally excluded: ordinary attempt bookkeeping
    // updates it, while the checksum protects the mutable delivery state itself.
    craftrelay_domain::sha256(
        format!(
            "{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
            state.event_id,
            state.delivery_status,
            state.attempt_count,
            state
                .next_retry_at_ms
                .map_or(String::new(), |v| v.to_string()),
            state.last_error.as_deref().unwrap_or(""),
            state.blocked_reason.as_deref().unwrap_or(""),
            state.kafka_topic.as_deref().unwrap_or(""),
            state
                .kafka_partition
                .map_or(String::new(), |v| v.to_string()),
            state.kafka_offset.map_or(String::new(), |v| v.to_string()),
            state
                .routing_fingerprint
                .as_ref()
                .map_or(String::new(), checksum_hex),
            state.profile_id.as_deref().unwrap_or(""),
            state
                .profile_version
                .map_or(String::new(), |v| v.to_string()),
            state.delivery_revision,
            state
                .confirmed_at_ms
                .map_or(String::new(), |v| v.to_string()),
            state.created_at_ms,
        )
        .as_bytes(),
    )
}

fn checksum_hex(checksum: &Checksum) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in checksum {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

fn load_delivery_state(conn: &Connection, event_id: &str) -> DeliveryStateResult {
    let row = match conn
        .query_row(
            "SELECT delivery_status, attempt_count, next_retry_at_ms, \
                    last_error, blocked_reason, kafka_topic, \
                    kafka_partition, kafka_offset, routing_fingerprint, \
                    profile_id, profile_version, delivery_revision, \
                    delivery_checksum, confirmed_at_ms, \
                    created_at_ms, updated_at_ms \
             FROM delivery_state WHERE event_id = ?1",
            params![event_id],
            |row| {
                let rf: Option<Vec<u8>> = row.get(8)?;
                let dc: Vec<u8> = row.get(12)?;
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i32>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<i32>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    rf,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<i32>>(10)?,
                    row.get::<_, i64>(11)?,
                    dc,
                    row.get::<_, Option<i64>>(13)?,
                    row.get::<_, i64>(14)?,
                    row.get::<_, i64>(15)?,
                ))
            },
        )
        .optional()
    {
        Ok(Some(r)) => r,
        Ok(None) => return DeliveryStateResult::NotFound,
        Err(e) => return DeliveryStateResult::Error(e.to_string()),
    };
    let (
        status,
        attempt_count,
        next_retry,
        last_err,
        blocked,
        topic,
        partition,
        offset,
        rf_bytes,
        prof_id,
        prof_ver,
        rev,
        dc_bytes,
        confirmed,
        created,
        updated,
    ) = row;
    if dc_bytes.len() != 32 {
        return DeliveryStateResult::Corrupted {
            event_id: event_id.to_string(),
            detail: "delivery checksum length invalid".into(),
        };
    }
    let mut delivery_checksum = [0u8; 32];
    delivery_checksum.copy_from_slice(&dc_bytes);
    let routing_fingerprint = match rf_bytes {
        Some(v) if v.len() == 32 => {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&v);
            Some(arr)
        }
        Some(_) => {
            return DeliveryStateResult::Corrupted {
                event_id: event_id.to_string(),
                detail: "routing fingerprint length invalid".into(),
            };
        }
        None => None,
    };
    if !matches!(
        status.as_str(),
        "DELIVERY_PENDING" | "DELIVERY_RETRYING" | "REPLICATED" | "DELIVERY_BLOCKED"
    ) {
        return DeliveryStateResult::Corrupted {
            event_id: event_id.to_string(),
            detail: format!("unsupported delivery state status: {status}"),
        };
    }
    let state = DeliveryStateRecord {
        event_id: event_id.to_string(),
        delivery_status: status,
        attempt_count,
        next_retry_at_ms: next_retry,
        last_error: last_err,
        blocked_reason: blocked,
        kafka_topic: topic,
        kafka_partition: partition,
        kafka_offset: offset,
        routing_fingerprint,
        profile_id: prof_id,
        profile_version: prof_ver,
        delivery_revision: rev,
        delivery_checksum,
        confirmed_at_ms: confirmed,
        created_at_ms: created,
        updated_at_ms: updated,
    };
    let expected = compute_delivery_checksum(&state);
    if state.delivery_checksum != expected {
        return DeliveryStateResult::Corrupted {
            event_id: event_id.to_string(),
            detail: "delivery checksum mismatch".into(),
        };
    }
    if let Err(detail) = validate_delivery_state_invariants(&state) {
        return DeliveryStateResult::Corrupted {
            event_id: event_id.to_string(),
            detail,
        };
    }
    DeliveryStateResult::Found(Box::new(state))
}

fn validate_delivery_state_invariants(state: &DeliveryStateRecord) -> Result<(), String> {
    let status = state.delivery_status.as_str();
    match status {
        "REPLICATED" => {
            require_non_empty(state.kafka_topic.as_deref(), "kafka_topic", status)?;
            require_some(
                state.routing_fingerprint.as_ref(),
                "routing_fingerprint",
                status,
            )?;
            require_non_empty(state.profile_id.as_deref(), "profile_id", status)?;
            require_positive_i32(state.profile_version, "profile_version", status)?;
            require_non_negative_i32(state.kafka_partition, "kafka_partition", status)?;
            require_non_negative_i64(state.kafka_offset, "kafka_offset", status)?;
            require_some(state.confirmed_at_ms.as_ref(), "confirmed_at_ms", status)?;
            require_absent(state.blocked_reason.as_ref(), "blocked_reason", status)?;
            require_absent(state.next_retry_at_ms.as_ref(), "next_retry_at_ms", status)?;
        }
        "DELIVERY_PENDING" => {
            require_delivery_routing_fields(state, status)?;
            require_absent(state.next_retry_at_ms.as_ref(), "next_retry_at_ms", status)?;
            require_absent(state.blocked_reason.as_ref(), "blocked_reason", status)?;
            require_no_confirmed_kafka_metadata(state, status)?;
        }
        "DELIVERY_RETRYING" => {
            require_delivery_routing_fields(state, status)?;
            require_some(state.next_retry_at_ms.as_ref(), "next_retry_at_ms", status)?;
            require_absent(state.blocked_reason.as_ref(), "blocked_reason", status)?;
            require_no_confirmed_kafka_metadata(state, status)?;
        }
        "DELIVERY_BLOCKED" => {
            require_non_empty(state.blocked_reason.as_deref(), "blocked_reason", status)?;
            require_absent(state.next_retry_at_ms.as_ref(), "next_retry_at_ms", status)?;
            require_no_confirmed_kafka_metadata(state, status)?;
        }
        _ => {}
    }
    Ok(())
}

fn require_delivery_routing_fields(
    state: &DeliveryStateRecord,
    status: &str,
) -> Result<(), String> {
    require_non_empty(state.kafka_topic.as_deref(), "kafka_topic", status)?;
    require_some(
        state.routing_fingerprint.as_ref(),
        "routing_fingerprint",
        status,
    )?;
    require_non_empty(state.profile_id.as_deref(), "profile_id", status)?;
    require_positive_i32(state.profile_version, "profile_version", status)
}

fn require_no_confirmed_kafka_metadata(
    state: &DeliveryStateRecord,
    status: &str,
) -> Result<(), String> {
    require_absent(state.kafka_partition.as_ref(), "kafka_partition", status)?;
    require_absent(state.kafka_offset.as_ref(), "kafka_offset", status)?;
    require_absent(state.confirmed_at_ms.as_ref(), "confirmed_at_ms", status)
}

fn require_some<T>(value: Option<&T>, field: &str, status: &str) -> Result<(), String> {
    if value.is_some() {
        Ok(())
    } else {
        Err(format!("{status} delivery state missing {field}"))
    }
}

fn require_non_empty(value: Option<&str>, field: &str, status: &str) -> Result<(), String> {
    match value {
        Some(v) if !v.is_empty() => Ok(()),
        Some(_) => Err(format!("{status} delivery state has empty {field}")),
        None => Err(format!("{status} delivery state missing {field}")),
    }
}

fn require_absent<T>(value: Option<&T>, field: &str, status: &str) -> Result<(), String> {
    if value.is_none() {
        Ok(())
    } else {
        Err(format!("{status} delivery state must not contain {field}"))
    }
}

fn require_positive_i32(value: Option<i32>, field: &str, status: &str) -> Result<(), String> {
    match value {
        Some(v) if v > 0 => Ok(()),
        Some(_) => Err(format!("{status} delivery state has invalid {field}")),
        None => Err(format!("{status} delivery state missing {field}")),
    }
}

fn require_non_negative_i32(value: Option<i32>, field: &str, status: &str) -> Result<(), String> {
    match value {
        Some(v) if v >= 0 => Ok(()),
        Some(_) => Err(format!("{status} delivery state has invalid {field}")),
        None => Err(format!("{status} delivery state missing {field}")),
    }
}

fn require_non_negative_i64(value: Option<i64>, field: &str, status: &str) -> Result<(), String> {
    match value {
        Some(v) if v >= 0 => Ok(()),
        Some(_) => Err(format!("{status} delivery state has invalid {field}")),
        None => Err(format!("{status} delivery state missing {field}")),
    }
}

fn load_delivery_candidate(conn: &Connection, event_id: &str) -> DeliveryCandidateResult {
    let delivery_state = match load_delivery_state(conn, event_id) {
        DeliveryStateResult::Found(state) => *state,
        DeliveryStateResult::NotFound => return DeliveryCandidateResult::NotFound,
        DeliveryStateResult::Corrupted { event_id, detail } => {
            return DeliveryCandidateResult::Corrupted { event_id, detail };
        }
        DeliveryStateResult::Error(e) => return DeliveryCandidateResult::Error(e),
    };

    let row = match conn
        .query_row(
            "SELECT installation_id, namespace, logical_stream_type, \
                    stream_key, stream_sequence, event_type, \
                    routing_version, envelope_checksum \
             FROM stored_envelope WHERE event_id = ?1",
            params![event_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                ))
            },
        )
        .optional()
    {
        Ok(Some(row)) => row,
        Ok(None) => {
            return DeliveryCandidateResult::Corrupted {
                event_id: event_id.to_string(),
                detail: "stored envelope missing for delivery state".into(),
            };
        }
        Err(e) => {
            return DeliveryCandidateResult::Corrupted {
                event_id: event_id.to_string(),
                detail: format!("delivery candidate envelope query error: {e}"),
            };
        }
    };

    let (
        installation_id,
        namespace,
        logical_stream_type,
        stream_key,
        stream_sequence,
        event_type,
        routing_version,
        envelope_checksum_bytes,
    ) = row;
    if envelope_checksum_bytes.len() != 32 {
        return DeliveryCandidateResult::Corrupted {
            event_id: event_id.to_string(),
            detail: format!(
                "envelope checksum length {}, expected 32",
                envelope_checksum_bytes.len()
            ),
        };
    }
    let mut envelope_checksum = [0u8; 32];
    envelope_checksum.copy_from_slice(&envelope_checksum_bytes);

    match recompute_envelope_checksum_from_row(conn, event_id) {
        Ok(expected) if envelope_checksum == expected => {}
        Ok(_) => {
            return DeliveryCandidateResult::Corrupted {
                event_id: event_id.to_string(),
                detail: "envelope checksum mismatch".into(),
            };
        }
        Err(detail) => {
            return DeliveryCandidateResult::Corrupted {
                event_id: event_id.to_string(),
                detail,
            };
        }
    }

    DeliveryCandidateResult::Found(Box::new(DeliveryCandidate {
        event_id: event_id.to_string(),
        installation_id,
        namespace,
        logical_stream_type,
        stream_key,
        stream_sequence,
        event_type,
        routing_version,
        delivery_state,
    }))
}

fn load_verified_lifecycle_for_transition(
    tx: &Transaction<'_>,
    event_id: &str,
) -> Result<PersistedStatus, CasResult> {
    match get_status_impl(tx, event_id) {
        StatusResult::Found(status) => Ok(status),
        StatusResult::NotFound => Err(CasResult::Corrupted {
            event_id: event_id.to_string(),
            detail: "lifecycle state missing for delivery transition".into(),
        }),
        StatusResult::Corrupted { event_id, detail } => {
            Err(CasResult::Corrupted { event_id, detail })
        }
    }
}

fn update_lifecycle_delivery_status(
    tx: &Transaction<'_>,
    event_id: &str,
    current: &PersistedStatus,
    new_delivery_status: &str,
    now_ms: i64,
) -> CasResult {
    let new_revision = current.lifecycle_revision + 1;
    let checksum = compute_lifecycle_checksum(
        event_id,
        new_revision,
        new_delivery_status,
        &current.retention_status,
    );
    let affected = match tx.execute(
        "UPDATE lifecycle_state SET \
         revision = ?1, lifecycle_checksum = ?2, \
         delivery_status = ?3, updated_at_ms = ?4 \
         WHERE event_id = ?5 AND revision = ?6",
        params![
            new_revision,
            checksum.as_slice(),
            new_delivery_status,
            now_ms,
            event_id,
            current.lifecycle_revision,
        ],
    ) {
        Ok(n) => n,
        Err(e) => return CasResult::Error(e.to_string()),
    };
    match affected {
        1 => CasResult::Updated,
        0 => CasResult::StaleRevision,
        n => CasResult::Error(format!(
            "lifecycle delivery update affected {n} rows, expected 1"
        )),
    }
}

fn collect_delivery_scan<I>(conn: &Connection, rows: Result<I, String>) -> DeliveryScanResult
where
    I: Iterator<Item = rusqlite::Result<DeliveryScanRow>>,
{
    let mut deliveries = Vec::new();
    let rows = match rows {
        Ok(rows) => rows,
        Err(e) => return DeliveryScanResult::Error(e),
    };
    for row in rows {
        let pending = match row {
            Ok(pending) => pending,
            Err(e) => {
                return DeliveryScanResult::Corrupted {
                    event_id: None,
                    detail: format!("delivery scan row conversion error: {e}"),
                };
            }
        };
        match load_delivery_state(conn, &pending.event_id) {
            DeliveryStateResult::Found(_) => {}
            DeliveryStateResult::NotFound => {
                return DeliveryScanResult::Corrupted {
                    event_id: Some(pending.event_id),
                    detail: "delivery state disappeared during scan".into(),
                };
            }
            DeliveryStateResult::Corrupted { event_id, detail } => {
                return DeliveryScanResult::Corrupted {
                    event_id: Some(event_id),
                    detail,
                };
            }
            DeliveryStateResult::Error(e) => return DeliveryScanResult::Error(e),
        }
        match verify_delivery_scan_envelope(conn, pending) {
            Ok(pending) => deliveries.push(pending),
            Err((event_id, detail)) => {
                return DeliveryScanResult::Corrupted {
                    event_id: Some(event_id),
                    detail,
                };
            }
        }
    }
    DeliveryScanResult::Ok(deliveries)
}

fn verify_delivery_scan_envelope(
    conn: &Connection,
    row: DeliveryScanRow,
) -> Result<PendingDelivery, (String, String)> {
    let event_id = row.event_id;
    let Some(journal_sequence) = row.journal_sequence else {
        return Err((event_id, "stored envelope missing for delivery scan".into()));
    };
    let Some(stream_key) = row.stream_key else {
        return Err((event_id, "stored envelope missing for delivery scan".into()));
    };
    let Some(stream_sequence) = row.stream_sequence else {
        return Err((event_id, "stored envelope missing for delivery scan".into()));
    };
    let Some(envelope_checksum_bytes) = row.envelope_checksum else {
        return Err((event_id, "stored envelope missing for delivery scan".into()));
    };
    if envelope_checksum_bytes.len() != 32 {
        return Err((
            event_id,
            format!(
                "envelope checksum length {}, expected 32",
                envelope_checksum_bytes.len()
            ),
        ));
    }
    let mut envelope_checksum = [0u8; 32];
    envelope_checksum.copy_from_slice(&envelope_checksum_bytes);
    match recompute_envelope_checksum_from_row(conn, &event_id) {
        Ok(expected) if envelope_checksum == expected => {}
        Ok(_) => {
            return Err((event_id, "envelope checksum mismatch".into()));
        }
        Err(detail) => {
            return Err((event_id, detail));
        }
    }

    Ok(PendingDelivery {
        event_id,
        journal_sequence,
        stream_key,
        stream_sequence,
        delivery_status: row.delivery_status,
        attempt_count: row.attempt_count,
        next_retry_at_ms: row.next_retry_at_ms,
    })
}

fn is_allowed_delivery_status(status: &str) -> bool {
    matches!(
        status,
        "LOCAL_ACCEPTED"
            | "DELIVERY_PENDING"
            | "DELIVERY_RETRYING"
            | "REPLICATED"
            | "DELIVERY_BLOCKED"
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

    fn topic() -> &'static str {
        "craftrelay.inst-a.economy.events"
    }

    fn confirmation<'a>(
        fp: &'a Checksum,
        topic: &'a str,
        partition: i32,
        offset: i64,
    ) -> ReplicatedDeliveryConfirmation<'a> {
        ReplicatedDeliveryConfirmation {
            expected_routing_fingerprint: fp,
            expected_profile_id: "p0",
            expected_profile_version: 1,
            kafka_topic: topic,
            kafka_partition: partition,
            kafka_offset: offset,
        }
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
    fn read_verified_payload_returns_stored_payload() {
        let j = mem();
        j.accept(&req("e1", 1));
        assert_eq!(
            j.read_verified_payload("e1"),
            PayloadReadResult::Found(b"test-payload".to_vec())
        );
    }

    #[test]
    fn read_verified_payload_missing_blob_is_corrupted() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(&j, "DELETE FROM payload_blob WHERE event_id='e1'");
        match j.read_verified_payload("e1") {
            PayloadReadResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("payload blob not found"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn read_verified_payload_rejects_blob_digest_mismatch() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE payload_blob \
             SET payload_digest=X'DEADBEEF00000000000000000000000000000000000000000000000000000000' \
             WHERE event_id='e1'",
        );
        match j.read_verified_payload("e1") {
            PayloadReadResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("differs from stored envelope digest"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn read_verified_payload_rejects_payload_digest_mismatch() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper(
            &j,
            "UPDATE payload_blob SET payload=X'DEAD' WHERE event_id='e1'",
        );
        match j.read_verified_payload("e1") {
            PayloadReadResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("payload digest mismatch"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
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
    fn unknown_delivery_status_with_valid_checksum_detected() {
        let j = mem();
        j.accept(&req("e1", 1));
        tamper_lifecycle(&j, "e1", "UNKNOWN_STATUS", "PRESENT");
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
        tamper_lifecycle(&j, "e1", "UNKNOWN_STATUS", "PRESENT");
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
        tamper_lifecycle(&j, "e1", "UNKNOWN_STATUS", "PRESENT");
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

    // --- Sprint 5: Delivery state tests ---

    #[test]
    fn schema_v2_created_on_open() {
        assert_eq!(mem().schema_version(), SCHEMA_VERSION);
        assert_eq!(SCHEMA_VERSION, 2);
    }

    #[test]
    fn create_delivery_pending_succeeds() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        assert_eq!(
            j.create_delivery_pending("e1", &fp, "p0-profile", 1, topic()),
            CasResult::Updated
        );
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "DELIVERY_PENDING");
                assert_eq!(ds.attempt_count, 0);
                assert_eq!(ds.delivery_revision, 1);
                assert!(ds.profile_id.as_deref() == Some("p0-profile"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn create_delivery_pending_nonexistent_event() {
        let j = mem();
        let fp = craftrelay_domain::sha256(b"routing");
        assert_eq!(
            j.create_delivery_pending("missing", &fp, "p0", 1, topic()),
            CasResult::NotFound
        );
    }

    #[test]
    fn create_delivery_pending_sql_error_is_not_hidden_as_not_found() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        tamper(
            &j,
            "ALTER TABLE stored_envelope RENAME TO stored_envelope_corrupt",
        );

        assert!(matches!(
            j.create_delivery_pending("e1", &fp, "p0", 1, topic()),
            CasResult::Error(_)
        ));
    }

    #[test]
    fn record_delivery_attempt_increments_count() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        let attempt = DeliveryAttemptRecord {
            event_id: "e1".into(),
            attempt_number: 1,
            outcome: "TRANSIENT_FAILURE".into(),
            error_code: Some("BROKER_UNAVAILABLE".into()),
            kafka_topic: None,
            kafka_partition: None,
            kafka_offset: None,
            profile_id: Some("p0".into()),
            profile_version: Some(1),
            attempted_at_ms: 1000,
        };
        assert_eq!(j.record_delivery_attempt(&attempt), CasResult::Updated);
        assert_eq!(delivery_attempt_count(&j, "e1"), 1);
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.attempt_count, 1);
                assert_eq!(ds.last_error.as_deref(), Some("BROKER_UNAVAILABLE"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn record_delivery_attempt_state_update_zero_rows_is_not_updated() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(
            &j,
            "CREATE TRIGGER delivery_attempt_revision_race \
             AFTER INSERT ON delivery_attempt \
             BEGIN \
                UPDATE delivery_state \
                SET delivery_revision = delivery_revision + 1 \
                WHERE event_id = NEW.event_id; \
             END;",
        );
        let attempt = DeliveryAttemptRecord {
            event_id: "e1".into(),
            attempt_number: 1,
            outcome: "TRANSIENT_FAILURE".into(),
            error_code: Some("BROKER_UNAVAILABLE".into()),
            kafka_topic: None,
            kafka_partition: None,
            kafka_offset: None,
            profile_id: Some("p0".into()),
            profile_version: Some(1),
            attempted_at_ms: 1000,
        };

        assert_eq!(
            j.record_delivery_attempt(&attempt),
            CasResult::StaleRevision
        );
        assert_eq!(delivery_attempt_count(&j, "e1"), 0);
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_revision, 1);
                assert_eq!(ds.attempt_count, 0);
                assert!(ds.last_error.is_none());
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn confirm_replicated_delivery_succeeds() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Updated
        );
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "REPLICATED");
                assert_eq!(ds.kafka_partition, Some(0));
                assert_eq!(ds.kafka_offset, Some(42));
                assert!(ds.confirmed_at_ms.is_some());
            }
            other => panic!("{other:?}"),
        }
        match j.get_status("e1") {
            StatusResult::Found(s) => {
                assert_eq!(s.delivery_status, "REPLICATED");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn replicated_state_missing_kafka_partition_with_valid_checksum_is_corrupted() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Updated
        );
        tamper_delivery_state_with_valid_checksum(&j, "e1", |state| {
            state.kafka_partition = None;
        });

        match j.get_delivery_state("e1") {
            DeliveryStateResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("kafka_partition"));
            }
            other => panic!("expected corrupted replicated state, got {other:?}"),
        }
    }

    #[test]
    fn replicated_state_missing_kafka_offset_with_valid_checksum_is_corrupted() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Updated
        );
        tamper_delivery_state_with_valid_checksum(&j, "e1", |state| {
            state.kafka_offset = None;
        });

        match j.get_delivery_state("e1") {
            DeliveryStateResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("kafka_offset"));
            }
            other => panic!("expected corrupted replicated state, got {other:?}"),
        }
    }

    #[test]
    fn replicated_state_missing_kafka_topic_with_valid_checksum_is_corrupted() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Updated
        );
        tamper_delivery_state_with_valid_checksum(&j, "e1", |state| {
            state.kafka_topic = None;
        });

        match j.get_delivery_state("e1") {
            DeliveryStateResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("kafka_topic"));
            }
            other => panic!("expected corrupted replicated state, got {other:?}"),
        }
    }

    #[test]
    fn replicated_state_missing_confirmed_at_with_valid_checksum_is_corrupted() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Updated
        );
        tamper_delivery_state_with_valid_checksum(&j, "e1", |state| {
            state.confirmed_at_ms = None;
        });

        match j.get_delivery_state("e1") {
            DeliveryStateResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("confirmed_at_ms"));
            }
            other => panic!("expected corrupted replicated state, got {other:?}"),
        }
    }

    #[test]
    fn pending_state_with_confirmed_kafka_metadata_and_valid_checksum_is_corrupted() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper_delivery_state_with_valid_checksum(&j, "e1", |state| {
            state.kafka_partition = Some(0);
            state.kafka_offset = Some(42);
            state.confirmed_at_ms = Some(1234);
        });

        match j.get_delivery_state("e1") {
            DeliveryStateResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("kafka_partition"));
            }
            other => panic!("expected corrupted pending state, got {other:?}"),
        }
    }

    #[test]
    fn confirm_replicated_delivery_stale_revision() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(
            j.confirm_replicated_delivery("e1", 99, confirmation(&fp, topic(), 0, 1)),
            CasResult::StaleRevision
        );
    }

    #[test]
    fn block_delivery_succeeds() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(
            j.block_delivery("e1", 1, "permanent broker failure"),
            CasResult::Updated
        );
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "DELIVERY_BLOCKED");
                assert_eq!(
                    ds.blocked_reason.as_deref(),
                    Some("permanent broker failure")
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn update_delivery_retry_succeeds() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(j.update_delivery_retry("e1", 1, 5000), CasResult::Updated);
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "DELIVERY_RETRYING");
                assert_eq!(ds.next_retry_at_ms, Some(5000));
                assert_eq!(ds.delivery_revision, 2);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn scan_pending_deliveries_ordered() {
        let j = mem();
        j.accept(&req("e1", 1));
        j.accept(&req("e2", 2));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        j.create_delivery_pending("e2", &fp, "p0", 1, topic());
        let pending = match j.scan_pending_deliveries(10) {
            DeliveryScanResult::Ok(pending) => pending,
            other => panic!("{other:?}"),
        };
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].event_id, "e1");
        assert_eq!(pending[1].event_id, "e2");
        assert!(pending[0].journal_sequence < pending[1].journal_sequence);
    }

    #[test]
    fn scan_pending_deliveries_respects_limit() {
        let j = mem();
        j.accept(&req("e1", 1));
        j.accept(&req("e2", 2));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        j.create_delivery_pending("e2", &fp, "p0", 1, topic());
        let pending = match j.scan_pending_deliveries(1) {
            DeliveryScanResult::Ok(pending) => pending,
            other => panic!("{other:?}"),
        };
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn delivery_candidate_returns_verified_envelope_and_state() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());

        match j.get_delivery_candidate("e1") {
            DeliveryCandidateResult::Found(candidate) => {
                assert_eq!(candidate.event_id, "e1");
                assert_eq!(candidate.installation_id, "inst-a");
                assert_eq!(candidate.namespace, "economy");
                assert_eq!(candidate.logical_stream_type, "account");
                assert_eq!(candidate.stream_key, "player-1");
                assert_eq!(candidate.stream_sequence, 1);
                assert_eq!(candidate.event_type, "transfer");
                assert_eq!(candidate.routing_version, 1);
                assert_eq!(candidate.delivery_state.delivery_status, "DELIVERY_PENDING");
            }
            other => panic!("expected candidate, got {other:?}"),
        }
    }

    #[test]
    fn delivery_candidate_rejects_corrupt_envelope() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(
            &j,
            "UPDATE stored_envelope SET namespace='HACKED' WHERE event_id='e1'",
        );

        match j.get_delivery_candidate("e1") {
            DeliveryCandidateResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("envelope checksum mismatch"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn delivery_candidate_rejects_missing_envelope_for_delivery_state() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(&j, "DELETE FROM stored_envelope WHERE event_id='e1'");

        match j.get_delivery_candidate("e1") {
            DeliveryCandidateResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id, "e1");
                assert!(detail.contains("stored envelope missing"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn scan_retrying_deliveries_filters_by_time() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        j.update_delivery_retry("e1", 1, 5000);
        assert_eq!(
            match j.scan_retrying_deliveries(1000, 10) {
                DeliveryScanResult::Ok(pending) => pending.len(),
                other => panic!("{other:?}"),
            },
            0
        );
        assert_eq!(
            match j.scan_retrying_deliveries(5000, 10) {
                DeliveryScanResult::Ok(pending) => pending.len(),
                other => panic!("{other:?}"),
            },
            1
        );
        assert_eq!(
            match j.scan_retrying_deliveries(9999, 10) {
                DeliveryScanResult::Ok(pending) => pending.len(),
                other => panic!("{other:?}"),
            },
            1
        );
    }

    #[test]
    fn corrupt_delivery_row_in_pending_scan_is_not_hidden() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(
            &j,
            "UPDATE delivery_state SET delivery_checksum=X'00' WHERE event_id='e1'",
        );

        match j.scan_pending_deliveries(10) {
            DeliveryScanResult::Corrupted { event_id, .. } => {
                assert_eq!(event_id.as_deref(), Some("e1"));
            }
            other => panic!("expected scan corruption, got {other:?}"),
        }
    }

    #[test]
    fn corrupt_delivery_row_in_retry_scan_is_not_hidden() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        j.update_delivery_retry("e1", 1, 5000);
        tamper(
            &j,
            "UPDATE delivery_state SET delivery_checksum=X'00' WHERE event_id='e1'",
        );

        assert!(matches!(
            j.scan_retrying_deliveries(5000, 10),
            DeliveryScanResult::Corrupted { .. }
        ));
    }

    #[test]
    fn corrupt_delivery_row_in_gate_scan_is_not_hidden() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(
            &j,
            "UPDATE delivery_state SET delivery_checksum=X'00' WHERE event_id='e1'",
        );

        assert!(matches!(
            j.scan_delivery_gate_states(10),
            DeliveryScanResult::Corrupted { .. }
        ));
    }

    #[test]
    fn corrupt_envelope_in_gate_scan_is_not_hidden() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(
            &j,
            "UPDATE stored_envelope SET stream_key='fake-stream' WHERE event_id='e1'",
        );

        match j.scan_delivery_gate_states(10) {
            DeliveryScanResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id.as_deref(), Some("e1"));
                assert!(detail.contains("envelope checksum mismatch"));
            }
            other => panic!("expected scan corruption, got {other:?}"),
        }
    }

    #[test]
    fn missing_envelope_in_gate_scan_is_not_hidden() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(&j, "DELETE FROM stored_envelope WHERE event_id='e1'");

        match j.scan_delivery_gate_states(10) {
            DeliveryScanResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id.as_deref(), Some("e1"));
                assert!(detail.contains("stored envelope missing"));
            }
            other => panic!("expected scan corruption, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_delivery_status_in_gate_scan_is_not_hidden() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(
            &j,
            "UPDATE delivery_state SET delivery_status='BROKEN' WHERE event_id='e1'",
        );

        match j.scan_delivery_gate_states(10) {
            DeliveryScanResult::Corrupted { event_id, detail } => {
                assert_eq!(event_id.as_deref(), Some("e1"));
                assert!(detail.contains("unsupported delivery state status"));
            }
            other => panic!("expected scan corruption, got {other:?}"),
        }
    }

    #[test]
    fn get_delivery_state_not_found() {
        let j = mem();
        assert!(matches!(
            j.get_delivery_state("missing"),
            DeliveryStateResult::NotFound
        ));
    }

    #[test]
    fn delivery_state_fk_requires_envelope() {
        let j = mem();
        let fp = craftrelay_domain::sha256(b"routing");
        assert_eq!(
            j.create_delivery_pending("orphan", &fp, "p0", 1, topic()),
            CasResult::NotFound
        );
    }

    #[test]
    fn confirm_also_updates_lifecycle_to_replicated() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 10));
        match j.get_status("e1") {
            StatusResult::Found(s) => {
                assert_eq!(s.delivery_status, "REPLICATED");
                assert_eq!(s.lifecycle_revision, 2);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn block_also_updates_lifecycle() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        j.block_delivery("e1", 1, "test");
        match j.get_status("e1") {
            StatusResult::Found(s) => {
                assert_eq!(s.delivery_status, "DELIVERY_BLOCKED");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn confirm_replicated_missing_lifecycle_does_not_mutate_delivery() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(&j, "DELETE FROM lifecycle_state WHERE event_id='e1'");

        assert!(matches!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Corrupted { .. }
        ));
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => assert_eq!(ds.delivery_status, "DELIVERY_PENDING"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn confirm_replicated_corrupt_lifecycle_checksum_does_not_return_updated() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(
            &j,
            "UPDATE lifecycle_state SET lifecycle_checksum=X'00' WHERE event_id='e1'",
        );

        assert!(matches!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Corrupted { .. }
        ));
    }

    #[test]
    fn confirm_replicated_stale_lifecycle_update_rolls_back_delivery() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(
            &j,
            "CREATE TRIGGER lifecycle_revision_race \
             AFTER UPDATE OF delivery_status ON delivery_state \
             WHEN NEW.event_id='e1' AND NEW.delivery_status='REPLICATED' \
             BEGIN \
                UPDATE lifecycle_state \
                SET revision = revision + 1 \
                WHERE event_id = NEW.event_id; \
             END",
        );

        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::StaleRevision
        );
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "DELIVERY_PENDING");
                assert_eq!(ds.kafka_offset, None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn block_delivery_missing_lifecycle_does_not_mutate_delivery() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(&j, "DELETE FROM lifecycle_state WHERE event_id='e1'");

        assert!(matches!(
            j.block_delivery("e1", 1, "operator block"),
            CasResult::Corrupted { .. }
        ));
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => assert_eq!(ds.delivery_status, "DELIVERY_PENDING"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn block_delivery_corrupt_lifecycle_checksum_does_not_return_updated() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(
            &j,
            "UPDATE lifecycle_state SET lifecycle_checksum=X'00' WHERE event_id='e1'",
        );

        assert!(matches!(
            j.block_delivery("e1", 1, "operator block"),
            CasResult::Corrupted { .. }
        ));
    }

    #[test]
    fn duplicate_confirm_is_idempotent() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Updated
        );
        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::AlreadyConfirmed
        );
    }

    #[test]
    fn duplicate_confirm_after_reopen_is_idempotent() {
        let (dir, path) = tmpdir("cr-j-dup-confirm");
        let fp = craftrelay_domain::sha256(b"routing");
        {
            let j = LocalJournal::open(&path, cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
            j.accept(&req("e1", 1));
            j.create_delivery_pending("e1", &fp, "p0", 1, topic());
            assert_eq!(
                j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
                CasResult::Updated
            );
        }
        let j = LocalJournal::open(&path, cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::AlreadyConfirmed
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn duplicate_confirm_with_different_offset_is_rejected_without_mutation() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Updated
        );
        match j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 43)) {
            CasResult::Corrupted { detail, .. } => {
                assert!(detail.contains("conflicting replicated confirmation"));
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => assert_eq!(ds.kafka_offset, Some(42)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn duplicate_confirm_with_different_topic_is_rejected_without_mutation() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert_eq!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Updated
        );
        assert!(matches!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, "wrong-topic", 0, 42)),
            CasResult::Corrupted { .. }
        ));
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => assert_eq!(ds.kafka_topic.as_deref(), Some(topic())),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn confirm_with_wrong_topic_is_rejected_by_journal() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        assert!(matches!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, "wrong-topic", 0, 42)),
            CasResult::Corrupted { .. }
        ));
        match j.get_delivery_state("e1") {
            DeliveryStateResult::Found(ds) => assert_eq!(ds.delivery_status, "DELIVERY_PENDING"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn delivery_state_tamper_is_detected() {
        let cases = [
            "UPDATE delivery_state SET delivery_status='REPLICATED' WHERE event_id='e1'",
            "UPDATE delivery_state SET attempt_count=99 WHERE event_id='e1'",
            "UPDATE delivery_state SET routing_fingerprint=X'010203' WHERE event_id='e1'",
            "UPDATE delivery_state SET profile_id='other' WHERE event_id='e1'",
            "UPDATE delivery_state SET profile_version=99 WHERE event_id='e1'",
            "UPDATE delivery_state SET kafka_topic='wrong-topic' WHERE event_id='e1'",
            "UPDATE delivery_state SET kafka_partition=7 WHERE event_id='e1'",
            "UPDATE delivery_state SET kafka_offset=8 WHERE event_id='e1'",
            "UPDATE delivery_state SET delivery_checksum=X'AA' WHERE event_id='e1'",
        ];

        for sql in cases {
            let j = mem();
            j.accept(&req("e1", 1));
            let fp = craftrelay_domain::sha256(b"routing");
            j.create_delivery_pending("e1", &fp, "p0", 1, topic());
            tamper(&j, sql);
            match j.get_delivery_state("e1") {
                DeliveryStateResult::Corrupted { .. } => {}
                other => panic!("{sql}: expected Corrupted, got {other:?}"),
            }
        }
    }

    #[test]
    fn corrupted_delivery_state_cannot_transition() {
        let j = mem();
        j.accept(&req("e1", 1));
        let fp = craftrelay_domain::sha256(b"routing");
        j.create_delivery_pending("e1", &fp, "p0", 1, topic());
        tamper(
            &j,
            "UPDATE delivery_state SET attempt_count=99 WHERE event_id='e1'",
        );

        assert!(matches!(
            j.confirm_replicated_delivery("e1", 1, confirmation(&fp, topic(), 0, 42)),
            CasResult::Corrupted { .. }
        ));
        assert!(matches!(
            j.block_delivery("e1", 1, "blocked"),
            CasResult::Corrupted { .. }
        ));
        assert!(matches!(
            j.update_delivery_retry("e1", 1, 5000),
            CasResult::Corrupted { .. }
        ));
        assert!(matches!(
            j.get_delivery_state("e1"),
            DeliveryStateResult::Corrupted { .. }
        ));
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
        conn.execute_batch(sql).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    }

    fn tamper_delivery_state_with_valid_checksum<F>(j: &LocalJournal, event_id: &str, mutate: F)
    where
        F: FnOnce(&mut DeliveryStateRecord),
    {
        let conn = j.conn.lock().unwrap();
        let mut state = match load_delivery_state(&conn, event_id) {
            DeliveryStateResult::Found(state) => *state,
            other => panic!("expected delivery state before tamper, got {other:?}"),
        };
        mutate(&mut state);
        state.delivery_checksum = compute_delivery_checksum(&state);
        let routing_fingerprint = state.routing_fingerprint.map(|value| value.to_vec());

        conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
        conn.execute(
            "UPDATE delivery_state SET \
             delivery_status = ?1, attempt_count = ?2, next_retry_at_ms = ?3, \
             last_error = ?4, blocked_reason = ?5, kafka_topic = ?6, \
             kafka_partition = ?7, kafka_offset = ?8, routing_fingerprint = ?9, \
             profile_id = ?10, profile_version = ?11, delivery_revision = ?12, \
             delivery_checksum = ?13, confirmed_at_ms = ?14, \
             created_at_ms = ?15, updated_at_ms = ?16 \
             WHERE event_id = ?17",
            params![
                state.delivery_status,
                state.attempt_count,
                state.next_retry_at_ms,
                state.last_error,
                state.blocked_reason,
                state.kafka_topic,
                state.kafka_partition,
                state.kafka_offset,
                routing_fingerprint,
                state.profile_id,
                state.profile_version,
                state.delivery_revision,
                state.delivery_checksum.as_slice(),
                state.confirmed_at_ms,
                state.created_at_ms,
                state.updated_at_ms,
                event_id,
            ],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    }

    fn delivery_attempt_count(j: &LocalJournal, event_id: &str) -> i64 {
        let conn = j.conn.lock().unwrap();
        conn.query_row(
            "SELECT COUNT(*) FROM delivery_attempt WHERE event_id = ?1",
            params![event_id],
            |row| row.get(0),
        )
        .unwrap()
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
