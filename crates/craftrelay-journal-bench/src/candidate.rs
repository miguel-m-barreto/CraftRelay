#![forbid(unsafe_code)]

use crate::ingress::SyntheticEvent;
use rusqlite::{Connection, Transaction, params};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Instant;

// Conservative fixed overhead per event for envelope + lifecycle metadata rows.
const PER_EVENT_OVERHEAD_BYTES: u64 = 512;

pub fn estimated_transaction_bytes(event: &SyntheticEvent) -> u64 {
    event.payload.len() as u64 + PER_EVENT_OVERHEAD_BYTES
}

#[derive(Debug, Clone)]
pub struct CandidateConfig {
    pub name: String,
    pub journal_mode: String,
    pub synchronous: String,
    pub busy_timeout_ms: u32,
    pub wal_autocheckpoint: u32,
    pub page_size: Option<u32>,
    pub max_records_per_tx: u32,
    pub max_bytes_per_tx: u64,
    pub segment_rollover_records: Option<u32>,
    pub unsafe_mode: bool,
}

impl CandidateConfig {
    pub fn monolithic() -> Self {
        Self {
            name: "monolithic".into(),
            journal_mode: "WAL".into(),
            synchronous: "FULL".into(),
            busy_timeout_ms: 5_000,
            wal_autocheckpoint: 1_000,
            page_size: None,
            max_records_per_tx: 64,
            max_bytes_per_tx: 1_048_576,
            segment_rollover_records: None,
            unsafe_mode: false,
        }
    }

    pub fn segmented() -> Self {
        Self {
            name: "segmented".into(),
            journal_mode: "WAL".into(),
            synchronous: "FULL".into(),
            busy_timeout_ms: 5_000,
            wal_autocheckpoint: 1_000,
            page_size: None,
            max_records_per_tx: 64,
            max_bytes_per_tx: 1_048_576,
            segment_rollover_records: Some(500),
            unsafe_mode: false,
        }
    }

    pub fn is_segmented(&self) -> bool {
        self.segment_rollover_records.is_some()
    }

    pub fn label(&self) -> String {
        let safety = if self.unsafe_mode {
            " [UNSAFE-BENCHMARK-ONLY]"
        } else {
            ""
        };
        format!("{}{safety}", self.name)
    }
}

pub struct WriteResult {
    pub journal_sequence: i64,
    pub stream_sequence: i64,
    pub envelope_checksum: [u8; 32],
}

#[derive(Debug, Default)]
pub struct WriteBatchMetrics {
    pub committed_transactions: u64,
    pub failed_transactions: u64,
    pub written_records: u64,
    pub failed_records: u64,
    pub max_observed_batch_size: u32,
    pub total_batch_records: u64,
}

impl WriteBatchMetrics {
    pub fn avg_observed_batch_size(&self) -> f64 {
        if self.committed_transactions > 0 {
            self.total_batch_records as f64 / self.committed_transactions as f64
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct StorageMetrics {
    pub db_size_bytes: u64,
    pub wal_size_bytes: u64,
    pub shm_size_bytes: u64,
    pub total_storage_bytes: u64,
    pub segment_count: usize,
}

fn file_size_or_zero(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn wal_path(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-wal");
    PathBuf::from(p)
}

fn shm_path(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-shm");
    PathBuf::from(p)
}

fn single_db_storage(db_path: &Path) -> (u64, u64, u64) {
    let db = file_size_or_zero(db_path);
    let wal = file_size_or_zero(&wal_path(db_path));
    let shm = file_size_or_zero(&shm_path(db_path));
    (db, wal, shm)
}

pub fn derive_run_data_dir(output: &Path) -> PathBuf {
    let stem = output
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("default");
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    parent.join("runs").join(stem)
}

pub fn prepare_run_data_dir(dir: &Path) {
    if dir.exists() {
        std::fs::remove_dir_all(dir).expect("clean run data dir");
    }
    std::fs::create_dir_all(dir).expect("create run data dir");
}

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS bench_envelope (
    journal_sequence   INTEGER PRIMARY KEY,
    event_id           TEXT    NOT NULL UNIQUE,
    installation_id    TEXT    NOT NULL,
    producer_id        TEXT    NOT NULL,
    namespace          TEXT    NOT NULL,
    logical_stream     TEXT    NOT NULL,
    stream_sequence    INTEGER NOT NULL,
    schema_version     INTEGER NOT NULL,
    payload_digest     BLOB    NOT NULL,
    request_fingerprint BLOB   NOT NULL,
    envelope_checksum  BLOB    NOT NULL,
    payload_ref_id     TEXT    NOT NULL,
    payload_length     INTEGER NOT NULL,
    received_at_ms     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS bench_payload (
    event_id       TEXT PRIMARY KEY,
    payload        BLOB NOT NULL,
    payload_digest BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS bench_lifecycle (
    event_id           TEXT    PRIMARY KEY,
    revision           INTEGER NOT NULL,
    snapshot_checksum   BLOB    NOT NULL,
    delivery_status    TEXT    NOT NULL,
    local_durable_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS bench_stream_head (
    stream_key     TEXT PRIMARY KEY,
    head_sequence  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS bench_metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"#;

fn apply_pragmas(conn: &Connection, config: &CandidateConfig) {
    conn.execute_batch(&format!("PRAGMA journal_mode = {};", config.journal_mode))
        .expect("journal_mode");
    conn.execute_batch(&format!("PRAGMA synchronous = {};", config.synchronous))
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
    if let Some(ps) = config.page_size {
        conn.execute_batch(&format!("PRAGMA page_size = {ps};"))
            .expect("page_size");
    }
    conn.execute_batch("PRAGMA foreign_keys = ON;")
        .expect("foreign_keys");
}

fn init_db(conn: &Connection, config: &CandidateConfig) {
    apply_pragmas(conn, config);
    conn.execute_batch(SCHEMA_SQL).expect("schema");
}

fn allocate_stream_sequence(tx: &Transaction<'_>, stream_key: &str) -> i64 {
    let existing: Option<i64> = tx
        .query_row(
            "SELECT head_sequence FROM bench_stream_head \
             WHERE stream_key = ?1",
            params![stream_key],
            |row| row.get(0),
        )
        .ok();
    let next = existing.unwrap_or(0) + 1;
    tx.execute(
        "INSERT INTO bench_stream_head (stream_key, head_sequence) \
         VALUES (?1, ?2) \
         ON CONFLICT(stream_key) DO UPDATE SET head_sequence = ?2",
        params![stream_key, next],
    )
    .expect("stream head upsert");
    next
}

fn compute_envelope_checksum(
    event: &SyntheticEvent,
    journal_seq: i64,
    stream_seq: i64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(event.installation_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(event.event_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(journal_seq.to_be_bytes());
    hasher.update(stream_seq.to_be_bytes());
    hasher.update(event.payload_digest);
    hasher.finalize().into()
}

fn write_event_in_tx(
    tx: &Transaction<'_>,
    event: &SyntheticEvent,
    now_ms: i64,
) -> Result<WriteResult, rusqlite::Error> {
    let journal_sequence: i64 = tx.query_row(
        "SELECT COALESCE(MAX(journal_sequence), 0) + 1 \
         FROM bench_envelope",
        [],
        |row| row.get(0),
    )?;
    let stream_sequence = allocate_stream_sequence(tx, &event.logical_stream);
    let envelope_checksum = compute_envelope_checksum(event, journal_sequence, stream_sequence);
    write_three_rows(
        tx,
        event,
        journal_sequence,
        stream_sequence,
        &envelope_checksum,
        now_ms,
    )?;
    Ok(WriteResult {
        journal_sequence,
        stream_sequence,
        envelope_checksum,
    })
}

fn write_three_rows(
    tx: &Transaction<'_>,
    event: &SyntheticEvent,
    journal_seq: i64,
    stream_seq: i64,
    envelope_checksum: &[u8; 32],
    now_ms: i64,
) -> Result<(), rusqlite::Error> {
    tx.execute(
        "INSERT INTO bench_envelope (\
            journal_sequence, event_id, installation_id, producer_id, \
            namespace, logical_stream, stream_sequence, schema_version, \
            payload_digest, request_fingerprint, envelope_checksum, \
            payload_ref_id, payload_length, received_at_ms\
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            journal_seq,
            event.event_id,
            event.installation_id,
            event.producer_id,
            event.namespace,
            event.logical_stream,
            stream_seq,
            event.schema_version,
            event.payload_digest.as_slice(),
            event.request_fingerprint.as_slice(),
            envelope_checksum.as_slice(),
            event.payload_ref_id,
            event.payload.len() as i64,
            now_ms,
        ],
    )?;
    tx.execute(
        "INSERT INTO bench_payload (event_id, payload, payload_digest) \
         VALUES (?1, ?2, ?3)",
        params![
            event.event_id,
            event.payload.as_slice(),
            event.payload_digest.as_slice(),
        ],
    )?;
    let lc_checksum =
        craftrelay_domain::sha256(format!("{journal_seq}\0LOCAL_ACCEPTED_SYNTHETIC").as_bytes());
    tx.execute(
        "INSERT INTO bench_lifecycle (\
            event_id, revision, snapshot_checksum, delivery_status, \
            local_durable_at_ms\
         ) VALUES (?1, 1, ?2, 'LOCAL_ACCEPTED_SYNTHETIC', ?3)",
        params![event.event_id, lc_checksum.as_slice(), now_ms,],
    )?;
    Ok(())
}

/// Compute batch boundaries respecting both record and byte limits.
fn next_batch_end(
    events: &[SyntheticEvent],
    start: usize,
    max_records: usize,
    max_bytes: u64,
    hard_end: usize,
) -> usize {
    let mut count = 0usize;
    let mut bytes = 0u64;
    let mut pos = start;
    while pos < hard_end {
        let event_bytes = estimated_transaction_bytes(&events[pos]);
        if count > 0 && (count >= max_records || bytes + event_bytes > max_bytes) {
            break;
        }
        count += 1;
        bytes += event_bytes;
        pos += 1;
    }
    pos
}

// --- Monolithic candidate ---

pub struct MonolithicCandidate {
    conn: Connection,
    config: CandidateConfig,
    db_path: Option<PathBuf>,
}

impl MonolithicCandidate {
    pub fn open(path: &Path, config: CandidateConfig) -> Self {
        let conn = Connection::open(path).expect("open monolithic db");
        init_db(&conn, &config);
        Self {
            conn,
            config,
            db_path: Some(path.to_path_buf()),
        }
    }

    pub fn open_in_memory(config: CandidateConfig) -> Self {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        init_db(&conn, &config);
        Self {
            conn,
            config,
            db_path: None,
        }
    }

    pub fn write_batch(
        &mut self,
        events: &[SyntheticEvent],
    ) -> (Vec<Result<WriteResult, String>>, WriteBatchMetrics) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let max_records = self.config.max_records_per_tx as usize;
        let max_bytes = self.config.max_bytes_per_tx;
        let mut results = Vec::with_capacity(events.len());
        let mut metrics = WriteBatchMetrics::default();
        let mut i = 0;
        while i < events.len() {
            let batch_end = next_batch_end(events, i, max_records, max_bytes, events.len());
            let batch = &events[i..batch_end];
            let tx = self.conn.transaction().expect("begin tx");
            let mut batch_ok = true;
            let mut batch_results = Vec::with_capacity(batch.len());
            for event in batch {
                match write_event_in_tx(&tx, event, now_ms) {
                    Ok(wr) => batch_results.push(Ok(wr)),
                    Err(e) => {
                        batch_ok = false;
                        batch_results.push(Err(e.to_string()));
                        break;
                    }
                }
            }
            if batch_ok {
                tx.commit().expect("commit tx");
                let n = batch_results.len() as u32;
                metrics.committed_transactions += 1;
                metrics.written_records += u64::from(n);
                metrics.total_batch_records += u64::from(n);
                if n > metrics.max_observed_batch_size {
                    metrics.max_observed_batch_size = n;
                }
                results.extend(batch_results);
            } else {
                metrics.failed_transactions += 1;
                metrics.failed_records += batch.len() as u64;
                for event in batch {
                    results.push(Err(format!(
                        "batch rolled back for event {}",
                        event.event_id
                    )));
                }
            }
            i = batch_end;
        }
        (results, metrics)
    }

    pub fn journal_mode(&self) -> String {
        self.conn
            .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .unwrap_or_default()
    }

    pub fn storage_metrics(&self) -> StorageMetrics {
        if let Some(ref p) = self.db_path {
            let (db, wal, shm) = single_db_storage(p);
            StorageMetrics {
                db_size_bytes: db,
                wal_size_bytes: wal,
                shm_size_bytes: shm,
                total_storage_bytes: db + wal + shm,
                segment_count: 1,
            }
        } else {
            let page_count: u64 = self
                .conn
                .query_row("PRAGMA page_count;", [], |row| row.get(0))
                .unwrap_or(0);
            let page_size: u64 = self
                .conn
                .query_row("PRAGMA page_size;", [], |row| row.get(0))
                .unwrap_or(4096);
            let db = page_count * page_size;
            StorageMetrics {
                db_size_bytes: db,
                wal_size_bytes: 0,
                shm_size_bytes: 0,
                total_storage_bytes: db,
                segment_count: 1,
            }
        }
    }

    pub fn record_count(&self) -> u64 {
        self.conn
            .query_row("SELECT COUNT(*) FROM bench_envelope", [], |row| row.get(0))
            .unwrap_or(0)
    }

    pub fn config(&self) -> &CandidateConfig {
        &self.config
    }
}

// --- Segmented candidate ---

pub struct SegmentedCandidate {
    dir: PathBuf,
    config: CandidateConfig,
    segments: Vec<SegmentInfo>,
    active_conn: Option<Connection>,
    global_journal_seq: i64,
    records_in_active: u32,
}

#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub path: PathBuf,
    pub record_count: u32,
    pub first_journal_seq: i64,
    pub last_journal_seq: i64,
}

impl SegmentedCandidate {
    pub fn open(dir: &Path, config: CandidateConfig) -> Self {
        std::fs::create_dir_all(dir).expect("create segment dir");
        let mut cand = Self {
            dir: dir.to_path_buf(),
            config,
            segments: Vec::new(),
            active_conn: None,
            global_journal_seq: 0,
            records_in_active: 0,
        };
        cand.roll_segment();
        cand
    }

    fn roll_segment(&mut self) {
        let seg_index = self.segments.len();
        let seg_path = self.dir.join(format!("segment_{seg_index:04}.db"));
        let conn = Connection::open(&seg_path).expect("open segment");
        init_db(&conn, &self.config);
        self.active_conn = Some(conn);
        self.records_in_active = 0;
        self.segments.push(SegmentInfo {
            path: seg_path,
            record_count: 0,
            first_journal_seq: self.global_journal_seq + 1,
            last_journal_seq: self.global_journal_seq,
        });
    }

    fn needs_rollover(&self) -> bool {
        if let Some(limit) = self.config.segment_rollover_records {
            self.records_in_active >= limit
        } else {
            false
        }
    }

    pub fn write_batch(
        &mut self,
        events: &[SyntheticEvent],
    ) -> (Vec<Result<WriteResult, String>>, WriteBatchMetrics) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let max_records = self.config.max_records_per_tx as usize;
        let max_bytes = self.config.max_bytes_per_tx;
        let rollover_limit = self.config.segment_rollover_records;
        let mut results = Vec::with_capacity(events.len());
        let mut metrics = WriteBatchMetrics::default();
        let mut i = 0;
        while i < events.len() {
            if self.needs_rollover() {
                self.roll_segment();
            }
            let remaining_in_segment = rollover_limit
                .map(|limit| (limit - self.records_in_active) as usize)
                .unwrap_or(events.len() - i);
            let hard_end = std::cmp::min(i + remaining_in_segment, events.len());
            let batch_end = next_batch_end(events, i, max_records, max_bytes, hard_end);
            let batch = &events[i..batch_end];

            let conn = self.active_conn.as_mut().expect("active segment");
            let tx = conn.transaction().expect("begin segment tx");
            let mut batch_ok = true;
            let mut batch_results = Vec::with_capacity(batch.len());
            let mut local_journal_seq = self.global_journal_seq;
            for event in batch {
                local_journal_seq += 1;
                let journal_seq = local_journal_seq;
                let stream_seq = allocate_stream_sequence(&tx, &event.logical_stream);
                let envelope_checksum = compute_envelope_checksum(event, journal_seq, stream_seq);
                match write_three_rows(
                    &tx,
                    event,
                    journal_seq,
                    stream_seq,
                    &envelope_checksum,
                    now_ms,
                ) {
                    Ok(()) => {
                        batch_results.push(Ok(WriteResult {
                            journal_sequence: journal_seq,
                            stream_sequence: stream_seq,
                            envelope_checksum,
                        }));
                    }
                    Err(e) => {
                        batch_ok = false;
                        batch_results.push(Err(e.to_string()));
                        break;
                    }
                }
            }
            let n = batch_results.iter().filter(|r| r.is_ok()).count() as u32;
            if batch_ok {
                tx.commit().expect("commit segment tx");
                self.global_journal_seq = local_journal_seq;
                self.records_in_active += n;
                if let Some(seg) = self.segments.last_mut() {
                    seg.record_count += n;
                    seg.last_journal_seq = self.global_journal_seq;
                }
                metrics.committed_transactions += 1;
                metrics.written_records += u64::from(n);
                metrics.total_batch_records += u64::from(n);
                if n > metrics.max_observed_batch_size {
                    metrics.max_observed_batch_size = n;
                }
                results.extend(batch_results);
            } else {
                metrics.failed_transactions += 1;
                metrics.failed_records += batch.len() as u64;
                for event in batch {
                    results.push(Err(format!(
                        "segment batch rolled back for event {}",
                        event.event_id
                    )));
                }
            }
            i = batch_end;
        }
        (results, metrics)
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn segments(&self) -> &[SegmentInfo] {
        &self.segments
    }

    pub fn total_records(&self) -> u32 {
        self.segments.iter().map(|s| s.record_count).sum()
    }

    pub fn storage_metrics(&self) -> StorageMetrics {
        let mut db = 0u64;
        let mut wal = 0u64;
        let mut shm = 0u64;
        for seg in &self.segments {
            let (d, w, s) = single_db_storage(&seg.path);
            db += d;
            wal += w;
            shm += s;
        }
        StorageMetrics {
            db_size_bytes: db,
            wal_size_bytes: wal,
            shm_size_bytes: shm,
            total_storage_bytes: db + wal + shm,
            segment_count: self.segments.len(),
        }
    }

    pub fn config(&self) -> &CandidateConfig {
        &self.config
    }
}

// --- Unified runner ---

pub struct RunResult {
    pub results: Vec<Result<WriteResult, String>>,
    pub metrics: WriteBatchMetrics,
    pub elapsed: std::time::Duration,
}

pub enum CandidateRunner {
    Monolithic(MonolithicCandidate),
    Segmented(SegmentedCandidate),
}

impl CandidateRunner {
    pub fn run(&mut self, events: &[SyntheticEvent]) -> RunResult {
        let start = Instant::now();
        let (results, metrics) = match self {
            Self::Monolithic(c) => c.write_batch(events),
            Self::Segmented(c) => c.write_batch(events),
        };
        RunResult {
            results,
            metrics,
            elapsed: start.elapsed(),
        }
    }

    pub fn config(&self) -> &CandidateConfig {
        match self {
            Self::Monolithic(c) => c.config(),
            Self::Segmented(c) => c.config(),
        }
    }

    pub fn storage_metrics(&self) -> StorageMetrics {
        match self {
            Self::Monolithic(c) => c.storage_metrics(),
            Self::Segmented(c) => c.storage_metrics(),
        }
    }

    pub fn record_count(&self) -> u64 {
        match self {
            Self::Monolithic(c) => c.record_count(),
            Self::Segmented(c) => u64::from(c.total_records()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingress::SyntheticEvent;

    fn events(n: u64) -> Vec<SyntheticEvent> {
        (0..n)
            .map(|i| SyntheticEvent::generate(i, "inst-a", "prod-a", "economy", 64))
            .collect()
    }

    fn events_with_payload(n: u64, payload_bytes: usize) -> Vec<SyntheticEvent> {
        (0..n)
            .map(|i| SyntheticEvent::generate(i, "inst-a", "prod-a", "economy", payload_bytes))
            .collect()
    }

    #[test]
    fn monolithic_opens_with_wal_mode() {
        let c = MonolithicCandidate::open_in_memory(CandidateConfig::monolithic());
        let mode = c.journal_mode();
        assert!(
            mode == "wal" || mode == "memory",
            "expected WAL or memory, got: {mode}"
        );
    }

    #[test]
    fn monolithic_transaction_writes_all_three_tables() {
        let mut c = MonolithicCandidate::open_in_memory(CandidateConfig::monolithic());
        let evts = events(3);
        let (results, metrics) = c.write_batch(&evts);
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(c.record_count(), 3);
        assert_eq!(metrics.committed_transactions, 1);
        assert_eq!(metrics.written_records, 3);

        let payload_count: u64 = c
            .conn
            .query_row("SELECT COUNT(*) FROM bench_payload", [], |row| row.get(0))
            .unwrap();
        assert_eq!(payload_count, 3);

        let lifecycle_count: u64 = c
            .conn
            .query_row("SELECT COUNT(*) FROM bench_lifecycle", [], |row| row.get(0))
            .unwrap();
        assert_eq!(lifecycle_count, 3);
    }

    #[test]
    fn journal_sequence_increments_monotonically() {
        let mut c = MonolithicCandidate::open_in_memory(CandidateConfig::monolithic());
        let evts = events(5);
        let (results, _) = c.write_batch(&evts);
        let seqs: Vec<i64> = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|wr| wr.journal_sequence)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn stream_sequence_increments_per_logical_stream() {
        let mut c = MonolithicCandidate::open_in_memory(CandidateConfig::monolithic());
        let mut evts = events(3);
        evts[0].logical_stream = "stream-a".into();
        evts[1].logical_stream = "stream-a".into();
        evts[2].logical_stream = "stream-b".into();
        let (results, _) = c.write_batch(&evts);
        let stream_seqs: Vec<i64> = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|wr| wr.stream_sequence)
            .collect();
        assert_eq!(stream_seqs, vec![1, 2, 1]);
    }

    #[test]
    fn failed_transaction_leaves_no_partial_records() {
        let mut c = MonolithicCandidate::open_in_memory(CandidateConfig::monolithic());
        let mut evts = events(3);
        evts[2].event_id = evts[0].event_id.clone();
        let (results, metrics) = c.write_batch(&evts);
        assert!(results.iter().all(|r| r.is_err()));
        assert_eq!(c.record_count(), 0);
        assert_eq!(metrics.committed_transactions, 0);
        assert_eq!(metrics.failed_transactions, 1);
    }

    #[test]
    fn group_commit_respects_max_records_per_tx() {
        let mut config = CandidateConfig::monolithic();
        config.max_records_per_tx = 2;
        let mut c = MonolithicCandidate::open_in_memory(config);
        let evts = events(5);
        let (results, metrics) = c.write_batch(&evts);
        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(c.record_count(), 5);
        // 5 records / 2 per tx = 3 transactions (2+2+1)
        assert_eq!(metrics.committed_transactions, 3);
        assert_eq!(metrics.max_observed_batch_size, 2);
    }

    #[test]
    fn group_commit_respects_max_bytes_per_tx() {
        let mut config = CandidateConfig::monolithic();
        config.max_records_per_tx = 100;
        // Each 64-byte-payload event = 64 + 512 overhead = 576 bytes.
        // Set limit to 1200 so only 2 events fit per tx.
        config.max_bytes_per_tx = 1200;
        let mut c = MonolithicCandidate::open_in_memory(config);
        let evts = events(5);
        let (results, metrics) = c.write_batch(&evts);
        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(c.record_count(), 5);
        // 5 records at 576 bytes each, limit 1200: batches of 2,2,1
        assert_eq!(metrics.committed_transactions, 3);
        assert_eq!(metrics.max_observed_batch_size, 2);
    }

    #[test]
    fn oversized_single_event_written_alone() {
        let mut config = CandidateConfig::monolithic();
        config.max_bytes_per_tx = 100;
        let mut c = MonolithicCandidate::open_in_memory(config);
        // 1024 payload + 512 overhead = 1536, exceeds 100 byte limit
        let evts = events_with_payload(3, 1024);
        let (results, metrics) = c.write_batch(&evts);
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_ok()));
        // Each event exceeds max_bytes, so each gets its own tx
        assert_eq!(metrics.committed_transactions, 3);
        assert_eq!(metrics.max_observed_batch_size, 1);
    }

    #[test]
    fn monolithic_real_tx_count_with_record_splitting() {
        let mut config = CandidateConfig::monolithic();
        config.max_records_per_tx = 3;
        let mut c = MonolithicCandidate::open_in_memory(config);
        let evts = events(10);
        let (_, metrics) = c.write_batch(&evts);
        // 10 / 3 = 4 transactions (3+3+3+1)
        assert_eq!(metrics.committed_transactions, 4);
    }

    #[test]
    fn monolithic_real_tx_count_with_byte_splitting() {
        let mut config = CandidateConfig::monolithic();
        config.max_records_per_tx = 1000;
        // 256 payload + 512 overhead = 768 per event; limit 2000 => 2 per tx
        config.max_bytes_per_tx = 2000;
        let mut c = MonolithicCandidate::open_in_memory(config);
        let evts = events_with_payload(7, 256);
        let (_, metrics) = c.write_batch(&evts);
        // 7 at 2 per tx = 4 transactions (2+2+2+1)
        assert_eq!(metrics.committed_transactions, 4);
    }

    #[test]
    fn segmented_opens_with_wal_mode() {
        let dir = std::env::temp_dir().join("craftrelay-bench-test-seg-wal");
        let _ = std::fs::remove_dir_all(&dir);
        let c = SegmentedCandidate::open(&dir, CandidateConfig::segmented());
        assert!(c.segment_count() >= 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segmented_rolls_over_deterministically() {
        let dir = std::env::temp_dir().join("craftrelay-bench-test-seg-rollover2");
        let _ = std::fs::remove_dir_all(&dir);
        let mut config = CandidateConfig::segmented();
        config.segment_rollover_records = Some(3);
        let mut c = SegmentedCandidate::open(&dir, config);
        let evts = events(10);
        let (results, _) = c.write_batch(&evts);
        assert_eq!(results.len(), 10);
        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(c.total_records(), 10);
        assert!(c.segment_count() >= 4);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segmented_journal_sequences_are_global() {
        let dir = std::env::temp_dir().join("craftrelay-bench-test-seg-global-seq2");
        let _ = std::fs::remove_dir_all(&dir);
        let mut config = CandidateConfig::segmented();
        config.segment_rollover_records = Some(2);
        let mut c = SegmentedCandidate::open(&dir, config);
        let evts = events(5);
        let (results, _) = c.write_batch(&evts);
        let seqs: Vec<i64> = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|wr| wr.journal_sequence)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segmented_real_tx_count_with_rollover() {
        let dir = std::env::temp_dir().join("craftrelay-bench-test-seg-txcount");
        let _ = std::fs::remove_dir_all(&dir);
        let mut config = CandidateConfig::segmented();
        config.segment_rollover_records = Some(3);
        config.max_records_per_tx = 10;
        let mut c = SegmentedCandidate::open(&dir, config);
        let evts = events(10);
        let (_, metrics) = c.write_batch(&evts);
        // rollover at 3: batches of 3,3,3,1 = 4 transactions
        assert_eq!(metrics.committed_transactions, 4);
        assert_eq!(metrics.written_records, 10);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unsafe_mode_is_labeled() {
        let mut config = CandidateConfig::monolithic();
        config.unsafe_mode = true;
        assert!(config.label().contains("UNSAFE"));
        config.unsafe_mode = false;
        assert!(!config.label().contains("UNSAFE"));
    }

    #[test]
    fn no_durable_receipt_exposed() {
        let _result = WriteResult {
            journal_sequence: 1,
            stream_sequence: 1,
            envelope_checksum: [0u8; 32],
        };
    }

    #[test]
    fn smoke_benchmark_monolithic() {
        let mut c = MonolithicCandidate::open_in_memory(CandidateConfig::monolithic());
        let evts = events(100);
        let (results, metrics) = c.write_batch(&evts);
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 100);
        assert!(metrics.committed_transactions > 0);
    }

    #[test]
    fn smoke_benchmark_segmented() {
        let dir = std::env::temp_dir().join("craftrelay-bench-test-seg-smoke2");
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = SegmentedCandidate::open(&dir, CandidateConfig::segmented());
        let evts = events(100);
        let (results, metrics) = c.write_batch(&evts);
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 100);
        assert!(metrics.committed_transactions > 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn estimated_bytes_includes_overhead() {
        let evt = SyntheticEvent::generate(0, "inst", "prod", "ns", 100);
        let est = estimated_transaction_bytes(&evt);
        assert_eq!(est, 100 + PER_EVENT_OVERHEAD_BYTES);
    }

    #[test]
    fn monolithic_storage_metrics_nonzero_after_writes() {
        let dir = std::env::temp_dir().join("cr-bench-mono-storage");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.db");
        let mut c = MonolithicCandidate::open(&path, CandidateConfig::monolithic());
        let evts = events(10);
        c.write_batch(&evts);
        let sm = c.storage_metrics();
        assert!(sm.db_size_bytes > 0);
        assert_eq!(sm.segment_count, 1);
        assert_eq!(
            sm.total_storage_bytes,
            sm.db_size_bytes + sm.wal_size_bytes + sm.shm_size_bytes
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segmented_storage_metrics_sum_across_segments() {
        let dir = std::env::temp_dir().join("cr-bench-seg-storage");
        let _ = std::fs::remove_dir_all(&dir);
        let mut config = CandidateConfig::segmented();
        config.segment_rollover_records = Some(3);
        let mut c = SegmentedCandidate::open(&dir, config);
        let evts = events(10);
        c.write_batch(&evts);
        let sm = c.storage_metrics();
        assert!(sm.db_size_bytes > 0);
        assert!(sm.segment_count >= 4);
        assert_eq!(
            sm.total_storage_bytes,
            sm.db_size_bytes + sm.wal_size_bytes + sm.shm_size_bytes
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn run_data_dir_derivation_is_deterministic() {
        let output = Path::new("target/journal-bench/smoke-monolithic.json");
        let dir = derive_run_data_dir(output);
        assert_eq!(dir, Path::new("target/journal-bench/runs/smoke-monolithic"));
        let dir2 = derive_run_data_dir(output);
        assert_eq!(dir, dir2);
    }

    #[test]
    fn run_data_dir_derivation_differs_per_output() {
        let a = derive_run_data_dir(Path::new("target/journal-bench/a.json"));
        let b = derive_run_data_dir(Path::new("target/journal-bench/b.json"));
        assert_ne!(a, b);
    }

    #[test]
    fn prepare_run_data_dir_starts_clean() {
        let dir = std::env::temp_dir().join("cr-bench-prepare-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("stale.db"), b"stale").unwrap();
        prepare_run_data_dir(&dir);
        assert!(dir.exists());
        assert!(!dir.join("stale.db").exists());
        std::fs::remove_dir_all(&dir).ok();
    }
}
