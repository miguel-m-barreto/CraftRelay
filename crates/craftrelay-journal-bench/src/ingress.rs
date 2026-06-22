#![forbid(unsafe_code)]

use crate::candidate::{CandidateRunner, StorageMetrics, WriteBatchMetrics};
use sha2::{Digest, Sha256};
use std::sync::mpsc;
use std::time::Instant;

pub const MAX_PAYLOAD_BYTES: usize = 1_048_576;
pub const MAX_METADATA_ENTRIES: usize = 32;
pub const DEFAULT_CHANNEL_CAPACITY: usize = 4_096;
const MAX_BOUNDED_ERRORS: usize = 64;

#[derive(Debug, Clone)]
pub struct SyntheticEvent {
    pub event_id: String,
    pub installation_id: String,
    pub producer_id: String,
    pub namespace: String,
    pub logical_stream: String,
    pub schema_version: i32,
    pub payload: Vec<u8>,
    pub payload_digest: [u8; 32],
    pub request_fingerprint: [u8; 32],
    pub payload_ref_id: String,
    pub metadata_count: usize,
}

impl SyntheticEvent {
    pub fn generate(
        index: u64,
        installation_id: &str,
        producer_id: &str,
        namespace: &str,
        payload_bytes: usize,
    ) -> Self {
        let event_id = format!(
            "01890f3e-{:04x}-7cc2-98c8-{:012x}",
            (index >> 32) & 0xFFFF,
            index & 0xFFFF_FFFF_FFFF
        );
        let logical_stream = format!("{namespace}.stream-{}", index % 8);
        let payload = deterministic_payload(index, payload_bytes);
        let payload_digest: [u8; 32] = Sha256::digest(&payload).into();
        let fingerprint: [u8; 32] =
            Sha256::digest(format!("{event_id}\0{namespace}\0{logical_stream}").as_bytes()).into();
        let payload_ref_id = format!("{installation_id}/{event_id}");
        Self {
            event_id,
            installation_id: installation_id.into(),
            producer_id: producer_id.into(),
            namespace: namespace.into(),
            logical_stream,
            schema_version: 1,
            payload,
            payload_digest,
            request_fingerprint: fingerprint,
            payload_ref_id,
            metadata_count: 1,
        }
    }
}

fn deterministic_payload(seed: u64, size: usize) -> Vec<u8> {
    let clamped = size.min(MAX_PAYLOAD_BYTES);
    let mut buf = vec![0u8; clamped];
    let mut state = seed;
    for byte in &mut buf {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *byte = (state >> 33) as u8;
    }
    buf
}

#[derive(Debug, Clone)]
pub struct IngressConfig {
    pub channel_capacity: usize,
    pub total_records: u64,
    pub installation_id: String,
    pub producer_id: String,
    pub namespace: String,
    pub payload_bytes: usize,
}

impl IngressConfig {
    pub fn new(total_records: u64, payload_bytes: usize) -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            total_records,
            installation_id: "bench-installation".into(),
            producer_id: "bench-producer".into(),
            namespace: "bench".into(),
            payload_bytes: payload_bytes.min(MAX_PAYLOAD_BYTES),
        }
    }
}

/// Full pipeline result from producer→channel→writer.
#[derive(Debug)]
pub struct PipelineResult {
    pub submitted: u64,
    pub accepted: u64,
    pub rejected_capacity: u64,
    pub write_metrics: WriteBatchMetrics,
    pub errors: Vec<String>,
    pub elapsed: std::time::Duration,
    pub storage: StorageMetrics,
}

/// Runs the complete bounded pipeline:
///   producer thread → sync_channel → writer thread → SQLite
pub fn run_pipeline(config: &IngressConfig, mut runner: CandidateRunner) -> PipelineResult {
    let (tx, rx) = mpsc::sync_channel::<SyntheticEvent>(config.channel_capacity);

    let total = config.total_records;
    let inst = config.installation_id.clone();
    let prod = config.producer_id.clone();
    let ns = config.namespace.clone();
    let payload_bytes = config.payload_bytes;

    let producer_handle = std::thread::spawn(move || {
        let mut submitted = 0u64;
        let mut rejected = 0u64;
        for i in 0..total {
            let event = SyntheticEvent::generate(i, &inst, &prod, &ns, payload_bytes);
            submitted += 1;
            if tx.try_send(event).is_err() {
                rejected += 1;
            }
        }
        (submitted, rejected)
    });

    let start = Instant::now();
    let mut all_metrics = WriteBatchMetrics::default();
    let mut errors = Vec::new();
    let max_records = runner.config().max_records_per_tx as usize;
    let mut batch = Vec::with_capacity(max_records);

    while let Ok(event) = rx.recv() {
        batch.push(event);
        while batch.len() < max_records {
            match rx.try_recv() {
                Ok(e) => batch.push(e),
                Err(_) => break,
            }
        }
        let run = runner.run(&batch);
        merge_metrics(&mut all_metrics, &run.metrics);
        collect_errors(&run.results, &mut errors);
        batch.clear();
    }
    if !batch.is_empty() {
        let run = runner.run(&batch);
        merge_metrics(&mut all_metrics, &run.metrics);
        collect_errors(&run.results, &mut errors);
    }
    let elapsed = start.elapsed();
    let storage = runner.storage_metrics();

    let (submitted, rejected) = producer_handle.join().expect("producer");

    PipelineResult {
        submitted,
        accepted: submitted - rejected,
        rejected_capacity: rejected,
        write_metrics: all_metrics,
        errors,
        elapsed,
        storage,
    }
}

fn merge_metrics(acc: &mut WriteBatchMetrics, batch: &WriteBatchMetrics) {
    acc.committed_transactions += batch.committed_transactions;
    acc.failed_transactions += batch.failed_transactions;
    acc.written_records += batch.written_records;
    acc.failed_records += batch.failed_records;
    acc.total_batch_records += batch.total_batch_records;
    if batch.max_observed_batch_size > acc.max_observed_batch_size {
        acc.max_observed_batch_size = batch.max_observed_batch_size;
    }
}

fn collect_errors(
    results: &[Result<crate::candidate::WriteResult, String>],
    errors: &mut Vec<String>,
) {
    for r in results {
        if let Err(e) = r {
            if errors.len() < MAX_BOUNDED_ERRORS {
                errors.push(e.clone());
            }
        }
    }
}

/// Simple non-threaded ingress for unit tests that just need events.
pub fn generate_events(config: &IngressConfig) -> Vec<SyntheticEvent> {
    (0..config.total_records)
        .map(|i| {
            SyntheticEvent::generate(
                i,
                &config.installation_id,
                &config.producer_id,
                &config.namespace,
                config.payload_bytes,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::{CandidateConfig, CandidateRunner, MonolithicCandidate};

    #[test]
    fn synthetic_event_fields_are_deterministic() {
        let a = SyntheticEvent::generate(42, "inst-a", "prod-a", "economy", 128);
        let b = SyntheticEvent::generate(42, "inst-a", "prod-a", "economy", 128);
        assert_eq!(a.event_id, b.event_id);
        assert_eq!(a.payload, b.payload);
        assert_eq!(a.payload_digest, b.payload_digest);
        assert_eq!(a.request_fingerprint, b.request_fingerprint);
        assert_eq!(a.payload.len(), 128);
    }

    #[test]
    fn payload_clamped_to_max() {
        let event = SyntheticEvent::generate(0, "inst", "prod", "ns", MAX_PAYLOAD_BYTES + 100);
        assert_eq!(event.payload.len(), MAX_PAYLOAD_BYTES);
    }

    #[test]
    fn bounded_writer_thread_pipeline() {
        let config = IngressConfig::new(50, 64);
        let candidate = MonolithicCandidate::open_in_memory(CandidateConfig::monolithic());
        let runner = CandidateRunner::Monolithic(candidate);
        let result = run_pipeline(&config, runner);
        assert_eq!(result.submitted, 50);
        assert_eq!(result.rejected_capacity, 0);
        assert_eq!(result.write_metrics.written_records, 50);
        assert!(result.write_metrics.committed_transactions > 0);
        assert!(result.errors.is_empty());
        assert_eq!(result.storage.segment_count, 1);
    }

    #[test]
    fn monolithic_pipeline_reports_nonzero_db_size() {
        let dir = std::env::temp_dir().join("cr-bench-mono-dbsize");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("mono.db");
        let config = IngressConfig::new(20, 64);
        let candidate = MonolithicCandidate::open(&path, CandidateConfig::monolithic());
        let runner = CandidateRunner::Monolithic(candidate);
        let result = run_pipeline(&config, runner);
        assert!(result.storage.db_size_bytes > 0);
        assert_eq!(result.storage.segment_count, 1);
        assert_eq!(
            result.storage.total_storage_bytes,
            result.storage.db_size_bytes
                + result.storage.wal_size_bytes
                + result.storage.shm_size_bytes
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn segmented_pipeline_reports_real_segment_count() {
        let dir = std::env::temp_dir().join("cr-bench-seg-count");
        let _ = std::fs::remove_dir_all(&dir);
        let mut seg_config = CandidateConfig::segmented();
        seg_config.segment_rollover_records = Some(3);
        let config = IngressConfig::new(10, 64);
        let seg_dir = dir.join("segments");
        let candidate = crate::candidate::SegmentedCandidate::open(&seg_dir, seg_config);
        let runner = CandidateRunner::Segmented(candidate);
        let result = run_pipeline(&config, runner);
        assert_eq!(result.write_metrics.written_records, 10);
        assert!(result.storage.segment_count >= 4);
        assert!(result.storage.db_size_bytes > 0);
        assert_eq!(
            result.storage.total_storage_bytes,
            result.storage.db_size_bytes
                + result.storage.wal_size_bytes
                + result.storage.shm_size_bytes
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn bounded_capacity_rejection_with_pipeline() {
        let config = IngressConfig {
            channel_capacity: 2,
            total_records: 20,
            installation_id: "inst".into(),
            producer_id: "prod".into(),
            namespace: "ns".into(),
            payload_bytes: 8,
        };
        let candidate = MonolithicCandidate::open_in_memory(CandidateConfig::monolithic());
        let runner = CandidateRunner::Monolithic(candidate);
        let result = run_pipeline(&config, runner);
        assert_eq!(result.submitted, 20);
        assert!(result.rejected_capacity > 0);
        assert_eq!(result.accepted, result.submitted - result.rejected_capacity);
        assert_eq!(result.write_metrics.written_records, result.accepted);
    }

    #[test]
    fn bounded_ingress_accepts_all_when_capacity_sufficient() {
        let config = IngressConfig::new(50, 64);
        let events = generate_events(&config);
        assert_eq!(events.len(), 50);
    }
}
