#![forbid(unsafe_code)]

use craftrelay_journal_bench::candidate::{
    CandidateConfig, CandidateRunner, MonolithicCandidate, SegmentedCandidate, derive_run_data_dir,
    prepare_run_data_dir,
};
use craftrelay_journal_bench::ingress::{self, IngressConfig};
use craftrelay_journal_bench::report::{BenchmarkConfig, BenchmarkReport, GroupCommitConfig};
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let candidate_name = arg_value(&args, "--candidate").unwrap_or_else(|| "monolithic".into());
    let records: u64 = arg_value(&args, "--records")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);
    let payload_bytes: usize = arg_value(&args, "--payload-bytes")
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);
    let output = arg_value(&args, "--output")
        .unwrap_or_else(|| format!("target/journal-bench/{candidate_name}.json"));
    let max_records_per_tx: u32 = arg_value(&args, "--max-records-per-tx")
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
    let max_bytes_per_tx: u64 = arg_value(&args, "--max-bytes-per-tx")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_048_576);
    let synchronous = arg_value(&args, "--synchronous").unwrap_or_else(|| "FULL".into());
    let unsafe_mode = args.contains(&"--unsafe".to_string());
    let segment_rollover: Option<u32> =
        arg_value(&args, "--segment-rollover").and_then(|s| s.parse().ok());

    let output_path = PathBuf::from(&output);
    let db_dir = derive_run_data_dir(&output_path);
    prepare_run_data_dir(&db_dir);

    let mut config = match candidate_name.as_str() {
        "monolithic" => CandidateConfig::monolithic(),
        "segmented" => {
            let mut c = CandidateConfig::segmented();
            if let Some(r) = segment_rollover {
                c.segment_rollover_records = Some(r);
            }
            c
        }
        other => {
            eprintln!("unknown candidate: {other}");
            std::process::exit(1);
        }
    };
    config.max_records_per_tx = max_records_per_tx;
    config.max_bytes_per_tx = max_bytes_per_tx;
    config.synchronous = synchronous;
    config.unsafe_mode = unsafe_mode;

    eprintln!(
        "Sprint 3 journal benchmark spike: {} \
         ({} records, {} B payload)",
        config.label(),
        records,
        payload_bytes
    );
    if config.unsafe_mode {
        eprintln!(
            "WARNING: unsafe benchmark mode — \
             NOT suitable for production durability"
        );
    }

    let cfg = config.clone();
    let runner: CandidateRunner = match candidate_name.as_str() {
        "monolithic" => {
            let path = db_dir.join("monolithic.db");
            CandidateRunner::Monolithic(MonolithicCandidate::open(&path, config))
        }
        "segmented" => {
            let seg_dir = db_dir.join("segments");
            CandidateRunner::Segmented(SegmentedCandidate::open(&seg_dir, config))
        }
        _ => unreachable!(),
    };

    let ingress_config = IngressConfig::new(records, payload_bytes);
    let pipeline = ingress::run_pipeline(&ingress_config, runner);

    let elapsed_ms = pipeline.elapsed.as_millis() as u64;
    let written = pipeline.write_metrics.written_records;
    let throughput = if elapsed_ms > 0 {
        written as f64 / (elapsed_ms as f64 / 1_000.0)
    } else {
        0.0
    };

    let report = BenchmarkReport {
        candidate: cfg.name.clone(),
        candidate_label: cfg.label(),
        unsafe_mode: cfg.unsafe_mode,
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        config: BenchmarkConfig {
            journal_mode: cfg.journal_mode.clone(),
            synchronous: cfg.synchronous.clone(),
            busy_timeout_ms: cfg.busy_timeout_ms,
            wal_autocheckpoint: cfg.wal_autocheckpoint,
            page_size: cfg.page_size,
            segment_rollover_records: cfg.segment_rollover_records,
        },
        submitted_records: pipeline.submitted,
        accepted_records: pipeline.accepted,
        written_records: written,
        rejected_capacity: pipeline.rejected_capacity,
        failed_writes: pipeline.write_metrics.failed_records,
        payload_size_bytes: payload_bytes,
        metadata_count: 1,
        group_commit: GroupCommitConfig {
            max_records_per_tx: cfg.max_records_per_tx,
            max_bytes_per_tx: cfg.max_bytes_per_tx,
        },
        committed_transactions: pipeline.write_metrics.committed_transactions,
        failed_transactions: pipeline.write_metrics.failed_transactions,
        elapsed_ms,
        throughput_records_per_sec: throughput,
        avg_batch_size: pipeline.write_metrics.avg_observed_batch_size(),
        max_batch_size: pipeline.write_metrics.max_observed_batch_size,
        db_size_bytes: pipeline.storage.db_size_bytes,
        wal_size_bytes: pipeline.storage.wal_size_bytes,
        shm_size_bytes: pipeline.storage.shm_size_bytes,
        total_storage_bytes: pipeline.storage.total_storage_bytes,
        segment_count: pipeline.storage.segment_count,
        errors: pipeline.errors,
        benchmark_only: true,
        no_durable_receipt: true,
    };

    report.write_to_file(&output_path).expect("write report");

    eprintln!("Written: {written}/{}", pipeline.submitted);
    eprintln!("Rejected (capacity): {}", pipeline.rejected_capacity);
    eprintln!("Failed writes: {}", pipeline.write_metrics.failed_records);
    eprintln!(
        "Transactions: {} committed, {} failed",
        pipeline.write_metrics.committed_transactions, pipeline.write_metrics.failed_transactions
    );
    eprintln!("Elapsed: {elapsed_ms} ms");
    eprintln!("Throughput: {throughput:.0} records/s");
    eprintln!(
        "Storage: {} db + {} wal + {} shm = {} total bytes",
        pipeline.storage.db_size_bytes,
        pipeline.storage.wal_size_bytes,
        pipeline.storage.shm_size_bytes,
        pipeline.storage.total_storage_bytes,
    );
    eprintln!("Segments: {}", pipeline.storage.segment_count);
    eprintln!("Report: {output}");
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1).cloned())
}
