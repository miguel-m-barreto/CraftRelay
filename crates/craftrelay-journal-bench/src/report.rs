#![forbid(unsafe_code)]

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkReport {
    pub candidate: String,
    pub candidate_label: String,
    pub unsafe_mode: bool,
    pub os: String,
    pub arch: String,
    pub config: BenchmarkConfig,
    pub submitted_records: u64,
    pub accepted_records: u64,
    pub written_records: u64,
    pub rejected_capacity: u64,
    pub failed_writes: u64,
    pub payload_size_bytes: usize,
    pub metadata_count: usize,
    pub group_commit: GroupCommitConfig,
    pub committed_transactions: u64,
    pub failed_transactions: u64,
    pub elapsed_ms: u64,
    pub throughput_records_per_sec: f64,
    pub avg_batch_size: f64,
    pub max_batch_size: u32,
    pub db_size_bytes: u64,
    pub wal_size_bytes: u64,
    pub shm_size_bytes: u64,
    pub total_storage_bytes: u64,
    pub segment_count: usize,
    pub errors: Vec<String>,
    pub benchmark_only: bool,
    pub no_durable_receipt: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkConfig {
    pub journal_mode: String,
    pub synchronous: String,
    pub busy_timeout_ms: u32,
    pub wal_autocheckpoint: u32,
    pub page_size: Option<u32>,
    pub segment_rollover_records: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupCommitConfig {
    pub max_records_per_tx: u32,
    pub max_bytes_per_tx: u64,
}

impl BenchmarkReport {
    pub fn write_to_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(path, json)
    }

    pub fn has_required_fields(&self) -> bool {
        !self.candidate.is_empty()
            && !self.os.is_empty()
            && self.submitted_records > 0
            && self.benchmark_only
            && self.no_durable_receipt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_json_includes_required_fields() {
        let report = test_report();
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"candidate\""));
        assert!(json.contains("\"os\""));
        assert!(json.contains("\"submitted_records\""));
        assert!(json.contains("\"written_records\""));
        assert!(json.contains("\"rejected_capacity\""));
        assert!(json.contains("\"failed_writes\""));
        assert!(json.contains("\"elapsed_ms\""));
        assert!(json.contains("\"throughput_records_per_sec\""));
        assert!(json.contains("\"db_size_bytes\""));
        assert!(json.contains("\"segment_count\""));
        assert!(json.contains("\"benchmark_only\":true"));
        assert!(json.contains("\"no_durable_receipt\":true"));
        assert!(json.contains("\"unsafe_mode\":false"));
        assert!(json.contains("\"group_commit\""));
        assert!(json.contains("\"committed_transactions\""));
        assert!(json.contains("\"failed_transactions\""));
        assert!(json.contains("\"max_bytes_per_tx\""));
        assert!(json.contains("\"wal_size_bytes\""));
        assert!(json.contains("\"shm_size_bytes\""));
        assert!(json.contains("\"total_storage_bytes\""));
        assert!(report.has_required_fields());
    }

    #[test]
    fn report_uses_real_transaction_count() {
        let mut report = test_report();
        report.committed_transactions = 7;
        report.failed_transactions = 2;
        let json = serde_json::to_string(&report).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["committed_transactions"], 7);
        assert_eq!(parsed["failed_transactions"], 2);
    }

    #[test]
    fn report_uses_real_db_size_and_segment_count() {
        let mut report = test_report();
        report.db_size_bytes = 131_072;
        report.wal_size_bytes = 4_096;
        report.shm_size_bytes = 32_768;
        report.total_storage_bytes = 131_072 + 4_096 + 32_768;
        report.segment_count = 4;
        let json = serde_json::to_string(&report).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["db_size_bytes"], 131_072);
        assert_eq!(parsed["wal_size_bytes"], 4_096);
        assert_eq!(parsed["shm_size_bytes"], 32_768);
        assert_eq!(parsed["total_storage_bytes"], 131_072 + 4_096 + 32_768);
        assert_eq!(parsed["segment_count"], 4);
    }

    #[test]
    fn report_reflects_non_default_config() {
        let mut report = test_report();
        report.config.synchronous = "OFF".into();
        report.unsafe_mode = true;
        report.candidate_label = "monolithic [UNSAFE-BENCHMARK-ONLY]".into();
        let json = serde_json::to_string(&report).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["config"]["synchronous"], "OFF");
        assert_eq!(parsed["unsafe_mode"], true);
        assert!(
            parsed["candidate_label"]
                .as_str()
                .unwrap()
                .contains("UNSAFE")
        );
    }

    #[test]
    fn report_writes_to_path() {
        let report = test_report();
        let dir = std::env::temp_dir().join("craftrelay-bench-test-rpt");
        let path = dir.join("test-report.json");
        report.write_to_file(&path).unwrap();
        assert!(path.exists());
        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        assert_eq!(parsed["candidate"], "test");
        std::fs::remove_dir_all(&dir).ok();
    }

    fn test_report() -> BenchmarkReport {
        BenchmarkReport {
            candidate: "test".into(),
            candidate_label: "test".into(),
            unsafe_mode: false,
            os: "test-os".into(),
            arch: "x86_64".into(),
            config: BenchmarkConfig {
                journal_mode: "WAL".into(),
                synchronous: "FULL".into(),
                busy_timeout_ms: 5_000,
                wal_autocheckpoint: 1_000,
                page_size: None,
                segment_rollover_records: None,
            },
            submitted_records: 100,
            accepted_records: 100,
            written_records: 100,
            rejected_capacity: 0,
            failed_writes: 0,
            payload_size_bytes: 128,
            metadata_count: 1,
            group_commit: GroupCommitConfig {
                max_records_per_tx: 64,
                max_bytes_per_tx: 1_048_576,
            },
            committed_transactions: 2,
            failed_transactions: 0,
            elapsed_ms: 50,
            throughput_records_per_sec: 2000.0,
            avg_batch_size: 50.0,
            max_batch_size: 64,
            db_size_bytes: 65536,
            wal_size_bytes: 0,
            shm_size_bytes: 0,
            total_storage_bytes: 65536,
            segment_count: 1,
            errors: vec![],
            benchmark_only: true,
            no_durable_receipt: true,
        }
    }
}
