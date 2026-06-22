# Testing

Sprint 3 adds the `craftrelay-journal-bench` crate with SQLite-based journal benchmark spike tests. This is benchmark-only code — it does not produce DurableReceipts and is not wired into the Paper Bridge. Unit tests cover: monolithic/segmented WAL mode open, atomic transaction writes (envelope + payload + lifecycle in one tx), monotonic journal sequence allocation, per-stream sequence allocation, failed transaction rollback (no partial records), group commit splitting by max-records-per-tx and max-bytes-per-tx, oversized single event written alone, real committed/failed transaction counts, segmented rollover at configured threshold, global journal sequence across segments, unsafe mode labeling, bounded writer thread pipeline (producer thread → sync_channel → writer thread → SQLite), bounded ingress capacity rejection, deterministic synthetic event generation, payload size clamping, storage metrics including WAL/SHM/total, run-data directory isolation and cleanup, report JSON shape with all required fields, and report file output. Smoke benchmarks for both candidates run 100 records each in normal tests. Long runs are not part of audit validation; run them explicitly:

```powershell
cargo run -p craftrelay-journal-bench -- --candidate monolithic --records 100 --payload-bytes 128 --output target/journal-bench/smoke-monolithic.json
cargo run -p craftrelay-journal-bench -- --candidate segmented --records 100 --payload-bytes 128 --output target/journal-bench/smoke-segmented.json
```

Larger benchmarks:

```powershell
cargo run -p craftrelay-journal-bench -- --candidate monolithic --records 10000 --payload-bytes 512 --output target/journal-bench/monolithic.json
cargo run -p craftrelay-journal-bench -- --candidate segmented --records 10000 --payload-bytes 512 --segment-rollover 500 --output target/journal-bench/segmented.json
```

Each run uses an isolated data directory derived from the `--output` path stem under `target/journal-bench/runs/`. The directory is cleaned before each run so repeated benchmarks start fresh without stale DB state. Reports are machine-readable JSON with candidate name, OS, config, submitted/written/rejected/failed counts, real committed/failed transaction counts, throughput, storage metrics (`db_size_bytes`, `wal_size_bytes`, `shm_size_bytes`, `total_storage_bytes`), segment count, and group commit configuration (max records and max bytes per tx). All benchmark output stays under gitignored `target/`.

Sprint 2 adds policy/admission/ownership validation tests across Rust and Java: producer registration (valid, duplicate, disabled, suspended, cross-installation), IntegrationManifest extended validation (duplicate events/queries/namespaces, invalid names, unbounded limits, BEST_EFFORT rejection, self-promotion rejection), credential/ACL evaluation (allow, deny, cross-installation, revoked/expired/unknown credentials, no matching rule), deterministic policy resolution (durability weakening, retention weakening, projection bypass, self-promotion, installation escape, ACL denied, disabled/suspended producer, not locally owned, unknown policy binding, effective reason codes), NODE_LOCAL ownership snapshot validation (valid, duplicate namespace, cross-installation, missing owner), quota/admission control (under-limit, per-producer in-flight/queue/bytes, namespace, global, P0 reserved capacity, lower-priority P0 reservation), Kafka profile validation (P0 RF=5/minISR=5/acks=all, weakened RF, invalid acks), and fake Agent fixture enforcement (unauthorized/disabled/suspended/over-quota/not-owned rejection). Cross-language shared vectors cover all Sprint 2 domains.

Sprint 1 adds Java/Rust contract tests for UUIDv7, positive numeric bounds, metadata canonicalization and duplicate rejection, immutable envelope input, lifecycle revision conflicts, bounded publish tracking, same-event retry, fake/non-durable status lookup, Bridge registration/readiness, typed reference clients, typed queries, exclusive next-offset barriers, authenticated token scope/expiry/MAC validation, and bounded detachable watches. Fake Agent and Query Service fixtures are in-memory boundary tests only; they provide no durability or authoritative freshness.

Sprint 0 validates formatting, Clippy, Rust unit/shared-vector tests, Java reactor tests, Buf lint, Compose parsing, static forbidden-dependency checks, revision conflict, installation scope, exclusive offsets, strict no-downgrade, bounded tracking, and protocol shape.

The Kafka barrier integration spike requires a prepared real Kafka fixture. Create three fixture partitions: empty, blocked by an open transaction, and containing offset gaps. Set `CRAFTRELAY_KAFKA_BOOTSTRAP`, `CRAFTRELAY_EMPTY_TOPIC`, `CRAFTRELAY_EMPTY_PARTITION`, `CRAFTRELAY_EMPTY_EXPECTED_LSO`, corresponding `CRAFTRELAY_OPEN_TX_*`, and corresponding `CRAFTRELAY_GAP_*` variables. Run `cargo test -p craftrelay-agent --test barrier_capture_kafka -- --ignored --nocapture`. The normal workspace test compiles this test but reports it as ignored; that is compile validation, not real-broker proof. The real-fixture test never polls records and separately proves the open-transaction high watermark is greater than LSO.

CraftRelay development and audit validation are Windows-native. WSL is not the expected validation environment for this repository. With the default audit mode, Rust 1.88.0 and Buf 1.50.1 must be installed on Windows and available on `PATH`. Missing tools produce captured error logs, exit code 127, and `validation_status: FAIL`.

An explicit Docker tooling mode is available when the pinned Rust or Buf tools are not installed on Windows:

```powershell
.\scripts\New-AuditBundle.ps1 -UseDockerForTooling
```

Docker mode first builds `deployment/tooling/rust-audit.Dockerfile` from the pinned `rust:1.88.0-bookworm` base with the deterministic local tag `craftrelay/rust-audit:1.88.0-bookworm`. The image adds only `cmake`, `g++`, `make`, and `pkg-config`, which are required to compile the vendored `librdkafka` dependency used by the compile-tested `rdkafka-sys` BarrierCaptureAdapter spike. The image build is a mandatory captured audit command.

Rust checks run in that local tooling image. Buf remains pinned to `bufbuild/buf:1.50.1`. Both containers bind-mount the Windows repository at `/workspace`, and every image build or container command is recorded as `docker` in `summary.json`. Maven Wrapper and Docker Compose validation remain native Windows commands. Docker mode is explicit and never selected automatically; the Windows-native repository workflow remains the default. Any failed native, image-build, or container command makes the audit fail.

Real Kafka LSO fixture execution remains `NEEDS_HUMAN_REVIEW` until the ignored test is run with the prepared broker state and its evidence is reviewed.

`buf breaking` is `NOT_APPLICABLE` because no released v1 baseline exists.
