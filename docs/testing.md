# Testing

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
