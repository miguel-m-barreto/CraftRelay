# Version sources

Checked 2026-06-20. Versions are intentionally pinned; selection is not a claim that each component is newest. Official primary sources are linked. Java 21 is the enforced compilation/runtime line.

| Component | Selected | Official source | Compatibility / rationale |
|---|---:|---|---|
| Rust | 1.88.0 | https://blog.rust-lang.org/2025/06/26/Rust-1.88.0/ | Edition 2024 capable; pinned by `rust-toolchain.toml`. |
| Rust audit tooling image | `craftrelay/rust-audit:1.88.0-bookworm` (local), base `rust:1.88.0-bookworm` | https://hub.docker.com/_/rust | Explicit Docker audit mode only. Adds `cmake`, `g++`, `make`, and `pkg-config` to compile `rdkafka-sys`/vendored `librdkafka` for the compile-tested BarrierCaptureAdapter spike. |
| Tokio | 1.45.1 | https://github.com/tokio-rs/tokio/releases/tag/tokio-1.45.1 | Future runtime source; not needed in Sprint 0. |
| tonic / prost | 0.13.1 / 0.13.5 | https://github.com/hyperium/tonic/releases/tag/v0.13.1 | Future generated transport; not needed in Sprint 0. |
| SQLite / rusqlite | 3.49.2 / 0.35.0 | https://sqlite.org/releaselog/3_49_2.html | Sprint 3 benchmark spike only; `bundled` feature compiles SQLite from source. Production journal backend remains EXPERIMENTAL pending Sprint 4 decision. |
| rdkafka / librdkafka | 0.38.0 / 2.12.1 | https://github.com/fede1024/rust-rdkafka/releases/tag/v0.38.0 | Only compile-tested read-committed barrier spike; no publisher. |
| curl headers (librdkafka build input) | 8.14.1 | https://curl.se/changes.html#8_14_1 | Exact CI source archive; curl runtime behavior is not used. |
| Apache Kafka | 3.9.1 | https://kafka.apache.org/downloads | KRaft Compose profile, RF=5/minISR=5 contract. |
| PostgreSQL | 17.5 | https://www.postgresql.org/docs/release/17.5/ | P0 profile contract; no application connection. |
| tokio-postgres / deadpool-postgres | 0.7.13 / 0.14.1 | https://docs.rs/tokio-postgres/0.7.13 | Future; absent from dependencies. |
| Java | 21 | https://docs.oracle.com/en/java/javase/21/ | Enforced `[21,22)` and `--release 21`. |
| Paper contract target | 1.21.4 | https://docs.papermc.io/paper/dev/project-setup/ | Minimum design target and maximum fixture target; no Paper runtime is compiled or tested in Sprint 0. |
| Maven / Wrapper | 3.9.9 / 3.3.2 | https://maven.apache.org/docs/3.9.9/release-notes.html | Wrapper download pinned. |
| gRPC Java / Protobuf | 1.72.0 / 4.30.2 | https://github.com/grpc/grpc-java/releases/tag/v1.72.0 | Future binding line; absent from Sprint 0 Java dependencies. |
| Buf CLI | 1.50.1 | https://github.com/bufbuild/buf/releases/tag/v1.50.1 | CI action and CLI version pinned. |
| OpenTelemetry | 1.49.0 | https://github.com/open-telemetry/opentelemetry-java/releases/tag/v1.49.0 | Future observability; no runtime dependency. |
| Prometheus | 3.4.1 | https://github.com/prometheus/prometheus/releases/tag/v3.4.1 | Compose image pinned. |
| Grafana | 12.0.1 | https://github.com/grafana/grafana/releases/tag/v12.0.1 | Optional Compose profile. |
| Redis | 8.0.2 | https://github.com/redis/redis/releases/tag/8.0.2 | Optional display-only live backend, never durability. |
| cargo-audit | 0.21.2 | https://github.com/rustsec/rustsec/releases/tag/cargo-audit%2Fv0.21.2 | Audit tool; never silently installed. |
| cargo-deny | 0.18.3 | https://github.com/EmbarkStudios/cargo-deny/releases/tag/0.18.3 | Policy tool; never silently installed. |

Container tags include immutable version components and never use `latest`. Operators must pin digests before production.
