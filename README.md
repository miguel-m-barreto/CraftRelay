# CraftRelay

CraftRelay is a planned external durability, projection, and typed-query platform for trusted Paper plugins. This repository is at **Sprint 0: contracts and foundation only**. It provides compile-tested models and skeletons; it does not provide a journal, Kafka publisher, projector, Query Service, ACK consumer, Live Projector, Redis query path, or DurableReceipt.

Authority order: [MASTER_PLAN.md](MASTER_PLAN.md), [PluginAdapter.md](PluginAdapter.md), then [PluginUsage.md](PluginUsage.md).

## Validation

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --locked
cargo build --workspace --all-features --locked
.\java\mvnw.cmd -B -ntp clean verify
buf lint
docker compose -f .\deployment\compose\compose.yml config --services
docker compose -f .\deployment\compose\compose.yml config --profiles
```

`buf breaking` is `NOT_APPLICABLE` until a released protocol baseline exists. Do not run the audit generator unless explicitly performing an audit.

