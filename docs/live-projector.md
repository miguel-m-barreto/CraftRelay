# Optional Live Projector

The future Live Projector consumes Kafka read-committed data and writes rebuildable Redis display read models. It is downstream only: Paper, Bridge, plugins, and Agent never dual-write Redis. Redis is non-authoritative and outside durability.

`PROJECTED_PLUS_LIVE` and `LIVE_ONLY` are display-only. They cannot satisfy strict/token reads or authorize money, ownership, premium, permission, unique-item, security, or irreversible operations. Live source metadata and `LiveProjectionCheckpoint` are separate from PostgreSQL projector checkpoints. Rebuilding/unavailable Redis yields projected fallback where explicitly allowed or explicit unavailable; it does not affect Agent, Kafka delivery, PostgreSQL projection, strict queries, or critical operations. Runtime sharding/rebuild algorithms remain EXPERIMENTAL.

