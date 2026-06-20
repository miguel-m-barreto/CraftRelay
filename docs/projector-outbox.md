# Projector outbox and ACK contract

Projector sequence validation, event dedup, domain mutation, immutable archive, checkpoint movement, and required ACK outbox insertion occur in one PostgreSQL transaction. The ACK publisher transports committed outbox rows. Agent ACK consumption deduplicates and persists its local ACK/state revision before committing the Kafka ACK offset. Required ACK without outbox and offset-first consumption are forbidden. Sprint 0 has no projector or ACK consumer runtime.

