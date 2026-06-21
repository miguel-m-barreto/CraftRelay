# CraftRelay v1 protocol skeleton

This directory defines contract shapes only. Sprint 1 provides compile-tested Java models and fake/in-memory client boundaries, but no transport service, durable persistence, Kafka publishing, projection, real query process, ACK consumption, Redis, or receipt implementation. Repeated fields are bounded by policy even where Protobuf cannot encode the bound.

`StoredEventEnvelope` intentionally has neither payload bytes nor mutable lifecycle fields. `required_next_offset` and `next_offset_to_resolve` are exclusive offsets. Display live modes are non-authoritative.
