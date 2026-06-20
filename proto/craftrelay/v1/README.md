# CraftRelay v1 protocol skeleton

This directory defines contract shapes only. It contains no transport service and no persistence, publishing, projection, query, ACK-consumption, Redis, or receipt implementation. Repeated fields are bounded by policy even where Protobuf cannot encode the bound.

`StoredEventEnvelope` intentionally has neither payload bytes nor mutable lifecycle fields. `required_next_offset` and `next_offset_to_resolve` are exclusive offsets. Display live modes are non-authoritative.

