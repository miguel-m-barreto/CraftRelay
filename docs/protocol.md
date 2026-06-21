# Protocol

Sprint 1 adds bounded handshake, envelope-input, publish-status, typed-query freshness, barrier, authenticated token, and detachable watch request/response shapes. Java validates immutable client intent before transport submission. Installation and producer identity remain authenticated session inputs rather than arbitrary request selectors. The in-memory Agent and Query fixtures are explicitly non-durable and non-authoritative.

The v1 Protobuf skeleton is under `proto/craftrelay/v1`. Positive signed fields use `int32`/`int64`; zero is valid only for Kafka offsets. Repeated collections are policy-bounded. Core queries use typed `oneof` parameters or registered typed schema handles—never SQL or arbitrary schema names. Mutable fields and payload bytes are deliberately absent from `StoredEventEnvelope`.
