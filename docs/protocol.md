# Protocol

The v1 Protobuf skeleton is under `proto/craftrelay/v1`. Positive signed fields use `int32`/`int64`; zero is valid only for Kafka offsets. Repeated collections are policy-bounded. Core queries use typed `oneof` parameters or registered typed schema handles—never SQL or arbitrary schema names. Mutable fields and payload bytes are deliberately absent from `StoredEventEnvelope`.

