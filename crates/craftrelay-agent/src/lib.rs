#![forbid(unsafe_code)]

pub mod barrier_capture;

/// Local durable journal is available via the `craftrelay-journal` crate.
/// Kafka publisher, ACK consumer, and replicated durability remain out of scope.
pub const LOCAL_JOURNAL_AVAILABLE: bool = true;
