#![forbid(unsafe_code)]

pub mod barrier_capture;

/// Sprint 0 marker. No journal, publisher, ACK consumer, or durable receipt exists.
pub const SPRINT_ZERO_NON_FUNCTIONAL: bool = true;
