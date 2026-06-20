#![forbid(unsafe_code)]

pub use craftrelay_domain::*;

pub const PROTOCOL_MAJOR: i32 = 1;
pub const MAX_ATTEMPT_SUMMARIES: usize = 16;
pub const MAX_BARRIER_PARTITIONS: usize = 1024;
pub const MAX_CONSISTENCY_TOKENS: usize = 32;
pub const MAX_WATCH_BUFFER_EVENTS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryDefinition {
    pub handle_id: String,
    pub parameter_schema: String,
    pub result_schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionAckOutboxRecord {
    pub event_id: String,
    pub projector_id: String,
    pub projection_revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckConsumptionContract {
    pub persist_before_offset_commit: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Readiness {
    NotReady,
    Ready,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionContext {
    GlobalServer,
    Entity,
    Region,
    AsyncOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventContractHandle(pub String);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryContractHandle(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrationManifest {
    pub integration_id: String,
    pub integration_version: i32,
    pub paper_plugin_id: String,
    pub max_pending_publishes: i32,
    pub max_pending_queries: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalProducerRegistration {
    pub installation_id: String,
    pub integration_id: String,
    pub producer_instance_id: String,
    pub next_producer_operation_sequence: i64,
}

pub trait BarrierCaptureAdapter {
    type Error;
    fn capture_read_committed_next_offsets(
        &self,
        topic: &str,
        partitions: &[i32],
        timeout_millis: u64,
    ) -> Result<std::collections::BTreeMap<i32, i64>, Self::Error>;
}
