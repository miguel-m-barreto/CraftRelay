#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub type Checksum = [u8; 32];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEventEnvelope {
    pub installation_id: String,
    pub event_id: String,
    pub producer_id: String,
    pub logical_stream: String,
    pub stream_sequence: i64,
    pub schema_version: i32,
    pub payload_digest: Checksum,
    pub request_fingerprint: Checksum,
    pub envelope_checksum: Checksum,
    pub journal_sequence: i64,
    pub payload_ref_id: String,
    pub payload_length: i64,
    pub payload_encoding: String,
    pub routing_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPayloadBlob {
    pub event_id: String,
    pub payload: Vec<u8>,
    pub payload_digest: Checksum,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionedState<T> {
    pub revision: i64,
    pub snapshot_checksum: Checksum,
    pub value: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadRetentionStatus {
    Present,
    Eligible,
    Removed,
    IntegrityBlocked,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    LocalAccepted,
    DeliveryPending,
    Replicated,
    DeliveryBlocked,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionStatus {
    NotRequired,
    Pending,
    Acknowledged,
    ProjectionBlocked,
}

pub type PayloadRetentionState = RevisionedState<PayloadRetentionStatus>;
pub type EventDeliveryState = RevisionedState<DeliveryStatus>;
pub type ProjectionTrackingState = RevisionedState<ProjectionStatus>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptSummary {
    pub attempt_number: i32,
    pub outcome_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishLifecycleSnapshot {
    pub event_id: String,
    pub revision: i64,
    pub snapshot_checksum: Checksum,
    pub delivery: DeliveryStatus,
    pub projection: ProjectionStatus,
    pub attempts: Vec<AttemptSummary>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestedFlushCriteria {
    pub max_records: i32,
    pub max_bytes: i64,
    pub max_age_millis: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveFlushCriteria {
    pub max_records: i32,
    pub max_bytes: i64,
    pub max_age_millis: i64,
}

impl EffectiveFlushCriteria {
    pub fn should_flush(self, records: i32, bytes: i64, age_millis: i64, draining: bool) -> bool {
        draining
            || records >= self.max_records
            || bytes >= self.max_bytes
            || age_millis >= self.max_age_millis
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecision {
    Accepted,
    Clamped(EffectiveFlushCriteria),
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushPolicyBounds {
    pub minimum: EffectiveFlushCriteria,
    pub maximum: EffectiveFlushCriteria,
}

pub fn resolve_flush_policy(
    requested: RequestedFlushCriteria,
    bounds: FlushPolicyBounds,
    reject_if_clamped: bool,
) -> PolicyDecision {
    if requested.max_records <= 0 || requested.max_bytes <= 0 || requested.max_age_millis <= 0 {
        return PolicyDecision::Rejected;
    }
    let effective = EffectiveFlushCriteria {
        max_records: requested
            .max_records
            .clamp(bounds.minimum.max_records, bounds.maximum.max_records),
        max_bytes: requested
            .max_bytes
            .clamp(bounds.minimum.max_bytes, bounds.maximum.max_bytes),
        max_age_millis: requested
            .max_age_millis
            .clamp(bounds.minimum.max_age_millis, bounds.maximum.max_age_millis),
    };
    let unchanged = effective.max_records == requested.max_records
        && effective.max_bytes == requested.max_bytes
        && effective.max_age_millis == requested.max_age_millis;
    if unchanged {
        PolicyDecision::Accepted
    } else if reject_if_clamped {
        PolicyDecision::Rejected
    } else {
        PolicyDecision::Clamped(effective)
    }
}

pub fn semantic_coalescing_allowed(event_class: &str) -> bool {
    !matches!(
        event_class,
        "P0_LEDGER" | "P0_OWNERSHIP" | "P0_PREMIUM" | "P0_UNIQUE_ITEM" | "P0_SECURITY"
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionBarrier {
    pub topology_version: i64,
    pub routing_version: i64,
    pub required_next_offsets: BTreeMap<i32, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionCheckpoint {
    pub projector_id: String,
    pub partition: i32,
    pub next_offset_to_resolve: i64,
    pub blocked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveProjectionCheckpoint {
    pub live_source_id: String,
    pub partition: i32,
    pub next_offset_to_resolve: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryConsistency {
    StrictLatestCommitted,
    AtLeastToken,
    AllowStale,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisplayQueryMode {
    ProjectedPlusLive,
    LiveOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionConsistencyToken {
    pub installation_id: String,
    pub projector_id: String,
    pub projection_name: String,
    pub expires_at_unix_millis: i64,
    pub required_next_offsets: BTreeMap<i32, i64>,
    pub checksum: Checksum,
    pub mac: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevisionDecision {
    Inserted,
    Duplicate,
    IntegrityConflict,
    Stale,
}

pub fn compare_revision(
    current_revision: i64,
    current_checksum: Checksum,
    incoming_revision: i64,
    incoming_checksum: Checksum,
) -> RevisionDecision {
    match incoming_revision.cmp(&current_revision) {
        std::cmp::Ordering::Greater => RevisionDecision::Inserted,
        std::cmp::Ordering::Less => RevisionDecision::Stale,
        std::cmp::Ordering::Equal if incoming_checksum == current_checksum => {
            RevisionDecision::Duplicate
        }
        std::cmp::Ordering::Equal => RevisionDecision::IntegrityConflict,
    }
}

pub fn sha256(bytes: &[u8]) -> Checksum {
    Sha256::digest(bytes).into()
}

pub fn scoped_name(installation_id: &str, name: &str) -> String {
    format!("{installation_id}\0{name}")
}

pub fn is_canonical_uuid_v7(value: &str) -> bool {
    let bytes = value.as_bytes();
    value.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        && bytes.iter().enumerate().all(|(index, byte)| {
            [8, 13, 18, 23].contains(&index)
                || byte.is_ascii_digit()
                || (b'a'..=b'f').contains(byte)
        })
        && bytes.get(14) == Some(&b'7')
        && bytes
            .get(19)
            .is_some_and(|b| matches!(b, b'8' | b'9' | b'a' | b'b'))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn flush_is_or_based() {
        let c = EffectiveFlushCriteria {
            max_records: 10,
            max_bytes: 100,
            max_age_millis: 50,
        };
        assert!(c.should_flush(10, 0, 0, false));
        assert!(c.should_flush(0, 100, 0, false));
        assert!(c.should_flush(0, 0, 50, false));
        assert!(c.should_flush(0, 0, 0, true));
    }
    #[test]
    fn requested_flush_is_clamped_or_rejected_by_policy() {
        let requested = RequestedFlushCriteria {
            max_records: 1,
            max_bytes: 10,
            max_age_millis: 1,
        };
        let bounds = FlushPolicyBounds {
            minimum: EffectiveFlushCriteria {
                max_records: 4,
                max_bytes: 100,
                max_age_millis: 5,
            },
            maximum: EffectiveFlushCriteria {
                max_records: 64,
                max_bytes: 65_536,
                max_age_millis: 1_000,
            },
        };
        assert!(matches!(
            resolve_flush_policy(requested, bounds, false),
            PolicyDecision::Clamped(_)
        ));
        assert_eq!(
            resolve_flush_policy(requested, bounds, true),
            PolicyDecision::Rejected
        );
        assert!(!semantic_coalescing_allowed("P0_LEDGER"));
        assert!(semantic_coalescing_allowed("ACTIVITY_DELTA"));
    }
    #[test]
    fn revision_conflict_is_detected() {
        assert_eq!(
            compare_revision(2, [1; 32], 2, [2; 32]),
            RevisionDecision::IntegrityConflict
        );
    }
    #[test]
    fn installation_scope_separates_names() {
        assert_ne!(scoped_name("a", "same"), scoped_name("b", "same"));
    }
    #[test]
    fn uuid_v7_shape() {
        assert!(is_canonical_uuid_v7("01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4a"));
    }
}
