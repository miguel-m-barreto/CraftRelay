#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub type Checksum = [u8; 32];

pub const MAX_METADATA_ENTRIES: usize = 32;
pub const MAX_METADATA_BYTES: usize = 8_192;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    InvalidUuid,
    NonPositiveInt32,
    NonPositiveInt64,
    MetadataCountExceeded,
    MetadataBytesExceeded,
    InvalidMetadataKey,
    DuplicateMetadataKey,
    TokenInvalidMac,
    TokenExpired,
    TokenScopeMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEntry {
    pub key: String,
    pub value: String,
}

pub fn positive_i32(value: i32) -> Result<i32, ValidationError> {
    (value > 0)
        .then_some(value)
        .ok_or(ValidationError::NonPositiveInt32)
}

pub fn positive_i64(value: i64) -> Result<i64, ValidationError> {
    (value > 0)
        .then_some(value)
        .ok_or(ValidationError::NonPositiveInt64)
}

pub fn canonicalize_metadata(
    entries: &[MetadataEntry],
) -> Result<Vec<MetadataEntry>, ValidationError> {
    if entries.len() > MAX_METADATA_ENTRIES {
        return Err(ValidationError::MetadataCountExceeded);
    }
    let mut keys = BTreeSet::new();
    let mut total_bytes = 0usize;
    for entry in entries {
        if entry.key.is_empty()
            || !entry.key.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
            })
        {
            return Err(ValidationError::InvalidMetadataKey);
        }
        if !keys.insert(entry.key.as_bytes().to_vec()) {
            return Err(ValidationError::DuplicateMetadataKey);
        }
        total_bytes = total_bytes
            .checked_add(entry.key.len() + entry.value.len())
            .ok_or(ValidationError::MetadataBytesExceeded)?;
    }
    if total_bytes > MAX_METADATA_BYTES {
        return Err(ValidationError::MetadataBytesExceeded);
    }
    let mut canonical = entries.to_vec();
    canonical.sort_by(|left, right| left.key.as_bytes().cmp(right.key.as_bytes()));
    Ok(canonical)
}

pub fn canonical_metadata_text(entries: &[MetadataEntry]) -> Result<String, ValidationError> {
    Ok(canonicalize_metadata(entries)?
        .iter()
        .map(|entry| format!("{}={}", entry.key, entry.value))
        .collect::<Vec<_>>()
        .join("|"))
}

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

pub fn hmac_sha256(key: &[u8], message: &[u8]) -> Checksum {
    const BLOCK_SIZE: usize = 64;
    let key_material = if key.len() > BLOCK_SIZE {
        sha256(key).to_vec()
    } else {
        key.to_vec()
    };
    let mut padded = [0u8; BLOCK_SIZE];
    padded[..key_material.len()].copy_from_slice(&key_material);
    let mut inner_pad = [0x36u8; BLOCK_SIZE];
    let mut outer_pad = [0x5cu8; BLOCK_SIZE];
    for index in 0..BLOCK_SIZE {
        inner_pad[index] ^= padded[index];
        outer_pad[index] ^= padded[index];
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner_digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    outer.finalize().into()
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
        assert!(!is_canonical_uuid_v7(
            "01890f3e-7b4c-4cc2-98c8-3f0f5f3f9b4a"
        ));
        assert!(!is_canonical_uuid_v7(
            "01890F3E-7B4C-7CC2-98C8-3F0F5F3F9B4A"
        ));
    }
    #[test]
    fn positive_numeric_boundaries() {
        assert_eq!(positive_i32(1), Ok(1));
        assert_eq!(positive_i32(i32::MAX), Ok(i32::MAX));
        assert_eq!(positive_i32(0), Err(ValidationError::NonPositiveInt32));
        assert_eq!(positive_i64(1), Ok(1));
        assert_eq!(positive_i64(i64::MAX), Ok(i64::MAX));
        assert_eq!(positive_i64(-1), Err(ValidationError::NonPositiveInt64));
    }
    #[test]
    fn metadata_is_sorted_and_duplicate_keys_are_rejected() {
        let entries = vec![
            MetadataEntry {
                key: "z".into(),
                value: "last".into(),
            },
            MetadataEntry {
                key: "a".into(),
                value: "first".into(),
            },
        ];
        assert_eq!(
            canonical_metadata_text(&entries),
            Ok("a=first|z=last".into())
        );
        assert_eq!(
            canonicalize_metadata(&[
                MetadataEntry {
                    key: "same".into(),
                    value: "a".into(),
                },
                MetadataEntry {
                    key: "same".into(),
                    value: "b".into(),
                },
            ]),
            Err(ValidationError::DuplicateMetadataKey)
        );
    }
}
