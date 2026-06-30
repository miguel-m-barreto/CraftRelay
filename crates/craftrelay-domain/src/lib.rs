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
    DeliveryRetrying,
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

// --- Sprint 2: Producer Registration ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerLifecycleState {
    Active,
    Disabled,
    Suspended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PriorityClass {
    P0,
    P1,
    P2,
    Background,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaClass {
    Critical,
    Standard,
    Bulk,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredProducer {
    pub installation_id: String,
    pub producer_id: String,
    pub producer_instance_id: String,
    pub integration_id: String,
    pub paper_plugin_id: String,
    pub lifecycle_state: ProducerLifecycleState,
    pub allowed_namespaces: Vec<String>,
    pub priority_class: PriorityClass,
    pub quota_class: QuotaClass,
    pub policy_binding_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerRegistrationRejection {
    Duplicate,
    Disabled,
    Suspended,
    CrossInstallation,
    InvalidManifest,
    NamespaceDenied,
}

pub fn validate_producer_registration(
    producer: &RegisteredProducer,
    requested_installation_id: &str,
    existing_producers: &[&RegisteredProducer],
) -> Result<(), ProducerRegistrationRejection> {
    if producer.installation_id != requested_installation_id {
        return Err(ProducerRegistrationRejection::CrossInstallation);
    }
    match producer.lifecycle_state {
        ProducerLifecycleState::Disabled => return Err(ProducerRegistrationRejection::Disabled),
        ProducerLifecycleState::Suspended => return Err(ProducerRegistrationRejection::Suspended),
        ProducerLifecycleState::Active => {}
    }
    for existing in existing_producers {
        if existing.producer_id == producer.producer_id
            && existing.installation_id == producer.installation_id
        {
            return Err(ProducerRegistrationRejection::Duplicate);
        }
    }
    if producer.producer_id.is_empty() || producer.integration_id.is_empty() {
        return Err(ProducerRegistrationRejection::InvalidManifest);
    }
    Ok(())
}

// --- Sprint 2: IntegrationManifest Validation ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEventDeclaration {
    pub event_type: String,
    pub min_schema_version: i32,
    pub max_schema_version: i32,
    pub namespace: String,
    pub event_class: String,
    pub requested_durability: DurabilityClass,
    pub requested_retention: RetentionClass,
    pub requested_priority: PriorityClass,
    pub requested_quota: QuotaClass,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestQueryDeclaration {
    pub query_id: String,
    pub min_schema_version: i32,
    pub max_schema_version: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DurabilityClass {
    LocalDurable,
    ReplicatedDurable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RetentionClass {
    Standard,
    Extended,
    Permanent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendedIntegrationManifest {
    pub integration_id: String,
    pub integration_version: i32,
    pub paper_plugin_id: String,
    pub producer_id: String,
    pub events: Vec<ManifestEventDeclaration>,
    pub queries: Vec<ManifestQueryDeclaration>,
    pub namespaces: Vec<String>,
    pub required_durability: DurabilityClass,
    pub required_retention: RetentionClass,
    pub requested_priority: PriorityClass,
    pub requested_quota: QuotaClass,
    pub max_pending_publishes: i32,
    pub max_pending_queries: i32,
    pub max_active_watches: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestViolation {
    DuplicateEvent,
    DuplicateQuery,
    DuplicateNamespace,
    InvalidEventName,
    InvalidQueryName,
    InvalidNamespaceName,
    UnboundedLimit,
    MissingLimit,
    BestEffortForbidden,
    SelfPromotionForbidden,
    InvalidSchemaVersion,
}

pub fn validate_manifest(
    manifest: &ExtendedIntegrationManifest,
    installation_priority_ceiling: PriorityClass,
) -> Result<(), Vec<ManifestViolation>> {
    let mut violations = Vec::new();
    if manifest.integration_id.is_empty() || manifest.paper_plugin_id.is_empty() {
        violations.push(ManifestViolation::InvalidEventName);
    }
    if manifest.integration_version <= 0 {
        violations.push(ManifestViolation::InvalidSchemaVersion);
    }
    if manifest.max_pending_publishes <= 0
        || manifest.max_pending_queries <= 0
        || manifest.max_active_watches <= 0
    {
        violations.push(ManifestViolation::MissingLimit);
    }
    if manifest.max_pending_publishes > 4_096
        || manifest.max_pending_queries > 4_096
        || manifest.max_active_watches > 4_096
    {
        violations.push(ManifestViolation::UnboundedLimit);
    }
    if manifest.requested_priority < installation_priority_ceiling {
        violations.push(ManifestViolation::SelfPromotionForbidden);
    }
    let mut event_names = std::collections::BTreeSet::new();
    for event in &manifest.events {
        if event.event_type.is_empty() || !is_valid_identifier(&event.event_type) {
            violations.push(ManifestViolation::InvalidEventName);
        }
        if !event_names.insert(&event.event_type) {
            violations.push(ManifestViolation::DuplicateEvent);
        }
        if event.min_schema_version <= 0 || event.max_schema_version <= 0 {
            violations.push(ManifestViolation::InvalidSchemaVersion);
        }
        if event.min_schema_version > event.max_schema_version {
            violations.push(ManifestViolation::InvalidSchemaVersion);
        }
        if event.namespace.is_empty() || !is_valid_identifier(&event.namespace) {
            violations.push(ManifestViolation::InvalidNamespaceName);
        }
        if event.event_class == "BEST_EFFORT" {
            violations.push(ManifestViolation::BestEffortForbidden);
        }
        if event.requested_priority < installation_priority_ceiling {
            violations.push(ManifestViolation::SelfPromotionForbidden);
        }
    }
    let mut query_ids = std::collections::BTreeSet::new();
    for query in &manifest.queries {
        if query.query_id.is_empty() || !is_valid_identifier(&query.query_id) {
            violations.push(ManifestViolation::InvalidQueryName);
        }
        if !query_ids.insert(&query.query_id) {
            violations.push(ManifestViolation::DuplicateQuery);
        }
        if query.min_schema_version <= 0 || query.max_schema_version <= 0 {
            violations.push(ManifestViolation::InvalidSchemaVersion);
        }
    }
    let mut ns_set = std::collections::BTreeSet::new();
    for ns in &manifest.namespaces {
        if ns.is_empty() || !is_valid_identifier(ns) {
            violations.push(ManifestViolation::InvalidNamespaceName);
        }
        if !ns_set.insert(ns) {
            violations.push(ManifestViolation::DuplicateNamespace);
        }
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

fn is_valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
        && value.as_bytes()[0].is_ascii_lowercase()
}

// --- Sprint 2: Credentials and ACL ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialKind {
    SharedSecret,
    MtlsCertificate,
    IpcToken,
    FakeTestOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialStatus {
    Active,
    Revoked,
    Expired,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialReference {
    pub credential_id: String,
    pub kind: CredentialKind,
    pub revision: i32,
    pub status: CredentialStatus,
    pub installation_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclAction {
    Publish,
    Query,
    Watch,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclDenyReason {
    NoMatchingRule,
    ExplicitDeny,
    CredentialInvalid,
    CredentialRevoked,
    CredentialExpired,
    CrossInstallation,
    NamespaceDenied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclPrincipal {
    pub producer_id: String,
    pub installation_id: String,
    pub credential: CredentialReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclRule {
    pub rule_id: String,
    pub action: AclAction,
    pub namespace_pattern: String,
    pub decision: AclDecision,
    pub priority: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclEvaluationResult {
    pub decision: AclDecision,
    pub deny_reason: Option<AclDenyReason>,
    pub matched_rule_id: Option<String>,
    pub policy_version: i64,
}

pub fn evaluate_acl(
    principal: &AclPrincipal,
    scope_installation_id: &str,
    scope_namespace: &str,
    action: AclAction,
    rules: &[AclRule],
    policy_version: i64,
) -> AclEvaluationResult {
    if principal.installation_id != scope_installation_id {
        return AclEvaluationResult {
            decision: AclDecision::Deny,
            deny_reason: Some(AclDenyReason::CrossInstallation),
            matched_rule_id: None,
            policy_version,
        };
    }
    match principal.credential.status {
        CredentialStatus::Active => {}
        CredentialStatus::Revoked => {
            return AclEvaluationResult {
                decision: AclDecision::Deny,
                deny_reason: Some(AclDenyReason::CredentialRevoked),
                matched_rule_id: None,
                policy_version,
            };
        }
        CredentialStatus::Expired => {
            return AclEvaluationResult {
                decision: AclDecision::Deny,
                deny_reason: Some(AclDenyReason::CredentialExpired),
                matched_rule_id: None,
                policy_version,
            };
        }
        CredentialStatus::Unknown => {
            return AclEvaluationResult {
                decision: AclDecision::Deny,
                deny_reason: Some(AclDenyReason::CredentialInvalid),
                matched_rule_id: None,
                policy_version,
            };
        }
    }
    let mut sorted_rules: Vec<&AclRule> = rules.iter().filter(|r| r.action == action).collect();
    sorted_rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    for rule in sorted_rules {
        if namespace_matches(&rule.namespace_pattern, scope_namespace) {
            return AclEvaluationResult {
                decision: rule.decision,
                deny_reason: if rule.decision == AclDecision::Deny {
                    Some(AclDenyReason::ExplicitDeny)
                } else {
                    None
                },
                matched_rule_id: Some(rule.rule_id.clone()),
                policy_version,
            };
        }
    }
    AclEvaluationResult {
        decision: AclDecision::Deny,
        deny_reason: Some(AclDenyReason::NoMatchingRule),
        matched_rule_id: None,
        policy_version,
    }
}

fn namespace_matches(pattern: &str, namespace: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        namespace.starts_with(prefix) && namespace.len() > prefix.len()
    } else {
        pattern == namespace
    }
}

// --- Sprint 2: Policy Resolution ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyRejectionReason {
    SelfPromotion,
    DurabilityWeakening,
    RetentionWeakening,
    ProjectionBypass,
    InstallationEscape,
    BestEffortForbidden,
    InvalidPolicyBinding,
    UnknownPolicyBinding,
    AclDenied,
    ProducerDisabled,
    ProducerSuspended,
    CrossInstallation,
    NotLocallyOwned,
    QuotaExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDecision {
    Admitted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyResolutionContext {
    pub installation_id: String,
    pub producer: RegisteredProducer,
    pub acl_result: AclEvaluationResult,
    pub ownership_snapshot_id: String,
    pub namespace: String,
    pub requested_durability: DurabilityClass,
    pub requested_retention: RetentionClass,
    pub requested_projection_policy_id: String,
    pub requested_priority: PriorityClass,
    pub requested_quota: QuotaClass,
    pub requested_flush: RequestedFlushCriteria,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectivePolicyResult {
    pub effective_producer_id: String,
    pub effective_namespace: String,
    pub effective_durability: DurabilityClass,
    pub effective_retention: RetentionClass,
    pub effective_projection_policy_id: String,
    pub effective_quota_class: QuotaClass,
    pub effective_priority_class: PriorityClass,
    pub effective_flush: EffectiveFlushCriteria,
    pub policy_version: i64,
    pub ownership_snapshot_id: String,
    pub admission_decision: AdmissionDecision,
    pub rejection_reason: Option<PolicyRejectionReason>,
    pub decision_detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyConfiguration {
    pub installation_id: String,
    pub minimum_durability: DurabilityClass,
    pub minimum_retention: RetentionClass,
    pub required_projection_policy_id: Option<String>,
    pub priority_ceiling: PriorityClass,
    pub valid_policy_bindings: Vec<String>,
    pub flush_bounds: FlushPolicyBounds,
    pub policy_version: i64,
}

pub fn resolve_policy(
    ctx: &PolicyResolutionContext,
    config: &PolicyConfiguration,
    owned_namespaces: &[String],
) -> EffectivePolicyResult {
    let rejected = |reason: PolicyRejectionReason, detail: &str| EffectivePolicyResult {
        effective_producer_id: ctx.producer.producer_id.clone(),
        effective_namespace: ctx.namespace.clone(),
        effective_durability: config.minimum_durability,
        effective_retention: config.minimum_retention,
        effective_projection_policy_id: config
            .required_projection_policy_id
            .clone()
            .unwrap_or_default(),
        effective_quota_class: ctx.producer.quota_class,
        effective_priority_class: ctx.producer.priority_class,
        effective_flush: EffectiveFlushCriteria {
            max_records: config.flush_bounds.minimum.max_records,
            max_bytes: config.flush_bounds.minimum.max_bytes,
            max_age_millis: config.flush_bounds.minimum.max_age_millis,
        },
        policy_version: config.policy_version,
        ownership_snapshot_id: ctx.ownership_snapshot_id.clone(),
        admission_decision: AdmissionDecision::Rejected,
        rejection_reason: Some(reason),
        decision_detail: detail.to_string(),
    };

    if ctx.installation_id != config.installation_id {
        return rejected(
            PolicyRejectionReason::InstallationEscape,
            "producer cannot escape installation scope",
        );
    }
    if ctx.producer.installation_id != ctx.installation_id {
        return rejected(
            PolicyRejectionReason::CrossInstallation,
            "producer installation does not match request",
        );
    }
    match ctx.producer.lifecycle_state {
        ProducerLifecycleState::Disabled => {
            return rejected(
                PolicyRejectionReason::ProducerDisabled,
                "producer is disabled",
            );
        }
        ProducerLifecycleState::Suspended => {
            return rejected(
                PolicyRejectionReason::ProducerSuspended,
                "producer is suspended",
            );
        }
        ProducerLifecycleState::Active => {}
    }
    if ctx.acl_result.decision == AclDecision::Deny {
        return rejected(PolicyRejectionReason::AclDenied, "ACL denied");
    }
    if ctx.requested_durability < config.minimum_durability {
        return rejected(
            PolicyRejectionReason::DurabilityWeakening,
            "cannot weaken durability below installation minimum",
        );
    }
    if ctx.requested_retention < config.minimum_retention {
        return rejected(
            PolicyRejectionReason::RetentionWeakening,
            "cannot weaken retention below installation minimum",
        );
    }
    if let Some(ref required) = config.required_projection_policy_id {
        if !ctx.requested_projection_policy_id.is_empty()
            && ctx.requested_projection_policy_id != *required
        {
            return rejected(
                PolicyRejectionReason::ProjectionBypass,
                "cannot bypass required projection policy",
            );
        }
    }
    if ctx.requested_priority < config.priority_ceiling {
        return rejected(
            PolicyRejectionReason::SelfPromotion,
            "producer cannot self-promote above priority ceiling",
        );
    }
    if !ctx.producer.policy_binding_id.is_empty()
        && !config
            .valid_policy_bindings
            .contains(&ctx.producer.policy_binding_id)
    {
        return rejected(
            PolicyRejectionReason::UnknownPolicyBinding,
            "producer policy binding is not recognized",
        );
    }
    if !owned_namespaces.contains(&ctx.namespace) {
        return rejected(
            PolicyRejectionReason::NotLocallyOwned,
            "namespace is not locally owned",
        );
    }
    let effective_flush =
        match resolve_flush_policy(ctx.requested_flush, config.flush_bounds, false) {
            PolicyDecision::Accepted => EffectiveFlushCriteria {
                max_records: ctx.requested_flush.max_records,
                max_bytes: ctx.requested_flush.max_bytes,
                max_age_millis: ctx.requested_flush.max_age_millis,
            },
            PolicyDecision::Clamped(clamped) => clamped,
            PolicyDecision::Rejected => {
                return rejected(
                    PolicyRejectionReason::InvalidPolicyBinding,
                    "requested flush criteria rejected by policy",
                );
            }
        };

    EffectivePolicyResult {
        effective_producer_id: ctx.producer.producer_id.clone(),
        effective_namespace: ctx.namespace.clone(),
        effective_durability: ctx.requested_durability,
        effective_retention: ctx.requested_retention,
        effective_projection_policy_id: config
            .required_projection_policy_id
            .clone()
            .unwrap_or_else(|| ctx.requested_projection_policy_id.clone()),
        effective_quota_class: ctx.producer.quota_class,
        effective_priority_class: ctx.producer.priority_class,
        effective_flush,
        policy_version: config.policy_version,
        ownership_snapshot_id: ctx.ownership_snapshot_id.clone(),
        admission_decision: AdmissionDecision::Admitted,
        rejection_reason: None,
        decision_detail: "policy resolved successfully".to_string(),
    }
}

// --- Sprint 2: Ownership Snapshot ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipMode {
    NodeLocal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipSnapshotId {
    pub snapshot_id: String,
    pub snapshot_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOwnershipEntry {
    pub namespace: String,
    pub owner_node_id: String,
    pub owner_agent_id: String,
    pub installation_id: String,
    pub mode: OwnershipMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipSnapshot {
    pub id: OwnershipSnapshotId,
    pub installation_id: String,
    pub node_id: String,
    pub mode: OwnershipMode,
    pub entries: Vec<NamespaceOwnershipEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipViolation {
    MissingOwner,
    DuplicateNamespace,
    CrossInstallation,
    UnsupportedMode,
    DynamicElectionForbidden,
    AmbiguousOwnership,
    NotLocallyOwned,
}

pub fn validate_ownership_snapshot(
    snapshot: &OwnershipSnapshot,
) -> Result<(), Vec<OwnershipViolation>> {
    let mut violations = Vec::new();
    if snapshot.mode != OwnershipMode::NodeLocal {
        violations.push(OwnershipViolation::UnsupportedMode);
    }
    let mut namespaces = std::collections::BTreeSet::new();
    for entry in &snapshot.entries {
        if entry.owner_node_id.is_empty() || entry.owner_agent_id.is_empty() {
            violations.push(OwnershipViolation::MissingOwner);
        }
        if !namespaces.insert(&entry.namespace) {
            violations.push(OwnershipViolation::DuplicateNamespace);
        }
        if entry.installation_id != snapshot.installation_id {
            violations.push(OwnershipViolation::CrossInstallation);
        }
        if entry.mode != OwnershipMode::NodeLocal {
            violations.push(OwnershipViolation::UnsupportedMode);
        }
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

pub fn is_namespace_locally_owned(snapshot: &OwnershipSnapshot, namespace: &str) -> bool {
    snapshot
        .entries
        .iter()
        .any(|e| e.namespace == namespace && e.owner_node_id == snapshot.node_id)
}

// --- Sprint 2: Quotas and Bounded Ingress ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerQuotaState {
    pub producer_id: String,
    pub in_flight_publishes: i32,
    pub max_in_flight_publishes: i32,
    pub queued_publishes: i32,
    pub max_queued_publishes: i32,
    pub in_flight_bytes: i64,
    pub max_in_flight_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceQuotaState {
    pub namespace: String,
    pub in_flight_publishes: i32,
    pub max_in_flight_publishes: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalQuotaState {
    pub in_flight_publishes: i32,
    pub max_in_flight_publishes: i32,
    pub reserved_p0_capacity: i32,
    pub used_p0_capacity: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejectionReason {
    ProducerInFlightExceeded,
    ProducerQueueExceeded,
    ProducerBytesExceeded,
    NamespaceInFlightExceeded,
    GlobalInFlightExceeded,
    P0CapacityReserved,
    ProducerDisabled,
    ProducerUnauthorized,
    NotLocallyOwned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionResult {
    pub decision: AdmissionDecision,
    pub rejection_reason: Option<AdmissionRejectionReason>,
    pub rejection_detail: String,
}

pub fn evaluate_admission(
    producer: &RegisteredProducer,
    producer_quota: &ProducerQuotaState,
    namespace_quota: &NamespaceQuotaState,
    global_quota: &GlobalQuotaState,
    payload_bytes: i64,
    namespace_owned: bool,
) -> AdmissionResult {
    let rejected = |reason: AdmissionRejectionReason, detail: &str| AdmissionResult {
        decision: AdmissionDecision::Rejected,
        rejection_reason: Some(reason),
        rejection_detail: detail.to_string(),
    };

    if producer.lifecycle_state == ProducerLifecycleState::Disabled {
        return rejected(
            AdmissionRejectionReason::ProducerDisabled,
            "producer is disabled",
        );
    }
    if !namespace_owned {
        return rejected(
            AdmissionRejectionReason::NotLocallyOwned,
            "namespace is not locally owned by this node",
        );
    }
    if producer_quota.in_flight_publishes >= producer_quota.max_in_flight_publishes {
        return rejected(
            AdmissionRejectionReason::ProducerInFlightExceeded,
            "producer in-flight publish limit reached",
        );
    }
    if producer_quota.queued_publishes >= producer_quota.max_queued_publishes {
        return rejected(
            AdmissionRejectionReason::ProducerQueueExceeded,
            "producer queued publish limit reached",
        );
    }
    if producer_quota.in_flight_bytes + payload_bytes > producer_quota.max_in_flight_bytes {
        return rejected(
            AdmissionRejectionReason::ProducerBytesExceeded,
            "producer in-flight bytes limit would be exceeded",
        );
    }
    if namespace_quota.in_flight_publishes >= namespace_quota.max_in_flight_publishes {
        return rejected(
            AdmissionRejectionReason::NamespaceInFlightExceeded,
            "namespace in-flight publish limit reached",
        );
    }
    let available_global = global_quota.max_in_flight_publishes - global_quota.in_flight_publishes;
    if available_global <= 0 {
        return rejected(
            AdmissionRejectionReason::GlobalInFlightExceeded,
            "global in-flight publish limit reached",
        );
    }
    let remaining_p0 = global_quota.reserved_p0_capacity - global_quota.used_p0_capacity;
    if producer.priority_class != PriorityClass::P0 {
        let non_p0_available = available_global - remaining_p0.max(0);
        if non_p0_available <= 0 {
            return rejected(
                AdmissionRejectionReason::P0CapacityReserved,
                "lower-priority producer cannot consume reserved P0 capacity",
            );
        }
    }

    AdmissionResult {
        decision: AdmissionDecision::Admitted,
        rejection_reason: None,
        rejection_detail: "admitted".to_string(),
    }
}

// --- Sprint 2: Profile Validation Skeletons ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaDurabilityProfile {
    pub profile_id: String,
    pub replication_factor: i32,
    pub min_insync_replicas: i32,
    pub required_acks: String,
    pub topic_reference: String,
    pub profile_version: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileValidationError {
    UnknownProfile,
    InvalidRf,
    InvalidMinIsr,
    InvalidAcks,
    WeakenedDurability,
}

pub fn validate_kafka_profile(
    profile: &KafkaDurabilityProfile,
) -> Result<(), ProfileValidationError> {
    if profile.replication_factor <= 0 {
        return Err(ProfileValidationError::InvalidRf);
    }
    if profile.min_insync_replicas <= 0 {
        return Err(ProfileValidationError::InvalidMinIsr);
    }
    if profile.min_insync_replicas > profile.replication_factor {
        return Err(ProfileValidationError::InvalidMinIsr);
    }
    if profile.required_acks != "all" && profile.required_acks != "-1" {
        return Err(ProfileValidationError::InvalidAcks);
    }
    Ok(())
}

pub fn validate_p0_kafka_profile(
    profile: &KafkaDurabilityProfile,
) -> Result<(), ProfileValidationError> {
    validate_kafka_profile(profile)?;
    if profile.replication_factor < 5 {
        return Err(ProfileValidationError::WeakenedDurability);
    }
    if profile.min_insync_replicas < 5 {
        return Err(ProfileValidationError::WeakenedDurability);
    }
    Ok(())
}

// --- Sprint 5: Extended Kafka Production Profile ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaProductionProfile {
    pub profile_id: String,
    pub profile_version: i32,
    pub replication_factor: i32,
    pub min_insync_replicas: i32,
    pub required_acks: String,
    pub enable_idempotence: bool,
    pub delivery_timeout_ms: i64,
    pub request_timeout_ms: i64,
    pub linger_ms: i64,
    pub max_in_flight_requests_per_connection: i32,
    pub compression: String,
    pub retention_ms: i64,
    pub retention_bytes: i64,
    pub cleanup_policy: String,
    pub unclean_leader_election: bool,
    pub topic_deletion_policy: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtendedProfileValidationError {
    Base(ProfileValidationError),
    IdempotenceRequired,
    InvalidCleanupPolicy,
    UncleanElectionForbidden,
    InvalidDeliveryTimeout,
    InvalidRequestTimeout,
    InvalidLingerMs,
    InvalidMaxInFlight,
    InvalidCompression,
    InvalidRetentionMs,
    InvalidRetentionBytes,
    TopicDeletionForbidden,
}

fn validate_production_base(
    profile: &KafkaProductionProfile,
) -> Result<(), ExtendedProfileValidationError> {
    let base = KafkaDurabilityProfile {
        profile_id: profile.profile_id.clone(),
        replication_factor: profile.replication_factor,
        min_insync_replicas: profile.min_insync_replicas,
        required_acks: profile.required_acks.clone(),
        topic_reference: String::new(),
        profile_version: profile.profile_version,
    };
    validate_kafka_profile(&base).map_err(ExtendedProfileValidationError::Base)?;
    if !profile.enable_idempotence {
        return Err(ExtendedProfileValidationError::IdempotenceRequired);
    }
    if profile.delivery_timeout_ms <= 0 {
        return Err(ExtendedProfileValidationError::InvalidDeliveryTimeout);
    }
    if profile.request_timeout_ms <= 0 {
        return Err(ExtendedProfileValidationError::InvalidRequestTimeout);
    }
    if profile.linger_ms < 0 {
        return Err(ExtendedProfileValidationError::InvalidLingerMs);
    }
    if !(1..=5).contains(&profile.max_in_flight_requests_per_connection) {
        return Err(ExtendedProfileValidationError::InvalidMaxInFlight);
    }
    if !matches!(
        profile.compression.as_str(),
        "none" | "gzip" | "snappy" | "lz4" | "zstd"
    ) {
        return Err(ExtendedProfileValidationError::InvalidCompression);
    }
    if profile.retention_ms <= 0 {
        return Err(ExtendedProfileValidationError::InvalidRetentionMs);
    }
    if profile.retention_bytes <= 0 {
        return Err(ExtendedProfileValidationError::InvalidRetentionBytes);
    }
    if !matches!(
        profile.cleanup_policy.as_str(),
        "delete" | "compact" | "delete,compact"
    ) {
        return Err(ExtendedProfileValidationError::InvalidCleanupPolicy);
    }
    if profile.unclean_leader_election {
        return Err(ExtendedProfileValidationError::UncleanElectionForbidden);
    }
    if profile.topic_deletion_policy != "PROTECTED" {
        return Err(ExtendedProfileValidationError::TopicDeletionForbidden);
    }
    Ok(())
}

pub fn validate_production_profile(
    profile: &KafkaProductionProfile,
) -> Result<(), ExtendedProfileValidationError> {
    validate_production_base(profile)
}

pub fn validate_p0_production_profile(
    profile: &KafkaProductionProfile,
) -> Result<(), ExtendedProfileValidationError> {
    validate_production_base(profile)?;
    if profile.replication_factor < 5 {
        return Err(ExtendedProfileValidationError::Base(
            ProfileValidationError::WeakenedDurability,
        ));
    }
    if profile.min_insync_replicas < 5 {
        return Err(ExtendedProfileValidationError::Base(
            ProfileValidationError::WeakenedDurability,
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionProfile {
    pub profile_id: String,
    pub retention_class: RetentionClass,
    pub retention_horizon_millis: i64,
    pub profile_version: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionPolicyProfile {
    pub policy_id: String,
    pub policy_version: i32,
    pub required_projector_ids: Vec<String>,
    pub archive_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaProfile {
    pub profile_id: String,
    pub quota_class: QuotaClass,
    pub max_in_flight_publishes: i32,
    pub max_queued_publishes: i32,
    pub max_in_flight_bytes: i64,
    pub profile_version: i32,
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

    // --- Sprint 2 Tests ---

    fn test_producer(state: ProducerLifecycleState) -> RegisteredProducer {
        RegisteredProducer {
            installation_id: "installation-a".into(),
            producer_id: "producer-a".into(),
            producer_instance_id: "instance-1".into(),
            integration_id: "reference".into(),
            paper_plugin_id: "ReferencePlugin".into(),
            lifecycle_state: state,
            allowed_namespaces: vec!["economy".into(), "mining".into()],
            priority_class: PriorityClass::P1,
            quota_class: QuotaClass::Standard,
            policy_binding_id: "binding-1".into(),
        }
    }

    #[test]
    fn valid_producer_registration() {
        let producer = test_producer(ProducerLifecycleState::Active);
        assert!(validate_producer_registration(&producer, "installation-a", &[]).is_ok());
    }

    #[test]
    fn duplicate_producer_registration_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let result = validate_producer_registration(&producer, "installation-a", &[&producer]);
        assert_eq!(result, Err(ProducerRegistrationRejection::Duplicate));
    }

    #[test]
    fn disabled_producer_rejected() {
        let producer = test_producer(ProducerLifecycleState::Disabled);
        assert_eq!(
            validate_producer_registration(&producer, "installation-a", &[]),
            Err(ProducerRegistrationRejection::Disabled)
        );
    }

    #[test]
    fn suspended_producer_rejected() {
        let producer = test_producer(ProducerLifecycleState::Suspended);
        assert_eq!(
            validate_producer_registration(&producer, "installation-a", &[]),
            Err(ProducerRegistrationRejection::Suspended)
        );
    }

    #[test]
    fn cross_installation_producer_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        assert_eq!(
            validate_producer_registration(&producer, "installation-b", &[]),
            Err(ProducerRegistrationRejection::CrossInstallation)
        );
    }

    fn test_manifest(priority: PriorityClass) -> ExtendedIntegrationManifest {
        ExtendedIntegrationManifest {
            integration_id: "reference".into(),
            integration_version: 1,
            paper_plugin_id: "ReferencePlugin".into(),
            producer_id: "producer-a".into(),
            events: vec![ManifestEventDeclaration {
                event_type: "economy.transfer".into(),
                min_schema_version: 1,
                max_schema_version: 1,
                namespace: "economy".into(),
                event_class: "P0_LEDGER".into(),
                requested_durability: DurabilityClass::ReplicatedDurable,
                requested_retention: RetentionClass::Permanent,
                requested_priority: priority,
                requested_quota: QuotaClass::Critical,
            }],
            queries: vec![ManifestQueryDeclaration {
                query_id: "economy.get-account".into(),
                min_schema_version: 1,
                max_schema_version: 1,
            }],
            namespaces: vec!["economy".into()],
            required_durability: DurabilityClass::ReplicatedDurable,
            required_retention: RetentionClass::Standard,
            requested_priority: priority,
            requested_quota: QuotaClass::Standard,
            max_pending_publishes: 128,
            max_pending_queries: 64,
            max_active_watches: 32,
        }
    }

    #[test]
    fn manifest_duplicate_event_rejected() {
        let mut manifest = test_manifest(PriorityClass::P1);
        manifest.events.push(manifest.events[0].clone());
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::DuplicateEvent)
        );
    }

    #[test]
    fn manifest_duplicate_query_rejected() {
        let mut manifest = test_manifest(PriorityClass::P1);
        manifest.queries.push(manifest.queries[0].clone());
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::DuplicateQuery)
        );
    }

    #[test]
    fn manifest_duplicate_namespace_rejected() {
        let mut manifest = test_manifest(PriorityClass::P1);
        manifest.namespaces.push("economy".into());
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::DuplicateNamespace)
        );
    }

    #[test]
    fn manifest_invalid_event_name_rejected() {
        let mut manifest = test_manifest(PriorityClass::P1);
        manifest.events[0].event_type = "UPPER_CASE".into();
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::InvalidEventName)
        );
    }

    #[test]
    fn manifest_invalid_query_name_rejected() {
        let mut manifest = test_manifest(PriorityClass::P1);
        manifest.queries[0].query_id = "".into();
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::InvalidQueryName)
        );
    }

    #[test]
    fn manifest_invalid_namespace_rejected() {
        let mut manifest = test_manifest(PriorityClass::P1);
        manifest.namespaces[0] = "INVALID".into();
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::InvalidNamespaceName)
        );
    }

    #[test]
    fn manifest_self_promotion_forbidden() {
        let manifest = test_manifest(PriorityClass::P0);
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::SelfPromotionForbidden)
        );
    }

    #[test]
    fn manifest_best_effort_forbidden() {
        let mut manifest = test_manifest(PriorityClass::P1);
        manifest.events[0].event_class = "BEST_EFFORT".into();
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::BestEffortForbidden)
        );
    }

    #[test]
    fn manifest_unbounded_limit_rejected() {
        let mut manifest = test_manifest(PriorityClass::P1);
        manifest.max_pending_publishes = 5000;
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::UnboundedLimit)
        );
    }

    #[test]
    fn manifest_missing_limit_rejected() {
        let mut manifest = test_manifest(PriorityClass::P1);
        manifest.max_pending_publishes = 0;
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::MissingLimit)
        );
    }

    #[test]
    fn manifest_invalid_schema_version_rejected() {
        let mut manifest = test_manifest(PriorityClass::P1);
        manifest.events[0].min_schema_version = 0;
        let result = validate_manifest(&manifest, PriorityClass::P1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&ManifestViolation::InvalidSchemaVersion)
        );
    }

    #[test]
    fn valid_manifest_accepted() {
        let manifest = test_manifest(PriorityClass::P1);
        assert!(validate_manifest(&manifest, PriorityClass::P1).is_ok());
    }

    fn test_credential(status: CredentialStatus) -> CredentialReference {
        CredentialReference {
            credential_id: "cred-1".into(),
            kind: CredentialKind::FakeTestOnly,
            revision: 1,
            status,
            installation_id: "installation-a".into(),
        }
    }

    fn test_principal(status: CredentialStatus) -> AclPrincipal {
        AclPrincipal {
            producer_id: "producer-a".into(),
            installation_id: "installation-a".into(),
            credential: test_credential(status),
        }
    }

    fn allow_rule() -> AclRule {
        AclRule {
            rule_id: "rule-allow".into(),
            action: AclAction::Publish,
            namespace_pattern: "economy".into(),
            decision: AclDecision::Allow,
            priority: 10,
        }
    }

    fn deny_rule() -> AclRule {
        AclRule {
            rule_id: "rule-deny".into(),
            action: AclAction::Publish,
            namespace_pattern: "economy".into(),
            decision: AclDecision::Deny,
            priority: 20,
        }
    }

    #[test]
    fn acl_allow_decision() {
        let result = evaluate_acl(
            &test_principal(CredentialStatus::Active),
            "installation-a",
            "economy",
            AclAction::Publish,
            &[allow_rule()],
            1,
        );
        assert_eq!(result.decision, AclDecision::Allow);
        assert_eq!(result.matched_rule_id, Some("rule-allow".into()));
    }

    #[test]
    fn acl_deny_decision() {
        let result = evaluate_acl(
            &test_principal(CredentialStatus::Active),
            "installation-a",
            "economy",
            AclAction::Publish,
            &[allow_rule(), deny_rule()],
            1,
        );
        assert_eq!(result.decision, AclDecision::Deny);
        assert_eq!(result.deny_reason, Some(AclDenyReason::ExplicitDeny));
    }

    #[test]
    fn acl_no_matching_rule_denies() {
        let result = evaluate_acl(
            &test_principal(CredentialStatus::Active),
            "installation-a",
            "mining",
            AclAction::Publish,
            &[allow_rule()],
            1,
        );
        assert_eq!(result.decision, AclDecision::Deny);
        assert_eq!(result.deny_reason, Some(AclDenyReason::NoMatchingRule));
    }

    #[test]
    fn acl_unknown_credential_rejected() {
        let result = evaluate_acl(
            &test_principal(CredentialStatus::Unknown),
            "installation-a",
            "economy",
            AclAction::Publish,
            &[allow_rule()],
            1,
        );
        assert_eq!(result.decision, AclDecision::Deny);
        assert_eq!(result.deny_reason, Some(AclDenyReason::CredentialInvalid));
    }

    #[test]
    fn acl_revoked_credential_rejected() {
        let result = evaluate_acl(
            &test_principal(CredentialStatus::Revoked),
            "installation-a",
            "economy",
            AclAction::Publish,
            &[allow_rule()],
            1,
        );
        assert_eq!(result.decision, AclDecision::Deny);
        assert_eq!(result.deny_reason, Some(AclDenyReason::CredentialRevoked));
    }

    #[test]
    fn acl_expired_credential_rejected() {
        let result = evaluate_acl(
            &test_principal(CredentialStatus::Expired),
            "installation-a",
            "economy",
            AclAction::Publish,
            &[allow_rule()],
            1,
        );
        assert_eq!(result.decision, AclDecision::Deny);
        assert_eq!(result.deny_reason, Some(AclDenyReason::CredentialExpired));
    }

    #[test]
    fn acl_cross_installation_rejected() {
        let result = evaluate_acl(
            &test_principal(CredentialStatus::Active),
            "installation-b",
            "economy",
            AclAction::Publish,
            &[allow_rule()],
            1,
        );
        assert_eq!(result.decision, AclDecision::Deny);
        assert_eq!(result.deny_reason, Some(AclDenyReason::CrossInstallation));
    }

    fn test_flush_bounds() -> FlushPolicyBounds {
        FlushPolicyBounds {
            minimum: EffectiveFlushCriteria {
                max_records: 4,
                max_bytes: 100,
                max_age_millis: 5,
            },
            maximum: EffectiveFlushCriteria {
                max_records: 512,
                max_bytes: 131_072,
                max_age_millis: 1_000,
            },
        }
    }

    fn test_policy_config() -> PolicyConfiguration {
        PolicyConfiguration {
            installation_id: "installation-a".into(),
            minimum_durability: DurabilityClass::ReplicatedDurable,
            minimum_retention: RetentionClass::Standard,
            required_projection_policy_id: Some("projection-v1".into()),
            priority_ceiling: PriorityClass::P1,
            valid_policy_bindings: vec!["binding-1".into()],
            flush_bounds: test_flush_bounds(),
            policy_version: 42,
        }
    }

    fn test_acl_allow() -> AclEvaluationResult {
        AclEvaluationResult {
            decision: AclDecision::Allow,
            deny_reason: None,
            matched_rule_id: Some("rule-1".into()),
            policy_version: 1,
        }
    }

    fn test_acl_deny() -> AclEvaluationResult {
        AclEvaluationResult {
            decision: AclDecision::Deny,
            deny_reason: Some(AclDenyReason::ExplicitDeny),
            matched_rule_id: Some("rule-deny".into()),
            policy_version: 1,
        }
    }

    fn test_policy_ctx(
        producer: RegisteredProducer,
        acl: AclEvaluationResult,
    ) -> PolicyResolutionContext {
        PolicyResolutionContext {
            installation_id: "installation-a".into(),
            producer,
            acl_result: acl,
            ownership_snapshot_id: "snapshot-1".into(),
            namespace: "economy".into(),
            requested_durability: DurabilityClass::ReplicatedDurable,
            requested_retention: RetentionClass::Standard,
            requested_projection_policy_id: "projection-v1".into(),
            requested_priority: PriorityClass::P1,
            requested_quota: QuotaClass::Standard,
            requested_flush: RequestedFlushCriteria {
                max_records: 16,
                max_bytes: 16_384,
                max_age_millis: 5,
            },
        }
    }

    #[test]
    fn policy_resolution_determinism() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let ctx = test_policy_ctx(producer.clone(), test_acl_allow());
        let config = test_policy_config();
        let owned = vec!["economy".to_string()];
        let result1 = resolve_policy(&ctx, &config, &owned);
        let result2 = resolve_policy(&ctx, &config, &owned);
        assert_eq!(result1, result2);
        assert_eq!(result1.admission_decision, AdmissionDecision::Admitted);
    }

    #[test]
    fn policy_durability_weakening_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let mut ctx = test_policy_ctx(producer, test_acl_allow());
        ctx.requested_durability = DurabilityClass::LocalDurable;
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(PolicyRejectionReason::DurabilityWeakening)
        );
    }

    #[test]
    fn policy_retention_weakening_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let mut ctx = test_policy_ctx(producer, test_acl_allow());
        let mut config = test_policy_config();
        config.minimum_retention = RetentionClass::Extended;
        ctx.requested_retention = RetentionClass::Standard;
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(PolicyRejectionReason::RetentionWeakening)
        );
    }

    #[test]
    fn policy_projection_bypass_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let mut ctx = test_policy_ctx(producer, test_acl_allow());
        ctx.requested_projection_policy_id = "bypass-policy".into();
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(PolicyRejectionReason::ProjectionBypass)
        );
    }

    #[test]
    fn policy_self_promotion_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let mut ctx = test_policy_ctx(producer, test_acl_allow());
        ctx.requested_priority = PriorityClass::P0;
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(PolicyRejectionReason::SelfPromotion)
        );
    }

    #[test]
    fn policy_installation_escape_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let mut ctx = test_policy_ctx(producer, test_acl_allow());
        ctx.installation_id = "installation-b".into();
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(PolicyRejectionReason::InstallationEscape)
        );
    }

    #[test]
    fn policy_acl_denied_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let ctx = test_policy_ctx(producer, test_acl_deny());
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(PolicyRejectionReason::AclDenied)
        );
    }

    #[test]
    fn policy_disabled_producer_rejected() {
        let producer = test_producer(ProducerLifecycleState::Disabled);
        let ctx = test_policy_ctx(producer, test_acl_allow());
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(PolicyRejectionReason::ProducerDisabled)
        );
    }

    #[test]
    fn policy_suspended_producer_rejected() {
        let producer = test_producer(ProducerLifecycleState::Suspended);
        let ctx = test_policy_ctx(producer, test_acl_allow());
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(PolicyRejectionReason::ProducerSuspended)
        );
    }

    #[test]
    fn policy_cross_installation_rejected() {
        let mut producer = test_producer(ProducerLifecycleState::Active);
        producer.installation_id = "installation-b".into();
        let ctx = test_policy_ctx(producer, test_acl_allow());
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
    }

    #[test]
    fn policy_not_locally_owned_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let ctx = test_policy_ctx(producer, test_acl_allow());
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["mining".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(PolicyRejectionReason::NotLocallyOwned)
        );
    }

    #[test]
    fn policy_unknown_binding_rejected() {
        let mut producer = test_producer(ProducerLifecycleState::Active);
        producer.policy_binding_id = "unknown-binding".into();
        let ctx = test_policy_ctx(producer, test_acl_allow());
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(PolicyRejectionReason::UnknownPolicyBinding)
        );
    }

    #[test]
    fn policy_flush_criteria_clamped() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let mut ctx = test_policy_ctx(producer, test_acl_allow());
        ctx.requested_flush = RequestedFlushCriteria {
            max_records: 1,
            max_bytes: 10,
            max_age_millis: 1,
        };
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Admitted);
        assert!(result.effective_flush.max_records >= 4);
    }

    #[test]
    fn policy_effective_reason_codes() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let ctx = test_policy_ctx(producer, test_acl_allow());
        let config = test_policy_config();
        let result = resolve_policy(&ctx, &config, &["economy".into()]);
        assert_eq!(result.admission_decision, AdmissionDecision::Admitted);
        assert!(result.rejection_reason.is_none());
        assert!(!result.decision_detail.is_empty());
        assert_eq!(result.policy_version, 42);
    }

    fn test_ownership_snapshot() -> OwnershipSnapshot {
        OwnershipSnapshot {
            id: OwnershipSnapshotId {
                snapshot_id: "snap-1".into(),
                snapshot_version: 1,
            },
            installation_id: "installation-a".into(),
            node_id: "node-1".into(),
            mode: OwnershipMode::NodeLocal,
            entries: vec![NamespaceOwnershipEntry {
                namespace: "economy".into(),
                owner_node_id: "node-1".into(),
                owner_agent_id: "agent-1".into(),
                installation_id: "installation-a".into(),
                mode: OwnershipMode::NodeLocal,
            }],
        }
    }

    #[test]
    fn valid_ownership_snapshot() {
        assert!(validate_ownership_snapshot(&test_ownership_snapshot()).is_ok());
    }

    #[test]
    fn ownership_missing_owner_rejected() {
        let mut snapshot = test_ownership_snapshot();
        snapshot.entries[0].owner_node_id = "".into();
        let result = validate_ownership_snapshot(&snapshot);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&OwnershipViolation::MissingOwner)
        );
    }

    #[test]
    fn ownership_duplicate_namespace_rejected() {
        let mut snapshot = test_ownership_snapshot();
        snapshot.entries.push(snapshot.entries[0].clone());
        let result = validate_ownership_snapshot(&snapshot);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&OwnershipViolation::DuplicateNamespace)
        );
    }

    #[test]
    fn ownership_cross_installation_rejected() {
        let mut snapshot = test_ownership_snapshot();
        snapshot.entries[0].installation_id = "installation-b".into();
        let result = validate_ownership_snapshot(&snapshot);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains(&OwnershipViolation::CrossInstallation)
        );
    }

    #[test]
    fn ownership_namespace_locally_owned() {
        let snapshot = test_ownership_snapshot();
        assert!(is_namespace_locally_owned(&snapshot, "economy"));
        assert!(!is_namespace_locally_owned(&snapshot, "mining"));
    }

    #[test]
    fn ownership_not_locally_owned_by_wrong_node() {
        let mut snapshot = test_ownership_snapshot();
        snapshot.entries[0].owner_node_id = "node-2".into();
        assert!(!is_namespace_locally_owned(&snapshot, "economy"));
    }

    fn test_producer_quota() -> ProducerQuotaState {
        ProducerQuotaState {
            producer_id: "producer-a".into(),
            in_flight_publishes: 5,
            max_in_flight_publishes: 100,
            queued_publishes: 0,
            max_queued_publishes: 50,
            in_flight_bytes: 1000,
            max_in_flight_bytes: 100_000,
        }
    }

    fn test_namespace_quota() -> NamespaceQuotaState {
        NamespaceQuotaState {
            namespace: "economy".into(),
            in_flight_publishes: 10,
            max_in_flight_publishes: 200,
        }
    }

    fn test_global_quota() -> GlobalQuotaState {
        GlobalQuotaState {
            in_flight_publishes: 20,
            max_in_flight_publishes: 500,
            reserved_p0_capacity: 50,
            used_p0_capacity: 10,
        }
    }

    #[test]
    fn quota_under_limit_admitted() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let result = evaluate_admission(
            &producer,
            &test_producer_quota(),
            &test_namespace_quota(),
            &test_global_quota(),
            100,
            true,
        );
        assert_eq!(result.decision, AdmissionDecision::Admitted);
    }

    #[test]
    fn per_producer_in_flight_quota_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let mut pq = test_producer_quota();
        pq.in_flight_publishes = 100;
        let result = evaluate_admission(
            &producer,
            &pq,
            &test_namespace_quota(),
            &test_global_quota(),
            100,
            true,
        );
        assert_eq!(result.decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(AdmissionRejectionReason::ProducerInFlightExceeded)
        );
    }

    #[test]
    fn per_producer_queue_quota_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let mut pq = test_producer_quota();
        pq.queued_publishes = 50;
        let result = evaluate_admission(
            &producer,
            &pq,
            &test_namespace_quota(),
            &test_global_quota(),
            100,
            true,
        );
        assert_eq!(result.decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(AdmissionRejectionReason::ProducerQueueExceeded)
        );
    }

    #[test]
    fn per_producer_bytes_quota_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let pq = test_producer_quota();
        let result = evaluate_admission(
            &producer,
            &pq,
            &test_namespace_quota(),
            &test_global_quota(),
            100_000,
            true,
        );
        assert_eq!(result.decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(AdmissionRejectionReason::ProducerBytesExceeded)
        );
    }

    #[test]
    fn namespace_quota_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let mut nq = test_namespace_quota();
        nq.in_flight_publishes = 200;
        let result = evaluate_admission(
            &producer,
            &test_producer_quota(),
            &nq,
            &test_global_quota(),
            100,
            true,
        );
        assert_eq!(result.decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(AdmissionRejectionReason::NamespaceInFlightExceeded)
        );
    }

    #[test]
    fn global_quota_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let gq = GlobalQuotaState {
            in_flight_publishes: 500,
            max_in_flight_publishes: 500,
            reserved_p0_capacity: 0,
            used_p0_capacity: 0,
        };
        let result = evaluate_admission(
            &producer,
            &test_producer_quota(),
            &test_namespace_quota(),
            &gq,
            100,
            true,
        );
        assert_eq!(result.decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(AdmissionRejectionReason::GlobalInFlightExceeded)
        );
    }

    #[test]
    fn lower_priority_cannot_consume_p0_reserved_capacity() {
        let mut producer = test_producer(ProducerLifecycleState::Active);
        producer.priority_class = PriorityClass::P2;
        let gq = GlobalQuotaState {
            in_flight_publishes: 460,
            max_in_flight_publishes: 500,
            reserved_p0_capacity: 50,
            used_p0_capacity: 10,
        };
        let result = evaluate_admission(
            &producer,
            &test_producer_quota(),
            &test_namespace_quota(),
            &gq,
            100,
            true,
        );
        assert_eq!(result.decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(AdmissionRejectionReason::P0CapacityReserved)
        );
    }

    #[test]
    fn p0_producer_admitted_with_reserved_capacity() {
        let mut producer = test_producer(ProducerLifecycleState::Active);
        producer.priority_class = PriorityClass::P0;
        let gq = GlobalQuotaState {
            in_flight_publishes: 460,
            max_in_flight_publishes: 500,
            reserved_p0_capacity: 50,
            used_p0_capacity: 10,
        };
        let result = evaluate_admission(
            &producer,
            &test_producer_quota(),
            &test_namespace_quota(),
            &gq,
            100,
            true,
        );
        assert_eq!(result.decision, AdmissionDecision::Admitted);
    }

    #[test]
    fn admission_disabled_producer_rejected() {
        let producer = test_producer(ProducerLifecycleState::Disabled);
        let result = evaluate_admission(
            &producer,
            &test_producer_quota(),
            &test_namespace_quota(),
            &test_global_quota(),
            100,
            true,
        );
        assert_eq!(result.decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(AdmissionRejectionReason::ProducerDisabled)
        );
    }

    #[test]
    fn admission_not_locally_owned_rejected() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let result = evaluate_admission(
            &producer,
            &test_producer_quota(),
            &test_namespace_quota(),
            &test_global_quota(),
            100,
            false,
        );
        assert_eq!(result.decision, AdmissionDecision::Rejected);
        assert_eq!(
            result.rejection_reason,
            Some(AdmissionRejectionReason::NotLocallyOwned)
        );
    }

    #[test]
    fn bounded_ingress_cannot_grow_unbounded() {
        let producer = test_producer(ProducerLifecycleState::Active);
        let pq = ProducerQuotaState {
            producer_id: "producer-a".into(),
            in_flight_publishes: i32::MAX - 1,
            max_in_flight_publishes: i32::MAX,
            queued_publishes: 0,
            max_queued_publishes: 50,
            in_flight_bytes: 0,
            max_in_flight_bytes: 100_000,
        };
        let gq = GlobalQuotaState {
            in_flight_publishes: 0,
            max_in_flight_publishes: i32::MAX,
            reserved_p0_capacity: 0,
            used_p0_capacity: 0,
        };
        let result = evaluate_admission(&producer, &pq, &test_namespace_quota(), &gq, 100, true);
        assert_eq!(result.decision, AdmissionDecision::Admitted);
    }

    #[test]
    fn kafka_profile_valid_p0() {
        let profile = KafkaDurabilityProfile {
            profile_id: "p0".into(),
            replication_factor: 5,
            min_insync_replicas: 5,
            required_acks: "all".into(),
            topic_reference: "events".into(),
            profile_version: 1,
        };
        assert!(validate_p0_kafka_profile(&profile).is_ok());
    }

    #[test]
    fn kafka_profile_weakened_rf_rejected() {
        let profile = KafkaDurabilityProfile {
            profile_id: "weak".into(),
            replication_factor: 3,
            min_insync_replicas: 3,
            required_acks: "all".into(),
            topic_reference: "events".into(),
            profile_version: 1,
        };
        assert_eq!(
            validate_p0_kafka_profile(&profile),
            Err(ProfileValidationError::WeakenedDurability)
        );
    }

    #[test]
    fn kafka_profile_invalid_acks_rejected() {
        let profile = KafkaDurabilityProfile {
            profile_id: "bad-acks".into(),
            replication_factor: 5,
            min_insync_replicas: 5,
            required_acks: "1".into(),
            topic_reference: "events".into(),
            profile_version: 1,
        };
        assert_eq!(
            validate_kafka_profile(&profile),
            Err(ProfileValidationError::InvalidAcks)
        );
    }

    #[test]
    fn namespace_wildcard_matching() {
        assert!(namespace_matches("*", "anything"));
        assert!(namespace_matches("economy", "economy"));
        assert!(!namespace_matches("economy", "mining"));
        assert!(namespace_matches("economy.*", "economy.transfer"));
        assert!(!namespace_matches("economy.*", "economy"));
    }

    // --- Sprint 5: Extended Production Profile Tests ---

    fn valid_production_profile() -> KafkaProductionProfile {
        KafkaProductionProfile {
            profile_id: "p0-production".into(),
            profile_version: 1,
            replication_factor: 5,
            min_insync_replicas: 5,
            required_acks: "all".into(),
            enable_idempotence: true,
            delivery_timeout_ms: 120_000,
            request_timeout_ms: 30_000,
            linger_ms: 5,
            max_in_flight_requests_per_connection: 5,
            compression: "lz4".into(),
            retention_ms: 604_800_000,
            retention_bytes: 1_073_741_824,
            cleanup_policy: "delete".into(),
            unclean_leader_election: false,
            topic_deletion_policy: "PROTECTED".into(),
        }
    }

    #[test]
    fn valid_p0_production_profile_accepted() {
        assert!(validate_p0_production_profile(&valid_production_profile()).is_ok());
    }

    #[test]
    fn production_profile_idempotence_required() {
        let mut p = valid_production_profile();
        p.enable_idempotence = false;
        assert_eq!(
            validate_production_profile(&p),
            Err(ExtendedProfileValidationError::IdempotenceRequired)
        );
    }

    #[test]
    fn production_profile_unclean_election_forbidden() {
        let mut p = valid_production_profile();
        p.unclean_leader_election = true;
        assert_eq!(
            validate_production_profile(&p),
            Err(ExtendedProfileValidationError::UncleanElectionForbidden)
        );
    }

    #[test]
    fn production_profile_invalid_cleanup_policy() {
        let mut p = valid_production_profile();
        p.cleanup_policy = "compact,delete".into();
        assert_eq!(
            validate_production_profile(&p),
            Err(ExtendedProfileValidationError::InvalidCleanupPolicy)
        );
    }

    #[test]
    fn production_profile_topic_deletion_forbidden() {
        let mut p = valid_production_profile();
        p.topic_deletion_policy = "ALLOWED".into();
        assert_eq!(
            validate_production_profile(&p),
            Err(ExtendedProfileValidationError::TopicDeletionForbidden)
        );
    }

    #[test]
    fn production_profile_invalid_max_in_flight() {
        let mut p = valid_production_profile();
        p.max_in_flight_requests_per_connection = 6;
        assert_eq!(
            validate_production_profile(&p),
            Err(ExtendedProfileValidationError::InvalidMaxInFlight)
        );
    }

    #[test]
    fn production_profile_invalid_compression() {
        let mut p = valid_production_profile();
        p.compression = "brotli".into();
        assert_eq!(
            validate_production_profile(&p),
            Err(ExtendedProfileValidationError::InvalidCompression)
        );
    }

    #[test]
    fn p0_production_profile_weakened_rf_rejected() {
        let mut p = valid_production_profile();
        p.replication_factor = 3;
        p.min_insync_replicas = 3;
        assert_eq!(
            validate_p0_production_profile(&p),
            Err(ExtendedProfileValidationError::Base(
                ProfileValidationError::WeakenedDurability
            ))
        );
    }

    #[test]
    fn delivery_retrying_variant_exists() {
        let status = DeliveryStatus::DeliveryRetrying;
        assert_ne!(status, DeliveryStatus::DeliveryPending);
        assert_ne!(status, DeliveryStatus::Replicated);
    }
}
