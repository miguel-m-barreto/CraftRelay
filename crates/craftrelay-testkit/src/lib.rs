#![forbid(unsafe_code)]

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SharedVectors {
    pub version: i32,
    pub uuid_v7: Vec<UuidVector>,
    pub positive_boundaries: PositiveBoundaries,
    pub payload_digest: Vec<DigestVector>,
    pub fingerprint: Vec<FingerprintVector>,
    pub installation_scope: Vec<InstallationScopeVector>,
    pub metadata_canonicalization: Vec<MetadataCanonicalizationVector>,
    pub metadata_rejections: Vec<MetadataRejectionVector>,
    pub envelope_input: Vec<CanonicalVector>,
    pub envelope_checksum: Vec<CanonicalVector>,
    pub revision_checksums: Vec<RevisionVector>,
    pub projection_barrier: ProjectionBarrierVector,
    pub consistency_token: ConsistencyTokenVector,
    pub query_freshness: Vec<QueryFreshnessVector>,
    pub watch_freshness: Vec<WatchFreshnessVector>,
    pub sprint2_producer_registration: Vec<ProducerRegistrationVector>,
    pub sprint2_acl: Vec<AclVector>,
    pub sprint2_policy_resolution: Vec<PolicyResolutionVector>,
    pub sprint2_ownership: Vec<OwnershipVector>,
    pub sprint2_quota_admission: Vec<QuotaAdmissionVector>,
    pub sprint2_kafka_profile: Vec<KafkaProfileVector>,
}
#[derive(Debug, Deserialize)]
pub struct UuidVector {
    pub value: String,
    pub valid: bool,
}
#[derive(Debug, Deserialize)]
pub struct PositiveBoundaries {
    pub int32_min: i32,
    pub int32_max: i32,
    pub int64_min: i64,
    pub int64_max: i64,
}
#[derive(Debug, Deserialize)]
pub struct DigestVector {
    pub payload_utf8: String,
    pub sha256: String,
}
#[derive(Debug, Deserialize)]
pub struct FingerprintVector {
    pub canonical_hex: String,
    pub sha256: String,
}
#[derive(Debug, Deserialize)]
pub struct InstallationScopeVector {
    pub installation_id: String,
    pub name: String,
    pub scoped: String,
}
#[derive(Debug, Deserialize)]
pub struct CanonicalVector {
    pub canonical: String,
    pub sha256: String,
}
#[derive(Debug, Deserialize)]
pub struct RevisionVector {
    pub revision: i64,
    pub canonical: String,
    pub sha256: String,
}

#[derive(Debug, Deserialize)]
pub struct MetadataCanonicalizationVector {
    pub entries: Vec<MetadataVectorEntry>,
    pub canonical: String,
    pub sha256: String,
}
#[derive(Debug, Deserialize)]
pub struct MetadataVectorEntry {
    pub key: String,
    pub value: String,
}
#[derive(Debug, Deserialize)]
pub struct MetadataRejectionVector {
    pub entries: Vec<MetadataVectorEntry>,
    pub rejection: String,
}
#[derive(Debug, Deserialize)]
pub struct ProjectionBarrierVector {
    pub barrier_version: i32,
    pub query_definition_version: i32,
    pub topology_version: i64,
    pub routing_version: i64,
    pub canonical: String,
    pub sha256: String,
    pub required_next_offset: std::collections::BTreeMap<String, i64>,
    pub exclusive: bool,
}
#[derive(Debug, Deserialize)]
pub struct ConsistencyTokenVector {
    pub token_version: i32,
    pub installation_id: String,
    pub authenticated_producer_id: String,
    pub projector_id: String,
    pub projection_name: String,
    pub query_scope: String,
    pub expires_at_unix_millis: i64,
    pub canonical: String,
    pub checksum: String,
    pub fixture_key_utf8: String,
    pub mac: String,
    pub authenticated: bool,
}
#[derive(Debug, Deserialize)]
pub struct QueryFreshnessVector {
    pub mode: String,
    pub proof: String,
    pub current: bool,
    pub authoritative: bool,
    pub silent_downgrade: bool,
}
#[derive(Debug, Deserialize)]
pub struct WatchFreshnessVector {
    pub state: String,
    pub current: bool,
    pub reason: String,
}

#[derive(Debug, Deserialize)]
pub struct ProducerRegistrationVector {
    pub producer_id: String,
    pub installation_id: String,
    pub lifecycle: String,
    pub valid: bool,
    #[serde(default)]
    pub rejection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AclRuleVector {
    pub rule_id: String,
    pub action: String,
    pub pattern: String,
    pub decision: String,
    pub priority: i32,
}

#[derive(Debug, Deserialize)]
pub struct AclVector {
    pub principal_installation: String,
    pub scope_installation: String,
    pub credential_status: String,
    pub namespace: String,
    pub action: String,
    pub rules: Vec<AclRuleVector>,
    pub expected_decision: String,
    #[serde(default)]
    pub expected_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PolicyResolutionVector {
    pub description: String,
    pub installation_id: String,
    pub producer_lifecycle: String,
    pub acl_decision: String,
    pub namespace: String,
    pub owned_namespaces: Vec<String>,
    pub requested_durability: String,
    pub minimum_durability: String,
    pub expected_admission: String,
    #[serde(default)]
    pub expected_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct OwnershipEntryVector {
    pub namespace: String,
    pub owner_node: String,
    pub installation: String,
    pub mode: String,
}

#[derive(Debug, Deserialize)]
pub struct OwnershipVector {
    pub description: String,
    pub mode: String,
    pub entries: Vec<OwnershipEntryVector>,
    pub installation_id: String,
    pub node_id: String,
    pub valid: bool,
    #[serde(default)]
    pub violation: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct QuotaAdmissionVector {
    pub description: String,
    pub producer_priority: String,
    pub in_flight: i32,
    pub max_in_flight: i32,
    pub global_in_flight: i32,
    pub global_max: i32,
    pub reserved_p0: i32,
    pub used_p0: i32,
    pub namespace_owned: bool,
    pub expected: String,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct KafkaProfileVector {
    pub description: String,
    pub rf: i32,
    pub min_isr: i32,
    pub acks: String,
    pub valid_p0: bool,
    #[serde(default)]
    pub error: Option<String>,
}

pub fn load_vectors() -> SharedVectors {
    serde_json::from_str(include_str!("../../../test-vectors/v1/shared-vectors.json"))
        .expect("checked-in vectors must parse")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_vectors_are_valid() {
        let vectors = load_vectors();
        assert_eq!(vectors.version, 1);
        for vector in vectors.uuid_v7 {
            assert_eq!(
                craftrelay_domain::is_canonical_uuid_v7(&vector.value),
                vector.valid
            );
        }
        assert_eq!(vectors.positive_boundaries.int32_min, 1);
        assert_eq!(vectors.positive_boundaries.int64_min, 1);
        let payload = &vectors.payload_digest[0];
        let actual = craftrelay_domain::sha256(payload.payload_utf8.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(actual, payload.sha256);
        let fingerprint = &vectors.fingerprint[0];
        let canonical = (0..fingerprint.canonical_hex.len())
            .step_by(2)
            .map(|index| {
                u8::from_str_radix(&fingerprint.canonical_hex[index..index + 2], 16).expect("hex")
            })
            .collect::<Vec<_>>();
        let actual = craftrelay_domain::sha256(&canonical)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(actual, fingerprint.sha256);
        for scope in &vectors.installation_scope {
            assert_eq!(
                craftrelay_domain::scoped_name(&scope.installation_id, &scope.name),
                scope.scoped
            );
        }
        for metadata in &vectors.metadata_canonicalization {
            let entries = metadata
                .entries
                .iter()
                .map(|entry| craftrelay_domain::MetadataEntry {
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                })
                .collect::<Vec<_>>();
            let canonical = craftrelay_domain::canonical_metadata_text(&entries).expect("metadata");
            assert_eq!(canonical, metadata.canonical);
            assert_eq!(
                hex(&craftrelay_domain::sha256(canonical.as_bytes())),
                metadata.sha256
            );
        }
        for rejection in &vectors.metadata_rejections {
            let entries = rejection
                .entries
                .iter()
                .map(|entry| craftrelay_domain::MetadataEntry {
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                })
                .collect::<Vec<_>>();
            assert_eq!(
                craftrelay_domain::canonicalize_metadata(&entries),
                Err(craftrelay_domain::ValidationError::DuplicateMetadataKey)
            );
            assert_eq!(rejection.rejection, "DUPLICATE_METADATA_KEY");
        }
        for envelope in &vectors.envelope_input {
            assert_eq!(
                hex(&craftrelay_domain::sha256(envelope.canonical.as_bytes())),
                envelope.sha256
            );
        }
        let envelope = &vectors.envelope_checksum[0];
        let actual = craftrelay_domain::sha256(envelope.canonical.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(actual, envelope.sha256);
        let revision = &vectors.revision_checksums[0];
        assert_eq!(revision.revision, 1);
        let actual = craftrelay_domain::sha256(revision.canonical.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(actual, revision.sha256);
        assert!(vectors.projection_barrier.exclusive);
        assert_eq!(vectors.projection_barrier.barrier_version, 1);
        assert_eq!(vectors.projection_barrier.query_definition_version, 1);
        assert!(
            vectors
                .projection_barrier
                .required_next_offset
                .values()
                .all(|offset| *offset >= 0)
        );
        assert_eq!(
            hex(&craftrelay_domain::sha256(
                vectors.projection_barrier.canonical.as_bytes()
            )),
            vectors.projection_barrier.sha256
        );
        assert_eq!(vectors.consistency_token.token_version, 1);
        assert!(vectors.consistency_token.authenticated);
        assert_eq!(
            hex(&craftrelay_domain::sha256(
                vectors.consistency_token.canonical.as_bytes()
            )),
            vectors.consistency_token.checksum
        );
        assert_eq!(
            hex(&craftrelay_domain::hmac_sha256(
                vectors.consistency_token.fixture_key_utf8.as_bytes(),
                vectors.consistency_token.canonical.as_bytes()
            )),
            vectors.consistency_token.mac
        );
        assert!(
            vectors
                .query_freshness
                .iter()
                .all(|value| !value.silent_downgrade)
        );
        assert!(
            vectors
                .watch_freshness
                .iter()
                .filter(|value| value.state != "CURRENT")
                .all(|value| !value.current)
        );
        let protocol = include_str!("../../../proto/craftrelay/v1/contracts.proto");
        let envelope = protocol
            .split("message StoredEventEnvelope")
            .nth(1)
            .expect("envelope")
            .split('}')
            .next()
            .expect("body");
        for forbidden in [
            "bytes payload =",
            "delivery_status",
            "kafka_partition =",
            "kafka_offset",
            "record_offset",
            "retry_count",
            "projection_status",
            "projection_progress",
            "retention_status",
            "payload_present",
            "payload_deleted",
            "payload_deletion",
        ] {
            assert!(
                !envelope.contains(forbidden),
                "StoredEventEnvelope contains forbidden mutable field: {forbidden}"
            );
        }
        assert!(protocol.contains("required_next_offset"));
        assert!(protocol.contains("next_offset_to_resolve"));

        // Sprint 2 vector assertions
        for reg in &vectors.sprint2_producer_registration {
            let lifecycle = match reg.lifecycle.as_str() {
                "ACTIVE" => craftrelay_domain::ProducerLifecycleState::Active,
                "DISABLED" => craftrelay_domain::ProducerLifecycleState::Disabled,
                "SUSPENDED" => craftrelay_domain::ProducerLifecycleState::Suspended,
                other => panic!("unknown lifecycle: {other}"),
            };
            let producer = craftrelay_domain::RegisteredProducer {
                installation_id: reg.installation_id.clone(),
                producer_id: reg.producer_id.clone(),
                producer_instance_id: "instance-1".into(),
                integration_id: "reference".into(),
                paper_plugin_id: "ReferencePlugin".into(),
                lifecycle_state: lifecycle,
                allowed_namespaces: vec!["economy".into()],
                priority_class: craftrelay_domain::PriorityClass::P1,
                quota_class: craftrelay_domain::QuotaClass::Standard,
                policy_binding_id: "binding-1".into(),
            };
            let result =
                craftrelay_domain::validate_producer_registration(&producer, "installation-a", &[]);
            assert_eq!(
                result.is_ok(),
                reg.valid,
                "producer registration vector: {}",
                reg.producer_id
            );
        }

        for acl in &vectors.sprint2_acl {
            let cred_status = match acl.credential_status.as_str() {
                "ACTIVE" => craftrelay_domain::CredentialStatus::Active,
                "REVOKED" => craftrelay_domain::CredentialStatus::Revoked,
                "EXPIRED" => craftrelay_domain::CredentialStatus::Expired,
                "UNKNOWN" => craftrelay_domain::CredentialStatus::Unknown,
                other => panic!("unknown credential status: {other}"),
            };
            let principal = craftrelay_domain::AclPrincipal {
                producer_id: "producer-a".into(),
                installation_id: acl.principal_installation.clone(),
                credential: craftrelay_domain::CredentialReference {
                    credential_id: "cred-1".into(),
                    kind: craftrelay_domain::CredentialKind::FakeTestOnly,
                    revision: 1,
                    status: cred_status,
                    installation_id: acl.principal_installation.clone(),
                },
            };
            let rules: Vec<craftrelay_domain::AclRule> = acl
                .rules
                .iter()
                .map(|r| craftrelay_domain::AclRule {
                    rule_id: r.rule_id.clone(),
                    action: match r.action.as_str() {
                        "PUBLISH" => craftrelay_domain::AclAction::Publish,
                        "QUERY" => craftrelay_domain::AclAction::Query,
                        other => panic!("unknown action: {other}"),
                    },
                    namespace_pattern: r.pattern.clone(),
                    decision: match r.decision.as_str() {
                        "ALLOW" => craftrelay_domain::AclDecision::Allow,
                        "DENY" => craftrelay_domain::AclDecision::Deny,
                        other => panic!("unknown decision: {other}"),
                    },
                    priority: r.priority,
                })
                .collect();
            let result = craftrelay_domain::evaluate_acl(
                &principal,
                &acl.scope_installation,
                &acl.namespace,
                craftrelay_domain::AclAction::Publish,
                &rules,
                1,
            );
            let expected = match acl.expected_decision.as_str() {
                "ALLOW" => craftrelay_domain::AclDecision::Allow,
                "DENY" => craftrelay_domain::AclDecision::Deny,
                other => panic!("unknown expected decision: {other}"),
            };
            assert_eq!(
                result.decision, expected,
                "ACL vector: ns={}",
                acl.namespace
            );
        }

        for ownership in &vectors.sprint2_ownership {
            let entries: Vec<craftrelay_domain::NamespaceOwnershipEntry> = ownership
                .entries
                .iter()
                .map(|e| craftrelay_domain::NamespaceOwnershipEntry {
                    namespace: e.namespace.clone(),
                    owner_node_id: e.owner_node.clone(),
                    owner_agent_id: if e.owner_node.is_empty() {
                        "".into()
                    } else {
                        "agent-1".into()
                    },
                    installation_id: e.installation.clone(),
                    mode: craftrelay_domain::OwnershipMode::NodeLocal,
                })
                .collect();
            let snapshot = craftrelay_domain::OwnershipSnapshot {
                id: craftrelay_domain::OwnershipSnapshotId {
                    snapshot_id: "snap-1".into(),
                    snapshot_version: 1,
                },
                installation_id: ownership.installation_id.clone(),
                node_id: ownership.node_id.clone(),
                mode: craftrelay_domain::OwnershipMode::NodeLocal,
                entries,
            };
            let result = craftrelay_domain::validate_ownership_snapshot(&snapshot);
            assert_eq!(
                result.is_ok(),
                ownership.valid,
                "ownership vector: {}",
                ownership.description
            );
        }

        for quota in &vectors.sprint2_quota_admission {
            let priority = match quota.producer_priority.as_str() {
                "P0" => craftrelay_domain::PriorityClass::P0,
                "P1" => craftrelay_domain::PriorityClass::P1,
                "P2" => craftrelay_domain::PriorityClass::P2,
                "BACKGROUND" => craftrelay_domain::PriorityClass::Background,
                other => panic!("unknown priority: {other}"),
            };
            let producer = craftrelay_domain::RegisteredProducer {
                installation_id: "installation-a".into(),
                producer_id: "producer-a".into(),
                producer_instance_id: "instance-1".into(),
                integration_id: "reference".into(),
                paper_plugin_id: "ReferencePlugin".into(),
                lifecycle_state: craftrelay_domain::ProducerLifecycleState::Active,
                allowed_namespaces: vec!["economy".into()],
                priority_class: priority,
                quota_class: craftrelay_domain::QuotaClass::Standard,
                policy_binding_id: "binding-1".into(),
            };
            let pq = craftrelay_domain::ProducerQuotaState {
                producer_id: "producer-a".into(),
                in_flight_publishes: quota.in_flight,
                max_in_flight_publishes: quota.max_in_flight,
                queued_publishes: 0,
                max_queued_publishes: 50,
                in_flight_bytes: 0,
                max_in_flight_bytes: 100_000,
            };
            let nq = craftrelay_domain::NamespaceQuotaState {
                namespace: "economy".into(),
                in_flight_publishes: 10,
                max_in_flight_publishes: 200,
            };
            let gq = craftrelay_domain::GlobalQuotaState {
                in_flight_publishes: quota.global_in_flight,
                max_in_flight_publishes: quota.global_max,
                reserved_p0_capacity: quota.reserved_p0,
                used_p0_capacity: quota.used_p0,
            };
            let result = craftrelay_domain::evaluate_admission(
                &producer,
                &pq,
                &nq,
                &gq,
                100,
                quota.namespace_owned,
            );
            let expected = match quota.expected.as_str() {
                "ADMITTED" => craftrelay_domain::AdmissionDecision::Admitted,
                "REJECTED" => craftrelay_domain::AdmissionDecision::Rejected,
                other => panic!("unknown expected: {other}"),
            };
            assert_eq!(
                result.decision, expected,
                "quota vector: {}",
                quota.description
            );
        }

        for profile in &vectors.sprint2_kafka_profile {
            let p = craftrelay_domain::KafkaDurabilityProfile {
                profile_id: "test".into(),
                replication_factor: profile.rf,
                min_insync_replicas: profile.min_isr,
                required_acks: profile.acks.clone(),
                topic_reference: "events".into(),
                profile_version: 1,
            };
            let result = craftrelay_domain::validate_p0_kafka_profile(&p);
            assert_eq!(
                result.is_ok(),
                profile.valid_p0,
                "kafka profile vector: {}",
                profile.description
            );
        }

        assert!(protocol.contains("ProducerLifecycleState"));
        assert!(protocol.contains("CredentialReference"));
        assert!(protocol.contains("AclEvaluationResult"));
        assert!(protocol.contains("EffectivePolicyResult"));
        assert!(protocol.contains("OwnershipSnapshot"));
        assert!(protocol.contains("AdmissionResult"));
        assert!(protocol.contains("KafkaDurabilityProfile"));
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
