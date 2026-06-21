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
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
