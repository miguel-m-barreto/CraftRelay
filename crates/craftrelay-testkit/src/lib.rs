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
    pub envelope_checksum: Vec<CanonicalVector>,
    pub revision_checksums: Vec<RevisionVector>,
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
}
