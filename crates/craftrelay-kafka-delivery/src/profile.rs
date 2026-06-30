use craftrelay_domain::{ExtendedProfileValidationError, KafkaProductionProfile};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileDriftKind {
    VersionDowngrade,
    RfWeakened,
    MinIsrWeakened,
    IdempotenceDisabled,
    UncleanElectionEnabled,
    AcksWeakened,
}

pub fn detect_profile_drift(
    current: &KafkaProductionProfile,
    previous: &KafkaProductionProfile,
) -> Vec<ProfileDriftKind> {
    let mut drifts = Vec::new();

    if current.profile_version < previous.profile_version {
        drifts.push(ProfileDriftKind::VersionDowngrade);
    }
    if current.replication_factor < previous.replication_factor {
        drifts.push(ProfileDriftKind::RfWeakened);
    }
    if current.min_insync_replicas < previous.min_insync_replicas {
        drifts.push(ProfileDriftKind::MinIsrWeakened);
    }
    if !current.enable_idempotence && previous.enable_idempotence {
        drifts.push(ProfileDriftKind::IdempotenceDisabled);
    }
    if current.unclean_leader_election && !previous.unclean_leader_election {
        drifts.push(ProfileDriftKind::UncleanElectionEnabled);
    }
    if current.required_acks != "all"
        && current.required_acks != "-1"
        && (previous.required_acks == "all" || previous.required_acks == "-1")
    {
        drifts.push(ProfileDriftKind::AcksWeakened);
    }

    drifts
}

pub fn is_profile_safe_for_delivery(
    profile: &KafkaProductionProfile,
) -> Result<(), ExtendedProfileValidationError> {
    craftrelay_domain::validate_p0_production_profile(profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_profile() -> KafkaProductionProfile {
        KafkaProductionProfile {
            profile_id: "prod".into(),
            profile_version: 2,
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
    fn no_drift_when_identical() {
        let a = base_profile();
        let b = base_profile();
        assert!(detect_profile_drift(&a, &b).is_empty());
    }

    #[test]
    fn version_downgrade_detected() {
        let mut current = base_profile();
        current.profile_version = 1;
        let previous = base_profile();
        let drifts = detect_profile_drift(&current, &previous);
        assert_eq!(drifts, vec![ProfileDriftKind::VersionDowngrade]);
    }

    #[test]
    fn rf_weakened_detected() {
        let mut current = base_profile();
        current.replication_factor = 3;
        let previous = base_profile();
        let drifts = detect_profile_drift(&current, &previous);
        assert_eq!(drifts, vec![ProfileDriftKind::RfWeakened]);
    }

    #[test]
    fn min_isr_weakened_detected() {
        let mut current = base_profile();
        current.min_insync_replicas = 1;
        let previous = base_profile();
        let drifts = detect_profile_drift(&current, &previous);
        assert_eq!(drifts, vec![ProfileDriftKind::MinIsrWeakened]);
    }

    #[test]
    fn idempotence_disabled_detected() {
        let mut current = base_profile();
        current.enable_idempotence = false;
        let previous = base_profile();
        let drifts = detect_profile_drift(&current, &previous);
        assert_eq!(drifts, vec![ProfileDriftKind::IdempotenceDisabled]);
    }

    #[test]
    fn unclean_election_enabled_detected() {
        let mut current = base_profile();
        current.unclean_leader_election = true;
        let previous = base_profile();
        let drifts = detect_profile_drift(&current, &previous);
        assert_eq!(drifts, vec![ProfileDriftKind::UncleanElectionEnabled]);
    }

    #[test]
    fn acks_weakened_detected() {
        let mut current = base_profile();
        current.required_acks = "1".into();
        let previous = base_profile();
        let drifts = detect_profile_drift(&current, &previous);
        assert_eq!(drifts, vec![ProfileDriftKind::AcksWeakened]);
    }

    #[test]
    fn multiple_drifts_at_once() {
        let mut current = base_profile();
        current.profile_version = 1;
        current.replication_factor = 3;
        current.enable_idempotence = false;
        let previous = base_profile();
        let drifts = detect_profile_drift(&current, &previous);
        assert_eq!(drifts.len(), 3);
        assert!(drifts.contains(&ProfileDriftKind::VersionDowngrade));
        assert!(drifts.contains(&ProfileDriftKind::RfWeakened));
        assert!(drifts.contains(&ProfileDriftKind::IdempotenceDisabled));
    }

    #[test]
    fn is_profile_safe_delegates_to_domain() {
        let profile = base_profile();
        assert!(is_profile_safe_for_delivery(&profile).is_ok());

        let mut bad = base_profile();
        bad.enable_idempotence = false;
        assert!(is_profile_safe_for_delivery(&bad).is_err());
    }
}
