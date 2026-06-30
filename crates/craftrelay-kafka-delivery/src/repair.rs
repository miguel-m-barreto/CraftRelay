use super::profile::ProfileDriftKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairClassification {
    EligibleForRetry,
    ProfileDrift { drift_kinds: Vec<ProfileDriftKind> },
    Stuck { reason: String },
    Inconsistent { detail: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairCandidate {
    pub event_id: String,
    pub stream_key: String,
    pub classification: RepairClassification,
    pub attempt_count: i32,
    pub blocked_reason: Option<String>,
}

pub fn classify_for_repair(
    delivery_status: &str,
    attempt_count: i32,
    blocked_reason: Option<&str>,
    profile_drifts: &[ProfileDriftKind],
    delivery_checksum_valid: bool,
) -> RepairClassification {
    if !delivery_checksum_valid {
        return RepairClassification::Inconsistent {
            detail: format!(
                "delivery checksum invalid for status={delivery_status}, reason={blocked_reason:?}",
            ),
        };
    }

    if !profile_drifts.is_empty() {
        return RepairClassification::ProfileDrift {
            drift_kinds: profile_drifts.to_vec(),
        };
    }

    if delivery_status == "DELIVERY_BLOCKED" {
        return RepairClassification::EligibleForRetry;
    }

    let max = super::retry::MAX_RETRY_ATTEMPTS;
    if delivery_status == "DELIVERY_RETRYING" && attempt_count >= max {
        return RepairClassification::Stuck {
            reason: format!("retry attempts exhausted: {attempt_count} >= {max}"),
        };
    }

    RepairClassification::Stuck {
        reason: format!(
            "unresolvable delivery state: status={delivery_status}, attempts={attempt_count}, reason={blocked_reason:?}",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_with_valid_checksum_and_no_drift_eligible() {
        let result = classify_for_repair("DELIVERY_BLOCKED", 3, Some("broker down"), &[], true);
        assert_eq!(result, RepairClassification::EligibleForRetry);
    }

    #[test]
    fn profile_drift_detected() {
        let drifts = vec![ProfileDriftKind::RfWeakened];
        let result = classify_for_repair("DELIVERY_BLOCKED", 1, None, &drifts, true);
        assert_eq!(
            result,
            RepairClassification::ProfileDrift {
                drift_kinds: vec![ProfileDriftKind::RfWeakened]
            }
        );
    }

    #[test]
    fn invalid_checksum_is_inconsistent() {
        let result = classify_for_repair("DELIVERY_BLOCKED", 1, None, &[], false);
        assert!(matches!(result, RepairClassification::Inconsistent { .. }));
    }

    #[test]
    fn retrying_past_max_is_stuck() {
        let result = classify_for_repair(
            "DELIVERY_RETRYING",
            super::super::retry::MAX_RETRY_ATTEMPTS,
            None,
            &[],
            true,
        );
        assert!(matches!(result, RepairClassification::Stuck { .. }));
    }

    #[test]
    fn unknown_status_is_stuck() {
        let result = classify_for_repair("SOME_OTHER_STATUS", 0, None, &[], true);
        assert!(matches!(result, RepairClassification::Stuck { .. }));
    }

    #[test]
    fn inconsistent_takes_precedence_over_drift() {
        let drifts = vec![ProfileDriftKind::RfWeakened];
        let result = classify_for_repair("DELIVERY_BLOCKED", 1, None, &drifts, false);
        assert!(matches!(result, RepairClassification::Inconsistent { .. }));
    }

    #[test]
    fn drift_takes_precedence_over_eligible() {
        let drifts = vec![ProfileDriftKind::AcksWeakened];
        let result = classify_for_repair("DELIVERY_BLOCKED", 1, None, &drifts, true);
        assert!(matches!(result, RepairClassification::ProfileDrift { .. }));
    }
}
