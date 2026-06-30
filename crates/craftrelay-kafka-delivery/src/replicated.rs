use craftrelay_domain::DeliveryStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionError {
    InvalidTransition {
        from: DeliveryStatus,
        to: DeliveryStatus,
    },
}

pub fn validate_transition(
    from: DeliveryStatus,
    to: DeliveryStatus,
) -> Result<(), TransitionError> {
    let valid = matches!(
        (from, to),
        (
            DeliveryStatus::DeliveryPending,
            DeliveryStatus::DeliveryRetrying
        ) | (DeliveryStatus::DeliveryPending, DeliveryStatus::Replicated)
            | (
                DeliveryStatus::DeliveryPending,
                DeliveryStatus::DeliveryBlocked
            )
            | (DeliveryStatus::DeliveryRetrying, DeliveryStatus::Replicated)
            | (
                DeliveryStatus::DeliveryRetrying,
                DeliveryStatus::DeliveryBlocked
            )
            | (
                DeliveryStatus::DeliveryBlocked,
                DeliveryStatus::DeliveryRetrying
            )
    );

    if valid {
        Ok(())
    } else {
        Err(TransitionError::InvalidTransition { from, to })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_to_retrying() {
        assert!(
            validate_transition(
                DeliveryStatus::DeliveryPending,
                DeliveryStatus::DeliveryRetrying
            )
            .is_ok()
        );
    }

    #[test]
    fn pending_to_replicated() {
        assert!(
            validate_transition(DeliveryStatus::DeliveryPending, DeliveryStatus::Replicated)
                .is_ok()
        );
    }

    #[test]
    fn pending_to_blocked() {
        assert!(
            validate_transition(
                DeliveryStatus::DeliveryPending,
                DeliveryStatus::DeliveryBlocked
            )
            .is_ok()
        );
    }

    #[test]
    fn retrying_to_replicated() {
        assert!(
            validate_transition(DeliveryStatus::DeliveryRetrying, DeliveryStatus::Replicated)
                .is_ok()
        );
    }

    #[test]
    fn retrying_to_blocked() {
        assert!(
            validate_transition(
                DeliveryStatus::DeliveryRetrying,
                DeliveryStatus::DeliveryBlocked
            )
            .is_ok()
        );
    }

    #[test]
    fn blocked_to_retrying_repair() {
        assert!(
            validate_transition(
                DeliveryStatus::DeliveryBlocked,
                DeliveryStatus::DeliveryRetrying
            )
            .is_ok()
        );
    }

    #[test]
    fn replicated_to_anything_rejected() {
        assert_eq!(
            validate_transition(DeliveryStatus::Replicated, DeliveryStatus::DeliveryRetrying),
            Err(TransitionError::InvalidTransition {
                from: DeliveryStatus::Replicated,
                to: DeliveryStatus::DeliveryRetrying
            })
        );
        assert!(
            validate_transition(DeliveryStatus::Replicated, DeliveryStatus::DeliveryBlocked)
                .is_err()
        );
        assert!(
            validate_transition(DeliveryStatus::Replicated, DeliveryStatus::DeliveryPending)
                .is_err()
        );
    }

    #[test]
    fn local_accepted_to_retrying_rejected() {
        assert_eq!(
            validate_transition(
                DeliveryStatus::LocalAccepted,
                DeliveryStatus::DeliveryRetrying
            ),
            Err(TransitionError::InvalidTransition {
                from: DeliveryStatus::LocalAccepted,
                to: DeliveryStatus::DeliveryRetrying
            })
        );
    }

    #[test]
    fn blocked_to_replicated_rejected() {
        assert_eq!(
            validate_transition(DeliveryStatus::DeliveryBlocked, DeliveryStatus::Replicated),
            Err(TransitionError::InvalidTransition {
                from: DeliveryStatus::DeliveryBlocked,
                to: DeliveryStatus::Replicated
            })
        );
    }
}
