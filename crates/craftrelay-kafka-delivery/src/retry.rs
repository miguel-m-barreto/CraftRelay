pub const MAX_RETRY_ATTEMPTS: i32 = 32;
pub const BASE_BACKOFF_MS: i64 = 1_000;
pub const MAX_BACKOFF_MS: i64 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry { next_retry_at_ms: i64 },
    Exhausted,
}

pub fn compute_next_retry(attempt_count: i32, now_ms: i64) -> RetryDecision {
    if attempt_count >= MAX_RETRY_ATTEMPTS {
        return RetryDecision::Exhausted;
    }
    let exponent = (attempt_count - 1).max(0) as u32;
    let backoff = BASE_BACKOFF_MS
        .saturating_mul(2_i64.saturating_pow(exponent))
        .min(MAX_BACKOFF_MS);
    RetryDecision::Retry {
        next_retry_at_ms: now_ms + backoff,
    }
}

pub fn should_retry(error: &super::producer::SendError, attempt_count: i32) -> bool {
    error.is_retriable() && attempt_count < MAX_RETRY_ATTEMPTS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer::SendError;

    #[test]
    fn first_retry_is_base_backoff_from_now() {
        let now = 10_000;
        let decision = compute_next_retry(1, now);
        assert_eq!(
            decision,
            RetryDecision::Retry {
                next_retry_at_ms: now + BASE_BACKOFF_MS
            }
        );
    }

    #[test]
    fn exponential_growth() {
        let now = 0;
        let d1 = compute_next_retry(1, now);
        let d2 = compute_next_retry(2, now);
        let d3 = compute_next_retry(3, now);

        assert_eq!(
            d1,
            RetryDecision::Retry {
                next_retry_at_ms: 1_000
            }
        );
        assert_eq!(
            d2,
            RetryDecision::Retry {
                next_retry_at_ms: 2_000
            }
        );
        assert_eq!(
            d3,
            RetryDecision::Retry {
                next_retry_at_ms: 4_000
            }
        );
    }

    #[test]
    fn capped_at_max_backoff() {
        let now = 0;
        let decision = compute_next_retry(30, now);
        match decision {
            RetryDecision::Retry { next_retry_at_ms } => {
                assert!(next_retry_at_ms <= MAX_BACKOFF_MS);
            }
            _ => panic!("expected Retry"),
        }
    }

    #[test]
    fn exhausted_after_max_retry_attempts() {
        assert_eq!(
            compute_next_retry(MAX_RETRY_ATTEMPTS, 0),
            RetryDecision::Exhausted
        );
        assert_eq!(
            compute_next_retry(MAX_RETRY_ATTEMPTS + 1, 0),
            RetryDecision::Exhausted
        );
    }

    #[test]
    fn retriable_errors_allow_retry() {
        assert!(should_retry(&SendError::BrokerUnavailable, 1));
        assert!(should_retry(&SendError::Timeout, 1));
        assert!(should_retry(&SendError::QueueFull, 1));
    }

    #[test]
    fn permanent_errors_do_not_retry() {
        assert!(!should_retry(&SendError::TopicNotFound, 1));
        assert!(!should_retry(&SendError::AuthorizationFailed, 1));
        assert!(!should_retry(&SendError::MessageTooLarge, 1));
    }

    #[test]
    fn should_retry_respects_attempt_count() {
        assert!(should_retry(&SendError::BrokerUnavailable, 1));
        assert!(should_retry(
            &SendError::BrokerUnavailable,
            MAX_RETRY_ATTEMPTS - 1
        ));
        assert!(!should_retry(
            &SendError::BrokerUnavailable,
            MAX_RETRY_ATTEMPTS
        ));
    }
}
