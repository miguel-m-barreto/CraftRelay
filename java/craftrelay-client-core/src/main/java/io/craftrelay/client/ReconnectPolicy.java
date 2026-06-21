package io.craftrelay.client;

import java.time.Duration;

public record ReconnectPolicy(int maxAttempts, Duration initialBackoff, Duration maximumBackoff) {
    public ReconnectPolicy {
        maxAttempts = ContractValidation.positiveInt32(maxAttempts, "maxAttempts");
        if (initialBackoff.isZero() || initialBackoff.isNegative()
                || maximumBackoff.compareTo(initialBackoff) < 0) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.INVALID_ARGUMENT,
                    "reconnect backoff must be positive and bounded");
        }
    }

    public Duration delayForAttempt(int attempt) {
        ContractValidation.positiveInt32(attempt, "attempt");
        long multiplier = 1L << Math.min(attempt - 1, 30);
        long millis;
        try {
            millis = Math.multiplyExact(initialBackoff.toMillis(), multiplier);
        } catch (ArithmeticException overflow) {
            millis = maximumBackoff.toMillis();
        }
        return Duration.ofMillis(Math.min(millis, maximumBackoff.toMillis()));
    }
}
