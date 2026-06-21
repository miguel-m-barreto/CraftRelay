package io.craftrelay.client;

import java.util.Arrays;
import java.util.Optional;

public final class LifecycleSnapshotTracker {
    private PublishLifecycleSnapshot current;

    public synchronized Decision apply(PublishLifecycleSnapshot incoming) {
        if (current == null || incoming.revision() > current.revision()) {
            current = incoming;
            return Decision.APPLIED;
        }
        if (incoming.revision() < current.revision()) {
            return Decision.STALE;
        }
        if (Arrays.equals(incoming.snapshotChecksum(), current.snapshotChecksum())) {
            return Decision.DUPLICATE;
        }
        throw ContractValidation.violation(
                ContractViolationException.Code.LIFECYCLE_INTEGRITY_CONFLICT,
                "same lifecycle revision has a different snapshot checksum");
    }

    public synchronized Optional<PublishLifecycleSnapshot> current() {
        return Optional.ofNullable(current);
    }

    public enum Decision { APPLIED, DUPLICATE, STALE }
}
