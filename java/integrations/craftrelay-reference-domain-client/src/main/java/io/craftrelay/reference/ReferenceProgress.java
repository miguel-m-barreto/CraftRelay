package io.craftrelay.reference;

import java.util.UUID;

public record ReferenceProgress(UUID playerId, long progress) {
    public ReferenceProgress {
        if (progress < 0) throw new IllegalArgumentException("progress must be non-negative");
    }
}
