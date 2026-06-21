package io.craftrelay.paper.api;
import java.util.UUID;
public record EventReference(UUID installationId, UUID eventId) {
    public EventReference {
        java.util.Objects.requireNonNull(installationId, "installationId");
        java.util.Objects.requireNonNull(eventId, "eventId");
        if (eventId.version() != 7 || eventId.variant() != 2) {
            throw new IllegalArgumentException("eventId must be UUIDv7");
        }
    }
}
