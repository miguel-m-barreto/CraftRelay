package io.craftrelay.paper.api;
public record ProjectionRequirement(String projectorId, ProjectionName projection) {
    public ProjectionRequirement {
        if (projectorId == null || projectorId.isBlank() || projectorId.length() > 128) {
            throw new IllegalArgumentException("projector ID must contain 1..128 characters");
        }
        java.util.Objects.requireNonNull(projection, "projection");
    }
}
