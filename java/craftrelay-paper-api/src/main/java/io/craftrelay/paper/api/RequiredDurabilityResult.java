package io.craftrelay.paper.api;

/** No Sprint 1 implementation can return durability success. */
public record RequiredDurabilityResult(Status status, String detail) {
    public enum Status { NOT_REACHED, TRANSPORT_INDETERMINATE, TRACKING_DETACHED }
}
