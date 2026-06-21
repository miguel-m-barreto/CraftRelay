package io.craftrelay.paper.api;

/** Sprint 1 fake acceptance is explicitly non-durable. */
public record LocalAcceptanceResult(Status status, String detail) {
    public enum Status { FAKE_ACCEPTED_NON_DURABLE, NOT_ACCEPTED, TRANSPORT_INDETERMINATE, TRACKING_DETACHED }
}
