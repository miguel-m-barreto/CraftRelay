package io.craftrelay.paper.api;
public record SubmissionResult(Status status) { public enum Status { NOT_ACCEPTED, TRANSPORT_INDETERMINATE, TRACKING_DETACHED } }

