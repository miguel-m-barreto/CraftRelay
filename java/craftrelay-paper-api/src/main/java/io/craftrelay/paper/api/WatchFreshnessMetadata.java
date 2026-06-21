package io.craftrelay.paper.api;

public record WatchFreshnessMetadata(
        State state,
        boolean current,
        String reason,
        long observedEntityVersion,
        ProjectionBarrierView observedBarrier) {
    public WatchFreshnessMetadata {
        if (state != State.CURRENT && current) {
            throw new IllegalArgumentException("detached/incomparable/conflicting watch cannot be current");
        }
    }
    public enum State { CURRENT, DETACHED, INCOMPARABLE, CONFLICT, CLOSED }
}
