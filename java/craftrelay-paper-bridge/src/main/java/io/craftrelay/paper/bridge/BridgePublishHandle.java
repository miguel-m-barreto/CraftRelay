package io.craftrelay.paper.bridge;

import io.craftrelay.client.PublishLifecycleSnapshot;
import io.craftrelay.client.SharedBridgeTransportRuntime;
import io.craftrelay.paper.api.LocalAcceptanceResult;
import io.craftrelay.paper.api.PublishHandle;
import io.craftrelay.paper.api.PublishStatusResult;
import io.craftrelay.paper.api.RequiredDurabilityResult;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class BridgePublishHandle implements PublishHandle {
    private final UUID eventId;
    private final SharedBridgeTransportRuntime runtime;
    private final CompletionStage<PublishLifecycleSnapshot> submission;
    private final Runnable onDetach;
    private final java.util.concurrent.atomic.AtomicBoolean detached = new java.util.concurrent.atomic.AtomicBoolean();
    private volatile TrackingState trackingState;

    BridgePublishHandle(
            UUID eventId,
            SharedBridgeTransportRuntime runtime,
            CompletionStage<PublishLifecycleSnapshot> submission,
            boolean attached,
            Runnable onDetach) {
        this.eventId = eventId;
        this.runtime = runtime;
        this.submission = submission;
        this.onDetach = onDetach;
        this.trackingState = attached ? TrackingState.ATTACHED : TrackingState.DETACHED;
        if (!attached) detached.set(true);
    }

    @Override public UUID eventId() { return eventId; }
    @Override public TrackingState trackingState() { return trackingState; }

    @Override
    public CompletionStage<LocalAcceptanceResult> awaitLocalAcceptance(Duration timeout) {
        validateTimeout(timeout);
        if (trackingState == TrackingState.DETACHED) {
            return CompletableFuture.completedFuture(new LocalAcceptanceResult(
                    LocalAcceptanceResult.Status.TRACKING_DETACHED, "bounded client tracking detached"));
        }
        return submission.thenApply(snapshot -> new LocalAcceptanceResult(
                LocalAcceptanceResult.Status.FAKE_ACCEPTED_NON_DURABLE,
                "Sprint 1 fake Agent fixture; no durable acceptance occurred"));
    }

    @Override
    public CompletionStage<RequiredDurabilityResult> awaitRequiredDurability(Duration timeout) {
        validateTimeout(timeout);
        if (trackingState == TrackingState.DETACHED) {
            return CompletableFuture.completedFuture(new RequiredDurabilityResult(
                    RequiredDurabilityResult.Status.TRACKING_DETACHED, "status lookup required"));
        }
        return submission.thenApply(snapshot -> new RequiredDurabilityResult(
                RequiredDurabilityResult.Status.NOT_REACHED,
                "Sprint 1 has no durable backend and cannot issue a durable receipt"));
    }

    @Override
    public CompletionStage<PublishStatusResult> getStatus(Duration timeout) {
        validateTimeout(timeout);
        return runtime.status(eventId.toString(), timeout).thenApply(snapshot -> snapshot
                .map(value -> new PublishStatusResult(
                        PublishStatusResult.Status.FOUND,
                        value.revision(),
                        value.snapshotChecksum(),
                        value.fakeNonDurable()))
                .orElseGet(() -> new PublishStatusResult(
                        PublishStatusResult.Status.NOT_FOUND, 0, new byte[0], false)));
    }

    @Override
    public void detachTracking() {
        trackingState = TrackingState.DETACHED;
        if (detached.compareAndSet(false, true)) onDetach.run();
    }

    private static void validateTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
