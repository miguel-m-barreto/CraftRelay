package io.craftrelay.paper.api;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/** Bounded asynchronous tracking handle. It is not a durable receipt. */
public interface PublishHandle {
    UUID eventId();
    TrackingState trackingState();
    CompletionStage<LocalAcceptanceResult> awaitLocalAcceptance(Duration timeout);
    CompletionStage<RequiredDurabilityResult> awaitRequiredDurability(Duration timeout);
    CompletionStage<PublishStatusResult> getStatus(Duration timeout);
    void detachTracking();

    enum TrackingState { ATTACHED, DETACHED }
}
