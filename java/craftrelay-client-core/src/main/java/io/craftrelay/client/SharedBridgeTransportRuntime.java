package io.craftrelay.client;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** One multiplexed transport boundary shared by every logical producer on a Paper server. */
public final class SharedBridgeTransportRuntime {
    private final AgentClient agent;
    private final QueryServiceClient queryService;
    private final ClientObservability.Metrics metrics;

    public SharedBridgeTransportRuntime(
            AgentClient agent,
            QueryServiceClient queryService,
            ClientObservability.Metrics metrics) {
        this.agent = agent;
        this.queryService = queryService;
        this.metrics = metrics;
    }

    public CompletionStage<PublishLifecycleSnapshot> submit(
            String authenticatedProducerId, ClientPublishRequest immutableRequest) {
        metrics.publishSubmitted(authenticatedProducerId);
        return agent.submit(immutableRequest);
    }

    /** Retry preserves the exact immutable request, including event ID and producer sequence. */
    public CompletionStage<PublishLifecycleSnapshot> retrySameRequest(
            String authenticatedProducerId, ClientPublishRequest immutableRequest) {
        metrics.reconnectAttempt(authenticatedProducerId);
        return agent.submit(immutableRequest);
    }

    public CompletionStage<Optional<PublishLifecycleSnapshot>> status(
            String eventId, Duration timeout) {
        return agent.getStatus(eventId, timeout);
    }

    public void recordPublishTrackingDetached(String authenticatedProducerId) {
        metrics.publishTrackingDetached(authenticatedProducerId);
    }

    public void recordWatchDetached(String authenticatedProducerId, String reason) {
        metrics.watchDetached(authenticatedProducerId, reason);
    }

    public CompletionStage<QueryServiceClient.Response> query(
            String authenticatedProducerId,
            String contractHandle,
            byte[] parameters,
            String freshnessMode,
            Duration timeout) {
        metrics.querySubmitted(authenticatedProducerId);
        return queryService.query(contractHandle, parameters, freshnessMode, timeout);
    }
}
