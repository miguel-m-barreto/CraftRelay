package io.craftrelay.paper.bridge;

import io.craftrelay.client.BoundedTracker;
import io.craftrelay.client.ClientPublishRequest;
import io.craftrelay.client.EnvelopeInput;
import io.craftrelay.client.MetadataEntry;
import io.craftrelay.client.SharedBridgeTransportRuntime;
import io.craftrelay.paper.api.DomainClient;
import io.craftrelay.paper.api.EventContractHandle;
import io.craftrelay.paper.api.PublishHandle;
import io.craftrelay.paper.api.PublishStatusResult;
import io.craftrelay.paper.api.QueryConsistency;
import io.craftrelay.paper.api.QueryFreshnessMetadata;
import io.craftrelay.paper.api.TypedQueryRequest;
import io.craftrelay.paper.api.TypedQueryResponse;
import io.craftrelay.paper.api.WatchFreshnessMetadata;
import io.craftrelay.paper.api.WatchHandle;
import io.craftrelay.paper.api.WatchRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Logical producer facade over the one shared Bridge transport runtime. */
public final class LogicalProducerClient implements DomainClient {
    private final ProducerRegistration registration;
    private final SharedBridgeTransportRuntime runtime;
    private final AtomicLong nextSequence = new AtomicLong(1);
    private final BoundedTracker<UUID, BridgePublishHandle> publishHandles;
    private final BoundedTracker<UUID, ClientPublishRequest> trackedRequests;
    private final AtomicInteger pendingQueries = new AtomicInteger();
    private final AtomicInteger activeWatches = new AtomicInteger();

    public LogicalProducerClient(ProducerRegistration registration, SharedBridgeTransportRuntime runtime) {
        this.registration = registration;
        this.runtime = runtime;
        this.publishHandles = new BoundedTracker<>(registration.manifest().maxPendingPublishes());
        this.trackedRequests = new BoundedTracker<>(registration.manifest().maxPendingPublishes());
    }

    @Override
    public PublishHandle submit(EventContractHandle handle, UUID eventId, byte[] typedPayload) {
        if (!registration.manifest().eventContractHandles().contains(handle.opaqueValue())) {
            throw new IllegalArgumentException("event contract handle is not issued by this integration manifest");
        }
        EnvelopeInput input = new EnvelopeInput(
                eventId.toString(),
                registration.manifest().integrationId(),
                "NODE_LOCAL",
                eventId.toString().getBytes(StandardCharsets.US_ASCII),
                handle.opaqueValue(),
                1,
                "DOMAIN_EVENT",
                eventId.toString(),
                "EVENT",
                "POLICY_RESOLVED_BY_AGENT",
                List.of(new MetadataEntry("integration.id", registration.manifest().integrationId())),
                typedPayload,
                1,
                handle.opaqueValue());
        CompletionStage<io.craftrelay.client.PublishLifecycleSnapshot> submission;
        boolean attached;
        var tracked = trackedRequests.get(eventId);
        if (tracked.isPresent()) {
            ClientPublishRequest request = tracked.get();
            if (!java.util.Arrays.equals(
                    request.envelopeInput().canonicalBytes(), input.canonicalBytes())) {
                throw io.craftrelay.client.ContractValidation.violation(
                        io.craftrelay.client.ContractViolationException.Code.LIFECYCLE_INTEGRITY_CONFLICT,
                        "same event_id retried with different immutable envelope input");
            }
            submission = runtime.retrySameRequest(registration.authenticatedProducerId(), request);
            attached = true;
        } else {
            ClientPublishRequest newRequest = new ClientPublishRequest(input, nextSequence.getAndIncrement());
            attached = trackedRequests.attach(eventId, newRequest);
            submission = attached
                    ? runtime.status(eventId.toString(), Duration.ofMillis(250)).thenCompose(existing ->
                            existing.<CompletionStage<io.craftrelay.client.PublishLifecycleSnapshot>>map(
                                            CompletableFuture::completedFuture)
                                    .orElseGet(() -> runtime.submit(
                                            registration.authenticatedProducerId(), newRequest)))
                    : CompletableFuture.failedFuture(new IllegalStateException(
                            "bounded publish tracking capacity exhausted; use status lookup before retry"));
        }
        BridgePublishHandle publishHandle = new BridgePublishHandle(
                eventId,
                runtime,
                submission,
                attached,
                () -> {
                    publishHandles.detach(eventId);
                    trackedRequests.detach(eventId);
                });
        if (attached) {
            publishHandles.attach(eventId, publishHandle);
        } else {
            runtime.recordPublishTrackingDetached(registration.authenticatedProducerId());
        }
        return publishHandle;
    }

    @Override
    public CompletionStage<PublishStatusResult> getPublishStatus(UUID eventId, Duration timeout) {
        io.craftrelay.client.ContractValidation.canonicalUuidV7(eventId.toString(), "eventId");
        validateTimeout(timeout);
        return runtime.status(eventId.toString(), timeout).thenApply(snapshot -> snapshot
                .map(value -> new PublishStatusResult(
                        PublishStatusResult.Status.FOUND,
                        value.revision(), value.snapshotChecksum(), value.fakeNonDurable()))
                .orElseGet(() -> new PublishStatusResult(
                        PublishStatusResult.Status.NOT_FOUND, 0, new byte[0], false)));
    }

    @Override
    public <R> CompletionStage<TypedQueryResponse<R>> query(
            TypedQueryRequest<R> request, QueryConsistency consistency, Duration timeout) {
        validateTimeout(timeout);
        if (!registration.manifest().queryContractHandles().contains(request.contract().opaqueValue())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("query contract handle is not issued by this integration manifest"));
        }
        byte[] encodedParameters = request.encodeParameters();
        if (encodedParameters.length > io.craftrelay.client.ContractLimits.MAX_QUERY_PARAMETER_BYTES) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("typed query parameters exceed bounded limit"));
        }
        if (pendingQueries.incrementAndGet() > registration.manifest().maxPendingQueries()) {
            pendingQueries.decrementAndGet();
            return CompletableFuture.failedFuture(
                    new IllegalStateException("per-producer pending query capacity exhausted"));
        }
        return runtime.query(
                registration.authenticatedProducerId(),
                request.contract().opaqueValue(),
                encodedParameters,
                consistency.mode().name(),
                timeout).thenApply(response -> {
                    QueryFreshnessMetadata.Proof proof = switch (response.proof()) {
                        case "STALE_ACCEPTED" -> QueryFreshnessMetadata.Proof.STALE_ACCEPTED;
                        default -> QueryFreshnessMetadata.Proof.UNAVAILABLE;
                    };
                    QueryFreshnessMetadata freshness = new QueryFreshnessMetadata(
                            consistency.mode(), proof, response.current(), response.authoritative(),
                            false, null, List.of());
                    return new TypedQueryResponse<>(request.decodeResult(response.typedResult()), freshness);
                }).whenComplete((ignored, failure) -> pendingQueries.decrementAndGet());
    }

    @Override
    public CompletionStage<WatchHandle> watch(WatchRequest request, Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("timeout must be positive"));
        }
        int count = activeWatches.incrementAndGet();
        InMemoryWatchHandle handle;
        if (count > registration.manifest().maxActiveWatches()) {
            activeWatches.decrementAndGet();
            runtime.recordWatchDetached(
                    registration.authenticatedProducerId(), "per-producer watch capacity exceeded");
            handle = new InMemoryWatchHandle(new WatchFreshnessMetadata(
                    WatchFreshnessMetadata.State.DETACHED, false,
                    "per-producer watch capacity exceeded", 0, null), () -> { });
        } else {
            handle = new InMemoryWatchHandle(new WatchFreshnessMetadata(
                    WatchFreshnessMetadata.State.CURRENT, true,
                    "fake watch has no backend updates", 0, null), activeWatches::decrementAndGet);
        }
        return CompletableFuture.completedFuture(handle);
    }

    public String producerInstanceId() { return registration.producerInstanceId(); }
    public long nextProducerOperationSequence() { return nextSequence.get(); }

    private static void validateTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
