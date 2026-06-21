package io.craftrelay.paper.api;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
public interface DomainClient {
    PublishHandle submit(EventContractHandle handle, UUID eventId, byte[] typedPayload);
    CompletionStage<PublishStatusResult> getPublishStatus(UUID eventId, Duration timeout);
    <R> CompletionStage<TypedQueryResponse<R>> query(TypedQueryRequest<R> request, QueryConsistency consistency, Duration timeout);
    CompletionStage<WatchHandle> watch(WatchRequest request, Duration timeout);
}
