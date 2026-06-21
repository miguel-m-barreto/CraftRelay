package io.craftrelay.reference;
import io.craftrelay.paper.api.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
/** Embedded typed AdapterClass example; it owns no transport resources. */
public final class ReferenceDomainAdapter {
    public static final EventContractHandle PROGRESS_DELTA =
            new EventContractHandle("manifest-issued:reference.progress-delta:v1");
    public static final QueryContractHandle GET_PROGRESS =
            new QueryContractHandle("manifest-issued:reference.get-progress:v1");
    private final DomainClient client;

    public ReferenceDomainAdapter(DomainClient client) { this.client = client; }

    public PublishHandle publishProgressDelta(UUID eventId, long positiveDelta) {
        if (positiveDelta <= 0) throw new IllegalArgumentException("positive delta required");
        return client.submit(
                PROGRESS_DELTA,
                eventId,
                Long.toString(positiveDelta).getBytes(StandardCharsets.UTF_8));
    }

    public java.util.concurrent.CompletionStage<TypedQueryResponse<ReferenceProgress>> getProgress(
            UUID playerId, QueryConsistency consistency, java.time.Duration timeout) {
        return client.query(new GetProgressRequest(playerId), consistency, timeout);
    }
}
