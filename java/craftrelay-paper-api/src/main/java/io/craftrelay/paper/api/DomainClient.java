package io.craftrelay.paper.api;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
public interface DomainClient { CompletionStage<SubmissionResult> submit(EventContractHandle handle, UUID eventId, byte[] typedPayload, Duration timeout); CompletionStage<TypedQueryResult> query(QueryContractHandle handle, byte[] typedParameters, QueryConsistency consistency, Duration timeout); }

