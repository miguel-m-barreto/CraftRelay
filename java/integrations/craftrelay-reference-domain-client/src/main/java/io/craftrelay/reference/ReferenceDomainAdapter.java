package io.craftrelay.reference;
import io.craftrelay.paper.api.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
/** Embedded typed AdapterClass example; it owns no transport resources. */
public final class ReferenceDomainAdapter { private static final EventContractHandle HANDLE=new EventContractHandle("manifest-issued:reference.progress-delta:v1"); private final DomainClient client; public ReferenceDomainAdapter(DomainClient client){this.client=client;} public CompletionStage<SubmissionResult> publishProgressDelta(UUID eventId,long positiveDelta){if(positiveDelta<=0)throw new IllegalArgumentException("positive delta required"); return client.submit(HANDLE,eventId,Long.toString(positiveDelta).getBytes(StandardCharsets.UTF_8),Duration.ofMillis(250));} }

