package io.craftrelay.client;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface AgentClient {
    CompletionStage<PublishLifecycleSnapshot> submit(ClientPublishRequest request);
    CompletionStage<Optional<PublishLifecycleSnapshot>> getStatus(String eventId, Duration timeout);
}
