package io.craftrelay.paper.api;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface CraftRelayService {
    Readiness readiness();
    DomainClient clientFor(RegisteredPluginHandle plugin);
    CompletionStage<List<ProjectionConsistencyTokenView>> tokensFor(EventReference event, List<ProjectionName> projections, Duration timeout);
    CompletionStage<ProjectionConsistencyTokenView> awaitProjectionToken(EventReference event, ProjectionRequirement requirement, Duration timeout);
    CompletionStage<List<ProjectionConsistencyTokenView>> awaitProjectionTokens(EventReference event, List<ProjectionRequirement> requirements, Duration timeout);
    enum Readiness { NOT_READY, READY, DEGRADED }
}
