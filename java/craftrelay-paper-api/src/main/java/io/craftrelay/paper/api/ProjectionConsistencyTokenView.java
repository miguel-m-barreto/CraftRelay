package io.craftrelay.paper.api;
import java.time.Instant;
import java.util.Map;
public record ProjectionConsistencyTokenView(String projectorId, String projectionName, Instant expiresAt, Map<Integer,Long> requiredNextOffsets, byte[] authenticatedToken) { public ProjectionConsistencyTokenView { requiredNextOffsets=Map.copyOf(requiredNextOffsets); authenticatedToken=authenticatedToken.clone(); } }

