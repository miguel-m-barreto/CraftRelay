package io.craftrelay.paper.api;

public record TypedQueryResponse<R>(R value, QueryFreshnessMetadata freshness) {
}
