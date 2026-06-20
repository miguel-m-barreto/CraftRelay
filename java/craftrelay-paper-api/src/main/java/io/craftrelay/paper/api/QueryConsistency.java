package io.craftrelay.paper.api;
import java.util.List;
public sealed interface QueryConsistency permits QueryConsistency.Strict, QueryConsistency.AtLeastTokens, QueryConsistency.AllowStale {
    record Strict() implements QueryConsistency {}
    record AtLeastTokens(List<ProjectionConsistencyTokenView> tokens) implements QueryConsistency { public AtLeastTokens { tokens=List.copyOf(tokens); if(tokens.isEmpty()) throw new IllegalArgumentException("tokens required"); } }
    record AllowStale() implements QueryConsistency {}
    static QueryConsistency strictLatestCommitted() { return new Strict(); }
    static QueryConsistency atLeastTokens(List<ProjectionConsistencyTokenView> tokens) { return new AtLeastTokens(tokens); }
    static QueryConsistency allowStale() { return new AllowStale(); }
}

