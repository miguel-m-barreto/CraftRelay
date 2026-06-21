package io.craftrelay.paper.api;
import java.util.List;
public sealed interface QueryConsistency permits QueryConsistency.Strict, QueryConsistency.AtLeastTokens, QueryConsistency.AllowStale {
    Mode mode();
    record Strict(ProjectionBarrierView capturedBarrier) implements QueryConsistency {
        @Override public Mode mode() { return Mode.STRICT_LATEST_COMMITTED; }
    }
    record AtLeastTokens(List<ProjectionConsistencyTokenView> tokens) implements QueryConsistency { public AtLeastTokens { tokens=List.copyOf(tokens); if(tokens.isEmpty() || tokens.size() > 32) throw new IllegalArgumentException("1..32 tokens required"); } @Override public Mode mode() { return Mode.AT_LEAST_TOKEN; } }
    record AllowStale() implements QueryConsistency { @Override public Mode mode() { return Mode.ALLOW_STALE; } }
    static QueryConsistency strictLatestCommitted() { return new Strict(null); }
    static QueryConsistency strictAtBarrier(ProjectionBarrierView barrier) { return new Strict(java.util.Objects.requireNonNull(barrier)); }
    static QueryConsistency atLeastTokens(List<ProjectionConsistencyTokenView> tokens) { return new AtLeastTokens(tokens); }
    static QueryConsistency allowStale() { return new AllowStale(); }
    enum Mode { STRICT_LATEST_COMMITTED, AT_LEAST_TOKEN, ALLOW_STALE }
}
