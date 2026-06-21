package io.craftrelay.paper.bridge;

import io.craftrelay.paper.api.WatchFreshnessMetadata;
import io.craftrelay.paper.api.WatchHandle;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Bounded Sprint 1 watch fixture; it performs no backend waiting. */
public final class InMemoryWatchHandle implements WatchHandle {
    private volatile WatchFreshnessMetadata freshness;
    private final Runnable onTerminal;
    private final java.util.concurrent.atomic.AtomicBoolean terminal = new java.util.concurrent.atomic.AtomicBoolean();

    public InMemoryWatchHandle(WatchFreshnessMetadata initial, Runnable onTerminal) {
        this.freshness = initial;
        this.onTerminal = onTerminal;
    }

    @Override public WatchFreshnessMetadata freshness() { return freshness; }

    @Override
    public CompletionStage<WatchFreshnessMetadata> next() {
        return CompletableFuture.completedFuture(freshness);
    }

    public void detachForFixture(String reason) {
        freshness = new WatchFreshnessMetadata(
                WatchFreshnessMetadata.State.DETACHED, false, reason, 0, null);
        finish();
    }

    @Override
    public void close() {
        freshness = new WatchFreshnessMetadata(
                WatchFreshnessMetadata.State.CLOSED, false, "closed", 0, null);
        finish();
    }

    private void finish() {
        if (terminal.compareAndSet(false, true)) onTerminal.run();
    }
}
