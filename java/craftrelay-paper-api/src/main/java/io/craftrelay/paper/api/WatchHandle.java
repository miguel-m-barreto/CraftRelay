package io.craftrelay.paper.api;

import java.util.concurrent.CompletionStage;

public interface WatchHandle extends AutoCloseable {
    WatchFreshnessMetadata freshness();
    CompletionStage<WatchFreshnessMetadata> next();
    @Override void close();
}
