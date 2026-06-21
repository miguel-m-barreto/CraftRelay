package io.craftrelay.paper.api;

public sealed interface WatchRequest permits WatchRequest.EntityVersion, WatchRequest.BarrierVector {
    int maxBufferEvents();
    long maxBufferBytes();

    record EntityVersion(
            QueryContractHandle contract,
            String entityId,
            long afterVersion,
            int maxBufferEvents,
            long maxBufferBytes) implements WatchRequest {
        public EntityVersion {
            java.util.Objects.requireNonNull(contract, "contract");
            if (entityId == null || entityId.isBlank() || entityId.length() > 256 || afterVersion < 0) {
                throw new IllegalArgumentException("invalid entity-version watch");
            }
            validateBounds(maxBufferEvents, maxBufferBytes);
        }
    }

    record BarrierVector(
            ProjectionBarrierView afterBarrier,
            int maxBufferEvents,
            long maxBufferBytes) implements WatchRequest {
        public BarrierVector {
            java.util.Objects.requireNonNull(afterBarrier, "afterBarrier");
            validateBounds(maxBufferEvents, maxBufferBytes);
        }
    }

    private static void validateBounds(int events, long bytes) {
        if (events <= 0 || events > 256 || bytes <= 0 || bytes > 1_048_576) {
            throw new IllegalArgumentException("watch buffers exceed bounded limits");
        }
    }
}
