package io.craftrelay.paper.api;

import java.util.List;

public record QueryFreshnessMetadata(
        QueryConsistency.Mode requestedMode,
        Proof proof,
        boolean current,
        boolean authoritative,
        boolean trackingDetached,
        ProjectionBarrierView provenBarrier,
        List<ProjectionConsistencyTokenView> provenTokens) {
    public QueryFreshnessMetadata {
        provenTokens = List.copyOf(provenTokens);
        if ((proof == Proof.STRICT_PROVEN || proof == Proof.TOKEN_PROVEN) && (!current || !authoritative)) {
            throw new IllegalArgumentException("authoritative proof must be current and authoritative");
        }
        if (trackingDetached && current) {
            throw new IllegalArgumentException("detached query tracking cannot be current");
        }
    }

    public enum Proof { STRICT_PROVEN, TOKEN_PROVEN, STALE_ACCEPTED, UNAVAILABLE }
}
