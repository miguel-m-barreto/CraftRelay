package io.craftrelay.paper.api;
import java.time.Instant;
import java.util.Map;
public record ProjectionConsistencyTokenView(
        int tokenVersion,
        String installationId,
        String authenticatedProducerId,
        String projectorId,
        String projectionName,
        String queryScope,
        Instant expiresAt,
        long topologyVersion,
        long routingVersion,
        Map<Integer,Long> requiredNextOffsets,
        byte[] authenticatedToken) {
    public ProjectionConsistencyTokenView {
        if (tokenVersion <= 0 || topologyVersion <= 0 || routingVersion <= 0) throw new IllegalArgumentException("token versions must be positive");
        for (String value : java.util.List.of(installationId, authenticatedProducerId, projectorId, projectionName, queryScope)) {
            if (value == null || value.isBlank() || value.length() > 256) throw new IllegalArgumentException("token scope fields must be bounded");
        }
        java.util.Objects.requireNonNull(expiresAt, "expiresAt");
        requiredNextOffsets=Map.copyOf(requiredNextOffsets);
        if(requiredNextOffsets.isEmpty() || requiredNextOffsets.size()>1024 || authenticatedToken.length==0 || authenticatedToken.length>16384) throw new IllegalArgumentException("token fields exceed bounds");
        authenticatedToken=authenticatedToken.clone();
    }
    @Override public byte[] authenticatedToken(){return authenticatedToken.clone();}
}
