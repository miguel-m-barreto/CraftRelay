package io.craftrelay.paper.bridge;

import java.util.Set;

public record IntegrationManifest(
        String integrationId,
        int integrationVersion,
        String paperPluginId,
        Set<String> eventContractHandles,
        Set<String> queryContractHandles,
        int maxPendingPublishes,
        int maxPendingQueries,
        int maxActiveWatches) {
    public IntegrationManifest {
        if (integrationId == null || integrationId.isBlank() || integrationId.length() > 128
                || paperPluginId == null || paperPluginId.isBlank() || paperPluginId.length() > 128) {
            throw new IllegalArgumentException("manifest identities must be bounded");
        }
        eventContractHandles = Set.copyOf(eventContractHandles);
        queryContractHandles = Set.copyOf(queryContractHandles);
        if (integrationVersion <= 0
                || maxPendingPublishes <= 0
                || maxPendingPublishes > 4_096
                || maxPendingQueries <= 0
                || maxPendingQueries > 4_096
                || maxActiveWatches <= 0
                || maxActiveWatches > 4_096
                || eventContractHandles.size() > 256
                || queryContractHandles.size() > 256) {
            throw new IllegalArgumentException("manifest values must be positive and bounded");
        }
    }
}
