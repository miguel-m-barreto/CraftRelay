package io.craftrelay.paper.bridge;
import java.util.Set;
public record IntegrationManifest(String integrationId, int integrationVersion, String paperPluginId, Set<String> eventContractHandles, Set<String> queryContractHandles, int maxPendingPublishes, int maxPendingQueries, int maxActiveWatches) { public IntegrationManifest { eventContractHandles=Set.copyOf(eventContractHandles); queryContractHandles=Set.copyOf(queryContractHandles); if(integrationVersion<=0||maxPendingPublishes<=0||maxPendingQueries<=0||maxActiveWatches<=0) throw new IllegalArgumentException("positive bounded values required"); } }

