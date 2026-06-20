package io.craftrelay.paper.bridge;

import io.craftrelay.paper.api.CraftRelayService;
import io.craftrelay.paper.api.RegisteredPluginHandle;
import java.util.Map;

/** Non-functional Sprint 0 service-registration/readiness skeleton. */
public final class BridgeSkeleton {
    private final Map<RegisteredPluginHandle, IntegrationManifest> manifests;
    private volatile CraftRelayService.Readiness readiness = CraftRelayService.Readiness.NOT_READY;
    public BridgeSkeleton(Map<RegisteredPluginHandle, IntegrationManifest> manifests) { this.manifests = Map.copyOf(manifests); }
    public IntegrationManifest resolve(RegisteredPluginHandle plugin) { var manifest=manifests.get(plugin); if(manifest==null) throw new IllegalArgumentException("unregistered plugin handle"); return manifest; }
    public CraftRelayService.Readiness readiness() { return readiness; }
    public void markReadyForFixtureOnly() { readiness = CraftRelayService.Readiness.READY; }
}
