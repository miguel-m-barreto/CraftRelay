package io.craftrelay.paper.api;
/** Host service-registry boundary; the real Paper binding is a later implementation. */
public interface BridgeServiceRegistration {
    void register(CraftRelayService service);
    void unregister(CraftRelayService service);

    /** Issues an opaque handle only while resolving an actual host plugin registration. */
    default RegisteredPluginHandle issuePluginHandle(Object hostPluginInstance) {
        if (hostPluginInstance == null || hostPluginInstance instanceof String) {
            throw new IllegalArgumentException("an actual host plugin instance is required");
        }
        return new BridgeIssuedPluginHandle(hostPluginInstance);
    }
}
