package io.craftrelay.paper.api;

/**
 * Opaque capability representing the actual plugin/integration registration resolved by
 * the host service registry. Domain plugins receive this handle; they cannot construct it
 * from a plugin name or producer ID.
 *
 * <p>This capability prevents accidental string-based producer selection. It is not a
 * security sandbox against malicious code already executing in the same JVM.</p>
 */
public sealed interface RegisteredPluginHandle permits BridgeIssuedPluginHandle {
}

final class BridgeIssuedPluginHandle implements RegisteredPluginHandle {
    BridgeIssuedPluginHandle() {
    }
}
