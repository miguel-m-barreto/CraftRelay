package io.craftrelay.paper.api;
/** Host service-registry boundary; the real Paper binding is a later implementation. */
public interface BridgeServiceRegistration { void register(CraftRelayService service); void unregister(CraftRelayService service); }

