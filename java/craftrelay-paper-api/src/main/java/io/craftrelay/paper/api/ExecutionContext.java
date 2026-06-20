package io.craftrelay.paper.api;
public interface ExecutionContext { Kind kind(); void execute(RegisteredPluginHandle owner, Runnable callback); enum Kind { GLOBAL_SERVER, ENTITY, REGION, ASYNC_ONLY } }
