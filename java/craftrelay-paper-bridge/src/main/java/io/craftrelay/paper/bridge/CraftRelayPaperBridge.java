package io.craftrelay.paper.bridge;

import io.craftrelay.client.SharedBridgeTransportRuntime;
import io.craftrelay.client.BridgeHandshake;
import io.craftrelay.paper.api.BridgeServiceRegistration;
import io.craftrelay.paper.api.CraftRelayService;
import io.craftrelay.paper.api.DomainClient;
import io.craftrelay.paper.api.EventReference;
import io.craftrelay.paper.api.ProjectionConsistencyTokenView;
import io.craftrelay.paper.api.ProjectionName;
import io.craftrelay.paper.api.ProjectionRequirement;
import io.craftrelay.paper.api.RegisteredPluginHandle;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/** Lifecycle-safe Sprint 1 Bridge. It owns one shared runtime and contains no domain logic. */
public final class CraftRelayPaperBridge implements CraftRelayService, AutoCloseable {
    private final BridgeServiceRegistration serviceRegistration;
    private final SharedBridgeTransportRuntime runtime;
    private final Map<RegisteredPluginHandle, ProducerRegistration> registrations;
    private final Map<RegisteredPluginHandle, LogicalProducerClient> clients = new ConcurrentHashMap<>();
    private final Map<EventReference, List<ProjectionConsistencyTokenView>> tokens = new ConcurrentHashMap<>();
    private volatile Readiness readiness = Readiness.NOT_READY;
    private volatile boolean started;

    public CraftRelayPaperBridge(
            BridgeServiceRegistration serviceRegistration,
            SharedBridgeTransportRuntime runtime,
            Map<RegisteredPluginHandle, ProducerRegistration> registrations) {
        this.serviceRegistration = serviceRegistration;
        this.runtime = runtime;
        if (registrations.size() > 256) {
            throw new IllegalArgumentException("Bridge registration count exceeds 256");
        }
        this.registrations = Map.copyOf(registrations);
    }

    public synchronized void startFixture(boolean compatibleHandshake) {
        startFixture(new BridgeHandshake.Response(
                compatibleHandshake,
                "fixture-producer",
                "01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4d",
                "fixture-session",
                1,
                1));
    }

    public synchronized void startFixture(BridgeHandshake.Response handshake) {
        if (started) {
            throw new IllegalStateException("Bridge already started");
        }
        serviceRegistration.register(this);
        started = true;
        readiness = handshake.compatible() ? Readiness.READY : Readiness.NOT_READY;
    }

    @Override public Readiness readiness() { return readiness; }

    @Override
    public DomainClient clientFor(RegisteredPluginHandle plugin) {
        ProducerRegistration registration = registrations.get(plugin);
        if (registration == null) {
            throw new IllegalArgumentException("unregistered plugin handle");
        }
        return clients.computeIfAbsent(plugin, ignored -> new LogicalProducerClient(registration, runtime));
    }

    @Override
    public CompletionStage<List<ProjectionConsistencyTokenView>> tokensFor(
            EventReference event, List<ProjectionName> projections, Duration timeout) {
        validateTimeout(timeout);
        if (projections.isEmpty() || projections.size() > 32) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("1..32 projections required"));
        }
        List<String> names = projections.stream().map(ProjectionName::value).toList();
        List<ProjectionConsistencyTokenView> selected = tokens.getOrDefault(event, List.of()).stream()
                .filter(token -> names.contains(token.projectionName()))
                .limit(32)
                .toList();
        return CompletableFuture.completedFuture(selected);
    }

    @Override
    public CompletionStage<ProjectionConsistencyTokenView> awaitProjectionToken(
            EventReference event, ProjectionRequirement requirement, Duration timeout) {
        return tokensFor(event, List.of(requirement.projection()), timeout).thenCompose(found ->
                found.isEmpty()
                        ? CompletableFuture.failedFuture(new IllegalStateException("token unavailable in Sprint 1 fixture"))
                        : CompletableFuture.completedFuture(found.getFirst()));
    }

    @Override
    public CompletionStage<List<ProjectionConsistencyTokenView>> awaitProjectionTokens(
            EventReference event, List<ProjectionRequirement> requirements, Duration timeout) {
        if (requirements.isEmpty() || requirements.size() > 32) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("1..32 token requirements required"));
        }
        return tokensFor(event, requirements.stream().map(ProjectionRequirement::projection).toList(), timeout);
    }

    public synchronized void registerTokenFixture(EventReference event, List<ProjectionConsistencyTokenView> eventTokens) {
        if (eventTokens.size() > 32) {
            throw new IllegalArgumentException("token fixture exceeds 32 entries");
        }
        if (!tokens.containsKey(event) && tokens.size() >= 128) {
            throw new IllegalArgumentException("token fixture event capacity exceeded");
        }
        tokens.put(event, List.copyOf(eventTokens));
    }

    @Override
    public synchronized void close() {
        readiness = Readiness.NOT_READY;
        if (started) {
            serviceRegistration.unregister(this);
            started = false;
        }
        clients.clear();
        tokens.clear();
    }

    private static void validateTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
