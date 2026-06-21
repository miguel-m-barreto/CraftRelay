package io.craftrelay.tests;

import static org.junit.jupiter.api.Assertions.*;

import io.craftrelay.client.*;
import io.craftrelay.client.fixture.FakeAgentFixture;
import io.craftrelay.client.fixture.FakeQueryServiceFixture;
import io.craftrelay.paper.api.*;
import io.craftrelay.paper.bridge.*;
import io.craftrelay.reference.GetProgressRequest;
import io.craftrelay.reference.ReferenceDomainAdapter;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

final class BridgeClientContractTest {
    private static final UUID EVENT_ONE = UUID.fromString("01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4a");
    private static final UUID EVENT_TWO = UUID.fromString("01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4b");

    @Test void serviceRegistrationReadinessAndShutdownAreExplicit() {
        Fixture fixture = fixture(2, 2);
        assertEquals(CraftRelayService.Readiness.NOT_READY, fixture.bridge.readiness());
        fixture.bridge.startFixture(true);
        assertSame(fixture.bridge, fixture.serviceRegistration.registered);
        assertEquals(CraftRelayService.Readiness.READY, fixture.bridge.readiness());
        fixture.bridge.close();
        assertNull(fixture.serviceRegistration.registered);
        assertEquals(CraftRelayService.Readiness.NOT_READY, fixture.bridge.readiness());
    }

    @Test void handshakeAndReconnectPoliciesAreBounded() {
        BridgeHandshake.Request request = new BridgeHandshake.Request(
                1, 0, new InstallationScope("installation-a", "node-a"),
                "reference", 1, "01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4c");
        assertEquals(1, request.protocolMajor());
        io.craftrelay.client.ReconnectPolicy policy = new io.craftrelay.client.ReconnectPolicy(
                4, Duration.ofMillis(10), Duration.ofMillis(100));
        assertEquals(Duration.ofMillis(10), policy.delayForAttempt(1));
        assertEquals(Duration.ofMillis(80), policy.delayForAttempt(4));
        assertEquals(Duration.ofMillis(100), policy.delayForAttempt(8));
    }

    @Test void boundedPublishTrackingDetachesWithoutClaimingDurability() {
        Fixture fixture = fixture(1, 2);
        fixture.bridge.startFixture(true);
        DomainClient client = fixture.bridge.clientFor(fixture.pluginHandle);
        PublishHandle first = client.submit(ReferenceDomainAdapter.PROGRESS_DELTA, EVENT_ONE, new byte[] {1});
        PublishHandle second = client.submit(ReferenceDomainAdapter.PROGRESS_DELTA, EVENT_TWO, new byte[] {2});
        assertEquals(PublishHandle.TrackingState.ATTACHED, first.trackingState());
        assertEquals(PublishHandle.TrackingState.DETACHED, second.trackingState());
        assertEquals(LocalAcceptanceResult.Status.FAKE_ACCEPTED_NON_DURABLE,
                first.awaitLocalAcceptance(Duration.ofSeconds(1)).toCompletableFuture().join().status());
        assertEquals(RequiredDurabilityResult.Status.NOT_REACHED,
                first.awaitRequiredDurability(Duration.ofSeconds(1)).toCompletableFuture().join().status());
        assertEquals(LocalAcceptanceResult.Status.TRACKING_DETACHED,
                second.awaitLocalAcceptance(Duration.ofSeconds(1)).toCompletableFuture().join().status());
        PublishStatusResult status = client.getPublishStatus(EVENT_ONE, Duration.ofSeconds(1))
                .toCompletableFuture().join();
        assertEquals(PublishStatusResult.Status.FOUND, status.status());
        assertTrue(status.fakeNonDurable());
        assertFalse(fixture.agent.durable());
        first.detachTracking();
        assertEquals(PublishHandle.TrackingState.DETACHED, first.trackingState());
        PublishHandle afterRelease = client.submit(
                ReferenceDomainAdapter.PROGRESS_DELTA,
                UUID.fromString("01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b49"),
                new byte[] {3});
        assertEquals(PublishHandle.TrackingState.ATTACHED, afterRelease.trackingState());
    }

    @Test void retryUsesSameImmutableRequestAndRejectsMutation() {
        FakeAgentFixture agent = new FakeAgentFixture(4);
        SharedBridgeTransportRuntime runtime = new SharedBridgeTransportRuntime(
                agent, new FakeQueryServiceFixture(Map.of()), ClientObservability.Metrics.noOp());
        ClientPublishRequest request = new ClientPublishRequest(ValidationContractTest.envelope(1, List.of()), 1);
        var first = runtime.submit("producer-a", request).toCompletableFuture().join();
        var retry = runtime.retrySameRequest("producer-a", request).toCompletableFuture().join();
        assertEquals(first.eventId(), retry.eventId());
        assertArrayEquals(first.snapshotChecksum(), retry.snapshotChecksum());

        EnvelopeInput changed = new EnvelopeInput(request.envelopeInput().eventId(), "reference", "progress",
                new byte[] {1}, "progress.delta", 1, "DELTA", "operation-1", "EVENT",
                "POLICY_RESOLVED_BY_AGENT", List.of(), new byte[] {99}, 1,
                "manifest-issued:reference.progress-delta:v1");
        CompletionException exception = assertThrows(CompletionException.class,
                () -> runtime.retrySameRequest("producer-a", new ClientPublishRequest(changed, 1))
                        .toCompletableFuture().join());
        assertInstanceOf(ContractViolationException.class, exception.getCause());
    }

    @Test void logicalProducerRetryReusesEventIdentityAndProducerSequence() {
        Fixture fixture = fixture(2, 2);
        fixture.bridge.startFixture(true);
        LogicalProducerClient client = (LogicalProducerClient) fixture.bridge.clientFor(fixture.pluginHandle);
        client.submit(ReferenceDomainAdapter.PROGRESS_DELTA, EVENT_ONE, new byte[] {7});
        long nextAfterFirst = client.nextProducerOperationSequence();
        client.submit(ReferenceDomainAdapter.PROGRESS_DELTA, EVENT_ONE, new byte[] {7});
        assertEquals(nextAfterFirst, client.nextProducerOperationSequence());
        assertThrows(ContractViolationException.class,
                () -> client.submit(ReferenceDomainAdapter.PROGRESS_DELTA, EVENT_ONE, new byte[] {8}));
    }

    @Test void producerSelectionUsesOpaqueRegisteredHandleAndNoStringSelector() throws Exception {
        assertEquals(RegisteredPluginHandle.class,
                CraftRelayService.class.getMethod("clientFor", RegisteredPluginHandle.class).getParameterTypes()[0]);
        assertTrue(RegisteredPluginHandle.class.isSealed());
        for (Class<?> implementation : RegisteredPluginHandle.class.getPermittedSubclasses()) {
            for (var constructor : implementation.getDeclaredConstructors()) {
                assertFalse(Modifier.isPublic(constructor.getModifiers()));
            }
        }
        assertFalse(java.util.Arrays.stream(CraftRelayService.class.getMethods())
                .anyMatch(method -> method.getName().equals("clientFor")
                        && java.util.Arrays.asList(method.getParameterTypes()).contains(String.class)));
        TestServiceRegistration registration = new TestServiceRegistration();
        assertThrows(IllegalArgumentException.class, () -> registration.issuePluginHandle("plugin-name"));
    }

    @Test void referenceAdapterUsesTypedPublishAndQueryContracts() {
        Fixture fixture = fixture(2, 2);
        fixture.bridge.startFixture(true);
        ReferenceDomainAdapter adapter = new ReferenceDomainAdapter(fixture.bridge.clientFor(fixture.pluginHandle));
        PublishHandle handle = adapter.publishProgressDelta(EVENT_ONE, 5);
        assertEquals(EVENT_ONE, handle.eventId());
        var query = adapter.getProgress(EVENT_ONE, QueryConsistency.allowStale(), Duration.ofSeconds(1))
                .toCompletableFuture().join();
        assertEquals(42, query.value().progress());
        assertEquals(QueryFreshnessMetadata.Proof.STALE_ACCEPTED, query.freshness().proof());
        assertFalse(query.freshness().authoritative());
    }

    @Test void typedQueryApiExposesNoArbitrarySqlConstruction() {
        assertTrue(TypedQueryRequest.class.isInterface());
        assertFalse(java.util.Arrays.stream(DomainClient.class.getMethods())
                .anyMatch(method -> method.getName().toLowerCase(java.util.Locale.ROOT).contains("sql")));
        GetProgressRequest request = new GetProgressRequest(EVENT_ONE);
        assertEquals(ReferenceDomainAdapter.GET_PROGRESS, request.contract());
        assertFalse(new String(request.encodeParameters(), StandardCharsets.US_ASCII).isBlank());
    }

    @Test void strictQueryNeverSilentlyReturnsFakeStaleData() {
        Fixture fixture = fixture(2, 2);
        fixture.bridge.startFixture(true);
        ReferenceDomainAdapter adapter = new ReferenceDomainAdapter(fixture.bridge.clientFor(fixture.pluginHandle));
        CompletionException failure = assertThrows(CompletionException.class,
                () -> adapter.getProgress(EVENT_ONE, QueryConsistency.strictLatestCommitted(), Duration.ofSeconds(1))
                        .toCompletableFuture().join());
        assertInstanceOf(QueryUnavailableException.class, failure.getCause());
        assertEquals(QueryUnavailableException.Code.STRICT_READ_NOT_IMPLEMENTED,
                ((QueryUnavailableException) failure.getCause()).code());
    }

    @Test void boundedTokenRetrievalUsesEventAndProjectionScope() {
        Fixture fixture = fixture(2, 2);
        fixture.bridge.startFixture(true);
        UUID installation = UUID.fromString("01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b40");
        EventReference event = new EventReference(installation, EVENT_ONE);
        ProjectionConsistencyTokenView token = new ProjectionConsistencyTokenView(
                1, installation.toString(), "producer-reference", "projector-a", "progress",
                "player:" + EVENT_ONE, Instant.parse("2030-01-01T00:00:00Z"), 3, 7,
                Map.of(0, 42L), new byte[32]);
        fixture.bridge.registerTokenFixture(event, List.of(token));
        var found = fixture.bridge.tokensFor(
                event, List.of(new ProjectionName("progress")), Duration.ofSeconds(1))
                .toCompletableFuture().join();
        assertEquals(List.of(token), found);
        assertThrows(IllegalArgumentException.class,
                () -> QueryConsistency.atLeastTokens(java.util.Collections.nCopies(33, token)));
    }

    @Test void watchesAreBoundedAndDetachAsNonCurrent() {
        Fixture fixture = fixture(2, 1);
        fixture.bridge.startFixture(true);
        DomainClient client = fixture.bridge.clientFor(fixture.pluginHandle);
        WatchRequest request = new WatchRequest.EntityVersion(
                ReferenceDomainAdapter.GET_PROGRESS, EVENT_ONE.toString(), 0, 4, 4096);
        WatchHandle first = client.watch(request, Duration.ofSeconds(1)).toCompletableFuture().join();
        WatchHandle second = client.watch(request, Duration.ofSeconds(1)).toCompletableFuture().join();
        assertTrue(first.freshness().current());
        assertEquals(WatchFreshnessMetadata.State.DETACHED, second.freshness().state());
        assertFalse(second.freshness().current());
        ((InMemoryWatchHandle) first).detachForFixture("transport disconnected");
        assertFalse(first.freshness().current());
        assertEquals(WatchFreshnessMetadata.State.DETACHED, first.freshness().state());
        assertThrows(IllegalArgumentException.class,
                () -> new WatchRequest.EntityVersion(
                        ReferenceDomainAdapter.GET_PROGRESS, EVENT_ONE.toString(), 0, 257, 4096));
    }

    private static Fixture fixture(int publishCapacity, int watchCapacity) {
        TestServiceRegistration serviceRegistration = new TestServiceRegistration();
        RegisteredPluginHandle pluginHandle = serviceRegistration.issuePluginHandle(new Object());
        IntegrationManifest manifest = new IntegrationManifest(
                "reference", 1, "ReferencePlugin",
                Set.of(ReferenceDomainAdapter.PROGRESS_DELTA.opaqueValue()),
                Set.of(ReferenceDomainAdapter.GET_PROGRESS.opaqueValue()),
                publishCapacity, 4, watchCapacity);
        ProducerRegistration producer = new ProducerRegistration(
                "installation-a", "node-a", "producer-reference",
                "01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4c", manifest);
        FakeAgentFixture agent = new FakeAgentFixture(16);
        SharedBridgeTransportRuntime runtime = new SharedBridgeTransportRuntime(
                agent,
                new FakeQueryServiceFixture(Map.of(
                        ReferenceDomainAdapter.GET_PROGRESS.opaqueValue(), "42".getBytes(StandardCharsets.US_ASCII))),
                ClientObservability.Metrics.noOp());
        CraftRelayPaperBridge bridge = new CraftRelayPaperBridge(
                serviceRegistration, runtime, Map.of(pluginHandle, producer));
        return new Fixture(serviceRegistration, pluginHandle, agent, bridge);
    }

    private record Fixture(
            TestServiceRegistration serviceRegistration,
            RegisteredPluginHandle pluginHandle,
            FakeAgentFixture agent,
            CraftRelayPaperBridge bridge) {
    }

    private static final class TestServiceRegistration implements BridgeServiceRegistration {
        private CraftRelayService registered;
        @Override public void register(CraftRelayService service) { registered = service; }
        @Override public void unregister(CraftRelayService service) { if (registered == service) registered = null; }
    }
}
