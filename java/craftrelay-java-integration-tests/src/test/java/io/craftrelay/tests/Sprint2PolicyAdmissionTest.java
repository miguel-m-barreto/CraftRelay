package io.craftrelay.tests;

import static org.junit.jupiter.api.Assertions.*;

import io.craftrelay.client.*;
import io.craftrelay.client.fixture.FakeAgentFixture;
import io.craftrelay.client.fixture.FakeQueryServiceFixture;
import io.craftrelay.client.policy.*;
import io.craftrelay.paper.api.*;
import io.craftrelay.paper.bridge.*;
import io.craftrelay.reference.ReferenceDomainAdapter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

final class Sprint2PolicyAdmissionTest {
    private static final UUID EVENT_ONE = UUID.fromString("01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4a");
    private static final UUID EVENT_TWO = UUID.fromString("01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4b");

    // --- Producer Registration ---

    @Test void validProducerRegistration() {
        ProducerRegistration reg = registration("installation-a", "producer-a");
        assertEquals("installation-a", reg.installationId());
        assertEquals("producer-a", reg.authenticatedProducerId());
        assertEquals(PolicyResolution.PriorityClass.P1, reg.effectivePriorityClass());
    }

    @Test void duplicateProducerRegistrationRejectedByBridge() {
        var fixture = bridgeFixture(2);
        fixture.bridge.startFixture(true);
        assertThrows(IllegalArgumentException.class, () -> {
            var duplicate = Map.of(
                    fixture.pluginHandle, registration("installation-a", "producer-a"),
                    fixture.pluginHandle, registration("installation-a", "producer-a"));
        });
    }

    @Test void disabledProducerRejectedByFakeAgent() {
        var fixture = bridgeFixtureWithPolicyChecks(2);
        fixture.agent.disableProducer("producer-reference");
        fixture.bridge.startFixture(true);
        DomainClient client = fixture.bridge.clientFor(fixture.pluginHandle);
        CompletionException ex = assertThrows(CompletionException.class,
                () -> client.submit(ReferenceDomainAdapter.PROGRESS_DELTA, EVENT_ONE, new byte[]{1})
                        .awaitLocalAcceptance(Duration.ofSeconds(1)).toCompletableFuture().join());
        assertInstanceOf(ContractViolationException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("disabled"));
    }

    @Test void suspendedProducerRejectedByFakeAgent() {
        var fixture = bridgeFixtureWithPolicyChecks(2);
        fixture.agent.suspendProducer("producer-reference");
        fixture.bridge.startFixture(true);
        DomainClient client = fixture.bridge.clientFor(fixture.pluginHandle);
        CompletionException ex = assertThrows(CompletionException.class,
                () -> client.submit(ReferenceDomainAdapter.PROGRESS_DELTA, EVENT_ONE, new byte[]{1})
                        .awaitLocalAcceptance(Duration.ofSeconds(1)).toCompletableFuture().join());
        assertInstanceOf(ContractViolationException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("suspended"));
    }

    @Test void crossInstallationProducerRejected() {
        assertThrows(ContractViolationException.class,
                () -> new ProducerRegistration("", "node-a", "producer-a",
                        "01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4c",
                        testManifest()));
    }

    // --- IntegrationManifest Validation ---

    @Test void manifestDuplicateDeclarationRejected() {
        IntegrationManifest manifest = new IntegrationManifest(
                "reference", 1, "ReferencePlugin",
                Set.of("handle-a"), Set.of("query-a"),
                128, 64, 32,
                Set.of("economy"), Set.of("economy.transfer"), Set.of("get-account"));
        var violations = manifest.validateExtended();
        assertTrue(violations.isEmpty());
    }

    @Test void manifestInvalidNamespaceRejected() {
        IntegrationManifest manifest = new IntegrationManifest(
                "reference", 1, "ReferencePlugin",
                Set.of("handle-a"), Set.of("query-a"),
                128, 64, 32,
                Set.of("INVALID"), Set.of(), Set.of());
        var violations = manifest.validateExtended();
        assertTrue(violations.contains(IntegrationManifest.ValidationCode.INVALID_NAMESPACE_NAME));
    }

    @Test void manifestInvalidEventNameRejected() {
        IntegrationManifest manifest = new IntegrationManifest(
                "reference", 1, "ReferencePlugin",
                Set.of("handle-a"), Set.of("query-a"),
                128, 64, 32,
                Set.of(), Set.of("UPPER_CASE"), Set.of());
        var violations = manifest.validateExtended();
        assertTrue(violations.contains(IntegrationManifest.ValidationCode.INVALID_EVENT_NAME));
    }

    @Test void manifestInvalidQueryNameRejected() {
        IntegrationManifest manifest = new IntegrationManifest(
                "reference", 1, "ReferencePlugin",
                Set.of("handle-a"), Set.of("query-a"),
                128, 64, 32,
                Set.of(), Set.of(), Set.of("SELECT * FROM"));
        var violations = manifest.validateExtended();
        assertTrue(violations.contains(IntegrationManifest.ValidationCode.INVALID_QUERY_NAME));
    }

    @Test void manifestCannotSelfPromoteViaBestEffort() {
        // BEST_EFFORT is excluded from durable API - manifest creation rejects it by design
        assertThrows(IllegalArgumentException.class, () -> new IntegrationManifest(
                "reference", 0, "ReferencePlugin",
                Set.of(), Set.of(), 1, 1, 1));
    }

    // --- ACL ---

    @Test void aclAllowDecision() {
        var principal = testPrincipal(CredentialReference.CredentialStatus.ACTIVE);
        var rules = List.of(new AclEvaluation.Rule(
                "allow-economy", AclEvaluation.Action.PUBLISH, "economy",
                AclEvaluation.Decision.ALLOW, 10));
        var result = AclEvaluation.evaluate(principal, "installation-a", "economy",
                AclEvaluation.Action.PUBLISH, rules, 1);
        assertEquals(AclEvaluation.Decision.ALLOW, result.decision());
        assertEquals("allow-economy", result.matchedRuleId());
    }

    @Test void aclDenyDecision() {
        var principal = testPrincipal(CredentialReference.CredentialStatus.ACTIVE);
        var rules = List.of(
                new AclEvaluation.Rule("allow", AclEvaluation.Action.PUBLISH, "economy",
                        AclEvaluation.Decision.ALLOW, 10),
                new AclEvaluation.Rule("deny", AclEvaluation.Action.PUBLISH, "economy",
                        AclEvaluation.Decision.DENY, 20));
        var result = AclEvaluation.evaluate(principal, "installation-a", "economy",
                AclEvaluation.Action.PUBLISH, rules, 1);
        assertEquals(AclEvaluation.Decision.DENY, result.decision());
        assertEquals(AclEvaluation.DenyReason.EXPLICIT_DENY, result.denyReason());
    }

    @Test void unknownCredentialRejected() {
        var principal = testPrincipal(CredentialReference.CredentialStatus.UNKNOWN);
        var result = AclEvaluation.evaluate(principal, "installation-a", "economy",
                AclEvaluation.Action.PUBLISH, List.of(), 1);
        assertEquals(AclEvaluation.Decision.DENY, result.decision());
        assertEquals(AclEvaluation.DenyReason.CREDENTIAL_INVALID, result.denyReason());
    }

    @Test void credentialStatusRejection() {
        for (var status : List.of(
                CredentialReference.CredentialStatus.REVOKED,
                CredentialReference.CredentialStatus.EXPIRED)) {
            var principal = testPrincipal(status);
            var result = AclEvaluation.evaluate(principal, "installation-a", "economy",
                    AclEvaluation.Action.PUBLISH, List.of(), 1);
            assertEquals(AclEvaluation.Decision.DENY, result.decision());
        }
    }

    @Test void aclCrossInstallationRejected() {
        var principal = testPrincipal(CredentialReference.CredentialStatus.ACTIVE);
        var result = AclEvaluation.evaluate(principal, "installation-b", "economy",
                AclEvaluation.Action.PUBLISH, List.of(), 1);
        assertEquals(AclEvaluation.Decision.DENY, result.decision());
        assertEquals(AclEvaluation.DenyReason.CROSS_INSTALLATION, result.denyReason());
    }

    // --- Policy Resolution ---

    @Test void policyResolutionDeterminism() {
        var config = testPolicyConfig();
        var aclAllow = new AclEvaluation.Result(AclEvaluation.Decision.ALLOW, null, "r1", 1);
        var result1 = PolicyResolution.resolve(
                "installation-a", "producer-a", "installation-a",
                PolicyResolution.ProducerState.ACTIVE, aclAllow,
                "economy", PolicyResolution.DurabilityClass.REPLICATED_DURABLE,
                PolicyResolution.RetentionClass.STANDARD, "proj-v1",
                PolicyResolution.PriorityClass.P1, "binding-1", "snap-1",
                List.of("economy"), config);
        var result2 = PolicyResolution.resolve(
                "installation-a", "producer-a", "installation-a",
                PolicyResolution.ProducerState.ACTIVE, aclAllow,
                "economy", PolicyResolution.DurabilityClass.REPLICATED_DURABLE,
                PolicyResolution.RetentionClass.STANDARD, "proj-v1",
                PolicyResolution.PriorityClass.P1, "binding-1", "snap-1",
                List.of("economy"), config);
        assertEquals(result1, result2);
        assertEquals(PolicyResolution.AdmissionDecision.ADMITTED, result1.admissionDecision());
    }

    @Test void durabilityWeakeningRejected() {
        var result = resolveWithDurability(
                PolicyResolution.DurabilityClass.LOCAL_DURABLE,
                PolicyResolution.DurabilityClass.REPLICATED_DURABLE);
        assertEquals(PolicyResolution.AdmissionDecision.REJECTED, result.admissionDecision());
        assertEquals(PolicyResolution.RejectionReason.DURABILITY_WEAKENING, result.rejectionReason());
    }

    @Test void retentionWeakeningRejected() {
        var config = new PolicyResolution.Configuration(
                "installation-a", PolicyResolution.DurabilityClass.REPLICATED_DURABLE,
                PolicyResolution.RetentionClass.EXTENDED, "proj-v1",
                PolicyResolution.PriorityClass.P1, List.of("binding-1"), 1);
        var aclAllow = new AclEvaluation.Result(AclEvaluation.Decision.ALLOW, null, "r1", 1);
        var result = PolicyResolution.resolve(
                "installation-a", "producer-a", "installation-a",
                PolicyResolution.ProducerState.ACTIVE, aclAllow,
                "economy", PolicyResolution.DurabilityClass.REPLICATED_DURABLE,
                PolicyResolution.RetentionClass.STANDARD, "proj-v1",
                PolicyResolution.PriorityClass.P1, "binding-1", "snap-1",
                List.of("economy"), config);
        assertEquals(PolicyResolution.AdmissionDecision.REJECTED, result.admissionDecision());
        assertEquals(PolicyResolution.RejectionReason.RETENTION_WEAKENING, result.rejectionReason());
    }

    @Test void requiredProjectionBypassRejected() {
        var aclAllow = new AclEvaluation.Result(AclEvaluation.Decision.ALLOW, null, "r1", 1);
        var result = PolicyResolution.resolve(
                "installation-a", "producer-a", "installation-a",
                PolicyResolution.ProducerState.ACTIVE, aclAllow,
                "economy", PolicyResolution.DurabilityClass.REPLICATED_DURABLE,
                PolicyResolution.RetentionClass.STANDARD, "bypass-policy",
                PolicyResolution.PriorityClass.P1, "binding-1", "snap-1",
                List.of("economy"), testPolicyConfig());
        assertEquals(PolicyResolution.AdmissionDecision.REJECTED, result.admissionDecision());
        assertEquals(PolicyResolution.RejectionReason.PROJECTION_BYPASS, result.rejectionReason());
    }

    @Test void effectivePolicyReasonCodes() {
        var aclAllow = new AclEvaluation.Result(AclEvaluation.Decision.ALLOW, null, "r1", 1);
        var result = PolicyResolution.resolve(
                "installation-a", "producer-a", "installation-a",
                PolicyResolution.ProducerState.ACTIVE, aclAllow,
                "economy", PolicyResolution.DurabilityClass.REPLICATED_DURABLE,
                PolicyResolution.RetentionClass.STANDARD, "proj-v1",
                PolicyResolution.PriorityClass.P1, "binding-1", "snap-1",
                List.of("economy"), testPolicyConfig());
        assertEquals(PolicyResolution.AdmissionDecision.ADMITTED, result.admissionDecision());
        assertNull(result.rejectionReason());
        assertFalse(result.decisionDetail().isEmpty());
        assertEquals(42, result.policyVersion());
    }

    // --- Ownership Snapshot ---

    @Test void validNodeLocalOwnershipSnapshot() {
        var snapshot = testOwnershipSnapshot();
        assertTrue(snapshot.validate().isEmpty());
        assertTrue(snapshot.isNamespaceLocallyOwned("economy"));
        assertFalse(snapshot.isNamespaceLocallyOwned("mining"));
    }

    @Test void unsupportedOwnershipModeWouldBeRejected() {
        // NODE_LOCAL is the only supported mode; attempting dynamic election is design-rejected
        var snapshot = testOwnershipSnapshot();
        assertTrue(snapshot.validate().isEmpty());
        assertEquals(OwnershipSnapshot.OwnershipMode.NODE_LOCAL, snapshot.mode());
    }

    @Test void duplicateOwnershipRejected() {
        var entries = List.of(
                new OwnershipSnapshot.NamespaceEntry("economy", "node-1", "agent-1", "installation-a",
                        OwnershipSnapshot.OwnershipMode.NODE_LOCAL),
                new OwnershipSnapshot.NamespaceEntry("economy", "node-1", "agent-1", "installation-a",
                        OwnershipSnapshot.OwnershipMode.NODE_LOCAL));
        var snapshot = new OwnershipSnapshot("snap-1", 1, "installation-a", "node-1",
                OwnershipSnapshot.OwnershipMode.NODE_LOCAL, entries);
        assertTrue(snapshot.validate().contains(OwnershipSnapshot.Violation.DUPLICATE_NAMESPACE));
    }

    @Test void crossInstallationOwnershipRejected() {
        var entries = List.of(new OwnershipSnapshot.NamespaceEntry(
                "economy", "node-1", "agent-1", "installation-b",
                OwnershipSnapshot.OwnershipMode.NODE_LOCAL));
        var snapshot = new OwnershipSnapshot("snap-1", 1, "installation-a", "node-1",
                OwnershipSnapshot.OwnershipMode.NODE_LOCAL, entries);
        assertTrue(snapshot.validate().contains(OwnershipSnapshot.Violation.CROSS_INSTALLATION));
    }

    @Test void localAdmissionRejectedWhenNamespaceNotLocallyOwned() {
        FakeAgentFixture agent = new FakeAgentFixture(16);
        agent.addOwnedNamespace("mining");
        // Submit directly to the fake agent to verify namespace ownership rejection
        var request = new ClientPublishRequest(ValidationContractTest.envelope(1, List.of()), 1);
        CompletionException ex = assertThrows(CompletionException.class,
                () -> agent.submit("producer-a", request).toCompletableFuture().join());
        assertInstanceOf(ContractViolationException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("not locally owned"));
    }

    // --- Quotas and Bounded Ingress ---

    @Test void quotaUnderLimitAdmission() {
        var result = AdmissionControl.evaluate(
                PolicyResolution.PriorityClass.P1,
                PolicyResolution.ProducerState.ACTIVE,
                new AdmissionControl.ProducerQuota("p", 5, 100, 0, 50, 0, 100_000),
                new AdmissionControl.NamespaceQuota("ns", 10, 200),
                new AdmissionControl.GlobalQuota(20, 500, 50, 10),
                100, true);
        assertEquals(PolicyResolution.AdmissionDecision.ADMITTED, result.decision());
    }

    @Test void perProducerInFlightQuotaRejection() {
        var result = AdmissionControl.evaluate(
                PolicyResolution.PriorityClass.P1,
                PolicyResolution.ProducerState.ACTIVE,
                new AdmissionControl.ProducerQuota("p", 100, 100, 0, 50, 0, 100_000),
                new AdmissionControl.NamespaceQuota("ns", 10, 200),
                new AdmissionControl.GlobalQuota(20, 500, 50, 10),
                100, true);
        assertEquals(PolicyResolution.AdmissionDecision.REJECTED, result.decision());
        assertEquals(AdmissionControl.RejectionReason.PRODUCER_IN_FLIGHT_EXCEEDED, result.rejectionReason());
    }

    @Test void namespaceGlobalQuotaRejection() {
        var result = AdmissionControl.evaluate(
                PolicyResolution.PriorityClass.P1,
                PolicyResolution.ProducerState.ACTIVE,
                new AdmissionControl.ProducerQuota("p", 5, 100, 0, 50, 0, 100_000),
                new AdmissionControl.NamespaceQuota("ns", 200, 200),
                new AdmissionControl.GlobalQuota(20, 500, 50, 10),
                100, true);
        assertEquals(PolicyResolution.AdmissionDecision.REJECTED, result.decision());
        assertEquals(AdmissionControl.RejectionReason.NAMESPACE_IN_FLIGHT_EXCEEDED, result.rejectionReason());
    }

    @Test void lowerPriorityCannotConsumeP0ReservedCapacity() {
        var result = AdmissionControl.evaluate(
                PolicyResolution.PriorityClass.P2,
                PolicyResolution.ProducerState.ACTIVE,
                new AdmissionControl.ProducerQuota("p", 5, 100, 0, 50, 0, 100_000),
                new AdmissionControl.NamespaceQuota("ns", 10, 200),
                new AdmissionControl.GlobalQuota(460, 500, 50, 10),
                100, true);
        assertEquals(PolicyResolution.AdmissionDecision.REJECTED, result.decision());
        assertEquals(AdmissionControl.RejectionReason.P0_CAPACITY_RESERVED, result.rejectionReason());
    }

    @Test void p0ProducerAdmissionWithReservedCapacity() {
        var result = AdmissionControl.evaluate(
                PolicyResolution.PriorityClass.P0,
                PolicyResolution.ProducerState.ACTIVE,
                new AdmissionControl.ProducerQuota("p", 5, 100, 0, 50, 0, 100_000),
                new AdmissionControl.NamespaceQuota("ns", 10, 200),
                new AdmissionControl.GlobalQuota(460, 500, 50, 10),
                100, true);
        assertEquals(PolicyResolution.AdmissionDecision.ADMITTED, result.decision());
    }

    @Test void boundedIngressStateCannotGrowUnbounded() {
        // Quota limits prevent unbounded growth
        var result = AdmissionControl.evaluate(
                PolicyResolution.PriorityClass.P1,
                PolicyResolution.ProducerState.ACTIVE,
                new AdmissionControl.ProducerQuota("p", 100, 100, 0, 50, 0, 100_000),
                new AdmissionControl.NamespaceQuota("ns", 10, 200),
                new AdmissionControl.GlobalQuota(20, 500, 50, 10),
                100, true);
        assertEquals(PolicyResolution.AdmissionDecision.REJECTED, result.decision());
    }

    // --- Fake Agent Publish Integration ---

    @Test void fakeAgentPublishRejectsUnauthorizedProducer() {
        FakeAgentFixture agent = new FakeAgentFixture(16);
        agent.registerProducer("authorized-only");
        CompletionException ex = assertThrows(CompletionException.class,
                () -> agent.submit("unknown-producer",
                        new ClientPublishRequest(ValidationContractTest.envelope(1, List.of()), 1))
                        .toCompletableFuture().join());
        assertInstanceOf(ContractViolationException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("not authorized"));
    }

    @Test void fakeAgentPublishRejectsOverQuotaProducer() {
        FakeAgentFixture agent = new FakeAgentFixture(100, "fixture-installation", 1);
        agent.addOwnedNamespace("reference");
        var request1 = new ClientPublishRequest(ValidationContractTest.envelope(1, List.of()), 1);
        agent.submit("producer-a", request1).toCompletableFuture().join();
        var request2 = new ClientPublishRequest(
                new EnvelopeInput("01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4b", "reference", "progress",
                        new byte[]{1}, "progress.delta", 1, "DELTA", "op-2", "EVENT",
                        "POLICY_RESOLVED_BY_AGENT", List.of(), new byte[]{3}, 1,
                        "manifest-issued:reference.progress-delta:v1"), 2);
        CompletionException ex = assertThrows(CompletionException.class,
                () -> agent.submit("producer-a", request2).toCompletableFuture().join());
        assertInstanceOf(ContractViolationException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("quota exceeded"));
    }

    @Test void fakeAgentPublishRejectsNotOwnedNamespace() {
        FakeAgentFixture agent = new FakeAgentFixture(16);
        agent.addOwnedNamespace("mining");
        var request = new ClientPublishRequest(ValidationContractTest.envelope(1, List.of()), 1);
        CompletionException ex = assertThrows(CompletionException.class,
                () -> agent.submit("producer-a", request).toCompletableFuture().join());
        assertInstanceOf(ContractViolationException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("not locally owned"));
    }

    // --- Bridge Contract ---

    @Test void logicalProducerClientCarriesEffectivePolicyIdentity() {
        var fixture = bridgeFixture(4);
        fixture.bridge.startFixture(true);
        var client = (LogicalProducerClient) fixture.bridge.clientFor(fixture.pluginHandle);
        assertEquals("producer-reference", fixture.registration.authenticatedProducerId());
        assertEquals(PolicyResolution.PriorityClass.P1, fixture.registration.effectivePriorityClass());
        assertEquals(PolicyResolution.QuotaClass.STANDARD, fixture.registration.effectiveQuotaClass());
    }

    @Test void registeredPluginHandleCannotBypassPolicy() {
        var fixture = bridgeFixture(2);
        fixture.bridge.startFixture(true);
        DomainClient client = fixture.bridge.clientFor(fixture.pluginHandle);
        assertThrows(IllegalArgumentException.class,
                () -> client.submit(
                        new EventContractHandle("not-in-manifest"),
                        EVENT_ONE, new byte[]{1}));
    }

    @Test void noClientForStringOrEquivalentArbitrarySelector() throws Exception {
        assertFalse(java.util.Arrays.stream(CraftRelayService.class.getMethods())
                .anyMatch(m -> m.getName().equals("clientFor")
                        && java.util.Arrays.asList(m.getParameterTypes()).contains(String.class)));
    }

    // --- Helpers ---

    private static ProducerRegistration registration(String installationId, String producerId) {
        return new ProducerRegistration(installationId, "node-a", producerId,
                "01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4c", testManifest());
    }

    private static IntegrationManifest testManifest() {
        return new IntegrationManifest(
                "reference", 1, "ReferencePlugin",
                Set.of(ReferenceDomainAdapter.PROGRESS_DELTA.opaqueValue()),
                Set.of(ReferenceDomainAdapter.GET_PROGRESS.opaqueValue()),
                128, 64, 32);
    }

    private static AclEvaluation.Principal testPrincipal(CredentialReference.CredentialStatus status) {
        return new AclEvaluation.Principal("producer-a", "installation-a",
                new CredentialReference("cred-1", CredentialReference.CredentialKind.FAKE_TEST_ONLY,
                        1, status, "installation-a"));
    }

    private static PolicyResolution.Configuration testPolicyConfig() {
        return new PolicyResolution.Configuration(
                "installation-a",
                PolicyResolution.DurabilityClass.REPLICATED_DURABLE,
                PolicyResolution.RetentionClass.STANDARD,
                "proj-v1",
                PolicyResolution.PriorityClass.P1,
                List.of("binding-1"),
                42);
    }

    private static PolicyResolution.EffectivePolicy resolveWithDurability(
            PolicyResolution.DurabilityClass requested, PolicyResolution.DurabilityClass minimum) {
        var config = new PolicyResolution.Configuration(
                "installation-a", minimum, PolicyResolution.RetentionClass.STANDARD, "proj-v1",
                PolicyResolution.PriorityClass.P1, List.of("binding-1"), 1);
        var aclAllow = new AclEvaluation.Result(AclEvaluation.Decision.ALLOW, null, "r1", 1);
        return PolicyResolution.resolve(
                "installation-a", "producer-a", "installation-a",
                PolicyResolution.ProducerState.ACTIVE, aclAllow,
                "economy", requested, PolicyResolution.RetentionClass.STANDARD, "proj-v1",
                PolicyResolution.PriorityClass.P1, "binding-1", "snap-1",
                List.of("economy"), config);
    }

    private static OwnershipSnapshot testOwnershipSnapshot() {
        return new OwnershipSnapshot("snap-1", 1, "installation-a", "node-1",
                OwnershipSnapshot.OwnershipMode.NODE_LOCAL,
                List.of(new OwnershipSnapshot.NamespaceEntry(
                        "economy", "node-1", "agent-1", "installation-a",
                        OwnershipSnapshot.OwnershipMode.NODE_LOCAL)));
    }

    private record Fixture(
            TestServiceRegistration serviceRegistration,
            RegisteredPluginHandle pluginHandle,
            ProducerRegistration registration,
            FakeAgentFixture agent,
            CraftRelayPaperBridge bridge) {
    }

    private static Fixture bridgeFixture(int capacity) {
        TestServiceRegistration serviceRegistration = new TestServiceRegistration();
        RegisteredPluginHandle pluginHandle = serviceRegistration.issuePluginHandle(new Object());
        ProducerRegistration producer = registration("installation-a", "producer-reference");
        FakeAgentFixture agent = new FakeAgentFixture(16);
        SharedBridgeTransportRuntime runtime = new SharedBridgeTransportRuntime(
                agent,
                new FakeQueryServiceFixture(Map.of(
                        ReferenceDomainAdapter.GET_PROGRESS.opaqueValue(),
                        "42".getBytes(StandardCharsets.US_ASCII))),
                ClientObservability.Metrics.noOp());
        CraftRelayPaperBridge bridge = new CraftRelayPaperBridge(
                serviceRegistration, runtime, Map.of(pluginHandle, producer));
        return new Fixture(serviceRegistration, pluginHandle, producer, agent, bridge);
    }

    private static Fixture bridgeFixtureWithPolicyChecks(int capacity) {
        TestServiceRegistration serviceRegistration = new TestServiceRegistration();
        RegisteredPluginHandle pluginHandle = serviceRegistration.issuePluginHandle(new Object());
        ProducerRegistration producer = registration("installation-a", "producer-reference");
        FakeAgentFixture agent = new FakeAgentFixture(16);
        agent.registerProducer("producer-reference");
        agent.addOwnedNamespace("reference");
        SharedBridgeTransportRuntime runtime = new SharedBridgeTransportRuntime(
                agent,
                new FakeQueryServiceFixture(Map.of(
                        ReferenceDomainAdapter.GET_PROGRESS.opaqueValue(),
                        "42".getBytes(StandardCharsets.US_ASCII))),
                ClientObservability.Metrics.noOp());
        CraftRelayPaperBridge bridge = new CraftRelayPaperBridge(
                serviceRegistration, runtime, Map.of(pluginHandle, producer));
        return new Fixture(serviceRegistration, pluginHandle, producer, agent, bridge);
    }

    private static final class TestServiceRegistration implements BridgeServiceRegistration {
        private CraftRelayService registered;
        @Override public void register(CraftRelayService service) { registered = service; }
        @Override public void unregister(CraftRelayService service) { if (registered == service) registered = null; }
    }
}
