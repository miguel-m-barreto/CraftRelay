package io.craftrelay.tests;

import static org.junit.jupiter.api.Assertions.*;

import io.craftrelay.client.*;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class TokenBarrierContractTest {
    private static final String EVENT_ID = "01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4a";
    private static final byte[] KEY = "fixture-hmac-key-material".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private final ConsistencyTokenValidator validator = new ConsistencyTokenValidator(
            new HmacSha256TokenMac(Map.of("fixture-key", KEY)));

    @Test void barrierCanonicalizationSortsExclusiveNextOffsetsAndCarriesVersions() {
        ProjectionBarrier barrier = barrier(3, 7, List.of(
                new ProjectionBarrier.PartitionBarrier("events", 1, 42),
                new ProjectionBarrier.PartitionBarrier("events", 0, 0)));
        assertEquals(1, barrier.barrierVersion());
        assertEquals(3, barrier.projectionTopologyVersion());
        assertEquals(7, barrier.routingVersion());
        assertEquals(0, barrier.partitions().getFirst().partition());
        assertEquals(0, barrier.partitions().getFirst().requiredNextOffset());
        assertArrayEquals(barrier.canonicalBytes(), barrier(3, 7, List.of(
                new ProjectionBarrier.PartitionBarrier("events", 0, 0),
                new ProjectionBarrier.PartitionBarrier("events", 1, 42))).canonicalBytes());
    }

    @Test void barrierVectorComparisonRejectsTopologyAndPartitionChanges() {
        ProjectionBarrier old = barrier(3, 7, List.of(new ProjectionBarrier.PartitionBarrier("events", 0, 4)));
        ProjectionBarrier advanced = barrier(3, 7, List.of(new ProjectionBarrier.PartitionBarrier("events", 0, 5)));
        ProjectionBarrier topology = barrier(4, 7, List.of(new ProjectionBarrier.PartitionBarrier("events", 0, 5)));
        ProjectionBarrier partitionSet = barrier(3, 7, List.of(
                new ProjectionBarrier.PartitionBarrier("events", 0, 5),
                new ProjectionBarrier.PartitionBarrier("events", 1, 0)));
        assertEquals(BarrierVectorComparison.Result.ADVANCED, BarrierVectorComparison.compare(old, advanced));
        assertEquals(BarrierVectorComparison.Result.INCOMPARABLE, BarrierVectorComparison.compare(old, topology));
        assertEquals(BarrierVectorComparison.Result.INCOMPARABLE, BarrierVectorComparison.compare(old, partitionSet));
    }

    @Test void tokenMacScopeAndExpiryValidation() {
        Instant now = Instant.parse("2030-01-01T00:00:00Z");
        ProjectionConsistencyToken token = validator.authenticate(unsignedToken(
                "installation-a", "producer-a", "projector-a", "accounts", "account:1",
                now.minusSeconds(1), now.plusSeconds(60)));
        validator.validate(token, scope("installation-a", "producer-a", "projector-a", "accounts", "account:1"), now);

        byte[] invalidMac = token.mac();
        invalidMac[0] ^= 1;
        ProjectionConsistencyToken tampered = copy(token, token.tokenChecksum(), invalidMac);
        assertCode(ContractViolationException.Code.TOKEN_INVALID_MAC,
                () -> validator.validate(tampered, scope("installation-a", "producer-a", "projector-a", "accounts", "account:1"), now));

        ProjectionConsistencyToken expired = validator.authenticate(unsignedToken(
                "installation-a", "producer-a", "projector-a", "accounts", "account:1",
                now.minusSeconds(120), now.minusSeconds(1)));
        assertCode(ContractViolationException.Code.TOKEN_EXPIRED,
                () -> validator.validate(expired, scope("installation-a", "producer-a", "projector-a", "accounts", "account:1"), now));
    }

    @Test void tokenRejectsCrossInstallationProducerAndProjectionScope() {
        Instant now = Instant.parse("2030-01-01T00:00:00Z");
        ProjectionConsistencyToken token = validator.authenticate(unsignedToken(
                "installation-a", "producer-a", "projector-a", "accounts", "account:1",
                now.minusSeconds(1), now.plusSeconds(60)));
        assertScopeMismatch(token, scope("installation-b", "producer-a", "projector-a", "accounts", "account:1"), now);
        assertScopeMismatch(token, scope("installation-a", "producer-b", "projector-a", "accounts", "account:1"), now);
        assertScopeMismatch(token, scope("installation-a", "producer-a", "projector-b", "accounts", "account:1"), now);
        assertScopeMismatch(token, scope("installation-a", "producer-a", "projector-a", "claims", "account:1"), now);
        assertScopeMismatch(token, scope("installation-a", "producer-a", "projector-a", "accounts", "account:2"), now);
    }

    private static ProjectionBarrier barrier(long topology, long routing, List<ProjectionBarrier.PartitionBarrier> partitions) {
        return new ProjectionBarrier(1, "barrier-1", "installation-a", "query-1", 1,
                topology, routing, bytes((byte) 1), Instant.EPOCH, partitions, bytes((byte) 2));
    }

    private static ProjectionConsistencyToken unsignedToken(
            String installation, String producer, String projector, String projection, String queryScope,
            Instant issuedAt, Instant expiresAt) {
        return new ProjectionConsistencyToken(1, installation, producer, projector, projection, queryScope,
                1, 3, 7, issuedAt, expiresAt, "agent-a", "fixture-key", EVENT_ID,
                List.of(new ProjectionBarrier.PartitionBarrier("events", 0, 42)),
                List.of(new ProjectionMutationReference("accounts", "account", "1", 9, "events", 0, 42)),
                new byte[32], new byte[32]);
    }

    private static ProjectionConsistencyToken copy(ProjectionConsistencyToken token, byte[] checksum, byte[] mac) {
        return new ProjectionConsistencyToken(token.tokenVersion(), token.installationId(),
                token.authenticatedProducerId(), token.projectorId(), token.projectionName(), token.queryScope(),
                token.queryDefinitionVersion(), token.topologyVersion(), token.routingVersion(), token.issuedAt(),
                token.expiresAt(), token.issuerAgentId(), token.keyId(), token.eventId(),
                token.requiredNextOffsets(), token.mutationReferences(), checksum, mac);
    }

    private static ConsistencyTokenValidator.TokenScope scope(
            String installation, String producer, String projector, String projection, String queryScope) {
        return new ConsistencyTokenValidator.TokenScope(installation, producer, projector, projection, queryScope);
    }

    private void assertScopeMismatch(ProjectionConsistencyToken token, ConsistencyTokenValidator.TokenScope scope, Instant now) {
        assertCode(ContractViolationException.Code.TOKEN_SCOPE_MISMATCH,
                () -> validator.validate(token, scope, now));
    }

    private static byte[] bytes(byte value) {
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, value);
        return bytes;
    }

    private static void assertCode(ContractViolationException.Code code, Runnable operation) {
        ContractViolationException exception = assertThrows(ContractViolationException.class, operation::run);
        assertEquals(code, exception.code());
    }
}
