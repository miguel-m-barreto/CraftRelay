package io.craftrelay.client;

import java.security.MessageDigest;
import java.time.Instant;

public final class ConsistencyTokenValidator {
    private final TokenMacProvider macProvider;
    private final ClientObservability.SecurityEvents securityEvents;

    public ConsistencyTokenValidator(TokenMacProvider macProvider) {
        this(macProvider, ClientObservability.SecurityEvents.noOp());
    }

    public ConsistencyTokenValidator(
            TokenMacProvider macProvider,
            ClientObservability.SecurityEvents securityEvents) {
        this.macProvider = macProvider;
        this.securityEvents = securityEvents;
    }

    public void validate(
            ProjectionConsistencyToken token,
            TokenScope expected,
            Instant serverNow) {
        byte[] canonical = token.unsignedCanonicalBytes();
        byte[] checksum = sha256(canonical);
        if (!MessageDigest.isEqual(checksum, token.tokenChecksum())
                || !MessageDigest.isEqual(macProvider.sign(token.keyId(), canonical), token.mac())) {
            securityEvents.tokenValidationFailed("TOKEN_INVALID_MAC");
            throw ContractValidation.violation(
                    ContractViolationException.Code.TOKEN_INVALID_MAC,
                    "projection consistency token authentication failed");
        }
        if (!token.expiresAt().isAfter(serverNow) || token.issuedAt().isAfter(serverNow)) {
            securityEvents.tokenValidationFailed("TOKEN_EXPIRED");
            throw ContractValidation.violation(
                    ContractViolationException.Code.TOKEN_EXPIRED,
                    "projection consistency token is expired or not yet valid");
        }
        if (!token.installationId().equals(expected.installationId())
                || !token.authenticatedProducerId().equals(expected.producerId())
                || !token.projectorId().equals(expected.projectorId())
                || !token.projectionName().equals(expected.projectionName())
                || !token.queryScope().equals(expected.queryScope())) {
            securityEvents.tokenValidationFailed("TOKEN_SCOPE_MISMATCH");
            throw ContractValidation.violation(
                    ContractViolationException.Code.TOKEN_SCOPE_MISMATCH,
                    "projection consistency token scope does not match the request");
        }
    }

    public ProjectionConsistencyToken authenticate(ProjectionConsistencyToken unsigned) {
        byte[] canonical = unsigned.unsignedCanonicalBytes();
        return copyWithAuthentication(unsigned, sha256(canonical), macProvider.sign(unsigned.keyId(), canonical));
    }

    private static ProjectionConsistencyToken copyWithAuthentication(
            ProjectionConsistencyToken token, byte[] checksum, byte[] mac) {
        return new ProjectionConsistencyToken(
                token.tokenVersion(), token.installationId(), token.authenticatedProducerId(),
                token.projectorId(), token.projectionName(), token.queryScope(),
                token.queryDefinitionVersion(), token.topologyVersion(), token.routingVersion(),
                token.issuedAt(), token.expiresAt(), token.issuerAgentId(), token.keyId(),
                token.eventId(), token.requiredNextOffsets(), token.mutationReferences(), checksum, mac);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    public record TokenScope(
            String installationId,
            String producerId,
            String projectorId,
            String projectionName,
            String queryScope) {
    }
}
