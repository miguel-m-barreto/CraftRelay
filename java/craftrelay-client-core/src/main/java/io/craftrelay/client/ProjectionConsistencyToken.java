package io.craftrelay.client;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

public record ProjectionConsistencyToken(
        int tokenVersion,
        String installationId,
        String authenticatedProducerId,
        String projectorId,
        String projectionName,
        String queryScope,
        int queryDefinitionVersion,
        long topologyVersion,
        long routingVersion,
        Instant issuedAt,
        Instant expiresAt,
        String issuerAgentId,
        String keyId,
        String eventId,
        List<ProjectionBarrier.PartitionBarrier> requiredNextOffsets,
        List<ProjectionMutationReference> mutationReferences,
        byte[] tokenChecksum,
        byte[] mac) {

    public ProjectionConsistencyToken {
        tokenVersion = ContractValidation.positiveInt32(tokenVersion, "tokenVersion");
        installationId = ContractValidation.boundedText(installationId, "installationId", 128);
        authenticatedProducerId = ContractValidation.boundedText(
                authenticatedProducerId, "authenticatedProducerId", 128);
        projectorId = ContractValidation.boundedText(projectorId, "projectorId", 128);
        projectionName = ContractValidation.boundedText(projectionName, "projectionName", 128);
        queryScope = ContractValidation.boundedText(queryScope, "queryScope", 256);
        queryDefinitionVersion = ContractValidation.positiveInt32(
                queryDefinitionVersion, "queryDefinitionVersion");
        topologyVersion = ContractValidation.positiveInt64(topologyVersion, "topologyVersion");
        routingVersion = ContractValidation.positiveInt64(routingVersion, "routingVersion");
        issuerAgentId = ContractValidation.boundedText(issuerAgentId, "issuerAgentId", 128);
        keyId = ContractValidation.boundedText(keyId, "keyId", 128);
        eventId = ContractValidation.canonicalUuidV7(eventId, "eventId");
        requiredNextOffsets = requiredNextOffsets.stream()
                .sorted(Comparator.comparing(ProjectionBarrier.PartitionBarrier::topic)
                        .thenComparingInt(ProjectionBarrier.PartitionBarrier::partition))
                .toList();
        mutationReferences = List.copyOf(mutationReferences);
        if (requiredNextOffsets.isEmpty()
                || requiredNextOffsets.size() > ContractLimits.MAX_BARRIER_PARTITIONS
                || mutationReferences.size() > ContractLimits.MAX_TOKEN_MUTATION_REFERENCES) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.BOUNDS_EXCEEDED,
                    "token vectors exceed configured bounds");
        }
        var partitionKeys = new HashSet<String>();
        for (ProjectionBarrier.PartitionBarrier offset : requiredNextOffsets) {
            if (!partitionKeys.add(offset.topic() + '\0' + offset.partition())) {
                throw ContractValidation.violation(
                        ContractViolationException.Code.INVALID_ARGUMENT,
                        "duplicate token partition requirement");
            }
        }
        long estimatedBytes = installationId.getBytes(StandardCharsets.UTF_8).length
                + authenticatedProducerId.getBytes(StandardCharsets.UTF_8).length
                + projectorId.getBytes(StandardCharsets.UTF_8).length
                + projectionName.getBytes(StandardCharsets.UTF_8).length
                + queryScope.getBytes(StandardCharsets.UTF_8).length
                + issuerAgentId.getBytes(StandardCharsets.UTF_8).length
                + keyId.getBytes(StandardCharsets.UTF_8).length
                + eventId.length() + 128L
                + requiredNextOffsets.size() * 280L
                + mutationReferences.size() * 900L;
        if (estimatedBytes > ContractLimits.MAX_TOKEN_BYTES) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.BOUNDS_EXCEEDED,
                    "token fields exceed " + ContractLimits.MAX_TOKEN_BYTES + " bytes");
        }
        tokenChecksum = ContractValidation.fixedChecksum(tokenChecksum, "tokenChecksum");
        mac = mac.clone();
        if (mac.length != 32) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.TOKEN_INVALID_MAC,
                    "token MAC must contain 32 bytes");
        }
    }

    @Override public byte[] tokenChecksum() { return tokenChecksum.clone(); }
    @Override public byte[] mac() { return mac.clone(); }

    public byte[] unsignedCanonicalBytes() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.writeInt(tokenVersion);
            write(output, installationId);
            write(output, authenticatedProducerId);
            write(output, projectorId);
            write(output, projectionName);
            write(output, queryScope);
            output.writeInt(queryDefinitionVersion);
            output.writeLong(topologyVersion);
            output.writeLong(routingVersion);
            output.writeLong(issuedAt.toEpochMilli());
            output.writeLong(expiresAt.toEpochMilli());
            write(output, issuerAgentId);
            write(output, keyId);
            write(output, eventId);
            output.writeInt(requiredNextOffsets.size());
            for (ProjectionBarrier.PartitionBarrier offset : requiredNextOffsets) {
                write(output, offset.topic());
                output.writeInt(offset.partition());
                output.writeLong(offset.requiredNextOffset());
            }
            output.writeInt(mutationReferences.size());
            for (ProjectionMutationReference mutation : mutationReferences) {
                write(output, mutation.projectionId());
                write(output, mutation.entityType());
                write(output, mutation.entityId());
                output.writeLong(mutation.domainVersion());
                write(output, mutation.topic());
                output.writeInt(mutation.partition());
                output.writeLong(mutation.requiredNextOffset());
            }
            output.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void write(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }
}
