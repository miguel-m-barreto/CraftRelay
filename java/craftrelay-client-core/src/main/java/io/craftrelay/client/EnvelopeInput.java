package io.craftrelay.client;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/** Immutable client intent. Acceptance-time envelope fields are deliberately absent. */
public record EnvelopeInput(
        String eventId,
        String namespace,
        String logicalStreamType,
        byte[] streamKey,
        String eventType,
        int schemaVersion,
        String operationType,
        String operationId,
        String eventRole,
        String requestedMinimumDurability,
        List<MetadataEntry> clientMetadata,
        byte[] typedPayload,
        int canonicalizationVersion,
        String eventContractHandle) {

    public EnvelopeInput {
        eventId = ContractValidation.canonicalUuidV7(eventId, "eventId");
        namespace = ContractValidation.boundedText(namespace, "namespace", 128);
        logicalStreamType = ContractValidation.boundedText(logicalStreamType, "logicalStreamType", 128);
        eventType = ContractValidation.boundedText(eventType, "eventType", 128);
        schemaVersion = ContractValidation.positiveInt32(schemaVersion, "schemaVersion");
        operationType = ContractValidation.boundedText(operationType, "operationType", 128);
        operationId = ContractValidation.boundedText(operationId, "operationId", 256);
        eventRole = ContractValidation.boundedText(eventRole, "eventRole", 64);
        requestedMinimumDurability = ContractValidation.boundedText(
                requestedMinimumDurability, "requestedMinimumDurability", 64);
        canonicalizationVersion = ContractValidation.positiveInt32(
                canonicalizationVersion, "canonicalizationVersion");
        eventContractHandle = ContractValidation.boundedText(
                eventContractHandle, "eventContractHandle", 256);
        streamKey = Objects.requireNonNull(streamKey, "streamKey").clone();
        if (streamKey.length == 0 || streamKey.length > 1_024) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.BOUNDS_EXCEEDED,
                    "streamKey must contain 1..1024 bytes");
        }
        typedPayload = Objects.requireNonNull(typedPayload, "typedPayload").clone();
        if (typedPayload.length > ContractLimits.MAX_PAYLOAD_BYTES) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.PAYLOAD_TOO_LARGE,
                    "typedPayload exceeds " + ContractLimits.MAX_PAYLOAD_BYTES + " bytes");
        }
        clientMetadata = CanonicalMetadata.validateAndSort(clientMetadata);
    }

    @Override
    public byte[] streamKey() {
        return streamKey.clone();
    }

    @Override
    public byte[] typedPayload() {
        return typedPayload.clone();
    }

    public byte[] canonicalBytes() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            write(output, eventId);
            write(output, namespace);
            write(output, logicalStreamType);
            write(output, streamKey);
            write(output, eventType);
            output.writeInt(schemaVersion);
            write(output, operationType);
            write(output, operationId);
            write(output, eventRole);
            write(output, requestedMinimumDurability);
            output.writeInt(clientMetadata.size());
            for (MetadataEntry entry : clientMetadata) {
                write(output, entry.key());
                write(output, entry.value());
            }
            write(output, typedPayload);
            output.writeInt(canonicalizationVersion);
            write(output, eventContractHandle);
            output.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void write(DataOutputStream output, String value) throws IOException {
        write(output, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void write(DataOutputStream output, byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }
}
