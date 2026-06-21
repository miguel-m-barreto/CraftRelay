package io.craftrelay.paper.bridge;

import io.craftrelay.client.ContractValidation;

public record ProducerRegistration(
        String installationId,
        String nodeId,
        String authenticatedProducerId,
        String producerInstanceId,
        IntegrationManifest manifest) {
    public ProducerRegistration {
        installationId = ContractValidation.boundedText(installationId, "installationId", 128);
        nodeId = ContractValidation.boundedText(nodeId, "nodeId", 128);
        authenticatedProducerId = ContractValidation.boundedText(
                authenticatedProducerId, "authenticatedProducerId", 128);
        producerInstanceId = ContractValidation.canonicalUuidV7(
                producerInstanceId, "producerInstanceId");
        java.util.Objects.requireNonNull(manifest, "manifest");
    }
}
