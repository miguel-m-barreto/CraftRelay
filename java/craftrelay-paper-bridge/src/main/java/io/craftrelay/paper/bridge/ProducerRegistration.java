package io.craftrelay.paper.bridge;

import io.craftrelay.client.ContractValidation;
import io.craftrelay.client.policy.PolicyResolution;

public record ProducerRegistration(
        String installationId,
        String nodeId,
        String authenticatedProducerId,
        String producerInstanceId,
        IntegrationManifest manifest,
        PolicyResolution.PriorityClass effectivePriorityClass,
        PolicyResolution.QuotaClass effectiveQuotaClass,
        String policyBindingId) {

    public ProducerRegistration(
            String installationId, String nodeId, String authenticatedProducerId,
            String producerInstanceId, IntegrationManifest manifest) {
        this(installationId, nodeId, authenticatedProducerId, producerInstanceId, manifest,
                PolicyResolution.PriorityClass.P1, PolicyResolution.QuotaClass.STANDARD, "");
    }

    public ProducerRegistration {
        installationId = ContractValidation.boundedText(installationId, "installationId", 128);
        nodeId = ContractValidation.boundedText(nodeId, "nodeId", 128);
        authenticatedProducerId = ContractValidation.boundedText(
                authenticatedProducerId, "authenticatedProducerId", 128);
        producerInstanceId = ContractValidation.canonicalUuidV7(
                producerInstanceId, "producerInstanceId");
        java.util.Objects.requireNonNull(manifest, "manifest");
        java.util.Objects.requireNonNull(effectivePriorityClass, "effectivePriorityClass");
        java.util.Objects.requireNonNull(effectiveQuotaClass, "effectiveQuotaClass");
        if (policyBindingId == null) policyBindingId = "";
    }
}
