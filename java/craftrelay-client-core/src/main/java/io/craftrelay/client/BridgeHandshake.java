package io.craftrelay.client;

import java.util.Objects;

public final class BridgeHandshake {
    private BridgeHandshake() {
    }

    public record Request(
            int protocolMajor,
            int protocolMinor,
            InstallationScope installationScope,
            String integrationId,
            int integrationVersion,
            String proposedProducerInstanceId) {
        public Request {
            protocolMajor = ContractValidation.positiveInt32(protocolMajor, "protocolMajor");
            if (protocolMinor < 0) {
                throw ContractValidation.violation(
                        ContractViolationException.Code.INVALID_ARGUMENT,
                        "protocolMinor must be non-negative");
            }
            Objects.requireNonNull(installationScope, "installationScope");
            integrationId = ContractValidation.boundedText(integrationId, "integrationId", 128);
            integrationVersion = ContractValidation.positiveInt32(integrationVersion, "integrationVersion");
            proposedProducerInstanceId = ContractValidation.canonicalUuidV7(
                    proposedProducerInstanceId, "proposedProducerInstanceId");
        }
    }

    public record Response(
            boolean compatible,
            String authenticatedProducerId,
            String producerInstanceId,
            String transportSessionId,
            int maxActivePublishHandles,
            int maxActiveWatches) {
        public Response {
            authenticatedProducerId = ContractValidation.boundedText(
                    authenticatedProducerId, "authenticatedProducerId", 128);
            producerInstanceId = ContractValidation.canonicalUuidV7(
                    producerInstanceId, "producerInstanceId");
            transportSessionId = ContractValidation.boundedText(
                    transportSessionId, "transportSessionId", 128);
            maxActivePublishHandles = ContractValidation.positiveInt32(
                    maxActivePublishHandles, "maxActivePublishHandles");
            maxActiveWatches = ContractValidation.positiveInt32(maxActiveWatches, "maxActiveWatches");
        }
    }
}
