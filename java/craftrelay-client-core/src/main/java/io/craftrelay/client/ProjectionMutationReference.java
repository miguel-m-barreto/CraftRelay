package io.craftrelay.client;

public record ProjectionMutationReference(
        String projectionId,
        String entityType,
        String entityId,
        long domainVersion,
        String topic,
        int partition,
        long requiredNextOffset) {
    public ProjectionMutationReference {
        projectionId = ContractValidation.boundedText(projectionId, "projectionId", 128);
        entityType = ContractValidation.boundedText(entityType, "entityType", 128);
        entityId = ContractValidation.boundedText(entityId, "entityId", 256);
        domainVersion = ContractValidation.positiveInt64(domainVersion, "domainVersion");
        topic = ContractValidation.boundedText(topic, "topic", 249);
        if (partition < 0 || requiredNextOffset < 0) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.INVALID_ARGUMENT,
                    "partition and exclusive requiredNextOffset must be non-negative");
        }
    }
}
