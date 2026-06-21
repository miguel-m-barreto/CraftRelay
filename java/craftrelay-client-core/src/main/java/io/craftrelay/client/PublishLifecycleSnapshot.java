package io.craftrelay.client;

import java.util.List;

public record PublishLifecycleSnapshot(
        String installationId,
        String eventId,
        long revision,
        byte[] snapshotChecksum,
        DeliveryStatus deliveryStatus,
        ProjectionStatus projectionStatus,
        RetentionStatus retentionStatus,
        List<AttemptSummary> attemptSummaries,
        boolean fakeNonDurable) {

    public PublishLifecycleSnapshot {
        installationId = ContractValidation.boundedText(installationId, "installationId", 128);
        eventId = ContractValidation.canonicalUuidV7(eventId, "eventId");
        revision = ContractValidation.positiveInt64(revision, "lifecycleRevision");
        snapshotChecksum = ContractValidation.fixedChecksum(snapshotChecksum, "snapshotChecksum");
        attemptSummaries = List.copyOf(attemptSummaries);
        if (attemptSummaries.size() > 16) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.BOUNDS_EXCEEDED,
                    "attempt summaries exceed 16");
        }
    }

    @Override
    public byte[] snapshotChecksum() {
        return snapshotChecksum.clone();
    }

    public enum DeliveryStatus { NOT_ACCEPTED, LOCAL_ACCEPTED_FAKE, PENDING, RETRYING, REPLICATED, DELIVERY_BLOCKED }
    public enum ProjectionStatus { NOT_REQUIRED, PENDING, ACKNOWLEDGED, BLOCKED }
    public enum RetentionStatus { PRESENT, ELIGIBLE, REMOVED, INTEGRITY_BLOCKED }
    public record AttemptSummary(int attemptNumber, String outcomeCode) {
        public AttemptSummary {
            ContractValidation.positiveInt32(attemptNumber, "attemptNumber");
            outcomeCode = ContractValidation.boundedText(outcomeCode, "outcomeCode", 128);
        }
    }
}
