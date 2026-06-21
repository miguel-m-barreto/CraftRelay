package io.craftrelay.client.policy;

public final class AdmissionControl {
    private AdmissionControl() {
    }

    public enum RejectionReason {
        PRODUCER_IN_FLIGHT_EXCEEDED, PRODUCER_QUEUE_EXCEEDED, PRODUCER_BYTES_EXCEEDED,
        NAMESPACE_IN_FLIGHT_EXCEEDED, GLOBAL_IN_FLIGHT_EXCEEDED, P0_CAPACITY_RESERVED,
        PRODUCER_DISABLED, PRODUCER_UNAUTHORIZED, NOT_LOCALLY_OWNED
    }

    public record ProducerQuota(
            String producerId, int inFlightPublishes, int maxInFlightPublishes,
            int queuedPublishes, int maxQueuedPublishes,
            long inFlightBytes, long maxInFlightBytes) {
    }

    public record NamespaceQuota(String namespace, int inFlightPublishes, int maxInFlightPublishes) {
    }

    public record GlobalQuota(
            int inFlightPublishes, int maxInFlightPublishes,
            int reservedP0Capacity, int usedP0Capacity) {
    }

    public record Result(
            PolicyResolution.AdmissionDecision decision,
            RejectionReason rejectionReason,
            String rejectionDetail) {
    }

    public static Result evaluate(
            PolicyResolution.PriorityClass producerPriority,
            PolicyResolution.ProducerState producerState,
            ProducerQuota producerQuota,
            NamespaceQuota namespaceQuota,
            GlobalQuota globalQuota,
            long payloadBytes,
            boolean namespaceOwned) {

        if (producerState == PolicyResolution.ProducerState.DISABLED) {
            return new Result(PolicyResolution.AdmissionDecision.REJECTED,
                    RejectionReason.PRODUCER_DISABLED, "producer is disabled");
        }
        if (!namespaceOwned) {
            return new Result(PolicyResolution.AdmissionDecision.REJECTED,
                    RejectionReason.NOT_LOCALLY_OWNED, "namespace is not locally owned by this node");
        }
        if (producerQuota.inFlightPublishes() >= producerQuota.maxInFlightPublishes()) {
            return new Result(PolicyResolution.AdmissionDecision.REJECTED,
                    RejectionReason.PRODUCER_IN_FLIGHT_EXCEEDED, "producer in-flight publish limit reached");
        }
        if (producerQuota.queuedPublishes() >= producerQuota.maxQueuedPublishes()) {
            return new Result(PolicyResolution.AdmissionDecision.REJECTED,
                    RejectionReason.PRODUCER_QUEUE_EXCEEDED, "producer queued publish limit reached");
        }
        if (producerQuota.inFlightBytes() + payloadBytes > producerQuota.maxInFlightBytes()) {
            return new Result(PolicyResolution.AdmissionDecision.REJECTED,
                    RejectionReason.PRODUCER_BYTES_EXCEEDED, "producer in-flight bytes limit would be exceeded");
        }
        if (namespaceQuota.inFlightPublishes() >= namespaceQuota.maxInFlightPublishes()) {
            return new Result(PolicyResolution.AdmissionDecision.REJECTED,
                    RejectionReason.NAMESPACE_IN_FLIGHT_EXCEEDED, "namespace in-flight publish limit reached");
        }
        int availableGlobal = globalQuota.maxInFlightPublishes() - globalQuota.inFlightPublishes();
        if (availableGlobal <= 0) {
            return new Result(PolicyResolution.AdmissionDecision.REJECTED,
                    RejectionReason.GLOBAL_IN_FLIGHT_EXCEEDED, "global in-flight publish limit reached");
        }
        int remainingP0 = globalQuota.reservedP0Capacity() - globalQuota.usedP0Capacity();
        if (producerPriority != PolicyResolution.PriorityClass.P0) {
            int nonP0Available = availableGlobal - Math.max(remainingP0, 0);
            if (nonP0Available <= 0) {
                return new Result(PolicyResolution.AdmissionDecision.REJECTED,
                        RejectionReason.P0_CAPACITY_RESERVED,
                        "lower-priority producer cannot consume reserved P0 capacity");
            }
        }
        return new Result(PolicyResolution.AdmissionDecision.ADMITTED, null, "admitted");
    }
}
