package io.craftrelay.client.policy;

import java.util.List;
import java.util.Objects;

public final class PolicyResolution {
    private PolicyResolution() {
    }

    public enum DurabilityClass { LOCAL_DURABLE, REPLICATED_DURABLE }
    public enum RetentionClass { STANDARD, EXTENDED, PERMANENT }
    public enum PriorityClass { P0, P1, P2, BACKGROUND }
    public enum QuotaClass { CRITICAL, STANDARD, BULK }
    public enum AdmissionDecision { ADMITTED, REJECTED }

    public enum RejectionReason {
        SELF_PROMOTION, DURABILITY_WEAKENING, RETENTION_WEAKENING, PROJECTION_BYPASS,
        INSTALLATION_ESCAPE, BEST_EFFORT_FORBIDDEN, INVALID_POLICY_BINDING,
        UNKNOWN_POLICY_BINDING, ACL_DENIED, PRODUCER_DISABLED, PRODUCER_SUSPENDED,
        CROSS_INSTALLATION, NOT_LOCALLY_OWNED, QUOTA_EXCEEDED
    }

    public record EffectivePolicy(
            String effectiveProducerId,
            String effectiveNamespace,
            DurabilityClass effectiveDurability,
            RetentionClass effectiveRetention,
            String effectiveProjectionPolicyId,
            QuotaClass effectiveQuotaClass,
            PriorityClass effectivePriorityClass,
            long policyVersion,
            String ownershipSnapshotId,
            AdmissionDecision admissionDecision,
            RejectionReason rejectionReason,
            String decisionDetail) {
    }

    public record Configuration(
            String installationId,
            DurabilityClass minimumDurability,
            RetentionClass minimumRetention,
            String requiredProjectionPolicyId,
            PriorityClass priorityCeiling,
            List<String> validPolicyBindings,
            long policyVersion) {
        public Configuration {
            Objects.requireNonNull(installationId);
            Objects.requireNonNull(minimumDurability);
            Objects.requireNonNull(minimumRetention);
            validPolicyBindings = List.copyOf(validPolicyBindings);
        }
    }

    public static EffectivePolicy resolve(
            String installationId,
            String producerId,
            String producerInstallationId,
            ProducerState producerState,
            AclEvaluation.Result aclResult,
            String namespace,
            DurabilityClass requestedDurability,
            RetentionClass requestedRetention,
            String requestedProjectionPolicyId,
            PriorityClass requestedPriority,
            String policyBindingId,
            String ownershipSnapshotId,
            List<String> ownedNamespaces,
            Configuration config) {

        if (!installationId.equals(config.installationId())) {
            return rejected(producerId, namespace, config, ownershipSnapshotId,
                    RejectionReason.INSTALLATION_ESCAPE, "producer cannot escape installation scope");
        }
        if (!producerInstallationId.equals(installationId)) {
            return rejected(producerId, namespace, config, ownershipSnapshotId,
                    RejectionReason.CROSS_INSTALLATION, "producer installation does not match request");
        }
        switch (producerState) {
            case DISABLED -> {
                return rejected(producerId, namespace, config, ownershipSnapshotId,
                        RejectionReason.PRODUCER_DISABLED, "producer is disabled");
            }
            case SUSPENDED -> {
                return rejected(producerId, namespace, config, ownershipSnapshotId,
                        RejectionReason.PRODUCER_SUSPENDED, "producer is suspended");
            }
            case ACTIVE -> { /* continue */ }
        }
        if (aclResult.decision() == AclEvaluation.Decision.DENY) {
            return rejected(producerId, namespace, config, ownershipSnapshotId,
                    RejectionReason.ACL_DENIED, "ACL denied");
        }
        if (requestedDurability.ordinal() < config.minimumDurability().ordinal()) {
            return rejected(producerId, namespace, config, ownershipSnapshotId,
                    RejectionReason.DURABILITY_WEAKENING, "cannot weaken durability below installation minimum");
        }
        if (requestedRetention.ordinal() < config.minimumRetention().ordinal()) {
            return rejected(producerId, namespace, config, ownershipSnapshotId,
                    RejectionReason.RETENTION_WEAKENING, "cannot weaken retention below installation minimum");
        }
        if (config.requiredProjectionPolicyId() != null
                && requestedProjectionPolicyId != null
                && !requestedProjectionPolicyId.isEmpty()
                && !requestedProjectionPolicyId.equals(config.requiredProjectionPolicyId())) {
            return rejected(producerId, namespace, config, ownershipSnapshotId,
                    RejectionReason.PROJECTION_BYPASS, "cannot bypass required projection policy");
        }
        if (requestedPriority.ordinal() < config.priorityCeiling().ordinal()) {
            return rejected(producerId, namespace, config, ownershipSnapshotId,
                    RejectionReason.SELF_PROMOTION, "producer cannot self-promote above priority ceiling");
        }
        if (policyBindingId != null && !policyBindingId.isEmpty()
                && !config.validPolicyBindings().contains(policyBindingId)) {
            return rejected(producerId, namespace, config, ownershipSnapshotId,
                    RejectionReason.UNKNOWN_POLICY_BINDING, "producer policy binding is not recognized");
        }
        if (!ownedNamespaces.contains(namespace)) {
            return rejected(producerId, namespace, config, ownershipSnapshotId,
                    RejectionReason.NOT_LOCALLY_OWNED, "namespace is not locally owned");
        }
        String effectiveProjection = config.requiredProjectionPolicyId() != null
                ? config.requiredProjectionPolicyId()
                : (requestedProjectionPolicyId != null ? requestedProjectionPolicyId : "");
        return new EffectivePolicy(
                producerId, namespace, requestedDurability, requestedRetention,
                effectiveProjection, QuotaClass.STANDARD, requestedPriority,
                config.policyVersion(), ownershipSnapshotId,
                AdmissionDecision.ADMITTED, null, "policy resolved successfully");
    }

    private static EffectivePolicy rejected(
            String producerId, String namespace, Configuration config, String ownershipSnapshotId,
            RejectionReason reason, String detail) {
        return new EffectivePolicy(
                producerId, namespace, config.minimumDurability(), config.minimumRetention(),
                config.requiredProjectionPolicyId() != null ? config.requiredProjectionPolicyId() : "",
                QuotaClass.STANDARD, config.priorityCeiling(),
                config.policyVersion(), ownershipSnapshotId,
                AdmissionDecision.REJECTED, reason, detail);
    }

    public enum ProducerState { ACTIVE, DISABLED, SUSPENDED }
}
