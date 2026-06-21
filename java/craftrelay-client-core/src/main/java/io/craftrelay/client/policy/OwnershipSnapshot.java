package io.craftrelay.client.policy;

import io.craftrelay.client.ContractValidation;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record OwnershipSnapshot(
        String snapshotId,
        long snapshotVersion,
        String installationId,
        String nodeId,
        OwnershipMode mode,
        List<NamespaceEntry> entries) {

    public OwnershipSnapshot {
        snapshotId = ContractValidation.boundedText(snapshotId, "snapshotId", 128);
        ContractValidation.positiveInt64(snapshotVersion, "snapshotVersion");
        installationId = ContractValidation.boundedText(installationId, "installationId", 128);
        nodeId = ContractValidation.boundedText(nodeId, "nodeId", 128);
        java.util.Objects.requireNonNull(mode, "mode");
        entries = List.copyOf(entries);
    }

    public enum OwnershipMode { NODE_LOCAL }

    public enum Violation {
        MISSING_OWNER, DUPLICATE_NAMESPACE, CROSS_INSTALLATION,
        UNSUPPORTED_MODE, DYNAMIC_ELECTION_FORBIDDEN
    }

    public record NamespaceEntry(
            String namespace, String ownerNodeId, String ownerAgentId,
            String installationId, OwnershipMode mode) {
        public NamespaceEntry {
            namespace = ContractValidation.boundedText(namespace, "namespace", 128);
            ownerNodeId = ContractValidation.boundedText(ownerNodeId, "ownerNodeId", 128);
            ownerAgentId = ContractValidation.boundedText(ownerAgentId, "ownerAgentId", 128);
            installationId = ContractValidation.boundedText(installationId, "installationId", 128);
            java.util.Objects.requireNonNull(mode, "mode");
        }
    }

    public List<Violation> validate() {
        var violations = new java.util.ArrayList<Violation>();
        if (mode != OwnershipMode.NODE_LOCAL) {
            violations.add(Violation.UNSUPPORTED_MODE);
        }
        Set<String> seen = new HashSet<>();
        for (NamespaceEntry entry : entries) {
            if (entry.ownerNodeId().isBlank() || entry.ownerAgentId().isBlank()) {
                violations.add(Violation.MISSING_OWNER);
            }
            if (!seen.add(entry.namespace())) {
                violations.add(Violation.DUPLICATE_NAMESPACE);
            }
            if (!entry.installationId().equals(installationId)) {
                violations.add(Violation.CROSS_INSTALLATION);
            }
            if (entry.mode() != OwnershipMode.NODE_LOCAL) {
                violations.add(Violation.UNSUPPORTED_MODE);
            }
        }
        return List.copyOf(violations);
    }

    public boolean isNamespaceLocallyOwned(String namespace) {
        return entries.stream().anyMatch(
                e -> e.namespace().equals(namespace) && e.ownerNodeId().equals(nodeId));
    }

    public List<String> ownedNamespaces() {
        return entries.stream()
                .filter(e -> e.ownerNodeId().equals(nodeId))
                .map(NamespaceEntry::namespace)
                .toList();
    }
}
