package io.craftrelay.paper.bridge;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record IntegrationManifest(
        String integrationId,
        int integrationVersion,
        String paperPluginId,
        Set<String> eventContractHandles,
        Set<String> queryContractHandles,
        int maxPendingPublishes,
        int maxPendingQueries,
        int maxActiveWatches,
        Set<String> declaredNamespaces,
        Set<String> declaredEventTypes,
        Set<String> declaredQueryIds) {

    public IntegrationManifest(
            String integrationId, int integrationVersion, String paperPluginId,
            Set<String> eventContractHandles, Set<String> queryContractHandles,
            int maxPendingPublishes, int maxPendingQueries, int maxActiveWatches) {
        this(integrationId, integrationVersion, paperPluginId,
                eventContractHandles, queryContractHandles,
                maxPendingPublishes, maxPendingQueries, maxActiveWatches,
                Set.of(), Set.of(), Set.of());
    }

    public IntegrationManifest {
        if (integrationId == null || integrationId.isBlank() || integrationId.length() > 128
                || paperPluginId == null || paperPluginId.isBlank() || paperPluginId.length() > 128) {
            throw new IllegalArgumentException("manifest identities must be bounded");
        }
        eventContractHandles = Set.copyOf(eventContractHandles);
        queryContractHandles = Set.copyOf(queryContractHandles);
        declaredNamespaces = Set.copyOf(declaredNamespaces);
        declaredEventTypes = Set.copyOf(declaredEventTypes);
        declaredQueryIds = Set.copyOf(declaredQueryIds);
        if (integrationVersion <= 0
                || maxPendingPublishes <= 0
                || maxPendingPublishes > 4_096
                || maxPendingQueries <= 0
                || maxPendingQueries > 4_096
                || maxActiveWatches <= 0
                || maxActiveWatches > 4_096
                || eventContractHandles.size() > 256
                || queryContractHandles.size() > 256
                || declaredNamespaces.size() > 256
                || declaredEventTypes.size() > 256
                || declaredQueryIds.size() > 256) {
            throw new IllegalArgumentException("manifest values must be positive and bounded");
        }
    }

    public enum ValidationCode {
        VALID, DUPLICATE_EVENT, DUPLICATE_QUERY, DUPLICATE_NAMESPACE,
        INVALID_EVENT_NAME, INVALID_QUERY_NAME, INVALID_NAMESPACE_NAME,
        UNBOUNDED_LIMIT, MISSING_LIMIT, BEST_EFFORT_FORBIDDEN,
        SELF_PROMOTION_FORBIDDEN, INVALID_SCHEMA_VERSION
    }

    public List<ValidationCode> validateExtended() {
        var violations = new ArrayList<ValidationCode>();
        for (String ns : declaredNamespaces) {
            if (!isValidIdentifier(ns)) {
                violations.add(ValidationCode.INVALID_NAMESPACE_NAME);
            }
        }
        for (String event : declaredEventTypes) {
            if (!isValidIdentifier(event)) {
                violations.add(ValidationCode.INVALID_EVENT_NAME);
            }
        }
        for (String query : declaredQueryIds) {
            if (!isValidIdentifier(query)) {
                violations.add(ValidationCode.INVALID_QUERY_NAME);
            }
        }
        return List.copyOf(violations);
    }

    private static boolean isValidIdentifier(String value) {
        if (value == null || value.isEmpty() || value.length() > 128) return false;
        if (!Character.isLowerCase(value.charAt(0))) return false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!(Character.isLowerCase(c) || Character.isDigit(c)
                    || c == '.' || c == '_' || c == '-')) {
                return false;
            }
        }
        return true;
    }
}
