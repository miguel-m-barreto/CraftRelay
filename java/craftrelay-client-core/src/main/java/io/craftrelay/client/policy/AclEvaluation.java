package io.craftrelay.client.policy;

import io.craftrelay.client.ContractValidation;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public final class AclEvaluation {
    private AclEvaluation() {
    }

    public enum Action { PUBLISH, QUERY, WATCH, ADMIN }
    public enum Decision { ALLOW, DENY }
    public enum DenyReason {
        NO_MATCHING_RULE, EXPLICIT_DENY, CREDENTIAL_INVALID, CREDENTIAL_REVOKED,
        CREDENTIAL_EXPIRED, CROSS_INSTALLATION, NAMESPACE_DENIED
    }

    public record Principal(String producerId, String installationId, CredentialReference credential) {
        public Principal {
            producerId = ContractValidation.boundedText(producerId, "producerId", 128);
            installationId = ContractValidation.boundedText(installationId, "installationId", 128);
            Objects.requireNonNull(credential, "credential");
        }
    }

    public record Rule(String ruleId, Action action, String namespacePattern, Decision decision, int priority) {
        public Rule {
            ruleId = ContractValidation.boundedText(ruleId, "ruleId", 128);
            Objects.requireNonNull(action, "action");
            namespacePattern = ContractValidation.boundedText(namespacePattern, "namespacePattern", 256);
            Objects.requireNonNull(decision, "decision");
        }
    }

    public record Result(Decision decision, DenyReason denyReason, String matchedRuleId, long policyVersion) {
    }

    public static Result evaluate(
            Principal principal,
            String scopeInstallationId,
            String scopeNamespace,
            Action action,
            List<Rule> rules,
            long policyVersion) {
        if (!principal.installationId().equals(scopeInstallationId)) {
            return new Result(Decision.DENY, DenyReason.CROSS_INSTALLATION, null, policyVersion);
        }
        switch (principal.credential().status()) {
            case REVOKED -> {
                return new Result(Decision.DENY, DenyReason.CREDENTIAL_REVOKED, null, policyVersion);
            }
            case EXPIRED -> {
                return new Result(Decision.DENY, DenyReason.CREDENTIAL_EXPIRED, null, policyVersion);
            }
            case UNKNOWN -> {
                return new Result(Decision.DENY, DenyReason.CREDENTIAL_INVALID, null, policyVersion);
            }
            case ACTIVE -> { /* continue */ }
        }
        List<Rule> sorted = rules.stream()
                .filter(r -> r.action() == action)
                .sorted(Comparator.comparingInt(Rule::priority).reversed())
                .toList();
        for (Rule rule : sorted) {
            if (namespaceMatches(rule.namespacePattern(), scopeNamespace)) {
                return new Result(
                        rule.decision(),
                        rule.decision() == Decision.DENY ? DenyReason.EXPLICIT_DENY : null,
                        rule.ruleId(),
                        policyVersion);
            }
        }
        return new Result(Decision.DENY, DenyReason.NO_MATCHING_RULE, null, policyVersion);
    }

    static boolean namespaceMatches(String pattern, String namespace) {
        if ("*".equals(pattern)) return true;
        if (pattern.endsWith(".*")) {
            String prefix = pattern.substring(0, pattern.length() - 2);
            return namespace.startsWith(prefix) && namespace.length() > prefix.length();
        }
        return pattern.equals(namespace);
    }
}
