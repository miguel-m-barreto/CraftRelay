package io.craftrelay.client.policy;

import io.craftrelay.client.ContractValidation;

public record CredentialReference(
        String credentialId,
        CredentialKind kind,
        int revision,
        CredentialStatus status,
        String installationId) {
    public CredentialReference {
        credentialId = ContractValidation.boundedText(credentialId, "credentialId", 128);
        java.util.Objects.requireNonNull(kind, "kind");
        java.util.Objects.requireNonNull(status, "status");
        installationId = ContractValidation.boundedText(installationId, "installationId", 128);
        ContractValidation.positiveInt32(revision, "revision");
    }

    public enum CredentialKind { SHARED_SECRET, MTLS_CERTIFICATE, IPC_TOKEN, FAKE_TEST_ONLY }
    public enum CredentialStatus { ACTIVE, REVOKED, EXPIRED, UNKNOWN }
}
