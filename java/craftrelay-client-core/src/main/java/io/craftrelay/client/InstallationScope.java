package io.craftrelay.client;

public record InstallationScope(String installationId, String nodeId) {
    public InstallationScope {
        installationId = ContractValidation.boundedText(installationId, "installationId", 128);
        nodeId = ContractValidation.boundedText(nodeId, "nodeId", 128);
    }

    public String scope(String value) {
        return installationId + '\0' + value;
    }
}
