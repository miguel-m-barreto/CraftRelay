package io.craftrelay.client;

import static io.craftrelay.client.ContractLimits.MAX_METADATA_KEY_BYTES;
import static io.craftrelay.client.ContractLimits.MAX_METADATA_VALUE_BYTES;

public record MetadataEntry(String key, String value) {
    public MetadataEntry {
        key = ContractValidation.boundedText(key, "metadata.key", MAX_METADATA_KEY_BYTES);
        value = ContractValidation.boundedText(value, "metadata.value", MAX_METADATA_VALUE_BYTES);
        if (!key.matches("[a-z0-9][a-z0-9._-]*")) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.INVALID_METADATA_KEY,
                    "metadata.key must match [a-z0-9][a-z0-9._-]*");
        }
    }
}
