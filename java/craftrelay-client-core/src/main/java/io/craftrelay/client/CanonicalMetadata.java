package io.craftrelay.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class CanonicalMetadata {
    private static final Comparator<MetadataEntry> UNSIGNED_UTF8_KEY_ORDER = (left, right) ->
            java.util.Arrays.compareUnsigned(
                    left.key().getBytes(StandardCharsets.UTF_8),
                    right.key().getBytes(StandardCharsets.UTF_8));

    private CanonicalMetadata() {
    }

    public static List<MetadataEntry> validateAndSort(List<MetadataEntry> entries) {
        if (entries == null || entries.size() > ContractLimits.MAX_METADATA_ENTRIES) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.METADATA_COUNT_EXCEEDED,
                    "metadata entry count exceeds " + ContractLimits.MAX_METADATA_ENTRIES);
        }
        Set<String> keys = new HashSet<>();
        int totalBytes = 0;
        List<MetadataEntry> canonical = new ArrayList<>(entries.size());
        for (MetadataEntry entry : entries) {
            if (!keys.add(entry.key())) {
                throw ContractValidation.violation(
                        ContractViolationException.Code.DUPLICATE_METADATA_KEY,
                        "duplicate metadata key: " + entry.key());
            }
            totalBytes = Math.addExact(totalBytes,
                    entry.key().getBytes(StandardCharsets.UTF_8).length
                            + entry.value().getBytes(StandardCharsets.UTF_8).length);
            canonical.add(entry);
        }
        if (totalBytes > ContractLimits.MAX_METADATA_BYTES) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.METADATA_BYTES_EXCEEDED,
                    "metadata bytes exceed " + ContractLimits.MAX_METADATA_BYTES);
        }
        canonical.sort(UNSIGNED_UTF8_KEY_ORDER);
        return List.copyOf(canonical);
    }
}
