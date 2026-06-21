package io.craftrelay.tests;

import static org.junit.jupiter.api.Assertions.*;

import io.craftrelay.client.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

final class ValidationContractTest {
    private static final String EVENT_ID = "01890f3e-7b4c-7cc2-98c8-3f0f5f3f9b4a";

    @Test void canonicalUuidV7Validation() {
        assertEquals(EVENT_ID, ContractValidation.canonicalUuidV7(EVENT_ID, "eventId"));
        assertCode(ContractViolationException.Code.INVALID_UUID,
                () -> ContractValidation.canonicalUuidV7("not-a-uuid", "eventId"));
        assertCode(ContractViolationException.Code.UUID_NOT_VERSION_7,
                () -> ContractValidation.canonicalUuidV7(
                        "01890f3e-7b4c-4cc2-98c8-3f0f5f3f9b4a", "eventId"));
    }

    @Test void positiveNumericAndSchemaBoundaries() {
        assertEquals(1, ContractValidation.positiveInt32(1, "value"));
        assertEquals(Integer.MAX_VALUE, ContractValidation.positiveInt32(Integer.MAX_VALUE, "value"));
        assertEquals(1L, ContractValidation.positiveInt64(1, "value"));
        assertEquals(Long.MAX_VALUE, ContractValidation.positiveInt64(Long.MAX_VALUE, "value"));
        assertCode(ContractViolationException.Code.NON_POSITIVE_INT32,
                () -> ContractValidation.positiveInt32(0, "schemaVersion"));
        assertCode(ContractViolationException.Code.NON_POSITIVE_INT64,
                () -> ContractValidation.positiveInt64(-1, "sequence"));
        assertCode(ContractViolationException.Code.NON_POSITIVE_INT32,
                () -> envelope(0, List.of()));
    }

    @Test void metadataRejectsDuplicatesCountAndBytes() {
        assertCode(ContractViolationException.Code.DUPLICATE_METADATA_KEY,
                () -> envelope(1, List.of(new MetadataEntry("same", "a"), new MetadataEntry("same", "b"))));
        List<MetadataEntry> tooMany = new ArrayList<>();
        for (int index = 0; index <= ContractLimits.MAX_METADATA_ENTRIES; index++) {
            tooMany.add(new MetadataEntry("key." + index, "value"));
        }
        assertCode(ContractViolationException.Code.METADATA_COUNT_EXCEEDED,
                () -> envelope(1, tooMany));
        String large = "x".repeat(ContractLimits.MAX_METADATA_VALUE_BYTES);
        List<MetadataEntry> tooLarge = java.util.stream.IntStream.range(0, 9)
                .mapToObj(index -> new MetadataEntry("key." + index, large)).toList();
        assertCode(ContractViolationException.Code.METADATA_BYTES_EXCEEDED,
                () -> envelope(1, tooLarge));
    }

    @Test void metadataAndEnvelopeCanonicalizationAreStable() {
        EnvelopeInput left = envelope(1, List.of(
                new MetadataEntry("z", "last"), new MetadataEntry("a", "first")));
        EnvelopeInput right = envelope(1, List.of(
                new MetadataEntry("a", "first"), new MetadataEntry("z", "last")));
        assertArrayEquals(left.canonicalBytes(), right.canonicalBytes());
        assertEquals(List.of("a", "z"), left.clientMetadata().stream().map(MetadataEntry::key).toList());
    }

    @Test void lifecycleRevisionChecksumConflictIsTyped() {
        LifecycleSnapshotTracker tracker = new LifecycleSnapshotTracker();
        assertEquals(LifecycleSnapshotTracker.Decision.APPLIED, tracker.apply(snapshot(1, (byte) 1)));
        assertEquals(LifecycleSnapshotTracker.Decision.DUPLICATE, tracker.apply(snapshot(1, (byte) 1)));
        assertCode(ContractViolationException.Code.LIFECYCLE_INTEGRITY_CONFLICT,
                () -> tracker.apply(snapshot(1, (byte) 2)));
    }

    static EnvelopeInput envelope(int schemaVersion, List<MetadataEntry> metadata) {
        return new EnvelopeInput(EVENT_ID, "reference", "progress", new byte[] {1},
                "progress.delta", schemaVersion, "DELTA", "operation-1", "EVENT",
                "POLICY_RESOLVED_BY_AGENT", metadata, new byte[] {2}, 1,
                "manifest-issued:reference.progress-delta:v1");
    }

    private static PublishLifecycleSnapshot snapshot(long revision, byte checksumByte) {
        byte[] checksum = new byte[32];
        Arrays.fill(checksum, checksumByte);
        return new PublishLifecycleSnapshot("installation-a", EVENT_ID, revision, checksum,
                PublishLifecycleSnapshot.DeliveryStatus.LOCAL_ACCEPTED_FAKE,
                PublishLifecycleSnapshot.ProjectionStatus.NOT_REQUIRED,
                PublishLifecycleSnapshot.RetentionStatus.PRESENT, List.of(), true);
    }

    private static void assertCode(ContractViolationException.Code code, Runnable operation) {
        ContractViolationException exception = assertThrows(ContractViolationException.class, operation::run);
        assertEquals(code, exception.code());
    }
}
