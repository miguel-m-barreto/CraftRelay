package io.craftrelay.paper.api;

import java.util.Arrays;

public record PublishStatusResult(
        Status status,
        long lifecycleRevision,
        byte[] snapshotChecksum,
        boolean fakeNonDurable) {
    public PublishStatusResult {
        snapshotChecksum = snapshotChecksum.clone();
        if (lifecycleRevision < 0 || (lifecycleRevision == 0 && status == Status.FOUND)) {
            throw new IllegalArgumentException("found status requires a positive lifecycle revision");
        }
        if (status == Status.FOUND && snapshotChecksum.length != 32) {
            throw new IllegalArgumentException("found status requires a 32-byte checksum");
        }
        if (status != Status.FOUND && snapshotChecksum.length != 0) {
            throw new IllegalArgumentException("absent status must not include a checksum");
        }
    }

    @Override public byte[] snapshotChecksum() { return snapshotChecksum.clone(); }

    public enum Status { FOUND, NOT_FOUND, TRANSPORT_INDETERMINATE }
}
