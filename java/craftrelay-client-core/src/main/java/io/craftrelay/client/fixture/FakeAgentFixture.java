package io.craftrelay.client.fixture;

import io.craftrelay.client.AgentClient;
import io.craftrelay.client.ClientPublishRequest;
import io.craftrelay.client.ContractValidation;
import io.craftrelay.client.ContractViolationException;
import io.craftrelay.client.PublishLifecycleSnapshot;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Bounded, in-memory, explicitly non-durable fixture. Never use as acceptance authority. */
public final class FakeAgentFixture implements AgentClient {
    private final int capacity;
    private final String installationId;
    private final Map<String, Entry> entries = new LinkedHashMap<>();

    public FakeAgentFixture(int capacity) {
        this(capacity, "fixture-installation");
    }

    public FakeAgentFixture(int capacity, String installationId) {
        this.capacity = ContractValidation.positiveInt32(capacity, "fakeAgentCapacity");
        this.installationId = ContractValidation.boundedText(installationId, "installationId", 128);
    }

    @Override
    public synchronized CompletionStage<PublishLifecycleSnapshot> submit(ClientPublishRequest request) {
        String eventId = request.envelopeInput().eventId();
        byte[] canonical = request.envelopeInput().canonicalBytes();
        Entry existing = entries.get(eventId);
        if (existing != null) {
            if (!Arrays.equals(existing.canonicalInput(), canonical)) {
                return CompletableFuture.failedFuture(ContractValidation.violation(
                        ContractViolationException.Code.LIFECYCLE_INTEGRITY_CONFLICT,
                        "same event_id retried with different immutable envelope input"));
            }
            return CompletableFuture.completedFuture(existing.snapshot());
        }
        if (entries.size() >= capacity) {
            return CompletableFuture.failedFuture(ContractValidation.violation(
                    ContractViolationException.Code.BOUNDS_EXCEEDED,
                    "fake Agent fixture capacity exhausted"));
        }
        byte[] checksum = sha256(ByteBuffer.allocate(8 + eventId.length())
                .putLong(1L).put(eventId.getBytes(java.nio.charset.StandardCharsets.US_ASCII)).array());
        PublishLifecycleSnapshot snapshot = new PublishLifecycleSnapshot(
                installationId,
                eventId,
                1,
                checksum,
                PublishLifecycleSnapshot.DeliveryStatus.LOCAL_ACCEPTED_FAKE,
                PublishLifecycleSnapshot.ProjectionStatus.NOT_REQUIRED,
                PublishLifecycleSnapshot.RetentionStatus.PRESENT,
                java.util.List.of(new PublishLifecycleSnapshot.AttemptSummary(1, "FAKE_NON_DURABLE_ACCEPTANCE")),
                true);
        entries.put(eventId, new Entry(canonical, snapshot));
        return CompletableFuture.completedFuture(snapshot);
    }

    @Override
    public synchronized CompletionStage<Optional<PublishLifecycleSnapshot>> getStatus(
            String eventId, Duration timeout) {
        if (timeout.isNegative() || timeout.isZero()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("timeout must be positive"));
        }
        Entry entry = entries.get(eventId);
        return CompletableFuture.completedFuture(entry == null ? Optional.empty() : Optional.of(entry.snapshot()));
    }

    public boolean durable() { return false; }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private record Entry(byte[] canonicalInput, PublishLifecycleSnapshot snapshot) {
        private Entry { canonicalInput = canonicalInput.clone(); }
        @Override public byte[] canonicalInput() { return canonicalInput.clone(); }
    }
}
