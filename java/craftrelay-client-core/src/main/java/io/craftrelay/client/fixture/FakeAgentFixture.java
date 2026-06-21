package io.craftrelay.client.fixture;

import io.craftrelay.client.AgentClient;
import io.craftrelay.client.ClientPublishRequest;
import io.craftrelay.client.ContractValidation;
import io.craftrelay.client.ContractViolationException;
import io.craftrelay.client.PublishLifecycleSnapshot;
import io.craftrelay.client.policy.PolicyResolution;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Bounded, in-memory, explicitly non-durable fixture. Never use as acceptance authority. */
public final class FakeAgentFixture implements AgentClient {
    private final int capacity;
    private final String installationId;
    private final Map<String, Entry> entries = new LinkedHashMap<>();
    private final Set<String> disabledProducers = new HashSet<>();
    private final Set<String> suspendedProducers = new HashSet<>();
    private final Set<String> authorizedProducers = new HashSet<>();
    private final Set<String> ownedNamespaces = new HashSet<>();
    private final Map<String, Integer> producerInFlight = new LinkedHashMap<>();
    private final int perProducerInFlightLimit;

    public FakeAgentFixture(int capacity) {
        this(capacity, "fixture-installation");
    }

    public FakeAgentFixture(int capacity, String installationId) {
        this(capacity, installationId, 256);
    }

    public FakeAgentFixture(int capacity, String installationId, int perProducerInFlightLimit) {
        this.capacity = ContractValidation.positiveInt32(capacity, "fakeAgentCapacity");
        this.installationId = ContractValidation.boundedText(installationId, "installationId", 128);
        this.perProducerInFlightLimit = perProducerInFlightLimit;
    }

    public void registerProducer(String producerId) {
        authorizedProducers.add(producerId);
    }

    public void disableProducer(String producerId) {
        disabledProducers.add(producerId);
    }

    public void suspendProducer(String producerId) {
        suspendedProducers.add(producerId);
    }

    public void addOwnedNamespace(String namespace) {
        ownedNamespaces.add(namespace);
    }

    @Override
    public synchronized CompletionStage<PublishLifecycleSnapshot> submit(ClientPublishRequest request) {
        return submit(null, request);
    }

    public synchronized CompletionStage<PublishLifecycleSnapshot> submit(
            String authenticatedProducerId, ClientPublishRequest request) {
        String eventId = request.envelopeInput().eventId();
        byte[] canonical = request.envelopeInput().canonicalBytes();

        if (authenticatedProducerId != null) {
            if (disabledProducers.contains(authenticatedProducerId)) {
                return CompletableFuture.failedFuture(ContractValidation.violation(
                        ContractViolationException.Code.INVALID_ARGUMENT,
                        "producer is disabled"));
            }
            if (suspendedProducers.contains(authenticatedProducerId)) {
                return CompletableFuture.failedFuture(ContractValidation.violation(
                        ContractViolationException.Code.INVALID_ARGUMENT,
                        "producer is suspended"));
            }
            if (!authorizedProducers.isEmpty() && !authorizedProducers.contains(authenticatedProducerId)) {
                return CompletableFuture.failedFuture(ContractValidation.violation(
                        ContractViolationException.Code.INVALID_ARGUMENT,
                        "producer is not authorized"));
            }
            int current = producerInFlight.getOrDefault(authenticatedProducerId, 0);
            if (current >= perProducerInFlightLimit) {
                return CompletableFuture.failedFuture(ContractValidation.violation(
                        ContractViolationException.Code.BOUNDS_EXCEEDED,
                        "producer in-flight quota exceeded"));
            }
        }

        String namespace = request.envelopeInput().namespace();
        if (!ownedNamespaces.isEmpty() && !ownedNamespaces.contains(namespace)) {
            return CompletableFuture.failedFuture(ContractValidation.violation(
                    ContractViolationException.Code.INVALID_ARGUMENT,
                    "namespace is not locally owned"));
        }

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
                List.of(new PublishLifecycleSnapshot.AttemptSummary(1, "FAKE_NON_DURABLE_ACCEPTANCE")),
                true);
        entries.put(eventId, new Entry(canonical, snapshot));
        if (authenticatedProducerId != null) {
            producerInFlight.merge(authenticatedProducerId, 1, Integer::sum);
        }
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

    public Set<String> disabledProducers() { return Collections.unmodifiableSet(disabledProducers); }
    public Set<String> ownedNamespaces() { return Collections.unmodifiableSet(ownedNamespaces); }

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
