package io.craftrelay.paper.api;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;

public record ProjectionBarrierView(
        int barrierVersion,
        String barrierId,
        String installationId,
        String queryId,
        int queryDefinitionVersion,
        long topologyVersion,
        long routingVersion,
        byte[] partitionSetChecksum,
        List<PartitionRequirement> requiredNextOffsets,
        Instant capturedAt,
        byte[] barrierChecksum) {
    public ProjectionBarrierView {
        if (barrierVersion <= 0 || queryDefinitionVersion <= 0 || topologyVersion <= 0 || routingVersion <= 0) {
            throw new IllegalArgumentException("barrier versions must be positive");
        }
        partitionSetChecksum = partitionSetChecksum.clone();
        barrierChecksum = barrierChecksum.clone();
        if (partitionSetChecksum.length != 32 || barrierChecksum.length != 32) {
            throw new IllegalArgumentException("barrier checksums must contain 32 bytes");
        }
        requiredNextOffsets = requiredNextOffsets.stream()
                .sorted(Comparator.comparing(PartitionRequirement::topic)
                        .thenComparingInt(PartitionRequirement::partition))
                .toList();
        if (requiredNextOffsets.isEmpty() || requiredNextOffsets.size() > 1_024) {
            throw new IllegalArgumentException("barrier partition vector must contain 1..1024 entries");
        }
        var keys = new java.util.HashSet<String>();
        if (requiredNextOffsets.stream().anyMatch(value -> !keys.add(value.topic() + '\0' + value.partition()))) {
            throw new IllegalArgumentException("barrier partition vector contains duplicates");
        }
    }
    @Override public byte[] partitionSetChecksum() { return partitionSetChecksum.clone(); }
    @Override public byte[] barrierChecksum() { return barrierChecksum.clone(); }

    public record PartitionRequirement(String topic, int partition, long requiredNextOffset) {
        public PartitionRequirement {
            if (topic == null || topic.isBlank() || partition < 0 || requiredNextOffset < 0) {
                throw new IllegalArgumentException("invalid exclusive next-offset requirement");
            }
        }
    }
}
