package io.craftrelay.client;

import java.util.HashMap;
import java.util.Map;

public final class BarrierVectorComparison {
    private BarrierVectorComparison() {
    }

    public static Result compare(ProjectionBarrier previous, ProjectionBarrier next) {
        if (previous.barrierVersion() != next.barrierVersion()
                || previous.queryDefinitionVersion() != next.queryDefinitionVersion()
                || previous.projectionTopologyVersion() != next.projectionTopologyVersion()
                || previous.routingVersion() != next.routingVersion()) {
            return Result.INCOMPARABLE;
        }
        Map<String, Long> oldOffsets = offsets(previous);
        Map<String, Long> newOffsets = offsets(next);
        if (!oldOffsets.keySet().equals(newOffsets.keySet())) {
            return Result.INCOMPARABLE;
        }
        boolean advanced = false;
        for (Map.Entry<String, Long> entry : oldOffsets.entrySet()) {
            long nextOffset = newOffsets.get(entry.getKey());
            if (nextOffset < entry.getValue()) return Result.CONFLICT;
            advanced |= nextOffset > entry.getValue();
        }
        return advanced ? Result.ADVANCED : Result.EQUAL;
    }

    private static Map<String, Long> offsets(ProjectionBarrier barrier) {
        Map<String, Long> values = new HashMap<>();
        for (ProjectionBarrier.PartitionBarrier partition : barrier.partitions()) {
            values.put(partition.topic() + '\0' + partition.partition(), partition.requiredNextOffset());
        }
        return values;
    }

    public enum Result { EQUAL, ADVANCED, INCOMPARABLE, CONFLICT }
}
