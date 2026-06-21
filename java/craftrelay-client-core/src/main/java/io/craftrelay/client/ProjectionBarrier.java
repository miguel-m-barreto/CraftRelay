package io.craftrelay.client;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

public record ProjectionBarrier(
        int barrierVersion,
        String barrierId,
        String installationId,
        String queryId,
        int queryDefinitionVersion,
        long projectionTopologyVersion,
        long routingVersion,
        byte[] partitionSetChecksum,
        Instant capturedAt,
        List<PartitionBarrier> partitions,
        byte[] barrierChecksum) {

    public ProjectionBarrier {
        barrierVersion = ContractValidation.positiveInt32(barrierVersion, "barrierVersion");
        barrierId = ContractValidation.boundedText(barrierId, "barrierId", 128);
        installationId = ContractValidation.boundedText(installationId, "installationId", 128);
        queryId = ContractValidation.boundedText(queryId, "queryId", 128);
        queryDefinitionVersion = ContractValidation.positiveInt32(
                queryDefinitionVersion, "queryDefinitionVersion");
        projectionTopologyVersion = ContractValidation.positiveInt64(
                projectionTopologyVersion, "projectionTopologyVersion");
        routingVersion = ContractValidation.positiveInt64(routingVersion, "routingVersion");
        partitionSetChecksum = ContractValidation.fixedChecksum(
                partitionSetChecksum, "partitionSetChecksum");
        barrierChecksum = ContractValidation.fixedChecksum(barrierChecksum, "barrierChecksum");
        partitions = partitions.stream()
                .sorted(Comparator.comparing(PartitionBarrier::topic)
                        .thenComparingInt(PartitionBarrier::partition))
                .toList();
        if (partitions.isEmpty() || partitions.size() > ContractLimits.MAX_BARRIER_PARTITIONS) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.BOUNDS_EXCEEDED,
                    "barrier partition count must be 1.." + ContractLimits.MAX_BARRIER_PARTITIONS);
        }
        for (int index = 1; index < partitions.size(); index++) {
            PartitionBarrier previous = partitions.get(index - 1);
            PartitionBarrier current = partitions.get(index);
            if (previous.topic().equals(current.topic()) && previous.partition() == current.partition()) {
                throw ContractValidation.violation(
                        ContractViolationException.Code.INVALID_ARGUMENT,
                        "duplicate barrier partition");
            }
        }
    }

    @Override
    public byte[] partitionSetChecksum() { return partitionSetChecksum.clone(); }

    @Override
    public byte[] barrierChecksum() { return barrierChecksum.clone(); }

    public byte[] canonicalBytes() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.writeInt(barrierVersion);
            write(output, barrierId);
            write(output, installationId);
            write(output, queryId);
            output.writeInt(queryDefinitionVersion);
            output.writeLong(projectionTopologyVersion);
            output.writeLong(routingVersion);
            write(output, partitionSetChecksum);
            output.writeInt(partitions.size());
            for (PartitionBarrier partition : partitions) {
                write(output, partition.topic());
                output.writeInt(partition.partition());
                output.writeLong(partition.requiredNextOffset());
            }
            output.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void write(DataOutputStream output, String value) throws IOException {
        write(output, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void write(DataOutputStream output, byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    public record PartitionBarrier(String topic, int partition, long requiredNextOffset) {
        public PartitionBarrier {
            topic = ContractValidation.boundedText(topic, "topic", 249);
            if (partition < 0 || requiredNextOffset < 0) {
                throw ContractValidation.violation(
                        ContractViolationException.Code.INVALID_ARGUMENT,
                        "partition and exclusive requiredNextOffset must be non-negative");
            }
        }
    }
}
