use craftrelay_protocol::BarrierCaptureAdapter;
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
};
use std::{collections::BTreeMap, time::Duration};

pub const MAX_PARTITIONS: usize = 1024;
pub const MAX_TIMEOUT_MILLIS: u64 = 30_000;

#[derive(Debug)]
pub enum CaptureError {
    InvalidBounds,
    Kafka(KafkaError),
    MissingPartition(i32),
}
impl From<KafkaError> for CaptureError {
    fn from(value: KafkaError) -> Self {
        Self::Kafka(value)
    }
}

pub struct RdkafkaBarrierCaptureAdapter {
    consumer: BaseConsumer,
}

impl RdkafkaBarrierCaptureAdapter {
    pub fn new(bootstrap_servers: &str, group_id: &str) -> Result<Self, KafkaError> {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("isolation.level", "read_committed")
            .set("enable.auto.commit", "false")
            .create()?;
        Ok(Self { consumer })
    }
}

impl BarrierCaptureAdapter for RdkafkaBarrierCaptureAdapter {
    type Error = CaptureError;
    fn capture_read_committed_next_offsets(
        &self,
        topic: &str,
        partitions: &[i32],
        timeout_millis: u64,
    ) -> Result<BTreeMap<i32, i64>, Self::Error> {
        if partitions.is_empty()
            || partitions.len() > MAX_PARTITIONS
            || timeout_millis == 0
            || timeout_millis > MAX_TIMEOUT_MILLIS
        {
            return Err(CaptureError::InvalidBounds);
        }
        let mut request = TopicPartitionList::new();
        for partition in partitions {
            request.add_partition_offset(topic, *partition, Offset::End)?;
        }
        // Sprint 0 compile-tested candidate: Offset::End is sent through librdkafka's
        // offsets-for-times API with read_committed configured. Whether this yields the
        // required LSO for empty partitions, open transactions, and offset gaps remains
        // pending real-broker fixture verification. No records are polled or payloads
        // materialized, and this adapter never substitutes fetch_watermarks output.
        let resolved = self
            .consumer
            .offsets_for_times(request, Duration::from_millis(timeout_millis))?;
        let mut result = BTreeMap::new();
        for partition in partitions {
            let element = resolved
                .find_partition(topic, *partition)
                .ok_or(CaptureError::MissingPartition(*partition))?;
            let offset = match element.offset() {
                Offset::Offset(value) => value,
                Offset::End => 0,
                _ => return Err(CaptureError::MissingPartition(*partition)),
            };
            result.insert(*partition, offset);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounds_are_explicit() {
        let config: Result<BaseConsumer, _> = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("group.id", "compile-only")
            .create();
        let adapter = RdkafkaBarrierCaptureAdapter {
            consumer: config.expect("configuration is valid"),
        };
        assert!(matches!(
            adapter.capture_read_committed_next_offsets("t", &[], 1),
            Err(CaptureError::InvalidBounds)
        ));
        let too_many = vec![0; MAX_PARTITIONS + 1];
        assert!(matches!(
            adapter.capture_read_committed_next_offsets("t", &too_many, 1),
            Err(CaptureError::InvalidBounds)
        ));
        assert!(matches!(
            adapter.capture_read_committed_next_offsets("t", &[0], MAX_TIMEOUT_MILLIS + 1),
            Err(CaptureError::InvalidBounds)
        ));
    }
}
