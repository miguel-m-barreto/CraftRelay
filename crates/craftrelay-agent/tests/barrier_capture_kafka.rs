use craftrelay_agent::barrier_capture::RdkafkaBarrierCaptureAdapter;
use craftrelay_protocol::BarrierCaptureAdapter;
use rdkafka::{
    ClientConfig,
    consumer::{BaseConsumer, Consumer},
};
use std::time::Duration;

fn setting(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Real-broker spike. It explicitly skips unless all documented fixture variables exist.
/// It never calls consumer poll and therefore never materializes Kafka payloads.
#[test]
#[ignore = "requires prepared real Kafka empty/open-transaction/gap fixture"]
fn captures_empty_open_transaction_and_gap_lsos_without_payloads() {
    let Some(bootstrap) = setting("CRAFTRELAY_KAFKA_BOOTSTRAP") else {
        eprintln!("SKIP: set Kafka spike fixture variables; see docs/testing.md");
        return;
    };
    let cases = [
        (
            "CRAFTRELAY_EMPTY_TOPIC",
            "CRAFTRELAY_EMPTY_PARTITION",
            "CRAFTRELAY_EMPTY_EXPECTED_LSO",
        ),
        (
            "CRAFTRELAY_OPEN_TX_TOPIC",
            "CRAFTRELAY_OPEN_TX_PARTITION",
            "CRAFTRELAY_OPEN_TX_EXPECTED_LSO",
        ),
        (
            "CRAFTRELAY_GAP_TOPIC",
            "CRAFTRELAY_GAP_PARTITION",
            "CRAFTRELAY_GAP_EXPECTED_LSO",
        ),
    ];
    let adapter = RdkafkaBarrierCaptureAdapter::new(&bootstrap, "craftrelay-sprint0-barrier-spike")
        .expect("consumer");
    for (topic_key, partition_key, expected_key) in cases {
        let topic = setting(topic_key).expect("complete fixture variables are required");
        let partition: i32 = setting(partition_key)
            .expect("partition")
            .parse()
            .expect("numeric partition");
        let expected: i64 = setting(expected_key)
            .expect("expected LSO")
            .parse()
            .expect("numeric LSO");
        let actual = adapter
            .capture_read_committed_next_offsets(&topic, &[partition], 5_000)
            .expect("capture");
        assert_eq!(actual[&partition], expected);
    }

    let topic = setting("CRAFTRELAY_OPEN_TX_TOPIC").expect("open topic");
    let partition: i32 = setting("CRAFTRELAY_OPEN_TX_PARTITION")
        .expect("open partition")
        .parse()
        .expect("numeric partition");
    let expected_lso: i64 = setting("CRAFTRELAY_OPEN_TX_EXPECTED_LSO")
        .expect("expected LSO")
        .parse()
        .expect("numeric LSO");
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "craftrelay-high-watermark-negative-proof")
        .set("isolation.level", "read_committed")
        .create()
        .expect("consumer");
    let (_, high_watermark) = consumer
        .fetch_watermarks(&topic, partition, Duration::from_secs(5))
        .expect("watermarks");
    assert!(
        high_watermark > expected_lso,
        "high watermark must differ from open-transaction LSO"
    );
}
