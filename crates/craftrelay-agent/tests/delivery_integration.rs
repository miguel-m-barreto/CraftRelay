use craftrelay_journal::{
    AcceptRequest, AcceptResult, AlwaysSafeDiskGuard, DeliveryStateResult, JournalConfig,
    LocalJournal, StatusResult,
};
use craftrelay_kafka_delivery::{
    producer::{FakeProducer, SendError, SendSuccess},
    routing::RoutingInput,
    service::{DeliveryOutcome, DeliveryService},
};

fn cfg() -> JournalConfig {
    JournalConfig::default()
}

fn test_req(event_id: &str, seq: i64) -> AcceptRequest {
    let payload = b"test-payload";
    AcceptRequest {
        installation_id: "inst-a".into(),
        event_id: event_id.into(),
        producer_id: "producer-a".into(),
        producer_instance_id: "instance-1".into(),
        producer_operation_sequence: seq,
        namespace: "economy".into(),
        logical_stream_type: "account".into(),
        stream_key: "player-1".into(),
        event_type: "transfer".into(),
        schema_version: 1,
        payload: payload.to_vec(),
        request_fingerprint: craftrelay_domain::sha256(payload),
        routing_version: 1,
    }
}

fn test_profile() -> craftrelay_domain::KafkaProductionProfile {
    craftrelay_domain::KafkaProductionProfile {
        profile_id: "p0-test".into(),
        profile_version: 1,
        replication_factor: 5,
        min_insync_replicas: 5,
        required_acks: "all".into(),
        enable_idempotence: true,
        delivery_timeout_ms: 120_000,
        request_timeout_ms: 30_000,
        linger_ms: 5,
        max_in_flight_requests_per_connection: 5,
        compression: "lz4".into(),
        retention_ms: 604_800_000,
        retention_bytes: 1_073_741_824,
        cleanup_policy: "delete".into(),
        unclean_leader_election: false,
        topic_deletion_policy: "PROTECTED".into(),
    }
}

fn test_routing() -> RoutingInput {
    RoutingInput {
        installation_id: "inst-a".into(),
        namespace: "economy".into(),
        logical_stream_type: "account".into(),
        stream_key: "player-1".into(),
        event_type: "transfer".into(),
        routing_version: 1,
    }
}

fn test_routing_fingerprint() -> craftrelay_domain::Checksum {
    craftrelay_kafka_delivery::routing::resolve_routing(&test_routing())
        .unwrap()
        .routing_checksum
}

fn test_topic() -> String {
    craftrelay_kafka_delivery::routing::resolve_routing(&test_routing())
        .unwrap()
        .topic
}

#[test]
fn accept_then_deliver_reaches_replicated() {
    let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();

    match j.accept(&test_req("e1", 1)) {
        AcceptResult::Accepted(a) => assert_eq!(a.event_id, "e1"),
        other => panic!("{other:?}"),
    }

    let fp = test_routing_fingerprint();
    j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

    let fake = FakeProducer::new();
    fake.enqueue_success(SendSuccess {
        topic: test_topic(),
        partition: 0,
        offset: 100,
    });

    let mut svc = DeliveryService::new(&j, &fake, test_profile());
    let outcome = svc.attempt_delivery("e1");

    match outcome {
        DeliveryOutcome::Replicated {
            kafka_partition,
            kafka_offset,
        } => {
            assert_eq!(kafka_partition, 0);
            assert_eq!(kafka_offset, 100);
        }
        other => panic!("expected Replicated, got {other:?}"),
    }

    match j.get_status("e1") {
        StatusResult::Found(s) => assert_eq!(s.delivery_status, "REPLICATED"),
        other => panic!("{other:?}"),
    }
    match j.get_delivery_state("e1") {
        DeliveryStateResult::Found(ds) => {
            assert_eq!(ds.delivery_status, "REPLICATED");
            assert_eq!(ds.kafka_offset, Some(100));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn accept_then_transient_failure_retries() {
    let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
    j.accept(&test_req("e1", 1));
    let fp = test_routing_fingerprint();
    j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

    let fake = FakeProducer::new();
    fake.enqueue_error(SendError::BrokerUnavailable);

    let mut svc = DeliveryService::new(&j, &fake, test_profile());
    let outcome = svc.attempt_delivery("e1");

    assert!(matches!(outcome, DeliveryOutcome::Retrying { .. }));

    match j.get_delivery_state("e1") {
        DeliveryStateResult::Found(ds) => {
            assert_eq!(ds.delivery_status, "DELIVERY_RETRYING");
            assert!(ds.next_retry_at_ms.is_some());
            assert_ne!(ds.delivery_status, "REPLICATED");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn ordering_gate_holds_seq2_until_seq1_delivered() {
    let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
    j.accept(&test_req("e1", 1));
    let mut r2 = test_req("e2", 2);
    r2.request_fingerprint = [0xBB; 32];
    j.accept(&r2);

    let fp = test_routing_fingerprint();
    j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
    j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());

    let fake = FakeProducer::new();
    fake.enqueue_success(SendSuccess {
        topic: test_topic(),
        partition: 0,
        offset: 1,
    });
    fake.enqueue_success(SendSuccess {
        topic: test_topic(),
        partition: 0,
        offset: 2,
    });

    let mut svc = DeliveryService::new(&j, &fake, test_profile());

    let out2 = svc.attempt_delivery("e2");
    assert!(
        matches!(
            out2,
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ),
        "e2 must wait for e1"
    );

    let out1 = svc.attempt_delivery("e1");
    assert!(matches!(out1, DeliveryOutcome::Replicated { .. }));

    let out2b = svc.attempt_delivery("e2");
    assert!(matches!(out2b, DeliveryOutcome::Replicated { .. }));
}
