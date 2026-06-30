use crate::gate::{GateDecision, OrderingGate, RecoveredStreamState};
use crate::producer::{KafkaProducer, SendRequest};
use crate::profile::is_profile_safe_for_delivery;
use crate::retry::{RetryDecision, compute_next_retry, should_retry};
use crate::routing::{RoutingInput, resolve_routing};
use craftrelay_domain::KafkaProductionProfile;
use craftrelay_journal::{
    CasResult, DeliveryAttemptRecord, DeliveryCandidateResult, DeliveryScanResult, LocalJournal,
    PayloadReadResult, ReplicatedDeliveryConfirmation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Replicated {
        kafka_partition: i32,
        kafka_offset: i64,
    },
    Retrying {
        next_retry_at_ms: i64,
    },
    Blocked {
        reason: String,
    },
    GateHeld {
        waiting_for_sequence: i64,
    },
    Error(String),
}

pub struct DeliveryService<'a> {
    journal: &'a LocalJournal,
    producer: &'a dyn KafkaProducer,
    gate: OrderingGate,
    profile: KafkaProductionProfile,
    shutdown: bool,
}

impl<'a> DeliveryService<'a> {
    pub fn new(
        journal: &'a LocalJournal,
        producer: &'a dyn KafkaProducer,
        profile: KafkaProductionProfile,
    ) -> Self {
        Self {
            journal,
            producer,
            gate: OrderingGate::new(),
            profile,
            shutdown: false,
        }
    }

    pub fn recover(&mut self) -> Result<(), String> {
        let persisted = match self.journal.scan_delivery_gate_states(10_000) {
            DeliveryScanResult::Ok(states) => states,
            DeliveryScanResult::Corrupted { event_id, detail } => {
                return Err(format!(
                    "delivery gate recovery scan corrupted event_id={event_id:?}: {detail}"
                ));
            }
            DeliveryScanResult::Error(e) => {
                return Err(format!("delivery gate recovery scan failed: {e}"));
            }
        };
        let mut states = Vec::with_capacity(persisted.len());
        for p in persisted {
            states.push(RecoveredStreamState {
                stream_key: p.stream_key.clone(),
                stream_sequence: p.stream_sequence,
                replicated: p.delivery_status == "REPLICATED",
                blocking: p.delivery_status != "REPLICATED",
                in_flight: false,
            });
        }
        self.gate.recover_from_scan(&states);
        Ok(())
    }

    pub fn shutdown(&mut self) {
        self.shutdown = true;
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    pub fn attempt_delivery(&mut self, event_id: &str) -> DeliveryOutcome {
        if self.shutdown {
            return DeliveryOutcome::Error("delivery service is shut down".into());
        }

        let candidate = match self.journal.get_delivery_candidate(event_id) {
            DeliveryCandidateResult::Found(candidate) => candidate,
            DeliveryCandidateResult::NotFound => {
                return DeliveryOutcome::Error("delivery candidate not found".into());
            }
            DeliveryCandidateResult::Corrupted { detail, .. } => {
                return DeliveryOutcome::Error(format!("delivery candidate corrupted: {detail}"));
            }
            DeliveryCandidateResult::Error(e) => return DeliveryOutcome::Error(e),
        };
        let state = &candidate.delivery_state;
        let stream_key = candidate.stream_key.as_str();
        let stream_sequence = candidate.stream_sequence;
        let routing_input = RoutingInput {
            installation_id: candidate.installation_id.clone(),
            namespace: candidate.namespace.clone(),
            logical_stream_type: candidate.logical_stream_type.clone(),
            stream_key: candidate.stream_key.clone(),
            event_type: candidate.event_type.clone(),
            routing_version: candidate.routing_version,
        };

        match self.gate.check(stream_key, stream_sequence) {
            GateDecision::Blocked {
                waiting_for_sequence,
            } => {
                return DeliveryOutcome::GateHeld {
                    waiting_for_sequence,
                };
            }
            GateDecision::Allowed => {}
        }

        if state.delivery_status == "REPLICATED" {
            let (Some(kafka_partition), Some(kafka_offset)) =
                (state.kafka_partition, state.kafka_offset)
            else {
                return DeliveryOutcome::Error(
                    "replicated delivery missing Kafka confirmation metadata".into(),
                );
            };
            self.gate.mark_completed(stream_key, stream_sequence);
            return DeliveryOutcome::Replicated {
                kafka_partition,
                kafka_offset,
            };
        }
        if state.delivery_status == "DELIVERY_BLOCKED" {
            self.gate.mark_blocking(stream_key, stream_sequence);
            return DeliveryOutcome::Blocked {
                reason: state
                    .blocked_reason
                    .clone()
                    .unwrap_or_else(|| "delivery blocked".into()),
            };
        }
        let routing = match resolve_routing(&routing_input) {
            Ok(r) => r,
            Err(e) => {
                return self.persist_block_before_send(
                    event_id,
                    stream_key,
                    stream_sequence,
                    state.delivery_revision,
                    &format!("routing error: {e:?}"),
                );
            }
        };
        if state.routing_fingerprint != Some(routing.routing_checksum) {
            return self.persist_block_before_send(
                event_id,
                stream_key,
                stream_sequence,
                state.delivery_revision,
                "routing fingerprint mismatch",
            );
        }
        if state.profile_id.as_deref() != Some(self.profile.profile_id.as_str())
            || state.profile_version != Some(self.profile.profile_version)
        {
            let reason = if state.profile_id.as_deref() != Some(self.profile.profile_id.as_str()) {
                "profile ID mismatch"
            } else {
                "profile version mismatch"
            };
            return self.persist_block_before_send(
                event_id,
                stream_key,
                stream_sequence,
                state.delivery_revision,
                reason,
            );
        }
        if state.kafka_topic.as_deref() != Some(routing.topic.as_str()) {
            return self.persist_block_before_send(
                event_id,
                stream_key,
                stream_sequence,
                state.delivery_revision,
                "routing topic mismatch",
            );
        }
        if let Err(e) = is_profile_safe_for_delivery(&self.profile) {
            return self.persist_block_before_send(
                event_id,
                stream_key,
                stream_sequence,
                state.delivery_revision,
                &format!("unsafe Kafka production profile: {e:?}"),
            );
        }

        let payload = match self.journal.read_verified_payload(event_id) {
            PayloadReadResult::Found(payload) => payload,
            PayloadReadResult::NotFound => {
                self.gate.mark_blocking(stream_key, stream_sequence);
                return DeliveryOutcome::Error("stored payload not found".into());
            }
            PayloadReadResult::Corrupted { detail, .. } => {
                self.gate.mark_blocking(stream_key, stream_sequence);
                return DeliveryOutcome::Error(format!("stored payload corrupted: {detail}"));
            }
            PayloadReadResult::Error(e) => {
                self.gate.mark_blocking(stream_key, stream_sequence);
                return DeliveryOutcome::Error(format!("stored payload read failed: {e}"));
            }
        };

        self.gate.mark_in_flight(stream_key, stream_sequence);

        let request = SendRequest {
            event_id: event_id.to_string(),
            topic: routing.topic.clone(),
            partition_key: routing.partition_key.clone(),
            payload,
            headers: vec![
                (
                    "routing_version".into(),
                    routing.routing_version.to_string(),
                ),
                ("profile_id".into(), self.profile.profile_id.clone()),
            ],
        };

        match self.producer.send(&request) {
            Ok(success) => {
                let result = self.journal.confirm_replicated_delivery(
                    event_id,
                    state.delivery_revision,
                    ReplicatedDeliveryConfirmation {
                        expected_routing_fingerprint: &routing.routing_checksum,
                        expected_profile_id: &self.profile.profile_id,
                        expected_profile_version: self.profile.profile_version,
                        kafka_topic: &success.topic,
                        kafka_partition: success.partition,
                        kafka_offset: success.offset,
                    },
                );
                match result {
                    craftrelay_journal::CasResult::Updated
                    | craftrelay_journal::CasResult::AlreadyConfirmed => {
                        self.gate.mark_completed(stream_key, stream_sequence);
                        DeliveryOutcome::Replicated {
                            kafka_partition: success.partition,
                            kafka_offset: success.offset,
                        }
                    }
                    other => {
                        self.gate.mark_blocking(stream_key, stream_sequence);
                        DeliveryOutcome::Error(format!("journal confirm failed: {other:?}"))
                    }
                }
            }
            Err(send_err) => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                let attempt = DeliveryAttemptRecord {
                    event_id: event_id.to_string(),
                    attempt_number: state.attempt_count + 1,
                    outcome: if send_err.is_permanent() {
                        "PERMANENT_FAILURE".into()
                    } else {
                        "TRANSIENT_FAILURE".into()
                    },
                    error_code: Some(format!("{send_err:?}")),
                    kafka_topic: Some(routing.topic.clone()),
                    kafka_partition: None,
                    kafka_offset: None,
                    profile_id: Some(self.profile.profile_id.clone()),
                    profile_version: Some(self.profile.profile_version),
                    attempted_at_ms: now_ms,
                };
                match self.journal.record_delivery_attempt(&attempt) {
                    CasResult::Updated => {}
                    other => {
                        return DeliveryOutcome::Error(format!(
                            "delivery attempt persistence failed: {other:?}"
                        ));
                    }
                }

                if should_retry(&send_err, state.attempt_count + 1) {
                    match compute_next_retry(state.attempt_count + 1, now_ms) {
                        RetryDecision::Retry { next_retry_at_ms } => {
                            match self.journal.update_delivery_retry(
                                event_id,
                                state.delivery_revision,
                                next_retry_at_ms,
                            ) {
                                CasResult::Updated => {
                                    self.gate.mark_blocking(stream_key, stream_sequence);
                                    DeliveryOutcome::Retrying { next_retry_at_ms }
                                }
                                other => DeliveryOutcome::Error(format!(
                                    "delivery retry persistence failed: {other:?}"
                                )),
                            }
                        }
                        RetryDecision::Exhausted => {
                            match self.journal.block_delivery(
                                event_id,
                                state.delivery_revision,
                                "retry exhausted",
                            ) {
                                CasResult::Updated => {
                                    self.gate.mark_blocking(stream_key, stream_sequence);
                                    DeliveryOutcome::Blocked {
                                        reason: "retry exhausted".into(),
                                    }
                                }
                                other => DeliveryOutcome::Error(format!(
                                    "delivery block persistence failed: {other:?}"
                                )),
                            }
                        }
                    }
                } else {
                    let reason = format!("permanent failure: {send_err:?}");
                    match self
                        .journal
                        .block_delivery(event_id, state.delivery_revision, &reason)
                    {
                        CasResult::Updated => {
                            self.gate.mark_blocking(stream_key, stream_sequence);
                            DeliveryOutcome::Blocked { reason }
                        }
                        other => DeliveryOutcome::Error(format!(
                            "delivery block persistence failed: {other:?}"
                        )),
                    }
                }
            }
        }
    }

    fn persist_block_before_send(
        &mut self,
        event_id: &str,
        stream_key: &str,
        stream_sequence: i64,
        delivery_revision: i64,
        reason: &str,
    ) -> DeliveryOutcome {
        match self
            .journal
            .block_delivery(event_id, delivery_revision, reason)
        {
            CasResult::Updated => {
                self.gate.mark_blocking(stream_key, stream_sequence);
                DeliveryOutcome::Blocked {
                    reason: reason.to_string(),
                }
            }
            other => {
                DeliveryOutcome::Error(format!("delivery block persistence failed: {other:?}"))
            }
        }
    }

    pub fn gate(&self) -> &OrderingGate {
        &self.gate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer::{FakeProducer, SendError, SendSuccess};
    use craftrelay_journal::{
        AcceptRequest, AlwaysSafeDiskGuard, DeliveryStateRecord, JournalConfig,
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

    fn test_profile() -> KafkaProductionProfile {
        KafkaProductionProfile {
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

    fn routing_from_req(req: &AcceptRequest) -> RoutingInput {
        RoutingInput {
            installation_id: req.installation_id.clone(),
            namespace: req.namespace.clone(),
            logical_stream_type: req.logical_stream_type.clone(),
            stream_key: req.stream_key.clone(),
            event_type: req.event_type.clone(),
            routing_version: req.routing_version,
        }
    }

    fn test_routing_fingerprint() -> craftrelay_domain::Checksum {
        resolve_routing(&test_routing()).unwrap().routing_checksum
    }

    fn routing_for_stream(stream_key: &str) -> RoutingInput {
        let mut routing = test_routing();
        routing.stream_key = stream_key.to_string();
        routing
    }

    fn test_topic() -> String {
        resolve_routing(&test_routing()).unwrap().topic
    }

    fn file_journal(name: &str) -> (std::path::PathBuf, std::path::PathBuf, LocalJournal) {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("{name}-{unique}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.db");
        let journal = LocalJournal::open(&path, cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        (dir, path, journal)
    }

    fn exec_sql(path: &std::path::Path, sql: &str) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch(sql).unwrap();
    }

    fn force_delivery_checksum_to_match(path: &std::path::Path, event_id: &str) {
        let conn = rusqlite::Connection::open(path).unwrap();
        let row = conn
            .query_row(
                "SELECT delivery_status, attempt_count, next_retry_at_ms, \
                        last_error, blocked_reason, kafka_topic, \
                        kafka_partition, kafka_offset, routing_fingerprint, \
                        profile_id, profile_version, delivery_revision, \
                        confirmed_at_ms, created_at_ms, updated_at_ms \
                 FROM delivery_state WHERE event_id = ?1",
                [event_id],
                |row| {
                    let rf: Option<Vec<u8>> = row.get(8)?;
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i32>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<i32>>(6)?,
                        row.get::<_, Option<i64>>(7)?,
                        rf,
                        row.get::<_, Option<String>>(9)?,
                        row.get::<_, Option<i32>>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, Option<i64>>(12)?,
                        row.get::<_, i64>(13)?,
                        row.get::<_, i64>(14)?,
                    ))
                },
            )
            .unwrap();
        let (
            delivery_status,
            attempt_count,
            next_retry_at_ms,
            last_error,
            blocked_reason,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            routing_fingerprint,
            profile_id,
            profile_version,
            delivery_revision,
            confirmed_at_ms,
            created_at_ms,
            updated_at_ms,
        ) = row;
        let routing_fingerprint = routing_fingerprint.map(|bytes| {
            let mut checksum = [0u8; 32];
            checksum.copy_from_slice(&bytes);
            checksum
        });
        let state = DeliveryStateRecord {
            event_id: event_id.to_string(),
            delivery_status,
            attempt_count,
            next_retry_at_ms,
            last_error,
            blocked_reason,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            routing_fingerprint,
            profile_id,
            profile_version,
            delivery_revision,
            delivery_checksum: [0u8; 32],
            confirmed_at_ms,
            created_at_ms,
            updated_at_ms,
        };
        let checksum = test_delivery_checksum(&state);
        conn.execute(
            "UPDATE delivery_state SET delivery_checksum = ?1 WHERE event_id = ?2",
            rusqlite::params![checksum.as_slice(), event_id],
        )
        .unwrap();
    }

    fn test_delivery_checksum(state: &DeliveryStateRecord) -> craftrelay_domain::Checksum {
        craftrelay_domain::sha256(
            format!(
                "{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
                state.event_id,
                state.delivery_status,
                state.attempt_count,
                state
                    .next_retry_at_ms
                    .map_or(String::new(), |v| v.to_string()),
                state.last_error.as_deref().unwrap_or(""),
                state.blocked_reason.as_deref().unwrap_or(""),
                state.kafka_topic.as_deref().unwrap_or(""),
                state
                    .kafka_partition
                    .map_or(String::new(), |v| v.to_string()),
                state.kafka_offset.map_or(String::new(), |v| v.to_string()),
                state
                    .routing_fingerprint
                    .as_ref()
                    .map_or(String::new(), checksum_hex),
                state.profile_id.as_deref().unwrap_or(""),
                state
                    .profile_version
                    .map_or(String::new(), |v| v.to_string()),
                state.delivery_revision,
                state
                    .confirmed_at_ms
                    .map_or(String::new(), |v| v.to_string()),
                state.created_at_ms,
            )
            .as_bytes(),
        )
    }

    fn checksum_hex(checksum: &craftrelay_domain::Checksum) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(64);
        for byte in checksum {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0F) as usize] as char);
        }
        out
    }

    fn assert_delivery_blocked(journal: &LocalJournal, event_id: &str) {
        match journal.get_delivery_state(event_id) {
            craftrelay_journal::DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "DELIVERY_BLOCKED");
                assert!(ds.blocked_reason.is_some());
            }
            other => panic!("{other:?}"),
        }
        match journal.get_status(event_id) {
            craftrelay_journal::StatusResult::Found(status) => {
                assert_eq!(status.delivery_status, "DELIVERY_BLOCKED");
            }
            other => panic!("{other:?}"),
        }
    }

    fn assert_delivery_pending(journal: &LocalJournal, event_id: &str) {
        match journal.get_delivery_state(event_id) {
            craftrelay_journal::DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "DELIVERY_PENDING");
                assert_eq!(ds.kafka_offset, None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn successful_delivery_returns_replicated() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 42,
        });

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        let outcome = svc.attempt_delivery("e1");

        match outcome {
            DeliveryOutcome::Replicated {
                kafka_partition,
                kafka_offset,
            } => {
                assert_eq!(kafka_partition, 0);
                assert_eq!(kafka_offset, 42);
            }
            other => panic!("expected Replicated, got {other:?}"),
        }
    }

    #[test]
    fn already_replicated_delivery_returns_persisted_metadata() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        let topic = test_topic();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &topic);
        assert_eq!(
            j.confirm_replicated_delivery(
                "e1",
                1,
                ReplicatedDeliveryConfirmation {
                    expected_routing_fingerprint: &fp,
                    expected_profile_id: "p0-test",
                    expected_profile_version: 1,
                    kafka_topic: &topic,
                    kafka_partition: 3,
                    kafka_offset: 99,
                },
            ),
            CasResult::Updated
        );

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        match svc.attempt_delivery("e1") {
            DeliveryOutcome::Replicated {
                kafka_partition,
                kafka_offset,
            } => {
                assert_eq!(kafka_partition, 3);
                assert_eq!(kafka_offset, 99);
            }
            other => panic!("expected persisted replicated metadata, got {other:?}"),
        }
        assert!(fake.sent_requests().is_empty());
    }

    #[test]
    fn already_replicated_delivery_missing_metadata_fails_closed() {
        let (dir, path, j) = file_journal("cr-delivery-replicated-missing-metadata");
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        let topic = test_topic();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &topic);
        assert_eq!(
            j.confirm_replicated_delivery(
                "e1",
                1,
                ReplicatedDeliveryConfirmation {
                    expected_routing_fingerprint: &fp,
                    expected_profile_id: "p0-test",
                    expected_profile_version: 1,
                    kafka_topic: &topic,
                    kafka_partition: 3,
                    kafka_offset: 99,
                },
            ),
            CasResult::Updated
        );
        exec_sql(
            &path,
            "UPDATE delivery_state SET kafka_partition = NULL WHERE event_id = 'e1'",
        );
        force_delivery_checksum_to_match(&path, "e1");

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Error(_)
        ));
        assert!(fake.sent_requests().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn delivery_sends_exactly_journal_stored_payload() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        let mut req = test_req("e1", 1);
        req.payload = b"journal-stored-payload".to_vec();
        req.request_fingerprint = craftrelay_domain::sha256(&req.payload);
        j.accept(&req);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 42,
        });

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Replicated { .. }
        ));

        let sent = fake.sent_requests();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload, b"journal-stored-payload".to_vec());
    }

    #[test]
    fn attempt_delivery_uses_stored_stream_gate_for_sequence_two() {
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
            offset: 2,
        });
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e2"),
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ));
        assert!(fake.sent_requests().is_empty());
    }

    #[test]
    fn caller_cannot_override_journal_payload() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        let mut req = test_req("e1", 1);
        req.payload = b"immutable-journal-payload".to_vec();
        req.request_fingerprint = craftrelay_domain::sha256(&req.payload);
        j.accept(&req);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 43,
        });

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Replicated { .. }
        ));

        let sent = fake.sent_requests();
        assert_eq!(sent.len(), 1);
        assert_ne!(sent[0].payload, b"caller-supplied-payload".to_vec());
        assert_eq!(sent[0].payload, b"immutable-journal-payload".to_vec());
    }

    #[test]
    fn routing_is_derived_from_stored_envelope() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        let mut req = test_req("e1", 1);
        req.stream_key = "player-2".into();
        req.request_fingerprint = [0xDD; 32];
        let routing = routing_from_req(&req);
        let resolved = resolve_routing(&routing).unwrap();
        j.accept(&req);
        j.create_delivery_pending(
            "e1",
            &resolved.routing_checksum,
            "p0-test",
            1,
            &resolved.topic,
        );

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: resolved.topic.clone(),
            partition: 0,
            offset: 47,
        });

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Replicated { .. }
        ));
        let sent = fake.sent_requests();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].topic, resolved.topic);
        assert_eq!(sent[0].partition_key, resolved.partition_key);
    }

    #[test]
    fn corrupted_stored_envelope_fails_closed_before_send() {
        let (dir, path, j) = file_journal("cr-delivery-corrupt-envelope-candidate");
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "UPDATE stored_envelope SET namespace='HACKED' WHERE event_id='e1'",
        );

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 48,
        });

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Error(_)
        ));
        assert!(fake.sent_requests().is_empty());
        assert_delivery_pending(&j, "e1");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn delivery_state_without_stored_envelope_fails_closed_before_send() {
        let (dir, path, j) = file_journal("cr-delivery-missing-envelope-candidate");
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "PRAGMA foreign_keys = OFF; \
             DELETE FROM stored_envelope WHERE event_id='e1'; \
             PRAGMA foreign_keys = ON;",
        );

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 49,
        });

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Error(_)
        ));
        assert!(fake.sent_requests().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_payload_blob_prevents_producer_send() {
        let (dir, path, j) = file_journal("cr-delivery-missing-payload");
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "PRAGMA foreign_keys = OFF; \
             DELETE FROM payload_blob WHERE event_id='e1'; \
             PRAGMA foreign_keys = ON;",
        );

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 44,
        });

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Error(_)
        ));
        assert!(fake.sent_requests().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupted_payload_blob_prevents_producer_send() {
        let (dir, path, j) = file_journal("cr-delivery-corrupt-payload");
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "UPDATE payload_blob SET payload=X'DEAD' WHERE event_id='e1'",
        );

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 45,
        });

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Error(_)
        ));
        assert!(fake.sent_requests().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn persisted_delivery_revision_is_used_for_confirmation() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        assert_eq!(j.update_delivery_retry("e1", 1, 5000), CasResult::Updated);

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 46,
        });

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Replicated { .. }
        ));
        match j.get_delivery_state("e1") {
            craftrelay_journal::DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "REPLICATED");
                assert_eq!(ds.delivery_revision, 3);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn retry_backoff_uses_persisted_attempt_count() {
        let (dir, path, j) = file_journal("cr-delivery-persisted-attempt-count");
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        assert_eq!(
            j.record_delivery_attempt(&DeliveryAttemptRecord {
                event_id: "e1".into(),
                attempt_number: 1,
                outcome: "TRANSIENT_FAILURE".into(),
                error_code: Some("BrokerUnavailable".into()),
                kafka_topic: Some(test_topic()),
                kafka_partition: None,
                kafka_offset: None,
                profile_id: Some("p0-test".into()),
                profile_version: Some(1),
                attempted_at_ms: 1000,
            }),
            CasResult::Updated
        );

        let fake = FakeProducer::new();
        fake.enqueue_error(SendError::BrokerUnavailable);

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Retrying { .. }
        ));

        let conn = rusqlite::Connection::open(&path).unwrap();
        let (attempt_number, attempted_at_ms): (i32, i64) = conn
            .query_row(
                "SELECT attempt_number, attempted_at_ms \
                 FROM delivery_attempt \
                 WHERE event_id='e1' \
                 ORDER BY id DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let next_retry_at_ms: i64 = conn
            .query_row(
                "SELECT next_retry_at_ms FROM delivery_state WHERE event_id='e1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attempt_number, 2);
        assert_eq!(next_retry_at_ms - attempted_at_ms, 2_000);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn transient_failure_returns_retrying() {
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
            craftrelay_journal::DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "DELIVERY_RETRYING");
                assert_eq!(ds.attempt_count, 1);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn permanent_failure_returns_blocked() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_error(SendError::TopicNotFound);

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        let outcome = svc.attempt_delivery("e1");

        assert!(matches!(outcome, DeliveryOutcome::Blocked { .. }));
        assert_delivery_blocked(&j, "e1");
    }

    #[test]
    fn transient_failure_with_attempt_persistence_failure_returns_error() {
        let (dir, path, j) = file_journal("cr-delivery-attempt-transient");
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "CREATE TRIGGER fail_delivery_attempt_insert \
             BEFORE INSERT ON delivery_attempt \
             BEGIN \
                SELECT RAISE(ABORT, 'attempt persistence blocked'); \
             END",
        );

        let fake = FakeProducer::new();
        fake.enqueue_error(SendError::BrokerUnavailable);

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        let outcome = svc.attempt_delivery("e1");

        assert!(matches!(outcome, DeliveryOutcome::Error(_)));
        match j.get_delivery_state("e1") {
            craftrelay_journal::DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "DELIVERY_PENDING");
                assert_eq!(ds.attempt_count, 0);
            }
            other => panic!("{other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn permanent_failure_with_attempt_persistence_failure_returns_error() {
        let (dir, path, j) = file_journal("cr-delivery-attempt-permanent");
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "CREATE TRIGGER fail_delivery_attempt_insert \
             BEFORE INSERT ON delivery_attempt \
             BEGIN \
                SELECT RAISE(ABORT, 'attempt persistence blocked'); \
             END",
        );

        let fake = FakeProducer::new();
        fake.enqueue_error(SendError::TopicNotFound);

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        let outcome = svc.attempt_delivery("e1");

        assert!(matches!(outcome, DeliveryOutcome::Error(_)));
        match j.get_delivery_state("e1") {
            craftrelay_journal::DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "DELIVERY_PENDING");
                assert_eq!(ds.attempt_count, 0);
                assert!(ds.blocked_reason.is_none());
            }
            other => panic!("{other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn seq_2_held_while_seq_1_is_retrying() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let mut r2 = test_req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        j.accept(&r2);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_error(SendError::BrokerUnavailable);
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Retrying { .. }
        ));
        assert!(matches!(
            svc.attempt_delivery("e2"),
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ));
    }

    #[test]
    fn seq_2_held_while_seq_1_is_blocked() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let mut r2 = test_req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        j.accept(&r2);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_error(SendError::TopicNotFound);
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Blocked { .. }
        ));
        assert!(matches!(
            svc.attempt_delivery("e2"),
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ));
    }

    #[test]
    fn seq_2_held_when_broker_ack_cannot_be_persisted() {
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
            topic: "wrong-topic".into(),
            partition: 0,
            offset: 1,
        });
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Error(_)
        ));
        assert!(matches!(
            svc.attempt_delivery("e2"),
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ));
    }

    #[test]
    fn seq_2_advances_after_seq_1_is_persisted_replicated() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let mut r2 = test_req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        j.accept(&r2);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_error(SendError::BrokerUnavailable);
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

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Retrying { .. }
        ));
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Replicated { .. }
        ));
        assert!(matches!(
            svc.attempt_delivery("e2"),
            DeliveryOutcome::Replicated { .. }
        ));
    }

    #[test]
    fn independent_stream_delivers_while_other_stream_is_retrying() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let mut r2 = test_req("e2", 2);
        r2.stream_key = "player-2".into();
        r2.request_fingerprint = [0xCC; 32];
        j.accept(&r2);
        let fp1 = test_routing_fingerprint();
        let mut routing2 = test_routing();
        routing2.stream_key = "player-2".into();
        let resolved2 = resolve_routing(&routing2).unwrap();
        j.create_delivery_pending("e1", &fp1, "p0-test", 1, &test_topic());
        j.create_delivery_pending(
            "e2",
            &resolved2.routing_checksum,
            "p0-test",
            1,
            &resolved2.topic,
        );

        let fake = FakeProducer::new();
        fake.enqueue_error(SendError::BrokerUnavailable);
        fake.enqueue_success(SendSuccess {
            topic: resolved2.topic.clone(),
            partition: 0,
            offset: 2,
        });
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Retrying { .. }
        ));
        assert!(matches!(
            svc.attempt_delivery("e2"),
            DeliveryOutcome::Replicated { .. }
        ));
    }

    #[test]
    fn ordering_gate_blocks_out_of_order() {
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

        let outcome2 = svc.attempt_delivery("e2");
        assert!(matches!(
            outcome2,
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ));

        let outcome1 = svc.attempt_delivery("e1");
        assert!(matches!(outcome1, DeliveryOutcome::Replicated { .. }));

        let outcome2b = svc.attempt_delivery("e2");
        assert!(matches!(outcome2b, DeliveryOutcome::Replicated { .. }));
    }

    #[test]
    fn unsafe_profile_blocks_before_producer_send() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 1,
        });
        let mut unsafe_profile = test_profile();
        unsafe_profile.min_insync_replicas = 3;
        let mut svc = DeliveryService::new(&j, &fake, unsafe_profile);

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Blocked { .. }
        ));
        assert!(fake.sent_requests().is_empty());
        assert_delivery_blocked(&j, "e1");
    }

    #[test]
    fn profile_id_mismatch_blocks_before_producer_send() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "old-profile", 1, &test_topic());

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Blocked { .. }
        ));
        assert!(fake.sent_requests().is_empty());
        assert_delivery_blocked(&j, "e1");
    }

    #[test]
    fn profile_version_mismatch_blocks_before_producer_send() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 2, &test_topic());

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Blocked { .. }
        ));
        assert!(fake.sent_requests().is_empty());
        assert_delivery_blocked(&j, "e1");
    }

    #[test]
    fn routing_fingerprint_mismatch_persists_block_before_producer_send() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let wrong_fp = craftrelay_domain::sha256(b"wrong-routing");
        j.create_delivery_pending("e1", &wrong_fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Blocked { .. }
        ));
        assert!(fake.sent_requests().is_empty());
        assert_delivery_blocked(&j, "e1");
    }

    #[test]
    fn routing_topic_mismatch_persists_block_before_producer_send() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, "wrong-topic");

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Blocked { .. }
        ));
        assert!(fake.sent_requests().is_empty());
        assert_delivery_blocked(&j, "e1");
    }

    #[test]
    fn unsupported_routing_version_persists_block_before_producer_send() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        let mut req = test_req("e1", 1);
        req.routing_version = 2;
        j.accept(&req);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Blocked { .. }
        ));
        assert!(fake.sent_requests().is_empty());
        assert_delivery_blocked(&j, "e1");
    }

    #[test]
    fn empty_routing_field_persists_block_before_producer_send() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        let mut req = test_req("e1", 1);
        req.namespace = String::new();
        j.accept(&req);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Blocked { .. }
        ));
        assert!(fake.sent_requests().is_empty());
        assert_delivery_blocked(&j, "e1");
    }

    #[test]
    fn recover_keeps_blocked_earlier_sequence_blocking_later_sequence() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let mut r2 = test_req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        j.accept(&r2);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());
        assert_eq!(
            j.block_delivery("e1", 1, "operator repair required"),
            craftrelay_journal::CasResult::Updated
        );

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 2,
        });
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        svc.recover().unwrap();

        assert!(matches!(
            svc.attempt_delivery("e2"),
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ));
        assert!(fake.sent_requests().is_empty());
    }

    #[test]
    fn recover_returns_error_on_corrupt_gate_scan() {
        let (dir, path, j) = file_journal("cr-delivery-recover-corrupt");
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "UPDATE delivery_state SET delivery_checksum=X'00' WHERE event_id='e1'",
        );

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(svc.recover().is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_earlier_stream_event_is_not_dropped_during_recover() {
        let (dir, path, j) = file_journal("cr-delivery-recover-corrupt-earlier");
        j.accept(&test_req("e1", 1));
        let mut r2 = test_req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        j.accept(&r2);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "UPDATE delivery_state SET delivery_checksum=X'00' WHERE event_id='e1'",
        );

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 2,
        });
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(svc.recover().is_err());
        assert!(fake.sent_requests().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_envelope_earlier_stream_event_fails_recover_closed() {
        let (dir, path, j) = file_journal("cr-delivery-recover-corrupt-envelope-earlier");
        j.accept(&test_req("e1", 1));
        let mut r2 = test_req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        j.accept(&r2);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "UPDATE stored_envelope SET stream_key='fake-stream' WHERE event_id='e1'",
        );

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 2,
        });
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(svc.recover().is_err());
        assert!(fake.sent_requests().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_predecessor_envelope_cannot_release_later_sequence() {
        let (dir, path, j) = file_journal("cr-delivery-recover-corrupt-envelope-predecessor");
        j.accept(&test_req("e1", 1));
        let mut r2 = test_req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        j.accept(&r2);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "UPDATE stored_envelope SET stream_key='fake-stream' WHERE event_id='e1'",
        );

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 2,
        });
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(svc.recover().is_err());
        assert!(matches!(
            svc.attempt_delivery("e2"),
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ));
        assert!(fake.sent_requests().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unsupported_status_earlier_stream_event_fails_recover_closed() {
        let (dir, path, j) = file_journal("cr-delivery-recover-broken-status");
        j.accept(&test_req("e1", 1));
        let mut r2 = test_req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        j.accept(&r2);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());
        exec_sql(
            &path,
            "UPDATE delivery_state SET delivery_status='BROKEN' WHERE event_id='e1'",
        );

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 2,
        });
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(svc.recover().is_err());
        assert!(matches!(
            svc.attempt_delivery("e2"),
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ));
        assert!(fake.sent_requests().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recover_handles_all_valid_gate_statuses() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();

        let mut replicated = test_req("replicated", 1);
        replicated.stream_key = "stream-replicated".into();
        j.accept(&replicated);
        let replicated_routing = routing_for_stream("stream-replicated");
        let replicated_resolved = resolve_routing(&replicated_routing).unwrap();
        j.create_delivery_pending(
            "replicated",
            &replicated_resolved.routing_checksum,
            "p0-test",
            1,
            &replicated_resolved.topic,
        );
        assert_eq!(
            j.confirm_replicated_delivery(
                "replicated",
                1,
                ReplicatedDeliveryConfirmation {
                    expected_routing_fingerprint: &replicated_resolved.routing_checksum,
                    expected_profile_id: "p0-test",
                    expected_profile_version: 1,
                    kafka_topic: &replicated_resolved.topic,
                    kafka_partition: 0,
                    kafka_offset: 10,
                },
            ),
            CasResult::Updated
        );

        let mut pending = test_req("pending", 2);
        pending.stream_key = "stream-pending".into();
        pending.request_fingerprint = [0xA1; 32];
        j.accept(&pending);
        let pending_routing = routing_for_stream("stream-pending");
        let pending_resolved = resolve_routing(&pending_routing).unwrap();
        j.create_delivery_pending(
            "pending",
            &pending_resolved.routing_checksum,
            "p0-test",
            1,
            &pending_resolved.topic,
        );

        let mut retrying = test_req("retrying", 3);
        retrying.stream_key = "stream-retrying".into();
        retrying.request_fingerprint = [0xA2; 32];
        j.accept(&retrying);
        let retrying_routing = routing_for_stream("stream-retrying");
        let retrying_resolved = resolve_routing(&retrying_routing).unwrap();
        j.create_delivery_pending(
            "retrying",
            &retrying_resolved.routing_checksum,
            "p0-test",
            1,
            &retrying_resolved.topic,
        );
        assert_eq!(
            j.update_delivery_retry("retrying", 1, 5000),
            CasResult::Updated
        );

        let mut blocked = test_req("blocked", 4);
        blocked.stream_key = "stream-blocked".into();
        blocked.request_fingerprint = [0xA3; 32];
        j.accept(&blocked);
        let blocked_routing = routing_for_stream("stream-blocked");
        let blocked_resolved = resolve_routing(&blocked_routing).unwrap();
        j.create_delivery_pending(
            "blocked",
            &blocked_resolved.routing_checksum,
            "p0-test",
            1,
            &blocked_resolved.topic,
        );
        assert_eq!(
            j.block_delivery("blocked", 1, "test block"),
            CasResult::Updated
        );

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        svc.recover().unwrap();

        assert_eq!(
            svc.gate().check("stream-replicated", 2),
            GateDecision::Allowed
        );
        assert_eq!(
            svc.gate().check("stream-pending", 2),
            GateDecision::Blocked {
                waiting_for_sequence: 1
            }
        );
        assert_eq!(
            svc.gate().check("stream-retrying", 2),
            GateDecision::Blocked {
                waiting_for_sequence: 1
            }
        );
        assert_eq!(
            svc.gate().check("stream-blocked", 2),
            GateDecision::Blocked {
                waiting_for_sequence: 1
            }
        );
    }

    #[test]
    fn service_persisted_block_survives_recover_and_blocks_later_sequence() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let mut r2 = test_req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        j.accept(&r2);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "old-profile", 1, &test_topic());
        j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Blocked { .. }
        ));
        assert_delivery_blocked(&j, "e1");

        let fake_after_recover = FakeProducer::new();
        fake_after_recover.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 2,
        });
        let mut recovered = DeliveryService::new(&j, &fake_after_recover, test_profile());
        recovered.recover().unwrap();

        assert!(matches!(
            recovered.attempt_delivery("e2"),
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ));
        assert!(fake_after_recover.sent_requests().is_empty());
    }

    #[test]
    fn invalid_routing_block_survives_recover_and_blocks_later_sequence() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        let mut r1 = test_req("e1", 1);
        r1.routing_version = 2;
        j.accept(&r1);
        let mut r2 = test_req("e2", 2);
        r2.request_fingerprint = [0xBB; 32];
        j.accept(&r2);
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());
        j.create_delivery_pending("e2", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Blocked { .. }
        ));
        assert!(fake.sent_requests().is_empty());
        assert_delivery_blocked(&j, "e1");

        let fake_after_recover = FakeProducer::new();
        fake_after_recover.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 2,
        });
        let mut recovered = DeliveryService::new(&j, &fake_after_recover, test_profile());
        recovered.recover().unwrap();

        assert!(matches!(
            recovered.attempt_delivery("e2"),
            DeliveryOutcome::GateHeld {
                waiting_for_sequence: 1
            }
        ));
        assert!(fake_after_recover.sent_requests().is_empty());
    }

    #[test]
    fn producer_success_on_wrong_topic_does_not_replicate() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: "wrong-topic".into(),
            partition: 0,
            offset: 99,
        });
        let mut svc = DeliveryService::new(&j, &fake, test_profile());

        assert!(matches!(
            svc.attempt_delivery("e1"),
            DeliveryOutcome::Error(_)
        ));
        match j.get_delivery_state("e1") {
            craftrelay_journal::DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "DELIVERY_PENDING");
                assert_eq!(ds.kafka_offset, None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn shutdown_prevents_delivery() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        let fake = FakeProducer::new();
        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        svc.shutdown();
        let outcome = svc.attempt_delivery("e1");
        assert!(matches!(outcome, DeliveryOutcome::Error(_)));
    }

    #[test]
    fn local_durable_alone_is_not_replicated() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        match j.get_status("e1") {
            craftrelay_journal::StatusResult::Found(s) => {
                assert_eq!(s.delivery_status, "LOCAL_ACCEPTED");
                assert_ne!(s.delivery_status, "REPLICATED");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn send_failure_is_not_replicated() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_error(SendError::BrokerUnavailable);

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        svc.attempt_delivery("e1");

        match j.get_delivery_state("e1") {
            craftrelay_journal::DeliveryStateResult::Found(ds) => {
                assert_ne!(ds.delivery_status, "REPLICATED");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn persisted_ack_reaches_replicated() {
        let j = LocalJournal::open_in_memory(cfg(), Box::new(AlwaysSafeDiskGuard)).unwrap();
        j.accept(&test_req("e1", 1));
        let fp = test_routing_fingerprint();
        j.create_delivery_pending("e1", &fp, "p0-test", 1, &test_topic());

        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: test_topic(),
            partition: 0,
            offset: 99,
        });

        let mut svc = DeliveryService::new(&j, &fake, test_profile());
        svc.attempt_delivery("e1");

        match j.get_delivery_state("e1") {
            craftrelay_journal::DeliveryStateResult::Found(ds) => {
                assert_eq!(ds.delivery_status, "REPLICATED");
                assert!(ds.confirmed_at_ms.is_some());
            }
            other => panic!("{other:?}"),
        }
        match j.get_status("e1") {
            craftrelay_journal::StatusResult::Found(s) => {
                assert_eq!(s.delivery_status, "REPLICATED");
            }
            other => panic!("{other:?}"),
        }
    }
}
