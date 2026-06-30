use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateDecision {
    Allowed,
    Blocked { waiting_for_sequence: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredStreamState {
    pub stream_key: String,
    pub stream_sequence: i64,
    pub replicated: bool,
    pub blocking: bool,
    pub in_flight: bool,
}

struct StreamGateState {
    highest_completed: i64,
    in_flight: Option<i64>,
    blocking: Option<i64>,
}

pub struct OrderingGate {
    streams: BTreeMap<String, StreamGateState>,
}

impl Default for OrderingGate {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderingGate {
    pub fn new() -> Self {
        OrderingGate {
            streams: BTreeMap::new(),
        }
    }

    pub fn check(&self, stream_key: &str, stream_sequence: i64) -> GateDecision {
        match self.streams.get(stream_key) {
            None => {
                if stream_sequence == 1 {
                    GateDecision::Allowed
                } else {
                    GateDecision::Blocked {
                        waiting_for_sequence: 1,
                    }
                }
            }
            Some(state) => {
                let next = state.highest_completed + 1;
                if let Some(in_flight) = state.in_flight {
                    return GateDecision::Blocked {
                        waiting_for_sequence: in_flight,
                    };
                }
                if let Some(blocking) = state.blocking {
                    if stream_sequence == blocking {
                        return GateDecision::Allowed;
                    }
                    return GateDecision::Blocked {
                        waiting_for_sequence: blocking,
                    };
                }
                if stream_sequence == next {
                    GateDecision::Allowed
                } else {
                    GateDecision::Blocked {
                        waiting_for_sequence: next,
                    }
                }
            }
        }
    }

    pub fn mark_in_flight(&mut self, stream_key: &str, stream_sequence: i64) {
        let state = self
            .streams
            .entry(stream_key.to_string())
            .or_insert(StreamGateState {
                highest_completed: 0,
                in_flight: None,
                blocking: None,
            });
        state.in_flight = Some(stream_sequence);
        state.blocking = None;
    }

    pub fn mark_blocking(&mut self, stream_key: &str, stream_sequence: i64) {
        let state = self
            .streams
            .entry(stream_key.to_string())
            .or_insert(StreamGateState {
                highest_completed: 0,
                in_flight: None,
                blocking: None,
            });
        if state.in_flight == Some(stream_sequence) {
            state.in_flight = None;
        }
        state.blocking = Some(stream_sequence);
    }

    pub fn mark_completed(&mut self, stream_key: &str, stream_sequence: i64) {
        let state = self
            .streams
            .entry(stream_key.to_string())
            .or_insert(StreamGateState {
                highest_completed: 0,
                in_flight: None,
                blocking: None,
            });
        state.highest_completed = stream_sequence;
        if state.in_flight == Some(stream_sequence) {
            state.in_flight = None;
        }
        if state.blocking == Some(stream_sequence) {
            state.blocking = None;
        }
    }

    pub fn recover_from_scan(&mut self, states: &[RecoveredStreamState]) {
        self.streams.clear();
        for recovered in states {
            let entry =
                self.streams
                    .entry(recovered.stream_key.clone())
                    .or_insert(StreamGateState {
                        highest_completed: 0,
                        in_flight: None,
                        blocking: None,
                    });
            if recovered.replicated && recovered.stream_sequence > entry.highest_completed {
                entry.highest_completed = recovered.stream_sequence;
            }
            if recovered.blocking {
                entry.blocking = match entry.blocking {
                    Some(existing) => Some(existing.min(recovered.stream_sequence)),
                    None => Some(recovered.stream_sequence),
                };
            }
            if recovered.in_flight {
                entry.in_flight = Some(recovered.stream_sequence);
            }
        }
    }

    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_1_allowed_on_empty_gate() {
        let gate = OrderingGate::new();
        assert_eq!(gate.check("stream-a", 1), GateDecision::Allowed);
    }

    #[test]
    fn seq_2_blocked_when_1_not_completed() {
        let gate = OrderingGate::new();
        assert_eq!(
            gate.check("stream-a", 2),
            GateDecision::Blocked {
                waiting_for_sequence: 1
            }
        );
    }

    #[test]
    fn seq_2_allowed_after_1_completed() {
        let mut gate = OrderingGate::new();
        gate.mark_completed("stream-a", 1);
        assert_eq!(gate.check("stream-a", 2), GateDecision::Allowed);
    }

    #[test]
    fn seq_2_blocked_when_1_in_flight() {
        let mut gate = OrderingGate::new();
        gate.mark_in_flight("stream-a", 1);
        assert_eq!(
            gate.check("stream-a", 1),
            GateDecision::Blocked {
                waiting_for_sequence: 1
            }
        );
    }

    #[test]
    fn different_streams_independent() {
        let mut gate = OrderingGate::new();
        gate.mark_in_flight("stream-a", 1);
        assert_eq!(gate.check("stream-b", 1), GateDecision::Allowed);
    }

    #[test]
    fn recovery_from_scan() {
        let mut gate = OrderingGate::new();
        gate.recover_from_scan(&[
            RecoveredStreamState {
                stream_key: "stream-a".into(),
                stream_sequence: 3,
                replicated: true,
                blocking: false,
                in_flight: false,
            },
            RecoveredStreamState {
                stream_key: "stream-b".into(),
                stream_sequence: 2,
                replicated: false,
                blocking: false,
                in_flight: true,
            },
        ]);
        assert_eq!(gate.check("stream-a", 4), GateDecision::Allowed);
        assert_eq!(
            gate.check("stream-b", 2),
            GateDecision::Blocked {
                waiting_for_sequence: 2
            }
        );
    }

    #[test]
    fn mark_completed_is_idempotent() {
        let mut gate = OrderingGate::new();
        gate.mark_completed("stream-a", 1);
        gate.mark_completed("stream-a", 1);
        assert_eq!(gate.check("stream-a", 2), GateDecision::Allowed);
    }

    #[test]
    fn stream_count_tracks_correctly() {
        let mut gate = OrderingGate::new();
        assert_eq!(gate.stream_count(), 0);
        gate.mark_in_flight("stream-a", 1);
        assert_eq!(gate.stream_count(), 1);
        gate.mark_in_flight("stream-b", 1);
        assert_eq!(gate.stream_count(), 2);
    }

    #[test]
    fn blocking_sequence_holds_later_sequence_but_can_retry_itself() {
        let mut gate = OrderingGate::new();
        gate.mark_blocking("stream-a", 1);
        assert_eq!(gate.check("stream-a", 1), GateDecision::Allowed);
        assert_eq!(
            gate.check("stream-a", 2),
            GateDecision::Blocked {
                waiting_for_sequence: 1
            }
        );
        assert_eq!(gate.check("stream-b", 1), GateDecision::Allowed);
    }

    #[test]
    fn recovery_blocks_on_lowest_unreplicated_sequence() {
        let mut gate = OrderingGate::new();
        gate.recover_from_scan(&[
            RecoveredStreamState {
                stream_key: "stream-a".into(),
                stream_sequence: 1,
                replicated: true,
                blocking: false,
                in_flight: false,
            },
            RecoveredStreamState {
                stream_key: "stream-a".into(),
                stream_sequence: 2,
                replicated: false,
                blocking: true,
                in_flight: false,
            },
            RecoveredStreamState {
                stream_key: "stream-a".into(),
                stream_sequence: 3,
                replicated: false,
                blocking: true,
                in_flight: false,
            },
        ]);
        assert_eq!(gate.check("stream-a", 2), GateDecision::Allowed);
        assert_eq!(
            gate.check("stream-a", 3),
            GateDecision::Blocked {
                waiting_for_sequence: 2
            }
        );
    }
}
