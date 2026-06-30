use craftrelay_domain::Checksum;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingInput {
    pub installation_id: String,
    pub namespace: String,
    pub logical_stream_type: String,
    pub stream_key: String,
    pub event_type: String,
    pub routing_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingResolution {
    pub topic: String,
    pub partition_key: String,
    pub routing_checksum: Checksum,
    pub routing_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingError {
    UnsupportedRoutingVersion,
    EmptyField,
}

pub fn resolve_routing(input: &RoutingInput) -> Result<RoutingResolution, RoutingError> {
    if input.routing_version != 1 {
        return Err(RoutingError::UnsupportedRoutingVersion);
    }
    if input.installation_id.is_empty()
        || input.namespace.is_empty()
        || input.logical_stream_type.is_empty()
        || input.stream_key.is_empty()
        || input.event_type.is_empty()
    {
        return Err(RoutingError::EmptyField);
    }

    let topic = format!(
        "craftrelay.{}.{}.events",
        input.installation_id, input.namespace
    );
    let partition_key = format!(
        "{}\0{}\0{}\0{}",
        input.installation_id, input.namespace, input.logical_stream_type, input.stream_key
    );
    let canonical = format!(
        "{}\0{}\0{}\0{}\0{}\0{}",
        input.installation_id,
        input.namespace,
        input.logical_stream_type,
        input.stream_key,
        input.event_type,
        input.routing_version
    );
    let routing_checksum = craftrelay_domain::sha256(canonical.as_bytes());

    Ok(RoutingResolution {
        topic,
        partition_key,
        routing_checksum,
        routing_version: input.routing_version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_input() -> RoutingInput {
        RoutingInput {
            installation_id: "install-1".into(),
            namespace: "economy".into(),
            logical_stream_type: "account".into(),
            stream_key: "player-abc".into(),
            event_type: "transfer".into(),
            routing_version: 1,
        }
    }

    #[test]
    fn determinism_same_input_same_output() {
        let input = sample_input();
        let r1 = resolve_routing(&input).unwrap();
        let r2 = resolve_routing(&input).unwrap();
        assert_eq!(r1, r2);
    }

    #[test]
    fn different_installation_different_topic() {
        let mut a = sample_input();
        let mut b = sample_input();
        a.installation_id = "install-a".into();
        b.installation_id = "install-b".into();
        let ra = resolve_routing(&a).unwrap();
        let rb = resolve_routing(&b).unwrap();
        assert_ne!(ra.topic, rb.topic);
    }

    #[test]
    fn different_stream_key_different_partition_key_same_topic() {
        let mut a = sample_input();
        let mut b = sample_input();
        a.stream_key = "key-1".into();
        b.stream_key = "key-2".into();
        let ra = resolve_routing(&a).unwrap();
        let rb = resolve_routing(&b).unwrap();
        assert_eq!(ra.topic, rb.topic);
        assert_ne!(ra.partition_key, rb.partition_key);
    }

    #[test]
    fn empty_field_rejected() {
        let mut input = sample_input();
        input.installation_id = String::new();
        assert_eq!(resolve_routing(&input), Err(RoutingError::EmptyField));

        let mut input = sample_input();
        input.namespace = String::new();
        assert_eq!(resolve_routing(&input), Err(RoutingError::EmptyField));

        let mut input = sample_input();
        input.logical_stream_type = String::new();
        assert_eq!(resolve_routing(&input), Err(RoutingError::EmptyField));

        let mut input = sample_input();
        input.stream_key = String::new();
        assert_eq!(resolve_routing(&input), Err(RoutingError::EmptyField));

        let mut input = sample_input();
        input.event_type = String::new();
        assert_eq!(resolve_routing(&input), Err(RoutingError::EmptyField));
    }

    #[test]
    fn unsupported_routing_version_rejected() {
        let mut input = sample_input();
        input.routing_version = 2;
        assert_eq!(
            resolve_routing(&input),
            Err(RoutingError::UnsupportedRoutingVersion)
        );

        input.routing_version = 0;
        assert_eq!(
            resolve_routing(&input),
            Err(RoutingError::UnsupportedRoutingVersion)
        );
    }

    #[test]
    fn checksum_changes_when_any_field_changes() {
        let base = resolve_routing(&sample_input()).unwrap().routing_checksum;

        let mut input = sample_input();
        input.installation_id = "other".into();
        assert_ne!(resolve_routing(&input).unwrap().routing_checksum, base);

        let mut input = sample_input();
        input.namespace = "other".into();
        assert_ne!(resolve_routing(&input).unwrap().routing_checksum, base);

        let mut input = sample_input();
        input.logical_stream_type = "other".into();
        assert_ne!(resolve_routing(&input).unwrap().routing_checksum, base);

        let mut input = sample_input();
        input.stream_key = "other".into();
        assert_ne!(resolve_routing(&input).unwrap().routing_checksum, base);

        let mut input = sample_input();
        input.event_type = "other".into();
        assert_ne!(resolve_routing(&input).unwrap().routing_checksum, base);
    }
}
