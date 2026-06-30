#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendRequest {
    pub event_id: String,
    pub topic: String,
    pub partition_key: String,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendSuccess {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError {
    BrokerUnavailable,
    TopicNotFound,
    AuthorizationFailed,
    MessageTooLarge,
    Timeout,
    QueueFull,
    Unknown(String),
}

impl SendError {
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            SendError::BrokerUnavailable | SendError::Timeout | SendError::QueueFull
        )
    }

    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            SendError::TopicNotFound | SendError::AuthorizationFailed | SendError::MessageTooLarge
        )
    }
}

pub trait KafkaProducer: Send + Sync {
    fn send(&self, request: &SendRequest) -> Result<SendSuccess, SendError>;
}

pub struct FakeProducer {
    results: std::sync::Mutex<Vec<Result<SendSuccess, SendError>>>,
    sent: std::sync::Mutex<Vec<SendRequest>>,
}

impl Default for FakeProducer {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeProducer {
    pub fn new() -> Self {
        FakeProducer {
            results: std::sync::Mutex::new(Vec::new()),
            sent: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn enqueue_success(&self, success: SendSuccess) {
        self.results.lock().unwrap().push(Ok(success));
    }

    pub fn enqueue_error(&self, error: SendError) {
        self.results.lock().unwrap().push(Err(error));
    }

    pub fn sent_requests(&self) -> Vec<SendRequest> {
        self.sent.lock().unwrap().clone()
    }
}

impl KafkaProducer for FakeProducer {
    fn send(&self, request: &SendRequest) -> Result<SendSuccess, SendError> {
        self.sent.lock().unwrap().push(request.clone());
        let mut results = self.results.lock().unwrap();
        if results.is_empty() {
            return Err(SendError::Unknown("no more enqueued results".into()));
        }
        results.remove(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> SendRequest {
        SendRequest {
            event_id: "evt-1".into(),
            topic: "craftrelay.install-1.economy.events".into(),
            partition_key: "pk-1".into(),
            payload: vec![1, 2, 3],
            headers: vec![("key".into(), "value".into())],
        }
    }

    #[test]
    fn fake_returns_enqueued_results_in_order() {
        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: "t1".into(),
            partition: 0,
            offset: 100,
        });
        fake.enqueue_error(SendError::Timeout);

        let r1 = fake.send(&sample_request());
        assert!(r1.is_ok());
        assert_eq!(r1.unwrap().offset, 100);

        let r2 = fake.send(&sample_request());
        assert_eq!(r2, Err(SendError::Timeout));
    }

    #[test]
    fn fake_records_sent_requests() {
        let fake = FakeProducer::new();
        fake.enqueue_success(SendSuccess {
            topic: "t1".into(),
            partition: 0,
            offset: 0,
        });
        let req = sample_request();
        let _ = fake.send(&req);
        let sent = fake.sent_requests();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].event_id, "evt-1");
    }

    #[test]
    fn fake_returns_unknown_when_empty() {
        let fake = FakeProducer::new();
        let result = fake.send(&sample_request());
        assert!(matches!(result, Err(SendError::Unknown(_))));
    }

    #[test]
    fn error_classification() {
        assert!(SendError::BrokerUnavailable.is_retriable());
        assert!(SendError::Timeout.is_retriable());
        assert!(SendError::QueueFull.is_retriable());
        assert!(!SendError::TopicNotFound.is_retriable());
        assert!(!SendError::AuthorizationFailed.is_retriable());
        assert!(!SendError::MessageTooLarge.is_retriable());
        assert!(!SendError::Unknown("x".into()).is_retriable());

        assert!(SendError::TopicNotFound.is_permanent());
        assert!(SendError::AuthorizationFailed.is_permanent());
        assert!(SendError::MessageTooLarge.is_permanent());
        assert!(!SendError::BrokerUnavailable.is_permanent());
        assert!(!SendError::Timeout.is_permanent());
        assert!(!SendError::QueueFull.is_permanent());
        assert!(!SendError::Unknown("x".into()).is_permanent());
    }
}
