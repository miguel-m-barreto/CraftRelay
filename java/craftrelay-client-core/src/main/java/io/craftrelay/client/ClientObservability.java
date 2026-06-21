package io.craftrelay.client;

public final class ClientObservability {
    private ClientObservability() {
    }

    public interface SecurityEvents {
        void rejected(String code);
        void tokenValidationFailed(String code);
        static SecurityEvents noOp() { return new SecurityEvents() {
            @Override public void rejected(String code) { }
            @Override public void tokenValidationFailed(String code) { }
        }; }
    }

    public interface Metrics {
        void publishSubmitted(String producerId);
        void publishTrackingDetached(String producerId);
        void reconnectAttempt(String producerId);
        void querySubmitted(String producerId);
        void watchDetached(String producerId, String reason);
        static Metrics noOp() { return new Metrics() {
            @Override public void publishSubmitted(String producerId) { }
            @Override public void publishTrackingDetached(String producerId) { }
            @Override public void reconnectAttempt(String producerId) { }
            @Override public void querySubmitted(String producerId) { }
            @Override public void watchDetached(String producerId, String reason) { }
        }; }
    }
}
