package io.craftrelay.paper.api;
public record EventContractHandle(String opaqueValue) {
    public EventContractHandle {
        if (opaqueValue == null || opaqueValue.isBlank() || opaqueValue.length() > 256) {
            throw new IllegalArgumentException("event contract handle must contain 1..256 characters");
        }
    }
}
