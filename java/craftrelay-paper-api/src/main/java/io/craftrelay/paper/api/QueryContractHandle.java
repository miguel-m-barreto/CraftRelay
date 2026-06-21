package io.craftrelay.paper.api;
public record QueryContractHandle(String opaqueValue) {
    public QueryContractHandle {
        if (opaqueValue == null || opaqueValue.isBlank() || opaqueValue.length() > 256) {
            throw new IllegalArgumentException("query contract handle must contain 1..256 characters");
        }
    }
}
