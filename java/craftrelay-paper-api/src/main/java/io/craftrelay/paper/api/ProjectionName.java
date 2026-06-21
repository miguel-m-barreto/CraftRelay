package io.craftrelay.paper.api;
public record ProjectionName(String value) {
    public ProjectionName {
        if (value == null || value.isBlank() || value.length() > 128) {
            throw new IllegalArgumentException("projection name must contain 1..128 characters");
        }
    }
}
