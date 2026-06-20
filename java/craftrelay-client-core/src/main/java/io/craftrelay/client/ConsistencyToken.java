package io.craftrelay.client;

import java.time.Instant;
import java.util.Map;

public record ConsistencyToken(int version, String installationId, String projectorId, String projectionName, Instant expiresAt, long topologyVersion, long routingVersion, Map<Integer, Long> requiredNextOffsets, byte[] checksum, byte[] mac) {
    public ConsistencyToken { requiredNextOffsets = Map.copyOf(requiredNextOffsets); checksum = checksum.clone(); mac = mac.clone(); if (mac.length == 0) throw new IllegalArgumentException("authenticated token requires MAC"); }
}

