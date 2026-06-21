package io.craftrelay.client;

public final class ContractLimits {
    public static final int MAX_METADATA_ENTRIES = 32;
    public static final int MAX_METADATA_BYTES = 8_192;
    public static final int MAX_METADATA_KEY_BYTES = 128;
    public static final int MAX_METADATA_VALUE_BYTES = 1_024;
    public static final int MAX_PAYLOAD_BYTES = 1_048_576;
    public static final int MAX_BARRIER_PARTITIONS = 1_024;
    public static final int MAX_TOKEN_MUTATION_REFERENCES = 64;
    public static final int MAX_TOKEN_BYTES = 16_384;
    public static final int MAX_QUERY_PARAMETER_BYTES = 65_536;
    public static final int MAX_WATCH_BUFFER_EVENTS = 256;
    public static final long MAX_WATCH_BUFFER_BYTES = 1_048_576;

    private ContractLimits() {
    }
}
