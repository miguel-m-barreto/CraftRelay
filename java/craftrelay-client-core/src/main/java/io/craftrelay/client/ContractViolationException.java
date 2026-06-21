package io.craftrelay.client;

/** Typed rejection raised before a request crosses the transport boundary. */
public final class ContractViolationException extends IllegalArgumentException {
    private final Code code;

    public ContractViolationException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public Code code() {
        return code;
    }

    public enum Code {
        INVALID_UUID,
        UUID_NOT_VERSION_7,
        INVALID_INSTALLATION_SCOPE,
        NON_POSITIVE_INT32,
        NON_POSITIVE_INT64,
        METADATA_COUNT_EXCEEDED,
        METADATA_BYTES_EXCEEDED,
        INVALID_METADATA_KEY,
        DUPLICATE_METADATA_KEY,
        PAYLOAD_TOO_LARGE,
        INVALID_CHECKSUM,
        LIFECYCLE_INTEGRITY_CONFLICT,
        TOKEN_INVALID_MAC,
        TOKEN_EXPIRED,
        TOKEN_SCOPE_MISMATCH,
        BOUNDS_EXCEEDED,
        INVALID_ARGUMENT
    }
}
