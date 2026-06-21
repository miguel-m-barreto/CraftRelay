package io.craftrelay.client;

import static io.craftrelay.client.ContractViolationException.Code;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

public final class ContractValidation {
    private ContractValidation() {
    }

    public static int positiveInt32(int value, String field) {
        if (value <= 0) {
            throw violation(Code.NON_POSITIVE_INT32, field + " must be a positive int32");
        }
        return value;
    }

    public static long positiveInt64(long value, String field) {
        if (value <= 0) {
            throw violation(Code.NON_POSITIVE_INT64, field + " must be a positive int64");
        }
        return value;
    }

    public static String canonicalUuidV7(String value, String field) {
        if (value == null || value.length() != 36 || !value.equals(value.toLowerCase(Locale.ROOT))) {
            throw violation(Code.INVALID_UUID, field + " must be canonical lowercase UUID text");
        }
        final UUID parsed;
        try {
            parsed = UUID.fromString(value);
        } catch (IllegalArgumentException exception) {
            throw violation(Code.INVALID_UUID, field + " must be a valid UUID");
        }
        if (!parsed.toString().equals(value)) {
            throw violation(Code.INVALID_UUID, field + " must use canonical UUID encoding");
        }
        if (parsed.version() != 7 || parsed.variant() != 2) {
            throw violation(Code.UUID_NOT_VERSION_7, field + " must be RFC-variant UUIDv7");
        }
        return value;
    }

    public static String boundedText(String value, String field, int maximumUtf8Bytes) {
        Objects.requireNonNull(value, field);
        int bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (value.isBlank() || bytes > maximumUtf8Bytes) {
            throw violation(Code.INVALID_ARGUMENT, field + " must contain 1.." + maximumUtf8Bytes + " UTF-8 bytes");
        }
        return value;
    }

    public static byte[] fixedChecksum(byte[] value, String field) {
        Objects.requireNonNull(value, field);
        if (value.length != 32) {
            throw violation(Code.INVALID_CHECKSUM, field + " must contain 32 bytes");
        }
        return value.clone();
    }

    public static ContractViolationException violation(Code code, String message) {
        return new ContractViolationException(code, message);
    }
}
