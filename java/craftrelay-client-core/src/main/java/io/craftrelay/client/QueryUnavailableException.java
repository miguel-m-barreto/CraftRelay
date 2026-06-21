package io.craftrelay.client;

public final class QueryUnavailableException extends RuntimeException {
    private final Code code;

    public QueryUnavailableException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public Code code() { return code; }

    public enum Code { STRICT_READ_NOT_IMPLEMENTED, TOKEN_READ_NOT_IMPLEMENTED, QUERY_SERVICE_UNAVAILABLE }
}
