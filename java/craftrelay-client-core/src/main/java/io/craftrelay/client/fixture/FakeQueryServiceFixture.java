package io.craftrelay.client.fixture;

import io.craftrelay.client.ContractLimits;
import io.craftrelay.client.QueryServiceClient;
import io.craftrelay.client.QueryUnavailableException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Typed-byte fixture only; it performs no SQL and proves no strict-read waiting. */
public final class FakeQueryServiceFixture implements QueryServiceClient {
    private final Map<String, byte[]> responses;

    public FakeQueryServiceFixture(Map<String, byte[]> responses) {
        if (responses.size() > 256 || responses.values().stream().anyMatch(value -> value.length > 1_048_576)) {
            throw new IllegalArgumentException("fake query fixture responses exceed bounds");
        }
        this.responses = responses.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey, entry -> entry.getValue().clone()));
    }

    @Override
    public CompletionStage<Response> query(
            String contractHandle, byte[] typedParameters, String freshnessMode, Duration timeout) {
        if (typedParameters.length > ContractLimits.MAX_QUERY_PARAMETER_BYTES) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("query parameters exceed bounds"));
        }
        if (timeout.isNegative() || timeout.isZero()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("timeout must be positive"));
        }
        byte[] response = responses.get(contractHandle);
        if (response == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("unknown typed query contract"));
        }
        if (!freshnessMode.equals("ALLOW_STALE")) {
            QueryUnavailableException.Code code = freshnessMode.equals("AT_LEAST_TOKEN")
                    ? QueryUnavailableException.Code.TOKEN_READ_NOT_IMPLEMENTED
                    : QueryUnavailableException.Code.STRICT_READ_NOT_IMPLEMENTED;
            return CompletableFuture.failedFuture(new QueryUnavailableException(
                    code, "Sprint 1 fake Query Service cannot prove authoritative freshness"));
        }
        return CompletableFuture.completedFuture(new Response(
                response, "STALE_ACCEPTED", false, false));
    }
}
