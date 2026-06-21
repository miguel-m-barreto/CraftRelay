package io.craftrelay.client;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public interface QueryServiceClient {
    CompletionStage<Response> query(String contractHandle, byte[] typedParameters, String freshnessMode, Duration timeout);

    record Response(byte[] typedResult, String proof, boolean current, boolean authoritative) {
        public Response { typedResult = typedResult.clone(); }
        @Override public byte[] typedResult() { return typedResult.clone(); }
    }
}
