package io.craftrelay.reference;

import io.craftrelay.paper.api.QueryContractHandle;
import io.craftrelay.paper.api.TypedQueryRequest;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Generated-equivalent typed query request; no SQL or arbitrary schema selector is exposed. */
public record GetProgressRequest(UUID playerId) implements TypedQueryRequest<ReferenceProgress> {
    @Override public QueryContractHandle contract() { return ReferenceDomainAdapter.GET_PROGRESS; }
    @Override public byte[] encodeParameters() { return playerId.toString().getBytes(StandardCharsets.US_ASCII); }

    @Override
    public ReferenceProgress decodeResult(byte[] encodedResult) {
        long progress = Long.parseLong(new String(encodedResult, StandardCharsets.US_ASCII));
        return new ReferenceProgress(playerId, progress);
    }
}
