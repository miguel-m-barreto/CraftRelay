package io.craftrelay.paper.api;

/** Generated clients own parameter encoding and result decoding; arbitrary SQL is absent. */
public interface TypedQueryRequest<R> {
    QueryContractHandle contract();
    byte[] encodeParameters();
    R decodeResult(byte[] encodedResult);
}
