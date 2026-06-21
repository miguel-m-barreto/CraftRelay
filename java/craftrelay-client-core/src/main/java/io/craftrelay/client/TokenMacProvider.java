package io.craftrelay.client;

/** Production implementations resolve key material outside plugin and Bridge APIs. */
public interface TokenMacProvider {
    byte[] sign(String keyId, byte[] canonicalToken);
}
