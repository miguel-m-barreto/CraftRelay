package io.craftrelay.client;

import java.security.GeneralSecurityException;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** In-memory key provider for tests/fixtures; production credential storage is out of scope. */
public final class HmacSha256TokenMac implements TokenMacProvider {
    private final Map<String, byte[]> keys;

    public HmacSha256TokenMac(Map<String, byte[]> keys) {
        if (keys.isEmpty() || keys.size() > 16) {
            throw new IllegalArgumentException("token key set must contain 1..16 keys");
        }
        this.keys = keys.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey, entry -> entry.getValue().clone()));
    }

    @Override
    public byte[] sign(String keyId, byte[] canonicalToken) {
        byte[] key = keys.get(keyId);
        if (key == null || key.length < 16) {
            throw ContractValidation.violation(
                    ContractViolationException.Code.TOKEN_INVALID_MAC,
                    "unknown or invalid token key");
        }
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(canonicalToken);
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("HmacSHA256 unavailable", exception);
        }
    }
}
