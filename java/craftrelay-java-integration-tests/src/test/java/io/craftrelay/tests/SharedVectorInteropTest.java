package io.craftrelay.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

final class SharedVectorInteropTest {
    @Test void javaMatchesSharedMetadataBarrierEnvelopeAndTokenVectors() throws Exception {
        String json = Files.readString(repositoryRoot().resolve("test-vectors/v1/shared-vectors.json"));
        assertDigest(json, "a=first\\|z=last", "efe0f44780d34543dcd2cd3261a94da5b77d794255ce287116b2604acb479fae");
        assertDigest(json, "v=1\\|topology=3\\|routing=7\\|events:0=0\\|events:1=42",
                "d73cf4167cc5ec91769dde542cbeef8ee6c78aefc4eff59a9b332cb1053fa090");
        String tokenCanonical = extract(json, "\\\"consistency_token\\\".*?\\\"canonical\\\":\\\"([^\\\"]+)\\\"");
        String key = extract(json, "\\\"fixture_key_utf8\\\":\\\"([^\\\"]+)\\\"");
        String expectedMac = extract(json, "\\\"mac\\\":\\\"([0-9a-f]{64})\\\"");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        assertEquals(expectedMac, HexFormat.of().formatHex(mac.doFinal(tokenCanonical.getBytes(StandardCharsets.UTF_8))));
    }

    private static void assertDigest(String json, String escapedCanonical, String expected) throws Exception {
        String canonical = escapedCanonical.replace("\\|", "|");
        assertTrue(json.contains("\"canonical\":\"" + canonical + "\""));
        assertEquals(expected, HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8))));
    }

    private static String extract(String json, String regex) {
        var matcher = Pattern.compile(regex).matcher(json);
        assertTrue(matcher.find(), regex);
        return matcher.group(1);
    }

    private static Path repositoryRoot() {
        Path root = Path.of("").toAbsolutePath();
        while (root != null && !Files.isRegularFile(root.resolve("MASTER_PLAN.md"))) root = root.getParent();
        return java.util.Objects.requireNonNull(root, "repository root");
    }
}
