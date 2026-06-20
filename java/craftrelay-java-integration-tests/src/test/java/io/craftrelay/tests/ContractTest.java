package io.craftrelay.tests;
import io.craftrelay.paper.api.*;
import org.junit.jupiter.api.Test;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.regex.Pattern;
import java.util.List;
import java.lang.reflect.Modifier;
import static org.junit.jupiter.api.Assertions.*;
final class ContractTest {
 @Test void strictDoesNotDowngrade(){assertInstanceOf(QueryConsistency.Strict.class,QueryConsistency.strictLatestCommitted());}
 @Test void tokenSetIsBoundedByCallerContract(){assertThrows(IllegalArgumentException.class,()->QueryConsistency.atLeastTokens(List.of()));}
 @Test void producerResolutionUsesOpaqueRegisteredPluginHandle() throws Exception {
   var clientFor=CraftRelayService.class.getMethod("clientFor",RegisteredPluginHandle.class);
   assertEquals(RegisteredPluginHandle.class,clientFor.getParameterTypes()[0]);
   assertTrue(RegisteredPluginHandle.class.isSealed());
   for(var implementation:RegisteredPluginHandle.class.getPermittedSubclasses()){
     for(var constructor:implementation.getDeclaredConstructors()){
       assertFalse(Modifier.isPublic(constructor.getModifiers()));
     }
   }
   assertFalse(java.util.Arrays.stream(CraftRelayService.class.getMethods())
       .anyMatch(method -> java.util.Arrays.asList(method.getParameterTypes()).contains(String.class)));
 }
 @Test void sharedVectorsExist() throws Exception {
   Path root=Path.of("").toAbsolutePath();
   while(root!=null&&!Files.isRegularFile(root.resolve("MASTER_PLAN.md"))){root=root.getParent();}
   assertNotNull(root,"repository root");
   String json=Files.readString(root.resolve("test-vectors/v1/shared-vectors.json"));
   assertTrue(json.contains("\"required_next_offset\""));
   assertTrue(json.contains("\"PROJECTED_PLUS_LIVE\""));
   assertTrue(json.contains("\"authenticated\":true"));
   String digest=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest("CraftRelay".getBytes(StandardCharsets.UTF_8)));
   assertTrue(json.contains(digest));
   var matcher=Pattern.compile("\\\"canonical_hex\\\":\\\"([0-9a-f]+)\\\",\\\"sha256\\\":\\\"([0-9a-f]{64})\\\"").matcher(json);
   assertTrue(matcher.find(),"fingerprint vector");
   String fingerprintDigest=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(HexFormat.of().parseHex(matcher.group(1))));
   assertEquals(matcher.group(2),fingerprintDigest);
 }
}
