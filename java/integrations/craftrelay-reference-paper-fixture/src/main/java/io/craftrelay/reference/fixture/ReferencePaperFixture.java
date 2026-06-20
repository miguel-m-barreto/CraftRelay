package io.craftrelay.reference.fixture;
import io.craftrelay.paper.api.*;
import io.craftrelay.reference.ReferenceDomainAdapter;
/** Fake compile fixture. Real Paper/Folia runtime behavior is outside Sprint 0. */
public final class ReferencePaperFixture { public ReferenceDomainAdapter enable(CraftRelayService service, RegisteredPluginHandle plugin){if(service.readiness()!=CraftRelayService.Readiness.READY) throw new IllegalStateException("NOT_READY"); return new ReferenceDomainAdapter(service.clientFor(plugin));} }
