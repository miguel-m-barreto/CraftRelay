# CraftRelay — PluginAdapter.md

**Status:** AUTHORITATIVE COMPANION DOCUMENT — CONTRACT-CORRECTED + BATCHING/FLUSH POLICY  
**Target:** CraftRelay v1  
**Authority:** This document defines the Paper-side integration architecture. `MASTER_PLAN.md` remains authoritative for durability, transport, projection, query, retention, batching policy ceilings, and release gates. `PluginUsage.md` is a practical supporting guide and is not authoritative when conflicts exist.  
**Decision:** One `CraftRelayPaperBridge` plugin per Paper server plus one embedded typed AdapterClass/generated client per first-party domain plugin.

---

# 1. Purpose

CraftRelay moves authoritative domain events from Paper plugins to external sidecars and returns typed projected data from PostgreSQL through external sidecars.

```text
Write:
Paper domain plugin
→ embedded domain AdapterClass/generated client
→ CraftRelayPaperBridge
→ CraftRelay Agent
→ local journal
→ Kafka
→ Projector
→ PostgreSQL

Read:
Paper domain plugin
→ embedded domain AdapterClass/generated client
→ CraftRelayPaperBridge
→ CraftRelay Query Service
→ PostgreSQL projection
```

The design must avoid two bad extremes:

1. every plugin implements its own gRPC channels, reconnect loops, queues, health checks, Kafka integration, SQL, and threading;
2. one giant Bridge plugin contains every domain rule and becomes a monolith.

The selected boundary is:

```text
shared transport and Paper lifecycle in one Bridge
+
domain-specific translation inside each domain plugin
```

---

# 2. Final decision

## 2.1 First-party CraftMMO plugins

For plugins developed and maintained by the same team:

```text
one CraftRelayPaperBridge per Paper server
+
one AdapterClass or generated typed domain client inside each plugin
```

Examples:

```text
MiningPlugin
└─ MiningCraftRelayAdapter

EconomyPlugin
└─ EconomyCraftRelayAdapter

ClaimsPlugin
└─ ClaimsCraftRelayAdapter

PremiumPlugin
└─ PremiumCraftRelayAdapter
```

There is no separate `MiningCraftRelayAdapterPlugin`, `EconomyCraftRelayAdapterPlugin`, or similar first-party plugin.

## 2.2 Third-party plugins

A separate compatibility adapter plugin is allowed only when the original plugin cannot be modified.

```text
ThirdPartyPlugin
+
CraftRelayThirdPartyCompatibilityPlugin
```

This exception requires an ADR because listener authority, duplicate event emission, startup order, plugin API compatibility, and failure semantics are different.

---

# 3. Component responsibilities

## 3.1 CraftRelayPaperBridge

`CraftRelayPaperBridge` is a thin infrastructure plugin.

It owns:

- the shared Java client runtime;
- the persistent bounded Agent channel/runtime;
- the persistent bounded Query Service channel/runtime;
- logical authenticated producer sessions;
- reconnect and backoff;
- bounded publish tracking;
- bounded token/query/watch tracking;
- health and readiness;
- service registration;
- metrics and diagnostics;
- bounded shutdown.

It does not own:

- the authoritative local journal;
- Kafka publishing logic;
- PostgreSQL access;
- SQL;
- event retention policy;
- effective durability policy;
- mining/economy/claims/premium business rules;
- projector logic;
- query handler logic;
- durable receipts.

The Bridge may forward a durable receipt only after receiving a lifecycle snapshot/receipt whose state was persisted by the Agent.

## 3.2 craftrelay-paper-api

`craftrelay-paper-api` is the small stable API used by plugins.

Domain plugins depend on it as `compileOnly` or provided scope. They must not shade a second Bridge implementation.

Conceptual API:

```java
public interface CraftRelayService {
    CraftRelayDomainClient clientFor(Plugin plugin);
    CraftRelayBridgeHealth health();
}
```

The actual API may be more strongly typed, but it must preserve these properties:

- service obtained from Paper/Bukkit service registry;
- access resolved from the actual `Plugin` instance;
- no arbitrary `producer_id` parameter;
- no direct access to internal gRPC stubs;
- asynchronous operations only;
- bounded timeouts and tracking;
- typed errors.

## 3.3 craftrelay-client-core

`craftrelay-client-core` contains:

- generated Protobuf/gRPC bindings;
- channel-independent request/status models;
- lifecycle revision validation;
- token validation helpers where appropriate;
- bounded tracking primitives;
- retry/status helpers that preserve `event_id`;
- no Paper-specific domain logic.

The Bridge packages and owns this runtime.

## 3.4 Domain AdapterClass/generated client

The domain adapter belongs to the domain plugin.

It owns:

- domain-friendly Java methods;
- typed Protobuf construction;
- canonical event/query identifiers;
- stream-key derivation declared by the contract;
- stable event/operation ID creation rules;
- mapping CraftRelay results into domain-facing result types;
- scheduling Paper mutations on the correct scheduler;
- plugin-specific feature flags and shadow-mode comparison.

It does not own:

- Kafka topics;
- replication factor;
- retention;
- effective priority;
- projection selection;
- SQL;
- retries using new event IDs;
- direct gRPC channels;
- unbounded queues.

## 3.5 External sidecars

The external processes remain authoritative for infrastructure concerns:

```text
CraftRelay Agent
→ authentication
→ policy resolution
→ local WAL/journal
→ Kafka delivery
→ publish lifecycle

CraftRelay Projector
→ schema validation
→ sequence integrity
→ deduplication
→ aggregates/read models
→ ACK outbox

CraftRelay Query Service
→ typed queries
→ strict barriers
→ read-your-write tokens
→ bounded watches
→ PostgreSQL reads
```

---

# 4. Why one shared Bridge

One Bridge avoids duplication of:

- gRPC channels;
- Netty/event-loop resources;
- reconnect loops;
- DNS/address handling;
- health checks;
- authentication setup;
- service lifecycle;
- metrics exporters;
- query/watch subscriptions;
- shutdown drains;
- dependency versions and shading.

With ten domain plugins, the desired shape is still:

```text
1 Bridge runtime
10 logical producer integrations
```

not:

```text
10 gRPC stacks
10 reconnect executors
10 health loops
10 independently shaded protocol runtimes
```

The Bridge reduces JVM overhead and centralizes correct Paper threading behavior without centralizing domain rules.

---

# 5. Why adapters stay inside each domain plugin

The adapter must understand domain language.

Examples:

```text
publishBlockBroken(...)
publishTransferRequested(...)
publishClaimOwnershipChanged(...)
getEconomyAccount(...)
watchPremiumOwnership(...)
```

The Bridge must not understand those meanings.

Keeping the adapter inside the plugin provides:

- domain code and domain integration versioned together;
- direct access to domain types without exposing them to the Bridge;
- smaller deployment surface;
- fewer Paper plugins;
- simpler startup order;
- easier tests;
- no domain-specific branching in infrastructure code.

---

# 6. Service registration and startup lifecycle

## 6.1 Bridge startup

Conceptual sequence:

```text
CraftRelayPaperBridge.onLoad
→ load non-secret static configuration
→ initialize bounded runtime structures

CraftRelayPaperBridge.onEnable
→ load credentials through approved secret mechanism
→ validate IntegrationManifests
→ create Agent and Query Service channel runtimes
→ perform bounded handshake/readiness checks
→ register CraftRelayService
→ expose health
```

V1 readiness decision:

```text
invalid Bridge config or invalid IntegrationManifest
→ Bridge fails enable

valid config but Agent/Query sidecar temporarily unavailable
→ Bridge enables
→ registers CraftRelayService as NOT_READY
→ starts bounded reconnect
```

The API must make readiness explicit. Dependent plugins may obtain the service while it is `NOT_READY`, but they must not enable authoritative features until their required integration reaches `READY`. A domain plugin may self-disable after a finite configured deadline if its policy requires strict startup.

## 6.2 Domain plugin startup

First-party `plugin.yml` concept:

```yaml
name: CraftMMOMining
depend:
  - CraftRelayPaperBridge
```

Conceptual Java:

```java
RegisteredServiceProvider<CraftRelayService> registration =
        getServer().getServicesManager()
                .getRegistration(CraftRelayService.class);

if (registration == null) {
    throw new IllegalStateException(
            "CraftRelayPaperBridge service is unavailable"
    );
}

CraftRelayService service = registration.getProvider();
CraftRelayDomainClient domainClient = service.clientFor(this);
MiningCraftRelayAdapter adapter =
        new MiningCraftRelayAdapter(this, domainClient);
```

The plugin must not fall back silently to direct SQL or local ad-hoc storage.

## 6.3 Disable and reload

On domain plugin disable:

- stop creating new operations;
- detach callbacks/watches owned by that plugin;
- prevent callbacks from mutating a disabled plugin;
- preserve durable event IDs for later status lookup;
- do not block the Paper thread waiting indefinitely.

On Bridge disable:

- stop new admission;
- perform only bounded drain/detach;
- close channels and executors;
- unregister the service;
- never emit false cancellation or false durability success.

Hot reload is not assumed safe. Production upgrades should use full server restart unless an accepted ADR proves reload behavior.

---

# 7. IntegrationManifest

Every first-party integration has a versioned manifest.

Conceptual form:

```yaml
integration:
  id: craftmmo-mining
  version: 1
  paperPluginId: CraftMMOMining
  producerId: craftmmo-mining
  protocolMin: 1
  protocolMax: 1

events:
  - type: mining.block_mutation
    schemaVersion: 1
    streamKeyStrategy: PLAYER_ID
    requestedOperationClass: GAMEPLAY_ACTIVITY
    eventClass: AGGREGATED_GAMEPLAY_ACTIVITY
    requestedFlush:
      maxDelayMillis: 1000
      maxRecords: 256
      maxBytes: 65536
    expectedProjectionPolicy: MINING_V1

queries:
  - id: mining.get_player_stats
    definitionVersion: 1
    consistency: STRICT_LATEST_COMMITTED

clientArtifact:
  id: craftrelay-mining-client-java
  version: 1
```

The manifest is not allowed to weaken policy. It declares compatibility and intent; the Agent resolves the effective policy.

Required fields:

```text
integration_id
integration_version
paper_plugin_id
producer_id
protocol compatibility
accepted event schemas
accepted query definitions
stream-key strategy identifiers
event class identifiers
requested flush criteria where the integration declares them
generated client artifact/version
manifest checksum
optional signature
```

Validation failures are explicit and fail closed.

---

# 8. Batching and plugin flush criteria

## 8.1 Purpose

A plugin may declare how long it is willing to wait before CraftRelay flushes work, but the plugin does not directly control effective global policy.

The plugin-facing model is:

```text
flush when:
records >= maxRecords
OR bytes >= maxBytes
OR age >= maxDelayMillis
```

The first condition reached triggers a flush. The conditions are OR, not AND.

## 8.2 Requested versus effective criteria

The plugin or generated client declares requested criteria. CraftRelay computes effective criteria.

```text
plugin config / generated client defaults
→ IntegrationManifest event class
→ CraftRelay policy registry
→ effective flush criteria
```

The plugin can express:

```text
CraftMMO Activity accepts up to 1s batching
Economy accepts only up to 5ms batching
```

CraftRelay validates whether those values are allowed for that event type.

## 8.3 Example: Activity accepts 1 second

Plugin-facing config:

```yaml
craftmmo-activity:
  batching:
    requestedFlush:
      maxDelayMillis: 1000
      maxRecords: 256
      maxBytes: 65536
```

Meaning:

```text
flush after 1s
OR 256 records
OR 64KB
whichever comes first
```

If 256 records arrive in 80ms, CraftRelay flushes at 80ms. If only 10 records arrive, CraftRelay flushes at 1s.

This is suitable for aggregate activity such as mining/farming/fishing/stat deltas when the event contract allows semantic coalescing.

## 8.4 Example: Economy accepts only 5ms

Plugin-facing config:

```yaml
craftmmo-economy:
  batching:
    requestedFlush:
      maxDelayMillis: 5
      maxRecords: 16
      maxBytes: 16384
```

Meaning:

```text
flush after 5ms
OR 16 records
OR 16KB
whichever comes first
```

Economy can use physical batching, but not semantic coalescing. Sixteen transfers may share one physical database transaction, but they remain sixteen independent transfers with individual event IDs, ledger rows, audit evidence, fingerprints, lifecycle states, and deduplication.

## 8.5 Physical batching versus semantic coalescing

Physical batching preserves event identity.

```text
Transfer A
Transfer B
Transfer C
→ one Projector transaction
→ three ledger entries
```

Semantic coalescing changes the event meaning.

```text
BlockBreak
BlockBreak
BlockBreak
→ one MiningActivityDelta
```

Semantic coalescing is good for activity/stat deltas and bad for ledgers, ownership, premium, unique items, permissions, and security actions.

## 8.6 What the plugin may configure

The plugin may configure requested values within its allowed class:

```yaml
batching:
  requestedFlush:
    maxDelayMillis: 250
    maxRecords: 128
    maxBytes: 32768
```

It may not configure:

```text
producer_id as an arbitrary string
Kafka topic
replication factor
retention class
Projector policy
effective priority/global reserved capacity
P0 status
semantic coalescing for a forbidden event class
```

## 8.7 IntegrationManifest event class

The generated client or manifest declares the event class:

```yaml
events:
  - type: economy.transfer_command
    schemaVersion: 1
    eventClass: LOW_LATENCY_LEDGER
    requestedFlush:
      maxDelayMillis: 5
      maxRecords: 16
      maxBytes: 16384

  - type: activity.player_action_delta
    schemaVersion: 1
    eventClass: AGGREGATED_GAMEPLAY_ACTIVITY
    requestedFlush:
      maxDelayMillis: 1000
      maxRecords: 256
      maxBytes: 65536
```

The Agent/Projector policy registry defines the ceilings, reserved-capacity class, and whether semantic coalescing is allowed.

## 8.8 Policy failures

If a plugin requests a value outside its allowed policy, CraftRelay either rejects startup/registration or clamps with diagnostics, depending on deployment mode.

Production critical deployments should prefer:

```text
STRICT_POLICY
```

Development may use:

```text
CLAMP_WITH_WARNING
```

Examples:

```text
Mining requests maxDelayMillis = 1
but AGGREGATED_GAMEPLAY_ACTIVITY is not allowed to consume low-latency reserved capacity
→ reject or clamp
```

```text
Economy requests semanticCoalescing = true
but LOW_LATENCY_LEDGER forbids semantic coalescing
→ reject
```

## 8.9 Plugin developer rule

Use requested flush criteria to describe latency tolerance:

```text
5ms for money/ownership/premium confirmation paths
50ms–250ms for normal player progression
250ms–1000ms for activity/stat deltas
1s+ only for background analytics or explicitly stale views
```

Do not use flush criteria to bypass CraftRelay policy. The effective policy is always computed by CraftRelay.

## 8.10 Interactive flush is optional, not a render path

A command or UI action must not automatically force CraftRelay to flush every pending batch. Interactive flush is a specific feature contract used only when the command requires projected exactness.

Rules:

- default scope is the relevant integration plus event class accumulator;
- it never scans Kafka payloads;
- it never filters Kafka records by player/entity;
- it never writes directly to PostgreSQL;
- it never flushes all producers globally;
- entity or shard-scoped accumulators are future optimizations and require explicit tests;
- timeout or overload returns a domain-facing non-current/degraded result rather than stale-as-current data.

Normal statistics commands should usually use a projected snapshot, explicit stale mode, or local pending overlay instead of forcing an interactive flush.

---

# 9. Producer identity and security boundary

## 8.1 Logical producer isolation

Even with one shared transport runtime, the Agent sees separate logical producers:

```text
craftmmo-mining
craftmmo-economy
craftmmo-claims
craftmmo-premium
```

This preserves:

- ACLs;
- quotas;
- priority ceilings;
- namespaces;
- metrics;
- diagnostics;
- audit trails.

## 8.2 clientFor(Plugin)

The service resolves the client from:

```text
Plugin instance supplied to clientFor(plugin)
→ canonical paper_plugin_id
→ validated IntegrationManifest
→ logical producer registration
```

It must not expose:

```java
clientFor("craftmmo-economy")
```

to arbitrary plugins.

This prevents accidental string-based producer selection. It is not a same-JVM security proof against malicious plugins with arbitrary object references. The Agent trusts only the authenticated logical producer session established by the Bridge-held credential or delegated identity.

## 8.3 Trust limitation

Paper plugins share one JVM and can potentially inspect or interfere with each other.

Therefore:

> `CraftRelayPaperBridge` logical producer separation is strong against mistakes and unauthorized API use, but is not a sandbox against a malicious plugin in the same JVM.

Critical CraftRelay producers must share the JVM only with trusted plugins. Tests must not claim that one malicious same-JVM plugin can never obtain another `Plugin` reference; tests must instead prove that the public API exposes no arbitrary `producer_id` selector and that the Agent authenticates logical sessions.

---

# 10. Write API flow

Example mining adapter:

```java
public final class MiningCraftRelayAdapter {
    private final Plugin plugin;
    private final CraftRelayDomainClient relay;

    public MiningCraftRelayAdapter(
            Plugin plugin,
            CraftRelayDomainClient relay
    ) {
        this.plugin = plugin;
        this.relay = relay;
    }

    public PublishHandle publishBlockBroken(
            UUID eventId,
            UUID playerId,
            UUID worldId,
            int x,
            int y,
            int z,
            int blockTypeId
    ) {
        BlockMutation payload = BlockMutation.newBuilder()
                .setPlayerId(Uuids.toBytes(playerId))
                .setWorldId(Uuids.toBytes(worldId))
                .setX(x)
                .setY(y)
                .setZ(z)
                .setBlockTypeId(blockTypeId)
                .setAction(BlockAction.BREAK)
                .build();

        return relay.submit(
                TypedPublishRequest.builder(MiningEvents.BLOCK_MUTATION_V1)
                        .eventId(eventId)
                        .streamKey(playerId.toString())
                        .payload(payload)
                        .build()
        );
    }
}
```

The AdapterClass knows the typed event contract. The Agent still decides:

- physical topic;
- effective durability;
- Kafka profile;
- retention;
- priority;
- quota;
- projector set;
- routing version.

## 9.1 Non-critical gameplay activity

A mining activity event may be submitted asynchronously without blocking the Paper gameplay thread.

The plugin may update non-authoritative visual feedback immediately only if the domain policy permits it. Authoritative effects still follow the accepted durability policy.

## 9.2 P0 durable-before-effect

Economy/premium/ownership flow:

```text
construct immutable command
→ submit through adapter
→ await required durability asynchronously
→ optionally await projection token
→ apply/confirm gameplay effect on Paper scheduler
```

No `.join()` or blocking wait on the gameplay thread.

## 9.3 Retry rule

A retry reuses:

- the same `event_id`;
- the same immutable request;
- the same payload;
- the same operation identity.

A transport error does not authorize generation of a new event ID.

## 9.4 Paper-side effects are not exactly-once by default

CraftRelay guarantees event durability and projected-state consistency. It does not by itself guarantee exactly-once execution of a Paper-side callback.

Failure examples:

```text
Agent confirms durability
→ Paper crashes before callback applies effect
```

```text
callback applies effect
→ Paper crashes before local completion is remembered
→ restart/retry may apply again
```

Critical domains must choose one of these patterns:

```text
Projected-state authority
→ query projection/token and update UI/cache from projected truth

Idempotent effect
→ use event_id/operation_id as a fence

Startup reconciliation
→ query projected pending/completed operations after restart

Desired-state command
→ event describes final desired state, not a transient callback
```

The adapter may provide helpers, but the domain owns idempotency/reconciliation semantics.

---

# 11. Read API flow

Example typed read:

```java
public CompletionStage<PlayerMiningStats> getPlayerStats(
        UUID playerId,
        Duration timeout
) {
    return relay.query(
            GetPlayerMiningStatsRequest.newBuilder()
                    .setPlayerId(Uuids.toBytes(playerId))
                    .setConsistency(
                            QueryConsistency.STRICT_LATEST_COMMITTED
                    )
                    .setTimeoutMillis(timeout.toMillis())
                    .build()
    );
}
```

The plugin does not send SQL, table names, column names, joins, or arbitrary predicates.

## 10.1 Strict initial fetch

Critical display flow:

```text
adapter strict query
→ Bridge Query channel
→ Query Service barrier/checkpoint validation
→ PostgreSQL primary snapshot
→ typed response
→ adapter maps response
→ plugin displays value
```

## 10.2 Read-your-write

```text
publish event
→ await required durability
→ awaitProjectionToken(eventId, projectorId, projectionId, timeout)
→ AT_LEAST_TOKEN query
→ response includes own projected write
```

For multi-projection reads, the adapter supplies a bounded list of per-projector/per-projection tokens required by the QueryDefinition. This is normally more efficient than capturing an unnecessary global barrier.

## 10.3 Watches

```text
strict initial fetch
→ bounded typed watch
→ monotonic updates
```

On detach, incompatible vector, checksum conflict, or disconnect:

- strict refetch;
- close the view;
- or mark it visibly not current.

The adapter must not keep showing an old authoritative value as current.

---

# 12. Threading model

## 11.1 Allowed on Paper gameplay thread

Only bounded small work:

- validate primitive arguments;
- allocate/accept a caller-provided event ID;
- create a small immutable request;
- reserve bounded queue capacity;
- submit asynchronously.

## 11.2 Forbidden on gameplay thread

- socket waits;
- disk IO;
- Kafka/PostgreSQL calls;
- `Future.get()`;
- `CompletionStage.join()`;
- retry loops;
- large compression/serialization;
- shutdown drain;
- unbounded lock waits.

## 11.3 Scheduler handoff

A completion callback runs on a client/runtime executor unless explicitly documented otherwise.

Any callback that changes Paper state must reschedule to the correct Paper scheduler/thread/region.

Conceptual helper:

```java
relayResult.thenAccept(result ->
        plugin.getServer().getScheduler().runTask(
                plugin,
                () -> applyResult(result)
        )
);
```

Folia/region-thread compatibility requires a dedicated accepted execution-context adapter and tests; generic main-thread assumptions are insufficient.

The API must support an execution context model such as:

```text
PaperExecutionContext
- GLOBAL_SERVER
- ENTITY
- REGION
- ASYNC_ONLY
```

A helper must refuse unsafe handoff when the required entity/region context is missing.

---

# 13. Bounded resources

Bridge configuration must bound:

```text
max_pending_publish_submissions
max_pending_query_requests
max_active_publish_handles
max_projection_token_waiters
max_active_snapshot_watches
max_watch_buffer_events
max_watch_buffer_bytes
max_callbacks_per_plugin
max_serialization_bytes_on_caller_thread
max_logical_producer_streams
max_inflight_per_producer
per_producer_publish_queue_bytes
per_producer_query_queue_bytes
reserved_p0_publish_slots
reserved_p0_callback_capacity
callback_dispatch_queue_limit
slow_callback_threshold
tracking_ttl
query_timeout_ceiling
shutdown_drain_timeout
reconnect_backoff_min/max
```

Per-producer limits and reserved P0 capacity must prevent one plugin, such as Mining, from exhausting resources needed by Economy, Premium, Claims, or other critical producers.

When a local tracking limit is reached:

- durable work already accepted by the Agent remains durable;
- tracking may return `TRACKING_DETACHED`;
- status lookup remains the recovery path;
- no unbounded queue is created.

---

# 14. Connection model

Recommended v1:

```text
one shared ManagedChannel/runtime to Agent endpoint
one shared ManagedChannel/runtime to Query Service endpoint
multiple logical producer sessions/streams
```

The exact number of underlying HTTP/2 connections is an implementation detail and may scale within strict bounds.

Required properties:

- no channel creation per request;
- no channel creation per event;
- no separate full transport stack per domain plugin;
- producer identity preserved per request/session;
- Agent and Query channels fail independently;
- reconnect uses bounded exponential backoff with jitter;
- health state is explicit;
- connection failures never imply request rejection automatically.

## 13.1 Logical producer streams and sequence ownership

V1 uses:

```text
one shared ManagedChannel/runtime to Agent
+ one bounded authenticated bidirectional publish stream per logical producer
```

For each logical producer runtime:

```text
producer_instance_id
→ generated once per Bridge process lifetime and logical producer

producer_operation_sequence
→ allocated monotonically by that logical producer runtime
```

Rules:

- reconnect within the same Bridge process keeps the same `producer_instance_id`;
- full Paper/Bridge restart creates a new `producer_instance_id`;
- each producer has a separate sequence allocator;
- retry of a still-tracked event reuses the original event ID, immutable request, and sequence;
- after tracking eviction, status lookup is attempted first;
- if the old sequence cannot be safely reused, retry relies on the same `event_id` under the current producer instance and Agent deduplication;
- one stream must not mix multiple producers unless an ADR defines authenticated per-message binding.

---

# 15. Failure behavior

## 14.1 Bridge unavailable during plugin startup

- hard-dependent first-party integration does not enable authoritative behavior;
- no direct DB/Kafka fallback;
- explicit diagnostic.

## 14.2 Agent unavailable

- Bridge does not claim local/replicated durability;
- admission obeys configured bounded behavior;
- transport ambiguity preserves event ID;
- reads may remain available through Query Service.

## 14.3 Query Service unavailable

- writes may remain available;
- strict queries fail explicitly;
- adapter does not silently use stale memory as current.

## 14.4 Plugin disabled while operation pending

- callback ownership is cancelled/detached locally;
- already accepted durable work remains in CraftRelay;
- event ID remains usable for later status lookup;
- no callback mutates disabled plugin state.

## 14.5 Bridge restart after Agent acceptance

- plugin retries same event ID or performs status lookup;
- Agent deduplicates the publish request;
- duplicate Paper-side gameplay effects are avoided only when the domain uses idempotent effects, projected-state authority, startup reconciliation, or desired-state commands.

## 14.6 Integration registration conflict

- no producer identity is guessed;
- affected integration fails closed;
- evidence and diagnostics are preserved.

---

# 16. Module layout

Recommended repository layout:

```text
java/
├─ craftrelay-client-core/
├─ craftrelay-paper-api/
├─ craftrelay-paper-bridge/
├─ craftrelay-java-integration-tests/
└─ integrations/
   ├─ craftrelay-reference-domain-client/
   └─ craftrelay-reference-paper-fixture/
```

CraftMMO domain repositories may contain:

```text
MiningPlugin/
├─ src/main/java/.../MiningCraftRelayAdapter.java
└─ dependency: craftrelay-mining-client-java

EconomyPlugin/
├─ src/main/java/.../EconomyCraftRelayAdapter.java
└─ dependency: craftrelay-economy-client-java
```

Generated domain clients may live in the CraftRelay monorepo or in domain repositories, but schema/version ownership must be explicit.

---

# 17. Dependency and classloader rules

- domain plugins depend on `craftrelay-paper-api` as provided/compileOnly;
- Bridge owns the implementation and gRPC runtime;
- domain generated message/client artifacts must avoid conflicting runtime duplication;
- shading/relocation strategy is verified on the supported Paper classloader model;
- no plugin bundles another `CraftRelayPaperBridge` implementation;
- no API type crosses plugin boundaries if its classloader identity is unstable;
- protocol/generated classes exposed through the public API require one authoritative loading strategy;
- compatibility is tested with multiple domain plugins enabled simultaneously.

V1 decision:

```text
craftrelay-paper-api
→ only API-owned interfaces, records, enums, handles, immutable value objects, and JDK types

domain Protobuf/generated domain DTOs
→ plugin-local to the owning domain plugin

gRPC / Netty / Protobuf transport runtime
→ Bridge-owned only
```

The public API boundary uses capability-scoped handles issued by the Bridge after IntegrationManifest validation:

```text
EventContractHandle
+ bounded immutable serialized payload

QueryContractHandle
+ bounded immutable serialized parameters
```

These bytes are not arbitrary untyped bytes. They are accepted only when bound to a registered contract handle, schema version, integration, producer capability, and size limit. The plugin cannot invent schema IDs, query IDs, producer IDs, Kafka topics, retention classes, or SQL.

No generated domain Protobuf type, gRPC stub, Netty class, or shaded transport class may appear in the public `craftrelay-paper-api`.

---

# 18. Domain integration examples

## 17.1 Mining

Adapter methods:

```text
publishBlockBroken
publishBlockPlaced
getPlayerMiningStats
watchPlayerMiningStats
```

Projector behavior:

- update current counters;
- update hourly/daily aggregates;
- optionally update chunk snapshot/activity;
- do not store every block action forever in PostgreSQL hot tables unless policy requires it.

## 17.2 Economy

Adapter methods:

```text
requestTransfer
getAccount
awaitTransferProjectionToken
watchAccount
```

Projector behavior:

- validate expected versions;
- update accounts atomically;
- insert permanent ledger entry;
- issue mutation references/token material.

## 17.3 Claims

Adapter methods:

```text
changeOwnership
getClaimSnapshot
watchClaim
```

Projector behavior:

- update current ownership;
- keep required audit history;
- enforce domain version.

---

# 19. Generated client policy

A generated domain client should expose methods such as:

```java
MiningRelay mining = domainClients.mining();

PublishHandle handle = mining.publishBlockBreak(...);
CompletionStage<PlayerMiningStats> stats =
        mining.getPlayerStats(...);
```

Generated code may contain:

- canonical event/query IDs;
- schema versions;
- Protobuf builders/parsers;
- request validation;
- typed result mapping.

Generated code must not contain:

- Kafka topic names as authority;
- effective durability/retention decisions;
- SQL;
- network channel creation;
- retries with new IDs;
- Paper gameplay rules.

---

# 20. Configuration

Bridge configuration contains transport/runtime settings, integration registrations, and optional requested flush preferences. It does not define effective durability, retention, Kafka, projection, reserved P0 capacity, or final event policy.

Conceptual:

```yaml
agent:
  address: 127.0.0.1:7401

queryService:
  address: 127.0.0.1:7402

runtime:
  maxPendingPublishes: 4096
  maxPendingQueries: 1024
  maxActiveWatches: 2048
  shutdownDrainMillis: 3000

batchingDefaults:
  policyMode: STRICT_POLICY

integrations:
  CraftMMOMining:
    manifest: integrations/craftmmo-mining.yaml
    credentialRef: secrets/craftmmo-mining
    requestedFlushOverrides:
      activity.player_action_delta:
        maxDelayMillis: 1000
        maxRecords: 256
        maxBytes: 65536

  CraftMMOEconomy:
    manifest: integrations/craftmmo-economy.yaml
    credentialRef: secrets/craftmmo-economy
    requestedFlushOverrides:
      economy.transfer_command:
        maxDelayMillis: 5
        maxRecords: 16
        maxBytes: 16384
```

Secrets must not be committed to plugin configuration or audit bundles.

The Agent policy registry remains authoritative for:

- topic;
- RF/minISR profile;
- durability;
- retention;
- quota;
- priority;
- routing;
- projector set;
- effective flush criteria and policy ceilings;
- whether semantic coalescing is allowed.

---

# 21. Testing requirements

## 21.1 Unit

- AdapterClass request construction;
- canonical stream key;
- stable event ID reuse;
- error mapping;
- manifest parsing/checksum;
- producer mapping;
- bounded local admission;
- requested flush criteria parsing;
- OR flush semantics;
- effective policy clamp/rejection behavior;
- physical batching versus semantic coalescing restrictions;
- interactive flush scope/timeout/degraded-result behavior;
- XP/progress UI local-first cache and reconciliation helpers where provided;
- callback ownership after plugin disable.

## 20.2 Paper integration

- Bridge service registers before dependent plugin enable;
- service unregisters on disable;
- missing Bridge dependency is explicit;
- multiple first-party plugins resolve distinct clients;
- public API exposes no arbitrary producer string selector;
- same-JVM malicious plugin impersonation remains outside the threat model;
- classloader compatibility;
- no duplicate gRPC runtime per plugin;
- callbacks reschedule through the accepted Paper/Folia execution context abstraction;
- no gameplay-thread blocking;
- no Query Service fetch per tick/action for XP bars, scoreboards, or transient UI;
- bounded shutdown.

## 20.3 Sidecar integration

- one shared channel/runtime carries multiple producer sessions;
- per-producer ACL/quota isolation;
- Agent reconnect with same event ID;
- Query Service failure independent from Agent;
- strict query and token flow through Bridge;
- watch detach/refetch behavior.

## 20.4 Failure injection

- kill Bridge before Agent acceptance;
- kill Bridge after Agent acceptance before callback;
- disconnect Agent only;
- disconnect Query Service only;
- disable domain plugin while publish/query/watch is pending;
- invalid IntegrationManifest;
- incompatible generated client version;
- duplicate integration registration;
- full Paper restart with pending Agent journal work.

---

# 22. Performance expectations

For 10–100 players and hundreds of events per second, one shared Bridge should be substantially cheaper than one transport stack per plugin.

Measure:

- Bridge CPU/RAM;
- shared channel count;
- event-loop/thread count;
- publish submissions/s;
- query requests/s;
- active watches;
- per-producer queue depth;
- serialization time;
- callback scheduling latency;
- reconnect time;
- JVM allocation rate;
- GC impact;
- Paper tick/region-thread impact.

Mandatory fast-path properties:

- no client/channel creation per operation;
- bounded request construction;
- shared persistent channels;
- no polling every tick;
- one watch can fan out locally to multiple consumers only when semantics and ownership are identical and fan-out is bounded;
- large serialization/compression off-thread;
- domain events remain compact.

---

# 23. Anti-patterns

Forbidden first-party designs:

```text
MiningPlugin → own gRPC channel → Agent
EconomyPlugin → own gRPC channel → Agent
ClaimsPlugin → own gRPC channel → Agent
```

```text
CraftRelayPaperBridge
if event == mining ...
if event == economy ...
if event == claims ...
```

```text
plugin supplies producer_id string
plugin supplies Kafka topic
plugin supplies SQL
plugin chooses retention
plugin chooses effective flush policy/global priority/P0 capacity
plugin enables semantic coalescing for ledger/ownership/premium events
plugin uses Query Service as a per-tick XP bar/progress render loop
plugin forces global flush for normal stats/display commands
plugin expects Kafka to filter batch payloads by player/entity
plugin retries using a new event_id
```

```text
MiningPlugin
MiningAdapterPlugin
EconomyPlugin
EconomyAdapterPlugin
```

Correct design:

```text
shared Bridge infrastructure
+ embedded typed domain adapters
+ external generic sidecars
+ domain projector/query modules
```

---

# 24. Rollout plan

1. Create `craftrelay-paper-api`.
2. Create a non-functional `craftrelay-paper-bridge` skeleton.
3. Define and validate `IntegrationManifest`.
4. Create one reference domain client and Paper fixture plugin.
5. Prove service registration/classloader/threading behavior.
6. Connect Bridge to fake Agent and fake Query Service.
7. Implement bounded publish/status/query/token/watch APIs.
8. Integrate one real plugin in shadow mode.
9. Compare existing persistence with CraftRelay projections.
10. Test Bridge restart and full Paper restart.
11. Enable authoritative use only after audit approval.

---

# 25. Definition of Done

The Paper integration is complete only when:

1. exactly one `CraftRelayPaperBridge` owns sidecar transport resources per Paper server;
2. `craftrelay-paper-api` is stable and minimal;
3. first-party domain plugins use embedded AdapterClasses/generated clients;
4. first-party adapter plugins do not exist;
5. third-party adapter plugins require an ADR;
6. logical producers remain separately authenticated/authorized/accounted;
7. producer identity cannot be chosen by an arbitrary plugin string; `clientFor(Plugin)` is not claimed as a malicious same-JVM security boundary;
8. Bridge queues/tracking/watches are bounded;
9. Bridge contains no domain, Kafka, PostgreSQL, or journal logic;
10. domain plugins contain no Kafka, PostgreSQL, journal, or independent sidecar runtime;
11. all APIs are asynchronous;
12. Paper gameplay threads never block on CraftRelay;
13. callbacks mutate Paper only through the accepted Paper/Folia execution context;
14. IntegrationManifest compatibility is validated before use;
15. Agent and Query Service failures are isolated;
16. reconnect preserves event identity and publish idempotency; Paper-side effect idempotency is domain-defined;
17. plugin/Bridge disable behavior is bounded and tested;
18. multiple domain plugins pass classloader and shared-runtime tests;
19. one real plugin passes shadow mode;
20. independent audit approves the integration;
21. requested flush criteria are validated against CraftRelay effective policy;
22. flush criteria use OR semantics across records, bytes, and time;
23. physical batching preserves event identity and auditability;
24. semantic coalescing is available only for explicit aggregate event contracts;
25. semantic coalescing is forbidden for P0 ledger/ownership/premium/unique-item/security events;
26. noisy producers cannot consume reserved P0 capacity through aggressive requested flush settings;
27. interactive flush is optional, scoped, bounded, and never scans Kafka payloads;
28. XP/progress bars use local-first feedback with async deltas and projected-state reconciliation instead of Query Service fetches per tick/action.

---

# 26. Final architecture statement

> CraftRelay Core remains external and generic. `CraftRelayPaperBridge` is one thin shared Paper integration plugin. Each first-party domain plugin contains its own typed AdapterClass or generated domain client. The Bridge owns transport and lifecycle; the Adapter owns domain translation; the Agent/Projector/Query Service own durability, projection, and authoritative reads.
