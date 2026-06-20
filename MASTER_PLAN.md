# CraftRelay — MASTER_PLAN.md

**Status:** AUTHORITATIVE DRAFT — ZERO-LOSS, STRICT-FRESHNESS, AND PAPER-BRIDGE PROFILE  
**Target:** CraftRelay v1  
**Document role:** Architecture, protocol, Paper integration, implementation, testing, audit, and release source of truth  
**Companion document:** `PluginAdapter.md`  
**Supporting guide:** `PluginUsage.md` — practical, non-authoritative usage guide for first-party plugins
**Supersedes:** Every earlier CraftRelay master plan  
**Primary objective:** RPO=0 for every confirmed authoritative event and no stale authoritative data presented as current within the documented deployment/failure model  
**Language policy:** Documentation may be written in Portuguese. Source code, identifiers, protocol names, test names, Codex prompts, and commit messages must be written in English.

---

## 0. Authority and execution gate

This document is the authoritative plan for CraftRelay.

`PluginAdapter.md` is the authoritative companion for Paper-side integration. `PluginUsage.md` is a practical supporting guide for first-party plugin developers. If `PluginUsage.md` conflicts with this plan or with `PluginAdapter.md`, this plan and `PluginAdapter.md` win.

CraftRelay is a reusable durability, transport, projection, and query platform for trusted Minecraft/Paper plugins and companion services developed by the same team. CraftRelay is **not** a Paper plugin and does **not** run inside the Minecraft server JVM.

The priority order for CraftRelay v1 is:

```text
durability and integrity
> availability
> latency
> convenience
```

The system must fail closed. If the required durability cannot be proven, CraftRelay must not report success.

No functional persistence code may begin until:

1. this plan is reviewed;
2. the Sprint 0 foundational ADRs exist and are marked `ACCEPTED`;
3. exact runtime, library, broker, database, and audit-tool versions are verified from official primary sources;
4. the minimal Rust workspace and Java reactor build cleanly;
5. the identity, ownership, lifecycle, payload, fingerprint, compatibility, Kafka profile, ACK, retention, strict-query freshness, and audit contracts are accepted;
6. the Sprint 0 audit scripts are functional, not placeholders;
7. the first audit bundle can be generated even when the repository has no first commit yet;
8. strict read barriers, read-your-write tokens, and bounded snapshot-watch semantics are represented in the protocol skeleton;
9. the `CraftRelayPaperBridge`, `craftrelay-paper-api`, producer-registration, `IntegrationManifest`, classloader, Paper/Folia threading, readiness, logical producer-session, callback, and typed AdapterClass/generated-client contracts are accepted;
10. `PluginAdapter.md` exists, is reviewed, and is consistent with this plan;
11. `PluginUsage.md` exists as a practical supporting guide and does not contradict this plan or `PluginAdapter.md`.

Changes to any of the following require an ADR:

- process boundaries;
- threat model;
- RPO=0 durability wording;
- fail-closed behavior;
- event identity;
- installation scoping;
- payload storage and retention;
- immutable envelope;
- mutable delivery/projection/retention states;
- lifecycle revisions;
- projection ACK transport;
- Kafka durability and retention profiles;
- PostgreSQL durability profile;
- ownership authority;
- stream identity;
- sequence allocation;
- fingerprint canonicalization;
- protocol compatibility;
- Java client tracking limits;
- query consistency and freshness barriers;
- projection consistency tokens;
- snapshot-watch behavior;
- audit workflow;
- atomicity guarantees;
- Paper Bridge lifecycle and service-registration contract;
- logical producer mapping inside the Paper JVM;
- integration manifest and domain-adapter contract;
- classloader/API boundary;
- Paper/Folia threading, callback, and dependency boundaries;
- producer instance/sequence ownership inside the Bridge.

The first implementation milestone must prove one complete vertical slice:

```text
first-party Paper plugin
→ embedded AdapterClass/generated domain client
→ CraftRelayPaperBridge
→ Rust Agent
→ durable local journal
→ Kafka RF=5/minISR=5 production profile
→ idempotent Rust Projector
→ PostgreSQL projection and immutable event archive
→ transactional ACK outbox
→ Agent ACK consumption
→ minimal Query Service
→ crash/restart recovery
```

The project must validate this path before building advanced fairness, broad administration tooling, Kubernetes deployment, complex repair tools, dynamic ownership, online routing migration, or a large generic projector ecosystem.

---

# 1. Mission

CraftRelay must allow trusted producers to publish authoritative events without every plugin implementing its own:

- write-ahead log;
- Kafka client;
- local durability barrier;
- PostgreSQL pool;
- retry scheduler;
- deduplication layer;
- backpressure;
- disk quota guard;
- crash recovery;
- projection checkpointing;
- projection ACK tracking;
- status tracking;
- observability;
- audit tooling;
- duplicated gRPC channels, reconnect loops, sidecar thread pools, and health checks in every Paper plugin;
- direct coupling between domain plugins and sidecar transport details.

CraftRelay must:

- keep blocking storage and network work outside Paper gameplay threads;
- provide explicit durability states;
- prioritize RPO=0 for confirmed authoritative events;
- model accepted-but-pending and accepted-but-blocked states explicitly;
- model ambiguous client transport results honestly;
- support idempotent retry using the same `event_id`;
- preserve ordering inside the supported ownership model;
- keep every operational structure bounded;
- separate write, projection, and query failure domains;
- isolate producers by installation, namespace, quota, priority, and policy;
- preserve evidence for conflicts, blocked work, and repair actions;
- serve authoritative reads through explicit freshness barriers;
- never present stale authoritative data as current;
- expose one shared, bounded, asynchronous Paper integration service per server;
- keep domain-specific event/query construction in typed AdapterClasses or generated domain clients;
- never silently discard a confirmed authoritative event.

---

# 2. Non-goals

CraftRelay v1 is not:

- the CraftRelay Core as a Paper plugin; `CraftRelayPaperBridge` is the only first-party in-process Paper integration component;
- a replacement for Kafka;
- a replacement for PostgreSQL;
- a new distributed broker;
- a consensus system;
- a dynamic owner-election system;
- a distributed transaction coordinator;
- a global sequencer;
- a generic SQL proxy;
- a shared ORM;
- a global exactly-once system;
- a sandbox against malicious plugins in the same JVM;
- a multi-datacenter consensus platform;
- a guarantee against simultaneous destruction of every durable copy;
- a generic gameplay rule engine;
- a guarantee of atomicity across independent publish calls;
- a best-effort telemetry service;
- an excuse for unlimited memory, disk, or audit growth;
- one custom CraftRelay Core deployment per plugin;
- one first-party adapter plugin per domain plugin;
- a place for domain logic inside `CraftRelayPaperBridge`;
- permission for domain plugins to create their own Kafka, PostgreSQL, journal, or sidecar transport clients.

`BEST_EFFORT` is excluded from the durable Publish API v1. A future telemetry API may be designed separately and must never return a `DurableReceipt`.

---

# 3. Zero-loss durability philosophy

## 3.1 RPO=0 target

CraftRelay targets:

> RPO=0 for every event whose effective required durability has been confirmed, within the documented storage, replication, retention, and operational threat model.

This means:

- confirmed authoritative events must remain recoverable after any failure covered by the deployment profile;
- unconfirmed events may be retried, rejected, or remain pending;
- CraftRelay must not claim success when the configured durability cannot be proven.

## 3.2 Fail closed

When durability cannot be proven:

```text
do not confirm success
do not weaken the policy
do not silently discard the event
do not delete the local payload
do not apply the irreversible gameplay effect
```

Failures may cause pending publishes, blocked publishes, degraded health, operator alerts, unavailable writes, or delayed gameplay effects. That is acceptable. Confirmed data loss is not.

## 3.3 Local WAL versus confirmed durability

For critical operations, local WAL is not the final success condition.

```text
local journal fsync only
→ accepted/staged/pending
→ not enough for P0 success
```

```text
local journal fsync
+ Kafka RF=5/minISR=5/acks=all confirmation
+ local persistence of Kafka confirmation
→ replicated durability reached
```

For P0 operations, the plugin must use `DURABLE_BEFORE_EFFECT`: the gameplay effect is applied only after the required durability result is reached.

## 3.4 Physical limits

No real system can guarantee zero loss after simultaneous destruction of:

```text
all Kafka brokers
+ all Agent journals
+ all PostgreSQL replicas
+ all backups/archives
+ all recovery metadata
```

CraftRelay’s promise is therefore strict but bounded:

> zero confirmed-event loss inside the declared failure model; fail closed outside it.

---

# 4. Executive architecture

```text
Paper JVM
├─ CraftRelayPaperBridge — thin shared integration plugin
│  ├─ craftrelay-paper-api implementation
│  ├─ shared bounded Agent channel/runtime
│  ├─ shared bounded Query Service channel/runtime
│  ├─ logical authenticated producer sessions
│  ├─ IntegrationManifest validation
│  ├─ bounded publish/query/token/watch tracking
│  ├─ reconnect, health, readiness, and shutdown coordination
│  └─ Paper ServicesManager registration
│
├─ MiningPlugin
│  └─ MiningCraftRelayAdapter / generated mining client
├─ EconomyPlugin
│  └─ EconomyCraftRelayAdapter / generated economy client
├─ ClaimsPlugin
│  └─ ClaimsCraftRelayAdapter / generated claims client
└─ PremiumPlugin
   └─ PremiumCraftRelayAdapter / generated premium client
        │
        │ typed asynchronous API; no direct Kafka/DB/journal access
        ▼
CraftRelayPaperBridge
        │
        │ persistent gRPC/Protobuf over loopback
        ▼
CraftRelay Agent — Rust
├─ authenticated producer sessions
├─ authoritative producer identity
├─ installation-scoped policy resolution
├─ static NODE_LOCAL ownership
├─ bounded admission control
├─ immutable StoredEventEnvelope
├─ EventPayloadBlob storage
├─ revisioned PayloadRetentionState
├─ revisioned EventDeliveryState
├─ revisioned ProjectionTrackingState
├─ bounded client tracking/subscriptions
├─ durable local journal
├─ Kafka RF=5/minISR=5 delivery profile
├─ persistent Kafka retry scheduler
├─ per-stream ordering gates
├─ DELIVERY_BLOCKED handling
├─ ACK consumer with local transaction before offset commit
├─ physical disk guard
├─ health
└─ metrics
        │
        ▼
Kafka
├─ ordered event topics
└─ projection ACK topics
        │
        ▼
CraftRelay Projector — Rust
├─ stored-envelope verification
├─ payload digest verification
├─ schema validation
├─ transactional sequence validation
├─ idempotent projection
├─ immutable event archive
├─ PostgreSQL checkpoint
├─ transactional projection ACK outbox
├─ ACK publisher
├─ poison/gap handling
└─ metrics
        │
        ▼
PostgreSQL
├─ projection tables
├─ event archive
├─ sequence associations
├─ checkpoints
└─ ACK outbox

CraftRelay Query Service — Rust
├─ typed snapshot API only
├─ read-only PostgreSQL access
├─ Kafka read-committed barrier capture
├─ strict projection-checkpoint waiting
├─ read-your-write consistency tokens
├─ read-only REPEATABLE READ transactions
├─ bounded request queue and connection pool
├─ bounded cache with barrier/version metadata
├─ bounded snapshot watches
├─ single-flight
├─ deadlines/circuit breakers
├─ explicit freshness results
└─ independent lifecycle
```

`CraftRelayPaperBridge` is the only first-party CraftRelay infrastructure plugin required inside one Paper server. Optional health commands, backlog summaries, administrative warnings, and permissions may be implemented as an admin module inside that Bridge. They are never required for durability.

First-party domain plugins contain AdapterClasses or generated typed domain clients, not separate adapter plugins. A separate adapter plugin is permitted only for an unmodifiable third-party plugin and requires an explicit integration ADR.


# 5. Core invariants

## 5.1 No silent loss

Every durable publish request must become one of:

- explicitly not accepted;
- transport-indeterminate from the client perspective;
- accepted and locally durable;
- accepted and waiting for required replication;
- accepted and retrying replication;
- accepted and `DELIVERY_BLOCKED`;
- replicated and confirmed;
- projected and ACKed when policy requires it;
- quarantined with preserved evidence.

An accepted event must not disappear because:

- a queue filled;
- Kafka was unavailable;
- PostgreSQL was unavailable;
- a process restarted;
- a retry failed;
- a duplicate appeared;
- a projector crashed;
- disk pressure occurred;
- an ACK was redelivered;
- a client subscription expired.

## 5.2 Nothing unbounded

Production paths must not use unbounded:

- channels;
- queues;
- caches;
- retry registries;
- in-flight maps;
- client handles;
- subscriptions;
- payload buffers;
- audit tables;
- outbox tables;
- ACK dedup tables;
- projector association tables.

Every structure needs limits by count, bytes, producer, namespace, age, priority, or physical quota.

Append-only does not mean keep forever in hot operational storage.

## 5.3 No blocking Paper gameplay threads

The Java client must not perform on a Paper gameplay thread:

- socket waits;
- disk IO;
- Kafka calls;
- PostgreSQL calls;
- retry loops;
- `Future.get()`;
- `CompletionStage.join()`;
- indefinite startup waits;
- large payload serialization;
- synchronous drain or shutdown.

A plugin may register callbacks and react later on an appropriate scheduler.

## 5.3.1 One shared Paper Bridge, no duplicated transport stacks

For first-party CraftMMO plugins:

```text
one CraftRelayPaperBridge per Paper server
+ one typed AdapterClass/generated client inside each domain plugin
```

Rules:

- domain plugins obtain `CraftRelayService` from the Paper service registry;
- domain plugins do not instantiate gRPC channels, Kafka clients, PostgreSQL pools, local journals, reconnect executors, or sidecar health loops;
- the Bridge owns shared transport/runtime resources and keeps them bounded;
- the Bridge contains no mining, economy, claims, premium, or other domain rules;
- an AdapterClass converts domain input into a typed event/query request and converts typed responses back into domain-facing results;
- first-party plugins do not require separate adapter plugins;
- a third-party compatibility adapter plugin is allowed only when the original plugin cannot be changed and an ADR accepts the exception.

## 5.4 At-least-once, not fake exactly-once

The system guarantee is:

```text
at-least-once delivery
+ installation-scoped event identity
+ canonical fingerprints/checksums
+ idempotent projectors
+ transactional checkpoints
+ structural dedup constraints
+ explicit sequence validation
+ integrity conflict detection
```

Duplicates are expected and must be safe.

## 5.5 Agent-authoritative policy

The client requests an operation. The Agent determines the effective producer identity, physical Kafka topic, durability requirement, retention class, Kafka durability profile, projection policy, required projector set, quota class, priority, routing version, and ownership scope.

The producer cannot self-promote or weaken policy.

## 5.6 No stale authoritative data presented as current

CraftRelay distinguishes durable event freshness from projected read freshness.

For authoritative gameplay decisions and authoritative player/admin displays:

- plugins never read PostgreSQL directly;
- the Query Service captures a projection barrier;
- the Query Service waits until every required projector checkpoint reaches that barrier;
- the Query Service reads the checkpoint and domain rows from one PostgreSQL primary snapshot;
- the response declares the barrier and freshness result;
- timeout or lag returns an explicit freshness failure, never silent stale fallback;
- a continuous view uses bounded monotonic snapshot watching;
- if watch tracking detaches, the UI must refetch, close, or visibly mark itself not current.

A response cannot remain “latest forever” after it is returned because concurrent writes may occur later. The strict contract is:

> For each partition in the barrier vector, the returned snapshot includes every relevant read-committed record below that partition’s exclusive `required_next_offset`, and no older snapshot is represented as current.

---

# 6. Technology and process boundaries

## 6.1 Rust

Rust is used for Agent, journal, Kafka scheduler/publisher, ACK consumer, Projector, ACK publisher, Query Service, CLI, and load/fault tools.

Reasons:

- explicit memory control;
- bounded-buffer control;
- no GC in the write plane;
- predictable footprint;
- self-contained binaries;
- memory safety;
- strong async ecosystem.

Rust is not chosen because Java is presumed universally slow.

## 6.2 Java

Java is used for:

- `craftrelay-client-core`, containing transport-independent client behavior and generated protocol bindings;
- `craftrelay-paper-api`, a small stable API used as `compileOnly`/provided by domain plugins;
- `craftrelay-paper-bridge`, the single thin shared Paper integration plugin;
- generated or handwritten typed domain clients such as `craftrelay-mining-client-java`;
- AdapterClasses embedded in first-party domain plugins;
- Paper integration fixtures.

The Bridge owns shared gRPC transport, reconnect, tracking, readiness, producer-session, and shutdown resources. Domain plugins own only domain adaptation and never own sidecar transport resources.

Sprint 0 must document minimum supported Paper version, maximum tested Paper version, minimum Java runtime, Java compilation target, plugin classloader assumptions, shading/relocation policy, service-registration behavior, Folia/Paper execution-context assumptions, and protocol compatibility range.

## 6.3 Protocol

CraftRelay uses Protocol Buffers, gRPC, a persistent bidirectional publish stream, unary status lookup, unary query calls, optional health stream, and explicit protocol-version negotiation.

## 6.4 CraftRelayPaperBridge contract

`CraftRelayPaperBridge` is a thin shared integration plugin with these responsibilities:

- create and own the shared Java client runtime;
- establish persistent bounded connections to the Agent and Query Service;
- expose one `CraftRelayService` through Paper/Bukkit `ServicesManager` or the accepted equivalent service registry;
- validate IntegrationManifests;
- establish logical authenticated producer sessions for registered integrations;
- multiplex publish, status, token, query, and watch operations;
- enforce local bounds and reserved P0 capacity before sending work to sidecars;
- expose health/readiness without claiming durability itself;
- coordinate reconnect and bounded shutdown;
- schedule no blocking work on Paper gameplay threads;
- publish bridge metrics and diagnostics.

Minimum Bridge resource-isolation limits:

```text
max_logical_producer_streams
max_inflight_per_producer
per_producer_publish_queue_bytes
per_producer_query_queue_bytes
reserved_p0_publish_slots
reserved_p0_callback_capacity
callback_dispatch_queue_limit
slow_callback_threshold
```

These limits prevent noisy producers from starving critical Economy, Premium, Claims, ownership, permission, or unique-item paths.

It must not:

- connect to Kafka or PostgreSQL;
- own the authoritative WAL;
- select effective durability/retention/projection policies;
- contain SQL;
- contain domain rules;
- transform one domain event into another;
- silently buffer unbounded work while the Agent is unavailable;
- report a durable receipt that was not issued from persisted Agent state.

## 6.5 Paper API and service discovery

`craftrelay-paper-api` is a small stable Java artifact. Domain plugins depend on it as `compileOnly`/provided scope.

Conceptual API:

```java
public interface CraftRelayService {
    CraftRelayDomainClient clientFor(Plugin plugin);
    CraftRelayBridgeHealth health();
}
```

The actual `Plugin` instance is used to resolve the integration. This prevents accidental string-based producer selection and resolves the authoritative manifest mapping. It is **not** a malicious same-JVM security boundary.

The service may be registered as `NOT_READY` when local configuration and manifests are valid but sidecars are temporarily unavailable. Dependent plugins may obtain handles in this state, but they must not enable authoritative features until their required integration reaches `READY`.

For v1, the initial Paper integration uses traditional `JavaPlugin`/`plugin.yml` with a hard dependency on `CraftRelayPaperBridge`. The newer Paper plugin-loader model may be supported later only after an ADR covers dependency ordering and classloader behavior.

## 6.6 Logical producer sessions inside one shared transport

The Bridge may share a gRPC channel/runtime, but the Agent must still observe separate logical producers.

Recommended v1 transport/session model:

```text
one shared ManagedChannel to Agent
+ one bounded authenticated bidirectional publish stream per logical producer
```

Each registered integration has:

```text
paper_plugin_id
integration_id
integration_version
producer_id
allowed namespaces
allowed event schemas
allowed query definitions
credential reference or delegated identity
quota class
priority ceiling
```

Per logical producer:

```text
producer_instance_id
→ generated once per Bridge process lifetime and logical producer

producer_operation_sequence
→ allocated monotonically by that logical producer runtime
```

Rules:

- reconnect within the same Bridge process keeps `producer_instance_id`;
- full Bridge/Paper restart creates a new `producer_instance_id`;
- each logical producer has a separate sequence allocator;
- a retry still tracked by the Bridge reuses the original event ID and original sequence;
- after tracking eviction, the Bridge performs status lookup before retry;
- if the original sequence cannot be safely proven, retry occurs only under a new producer instance;
- a stream must not carry multiple producers unless each message has an authenticated formal binding accepted by ADR; v1 uses one authenticated stream per logical producer.

## 6.7 Domain AdapterClass and generated client contract

Every first-party domain plugin contains one small AdapterClass or generated typed client facade.

Examples:

```text
MiningPlugin  → MiningCraftRelayAdapter
EconomyPlugin → EconomyCraftRelayAdapter
ClaimsPlugin  → ClaimsCraftRelayAdapter
```

The adapter:

- accepts domain-native Java values;
- creates canonical typed Protobuf payloads/requests;
- selects only declared event/query identifiers from its generated contract;
- derives the canonical stream key using the domain contract;
- supplies stable operation IDs/event IDs where required;
- returns `PublishHandle`, typed futures, tokens, and watch handles;
- maps typed CraftRelay failures into domain-facing results;
- schedules gameplay mutations back onto the accepted Paper/Folia execution context.

The adapter does not:

- choose Kafka topics;
- choose replication factor or retention;
- execute SQL;
- implement retries with new event IDs;
- call blocking methods on gameplay threads;
- keep unbounded local queues;
- bypass the Bridge.

## 6.8 IntegrationManifest

Every domain integration has a versioned manifest validated at startup and deployment time:

```text
IntegrationManifest
- integration_id
- integration_version
- paper_plugin_id
- producer_id
- protocol compatibility range
- event schemas and versions
- query definitions and versions
- stream-key strategies
- requested operation classes
- expected projection policies
- generated client artifact version
- manifest checksum
- optional signature
```

The manifest is declarative. The Agent remains authoritative for effective durability, Kafka, retention, projection, quota, and routing policies.

Startup validation fails closed when:

- a required schema/query is absent;
- versions are incompatible;
- two plugins claim the same first-party integration identity;
- producer mapping is missing;
- a generated client contract is newer than the accepted server contract;
- manifest checksum/signature is invalid where required.

## 6.9 Classloader and API boundary

For v1, the classloader/API boundary is:

```text
craftrelay-paper-api
→ only API-owned interfaces, records, enums, handles, and JDK types

domain Protobuf classes
→ plugin-local

gRPC / Netty / Protobuf runtime
→ Bridge-owned implementation detail
```

The public Bridge API must not expose domain-generated Protobuf classes or gRPC runtime classes across unstable plugin classloader boundaries.

The boundary uses:

```text
EventContractHandle
+ immutable bounded serialized payload

QueryContractHandle
+ immutable bounded serialized parameters
```

These bytes are not arbitrary untyped payloads. The handles are issued by the Bridge from an accepted `IntegrationManifest`, are capability-scoped, versioned, and bounded, and correspond to registered schemas/query definitions.

## 6.10 Paper/Folia execution context and callback rule

CraftRelay guarantees event durability and projected-state consistency. It does **not** by itself guarantee exactly-once execution of a Paper-side callback.

Critical domains must use at least one of:

- projected-state authority;
- idempotent effect fenced by `event_id` or `operation_id`;
- startup reconciliation;
- desired-state command semantics.

Any callback that mutates Paper state must reschedule through an accepted execution context:

```text
PaperExecutionContext
- GLOBAL_SERVER
- ENTITY
- REGION
- ASYNC_ONLY
```

`BukkitScheduler.runTask(...)` is acceptable only for traditional Paper/global-server operations. Folia/entity/region-affine operations require the correct region/entity scheduler or must be rejected as unsafe.


# 7. Stable identities

## 7.1 installation_id

A CraftRelay installation has a stable `installation_id`.

It is the formal isolation boundary when Kafka or PostgreSQL are shared.

It participates in event storage identity, logical stream identity, Kafka key, projector sequence identity, ACK identity, query authorization, audit records, and deployment profiles.

Rules:

- explicitly provisioned;
- canonical;
- stable across restarts;
- never regenerated automatically;
- included in every persistent identity that could collide cross-installation.

## 7.2 node_id

`node_id` identifies one game node or NODE_LOCAL scope inside an installation.

It is stable across restarts.

## 7.3 agent_id

`agent_id` is a stable deployment identity bound to one `installation_id`, one journal, one node configuration, and one ownership snapshot lineage.

It survives restarts and must not be regenerated automatically.

Replacing it for an existing journal requires an audited migration.

## 7.4 producer_id

The authoritative `producer_id` comes from the authenticated session.

It is never trusted from the request.

## 7.5 producer_instance_id

The client proposes `producer_instance_id` during authenticated handshake.

The Agent validates it, binds it to the authenticated producer session, and uses it for sequence conflict detection and diagnostics.

It is not proof of one physical process.

## 7.6 transport_session_id

Identifies one gRPC session.

It is audit context only.

It is not event identity and not part of the immutable request fingerprint.

## 7.7 paper_plugin_id

`paper_plugin_id` is the canonical plugin identity from the accepted integration registration. It is used only to resolve the logical producer and integration inside the Bridge. It is not trusted when supplied as an arbitrary request string.

## 7.8 integration_id and integration_version

`integration_id` identifies the domain contract between one Paper plugin, its typed Java client/adapter, the Agent policy registry, Projector modules, and Query handlers.

`integration_version` changes when that contract changes incompatibly. Compatibility is validated during startup before the plugin may publish authoritative work.

---

# 8. Event identity

## 8.1 event_id

Official rule:

> `event_id` is a client-generated canonical UUIDv7 intended to be globally unique. The formal persisted identity is `(installation_id, event_id)`. The value remains unchanged across every retry of the same immutable request.

## 8.2 Validation

The Agent rejects event IDs unless they are exactly 36 characters, canonical hyphenated UUID text, lowercase, version 7, valid RFC variant, free of whitespace, non-zero, and identical to parse-and-reencode canonical form.

## 8.3 UUIDv7 timestamp

Official rule:

> The timestamp embedded in UUIDv7 is not authoritative and is never used for event ordering, expiry, retention, domain time, conflict resolution, or security decisions.

## 8.4 Event storage constraint

```text
UNIQUE(
  installation_id,
  event_id
)
```

---

# 9. Ownership authority

## 9.1 v1 ownership model

CraftRelay v1 supports only:

```text
NODE_LOCAL
```

It does not support dynamic owner election, runtime failover, runtime ownership transfer, global multi-writer streams, or consensus fencing.

Official rule:

> CraftRelay v1 does not dynamically elect stream owners. The initial vertical slice supports statically provisioned NODE_LOCAL ownership only.

## 9.2 OwnershipSnapshot

```text
OwnershipSnapshot
- installation_id
- snapshot_id
- configuration_version
- generated_at
- node_id
- assignments
- checksum
- optional signature
```

It is a deployment artifact. The Agent validates it at startup and does not become ready if invalid.

PostgreSQL, Kafka, and the local journal are not ownership authorities.

## 9.3 Ownership transfer

Unsupported in runtime v1.

Any ownership change requires stopping the affected namespace and performing an audited offline migration.

---

# 10. Logical stream identity and Kafka routing

## 10.1 Logical stream identity

```text
installation_id
node_id
namespace
logical_stream_type
stream_key
```

## 10.2 Physical routing

```text
physical_kafka_topic
routing_version
canonical_kafka_key
partition
offset
```

## 10.3 Kafka key

```text
installation_id
node_id
namespace
logical_stream_type
stream_key
routing_version
```

## 10.4 Stream sequence constraint

```text
UNIQUE(
  installation_id,
  node_id,
  namespace,
  logical_stream_type,
  stream_key,
  stream_sequence
)
```

## 10.5 Runtime routing migration

Unsupported in v1.

Any topic/routing change requires audited offline migration.

---

# 11. Numeric types

## 11.1 Positive int64

Use positive signed `int64` for `producer_operation_sequence`, `journal_sequence`, `stream_sequence`, `domain_version`, `snapshot_version`, `lifecycle_revision`, `projection_revision`, and `retention_revision`.

Valid range:

```text
1 .. 9_223_372_036_854_775_807
```

Zero and negative values are invalid. No wraparound.

## 11.2 Positive int32

Use positive signed `int32` for `schema_version`, protocol-major/minor where numeric, and small bounded version fields.

Valid range:

```text
1 .. 2_147_483_647
```

Zero and negative values are invalid unless a field’s enum explicitly defines zero as `*_UNSPECIFIED`.

## 11.3 Reason

Signed types map cleanly to Java `long`/`int`, Rust `i64`/`i32`, SQLite `INTEGER`, and PostgreSQL `BIGINT`/`INTEGER`.

Avoid unsigned interoperability traps.

## 11.4 Exhaustion

- journal sequence exhaustion blocks ingestion;
- stream sequence exhaustion blocks that stream;
- producer sequence exhaustion requires new producer instance;
- revision exhaustion blocks updates for that state object.

---

# 12. Producer operation sequence

Within one `producer_instance_id`:

- sequence starts at 1;
- newly accepted events strictly increase;
- gaps are allowed;
- retry may reuse a sequence only for the same `event_id`;
- same sequence with another event ID is conflict;
- a restarted producer may create a new producer instance and start at 1.

Agent state:

```text
installation_id
producer_id
producer_instance_id
highest_newly_accepted_sequence
```

Rules:

```text
sequence > highest
→ candidate new event

same sequence + same event_id
→ retry/status path

same sequence + different event_id
→ PRODUCER_SEQUENCE_CONFLICT
```

Event-ID dedup remains authoritative across producer instances.

---

# 13. Metadata and request protocol

## 13.1 MetadataEntry

Do not use Protobuf `map<string,string>` for fingerprinted metadata.

```protobuf
message MetadataEntry {
  string key = 1;
  string value = 2;
}
```

Use:

```protobuf
repeated MetadataEntry client_metadata = 12;
```

Agent validation:

1. count limit;
2. byte limit;
3. key/value format;
4. duplicate-key rejection;
5. unsigned UTF-8 byte sorting;
6. canonicalization.

## 13.2 ClientPublishRequest

Conceptual v1 skeleton:

```protobuf
message ClientPublishRequest {
  string event_id = 1;
  int64 producer_operation_sequence = 2;

  string namespace = 3;
  string logical_stream_type = 4;
  string event_type = 5;
  int32 schema_version = 6;
  string stream_key = 7;

  string operation_type = 8;
  string operation_id = 9;
  string event_role = 10;

  MinimumDurability requested_minimum_durability = 11;

  repeated MetadataEntry client_metadata = 12;
  bytes payload = 13;

  optional bytes advisory_request_fingerprint = 14;
}
```

The authenticated session supplies `installation_id`, `producer_id`, `producer_instance_id`, `transport_session_id`, credentials, and ACL context.

## 13.3 client_created_at

Excluded from the durable request in v1.

Client clocks are not authoritative and may change across retry.

Attempt timestamps belong to bounded audit records.

---

# 14. Immutable envelope, payload blob, and mutable states

## 14.1 StoredEventEnvelope — immutable

Created once inside the local acceptance transaction and never updated.

Contains:

- `installation_id`;
- `event_id`;
- authenticated `producer_id`;
- original `producer_instance_id`;
- original `producer_operation_sequence`;
- `agent_id`;
- `node_id`;
- ownership snapshot ID;
- namespace;
- logical stream type;
- stream key;
- event type;
- `schema_version`;
- operation type, ID, and role;
- resolved physical Kafka topic;
- routing version;
- canonical Kafka key;
- requested minimum durability;
- effective required durability;
- priority class;
- quota class;
- retention class;
- Kafka event profile ID/version;
- projection policy ID/version;
- required projector set;
- journal sequence;
- stream sequence;
- payload reference ID;
- payload digest;
- payload length;
- payload encoding;
- request fingerprint/version;
- stored-envelope checksum/version;
- canonical client metadata;
- immutable system metadata;
- `received_at`;
- `local_durable_recorded_at`.

It does not contain physical payload bytes, Kafka partition/offset, retry count, mutable lifecycle state, blocked reason, projection progress, payload presence, or deletion state.

## 14.2 Payload reference

`payload_ref_id` is logical and stable.

It must never be a physical file path, SQLite row ID, page number, segment offset, or storage location.

For v1, it may be deterministic from:

```text
installation_id + event_id
```

## 14.3 EventPayloadBlob

Stores the local payload bytes while present.

```text
installation_id
event_id
payload_ref_id
payload_bytes
payload_digest
payload_length
created_at
```

The blob is immutable while present.

It may be removed only through a retention transaction.

## 14.4 PayloadRetentionState

Mutable and revisioned:

```text
installation_id
event_id

retention_revision
retention_snapshot_checksum

payload_presence
retention_basis

deletion_eligible_at
deleted_at
deletion_reason

basis_at_deletion
profile_version_at_deletion
updated_at
```

`payload_presence`:

```text
PRESENT_LOCALLY
LOCAL_PAYLOAD_REMOVED
```

`retention_basis`:

```text
LOCAL_COPY_REQUIRED
KAFKA_PROFILE_SUFFICIENT
REQUIRED_PROJECTORS_CONFIRMED
```

Future possible value:

```text
ARCHIVAL_COPY_CONFIRMED
```

`REQUIRED_PROJECTORS_CONFIRMED` is not a storage owner. It is a condition that permits local payload removal.

## 14.5 EventDeliveryState

Mutable and revisioned:

```text
installation_id
event_id

lifecycle_revision
lifecycle_snapshot_checksum

lifecycle_state
local_durability_state
replication_state
required_result

kafka_partition
kafka_offset

attempt_summary_ref
first_delivery_attempt_at
last_delivery_attempt_at
next_retry_at

last_error_code
last_error_at
blocked_reason

replicated_durable_recorded_at
updated_at
```

## 14.6 ProjectionTrackingState

Mutable and revisioned:

```text
installation_id
event_id

projection_revision
projection_snapshot_checksum

aggregate_projection_state
required_projector_statuses
last_updated_at
```

## 14.7 Bounded audit records

`PublishAttemptAudit` is not an infinite append-only table.

Operational storage keeps:

- first significant failure;
- latest failure;
- total attempt counters;
- counters by error class;
- last N detailed attempts;
- finite retention window;
- per-event/per-producer/global quotas.

A separate external archive may keep longer evidence, but hot journal storage must be capacity-managed.

## 14.8 Immutability rule

Retries and repairs may create audit records and update mutable states.

They must never mutate `StoredEventEnvelope`, `EventPayloadBlob` contents, original producer context, request fingerprint, payload digest, stream sequence, or journal sequence.

---

# 15. Fingerprints and checksums

## 15.1 Request fingerprint

Calculated by the Agent over immutable producer intent before the transaction.

Includes event ID, authenticated producer ID, namespace, logical stream type, event type, `schema_version`, stream key, operation identity, requested minimum durability, payload bytes, canonical client metadata, and canonicalization version.

Excludes installation ID, producer instance, producer sequence, transport session, timestamps, Agent policy, physical routing, priority, retention, projection policy, and journal/stream sequences.

## 15.2 Stored-envelope checksum

Calculated inside the local acceptance transaction after sequence allocation and envelope construction.

Covers the immutable envelope including journal sequence, stream sequence, payload ref ID, payload digest, payload length, payload encoding, policies, routing, and system metadata.

It does not cover mutable states or physical payload presence.

## 15.3 Snapshot checksums

Each revisioned state has its own canonical snapshot checksum:

```text
lifecycle_snapshot_checksum
projection_snapshot_checksum
retention_snapshot_checksum
```

Rules:

```text
same revision + same checksum
→ idempotent duplicate

same revision + different checksum
→ INTEGRITY_CONFLICT
```

## 15.4 Algorithm

Baseline:

```text
SHA-256
```

Changing hash or canonicalization requires a new canonicalization version.

## 15.5 Canonicalization v1

- UTF-8 valid strings;
- no Unicode normalization;
- fixed-width big-endian integers;
- explicit field ID;
- presence marker;
- length framing;
- null distinct from empty;
- sorted metadata;
- sorted required projector IDs;
- payload bytes length-prefixed in request fingerprint;
- payload digest/length/ref in envelope checksum;
- unknown fields excluded unless a new contract includes them.

Deterministic Protobuf serialization is not the contract.

---

# 16. Local acceptance transaction

## 16.1 Correct sequence allocation and checksum timing

The final envelope and state checksums must not be calculated before `journal_sequence` and `stream_sequence` exist.

Correct flow:

```text
outside transaction:
→ validate request
→ calculate request fingerprint
→ calculate payload digest and payload length
→ determine logical payload_ref_id
→ select received_at

BEGIN IMMEDIATE
→ repeat final dedup/conflict checks
→ allocate journal_sequence
→ allocate stream_sequence
→ select local_durable_recorded_at
→ construct StoredEventEnvelope
→ construct initial EventDeliveryState
→ construct initial PayloadRetentionState
→ calculate stored-envelope checksum
→ calculate lifecycle snapshot checksum
→ calculate retention snapshot checksum
→ insert StoredEventEnvelope
→ insert EventPayloadBlob
→ insert EventDeliveryState
→ insert PayloadRetentionState
→ update producer sequence head
→ update logical stream head
COMMIT with durability barrier
→ emit latest lifecycle snapshot
```

Sequences are allocated and persisted atomically. A failed transaction must not create externally visible sequence gaps.

## 16.2 Initial lifecycle state

For `LOCAL_DURABLE` requirement, revision 1 is:

```text
lifecycle_state = LOCAL_ACCEPTED
local_durability_state = DURABLE
replication_state = NOT_REQUIRED
required_result = REACHED
```

For `REPLICATED_DURABLE` requirement, revision 1 is:

```text
lifecycle_state = LOCAL_ACCEPTED
local_durability_state = DURABLE
replication_state = PENDING
required_result = PENDING
```

No second artificial transaction exists only to initialize `PENDING`.

## 16.3 local_durable_recorded_at

Timestamp selected immediately before the local durability commit and persisted atomically.

It becomes externally valid only if the commit succeeds.

It is not the exact physical flush instant.

---

# 17. Publish lifecycle and bounded client tracking

## 17.1 Publish snapshots

Publish is multi-stage and revisioned.

```protobuf
message PublishUpdate {
  string event_id = 1;
  int64 lifecycle_revision = 2;
  bytes lifecycle_snapshot_checksum = 3;
  PublishLifecycleSnapshot snapshot = 4;
}
```

Snapshots are complete, not fragile deltas.

## 17.2 Revision rules

- initial accepted state is revision 1;
- every persisted lifecycle mutation increments revision by exactly 1;
- lower revision is ignored by clients;
- same revision plus same checksum is duplicate;
- same revision plus different checksum is `LIFECYCLE_INTEGRITY_CONFLICT`;
- higher revision with valid checksum and structurally valid state replaces local snapshot;
- clients may receive revision jumps and must not require all intermediate states;
- the Agent must validate each persisted transition;
- status lookup returns the latest persisted revision.

## 17.3 Minimum state machine

```text
LOCAL_ACCEPTED
→ REPLICATION_PENDING
→ REPLICATION_RETRYING
→ REPLICATED
```

or:

```text
LOCAL_ACCEPTED
→ REPLICATION_PENDING
→ DELIVERY_BLOCKED
```

Repair:

```text
DELIVERY_BLOCKED
→ REPLICATION_RETRYING
→ REPLICATED
```

A client that jumps from revision 2 to revision 7 validates the final snapshot and checksum; it does not reject merely because revisions 3-6 were not observed.

## 17.4 Java API

Primary API:

```java
PublishHandle submit(ClientPublishRequest request);
```

Conceptual handle:

```java
interface PublishHandle {
    UUID eventId();

    CompletionStage<LocalAcceptanceResult> awaitLocalAcceptance(
        Duration timeout
    );

    CompletionStage<RequiredDurabilityWaitResult> awaitRequiredDurability(
        Duration timeout
    );

    CompletionStage<PublishLifecycleSnapshot> getStatus();
}
```

Convenience API requires a timeout:

```java
CompletionStage<DurableReceipt> publish(
    ClientPublishRequest request,
    Duration timeout
);
```

`publish()` succeeds only when the effective required durability level is reached.

## 17.5 Bounded tracking

Client and Agent tracking are ephemeral and bounded.

Required limits:

```text
max_active_publish_handles
max_pending_update_subscriptions
max_tracked_local_acceptance_waiters
max_tracked_required_durability_waiters
tracking_ttl
max_tracking_bytes
per_producer_tracking_limit
global_tracking_limit
```

When a limit or TTL is reached:

```text
event remains durable/pending in journal
subscription may detach
waiter completes with TRACKING_DETACHED
client continues through status lookup
```

`TRACKING_DETACHED` is not a durable lifecycle state.

`WatchPublishStatus(event_id, after_revision)` may be added later; v1 requires `GetPublishStatus`.

---

# 18. Rejections and indeterminate transport

Rejection categories:

```text
PERMANENT
TRANSIENT
CONFLICT
```

Conflict examples:

- same `(installation_id,event_id)` with different request fingerprint;
- same producer instance sequence with different event ID.

Timeout, disconnect, or cancellation may be transport-indeterminate.

Rules:

- do not create a new event ID;
- retry same immutable request;
- query status;
- never treat transport failure as rejection.

After local acceptance, the Agent must never return `PublishRejection` for that event. Later failure becomes pending, retrying, or blocked lifecycle state.

---

# 19. Durable receipt

```protobuf
message DurableReceipt {
  string event_id = 1;
  string origin_agent_id = 2;

  int64 journal_sequence = 3;
  int64 stream_sequence = 4;
  int64 lifecycle_revision = 5;

  DurableLevel level = 6;
  ReceiptDisposition disposition = 7;

  int64 durable_recorded_at_epoch_millis = 8;
  string diagnostic_id = 9;
}
```

`DurableLevel`:

```text
DURABLE_LEVEL_UNSPECIFIED
LOCAL_DURABLE
REPLICATED_DURABLE
```

`ReceiptDisposition`:

```text
RECEIPT_DISPOSITION_UNSPECIFIED
NEWLY_ACCEPTED
EXISTING_DUPLICATE
```

Duplicate disposition is not a durability level.

---

# 20. Deduplication and retry immutability

Same request:

```text
same installation_id
+ same event_id
+ same request_fingerprint
→ return latest lifecycle/status
```

Conflict:

```text
same installation_id
+ same event_id
+ different request_fingerprint
→ DUPLICATE_CONFLICT
```

Producer-sequence conflict:

```text
same installation_id
+ same producer_id
+ same producer_instance_id
+ same sequence
+ different event_id
→ PRODUCER_SEQUENCE_CONFLICT
```

Retries may update bounded attempt summaries and audit records, but never mutate the envelope or payload blob.

---

# 21. Atomicity

CraftRelay v1 provides atomic durability for one event at a time.

Independent publish calls are never atomic, even if they share an operation ID.

Unsafe:

```text
publish DEBIT
publish CREDIT
```

Correct v1 representation:

```text
publish TRANSFER_COMMAND
```

The Projector applies all domain changes in one PostgreSQL transaction.

Future `PublishBatchAtomic` requires ADR and is out of v1.

---

# 22. Batching, coalescing, and flush policy

## 22.1 Purpose

CraftRelay uses batching to reduce filesystem, Kafka, and PostgreSQL overhead without weakening durability, identity, ordering, deduplication, auditability, projection correctness, or strict-read freshness.

The base rule is:

```text
flush when the first configured condition is reached:
records >= max_records
OR bytes >= max_bytes
OR age >= max_delay_millis
OR shutdown/drain requires flush
```

The conditions are always OR, never AND. Low traffic must flush by time. High traffic may flush by record count or bytes before the time limit.

Interactive or consistency-driven early flush is an explicit bounded operation, not the default behavior for ordinary commands, UI rendering, scoreboards, XP bars, or statistics displays.

## 22.2 Requested versus effective policy

A Paper plugin or integration may declare requested flush criteria, but CraftRelay computes the effective policy.

```text
Integration/Plugin request
→ IntegrationManifest event class
→ Agent/Projector policy registry
→ effective flush criteria
```

A plugin is not allowed to directly choose effective global priority, Kafka topic, retention, durability, projection, reserved P0 capacity, or unbounded batching behavior.

Requested values are either accepted, clamped, or rejected according to the policy mode:

```text
STRICT_POLICY
→ out-of-range request fails startup or registration

CLAMP_WITH_WARNING
→ out-of-range request is reduced to the allowed ceiling and emits diagnostics
```

Production P0 deployments should prefer `STRICT_POLICY` for critical integrations.

## 22.3 Flush criteria model

Conceptual type:

```text
RequestedFlushCriteria
- max_delay_millis
- max_records
- max_bytes
- optional requested_latency_class
```

Effective type:

```text
EffectiveFlushCriteria
- max_delay_millis
- max_records
- max_bytes
- event_class
- producer_quota_class
- reserved_capacity_class
- semantic_coalescing_allowed
- physical_batching_allowed
- policy_version
```

The effective policy is reported or persisted sufficiently for audit, metrics, diagnostics, and replay/debugging.

## 22.4 Four batching layers

CraftRelay distinguishes four batching layers.

### Domain semantic coalescing

Occurs in the plugin, generated domain client, AdapterClass, or a controlled Bridge-side helper before individual semantic events are submitted.

Example:

```text
25 block-break actions inside a 1s window
→ 1 MiningActivityDelta event
```

This changes domain semantics and may remove individual action identity. It is allowed only when the event contract explicitly permits it.

### Agent journal group commit

Occurs inside the Agent. Multiple accepted publish requests may share one physical journal transaction/fsync.

This does not change event semantics. Each event keeps its own `event_id`, request fingerprint, sequence, envelope, lifecycle, status, and audit identity.

An event is not locally accepted until the transaction containing it commits with the required local durability barrier.

### Kafka producer batching

Occurs inside the Agent’s Kafka producer path. Multiple Kafka records may be sent efficiently while preserving per-record identity, keying, ordering rules, ACK handling, retry behavior, and lifecycle updates.

This does not change event semantics.

### Projector database microbatch

Occurs inside the Projector. Multiple Kafka records may be applied in one PostgreSQL transaction when sequence rules, deduplication, projection policy, checkpointing, and outbox semantics remain valid.

This does not change event semantics. It affects projection/token latency, not the Agent’s earlier replicated durability result.

## 22.5 Physical batching versus semantic coalescing

Physical batching is generally allowed when every event remains individually identifiable and auditable.

```text
EconomyTransfer A
EconomyTransfer B
EconomyTransfer C
→ one PostgreSQL transaction
→ three independent ledger entries
```

Semantic coalescing merges multiple domain actions into one event.

```text
BlockBreak
BlockBreak
BlockBreak
→ one MiningActivityDelta
```

Semantic coalescing is forbidden for events that require individual identity, permanent ledger semantics, uniqueness, or strict per-operation audit.

Forbidden for v1:

```text
economy transfers
premium grants/revokes
claim ownership changes requiring audit
unique item grants/transfers
permissions/security changes
admin/security ledger actions
any P0 operation requiring individual dedup/audit
```

Allowed only by explicit event contract for:

```text
gameplay activity counters
mining/farming/fishing deltas
non-critical XP/stat deltas
rankings/analytics/telemetry
reconstructible aggregate state
```

## 22.6 Event classes

Baseline event classes:

```text
LOW_LATENCY_LEDGER
LOW_LATENCY_AUTHORITY
AGGREGATED_GAMEPLAY_ACTIVITY
RECOVERABLE_STATE_DELTA
BACKGROUND_ANALYTICS
TELEMETRY_NON_DURABLE_FUTURE
```

`TELEMETRY_NON_DURABLE_FUTURE` is reserved for a future non-durable telemetry API and must not return `DurableReceipt` in v1.

Example policy ceilings:

```yaml
LOW_LATENCY_LEDGER:
  semanticCoalescingAllowed: false
  physicalBatchingAllowed: true
  maxDelayMillisCeiling: 5
  maxRecordsCeiling: 32
  maxBytesCeiling: 32768
  reservedCapacity: true
  permanentLedgerRequired: true

AGGREGATED_GAMEPLAY_ACTIVITY:
  semanticCoalescingAllowed: true
  physicalBatchingAllowed: true
  maxDelayMillisCeiling: 1000
  maxRecordsCeiling: 512
  maxBytesCeiling: 131072
  reservedCapacity: false
  permanentLedgerRequired: false
```

## 22.7 Per-integration examples

Economy:

```yaml
integration: craftmmo-economy

events:
  - type: economy.transfer_command
    schemaVersion: 1
    eventClass: LOW_LATENCY_LEDGER
    requestedFlush:
      maxDelayMillis: 5
      maxRecords: 16
      maxBytes: 16384
```

Meaning:

```text
no semantic coalescing
individual event identity preserved
flush physically when records OR bytes OR time limit is reached
reserved low-latency capacity applies
```

Activity/mining:

```yaml
integration: craftmmo-activity

events:
  - type: activity.player_action_delta
    schemaVersion: 1
    eventClass: AGGREGATED_GAMEPLAY_ACTIVITY
    requestedFlush:
      maxDelayMillis: 1000
      maxRecords: 256
      maxBytes: 65536
```

Meaning:

```text
semantic delta/coalescing may be allowed by this event contract
flush when records OR bytes OR 1s is reached
not allowed to consume P0 reserved capacity
```

## 22.8 Deadline helps scheduling, but does not replace policy

Flush criteria express urgency, but they do not fully replace resource policy.

`max_delay_millis <= 5` implies low latency needs, but CraftRelay must still enforce:

- whether that event type is allowed to request such a low delay;
- whether it may use reserved P0 capacity;
- which producer quota applies;
- what happens under overload;
- whether semantic coalescing is allowed;
- whether a permanent ledger is required.

If every plugin requested `maxDelayMillis = 1`, the system would lose meaningful priority. Therefore, CraftRelay validates requested deadlines against event-class policy.

## 22.9 Explicit drain and interactive flush

Flush may be forced before the normal limits when required by infrastructure lifecycle:

- graceful shutdown/drain;
- plugin disable;
- producer stream close;
- operator repair/drain workflow.

A plugin-facing command or query may request an explicit interactive flush only when the feature requires projected exactness. This operation is optional, bounded, and scoped. It must not become the default for ordinary commands or visual updates.

Interactive flush rules:

- it closes and submits the smallest configured accumulator needed by the feature;
- the default accumulator scope is integration plus event class;
- future entity or shard scopes require explicit contract and tests;
- it never scans Kafka payloads;
- it never asks Kafka to select records by player/entity;
- it never writes directly to PostgreSQL;
- it never flushes all CraftRelay producers globally;
- it never bypasses Agent, Kafka, Projector, or Query Service;
- it is bounded by timeout, queue, waiter, and producer limits;
- if it cannot complete, the feature returns a non-current/degraded result instead of pretending stale data is current.

Forced or interactive flush must still respect durability, ordering, deduplication, retention, projection, and P0 capacity rules.

## 22.10 Commands, UI, XP bars, and local-first feedback

CraftRelay is not a UI render loop. Kafka, the Projector, and the Query Service must not be used as the per-tick path for progress bars, scoreboards, action bars, or moment-to-moment visual feedback.

Feature classes should use different patterns.

### Critical commands

Examples:

```text
/pay
/buy
/claim
/premium redeem
```

Recommended flow:

```text
create one immutable command event
→ publish through AdapterClass
→ await required durability where policy requires it
→ await one or more projection tokens when read-your-write is needed
→ perform AT_LEAST_TOKEN query
→ reply to the player or apply the Paper-side effect through an idempotent/reconciled path
```

These commands should not use semantic coalescing. Physical batching is allowed only while every operation keeps its own event identity, ledger/audit evidence, lifecycle, and deduplication.

### Normal statistics and activity commands

Examples:

```text
/mining stats
/skills
/activity
/rankings
```

Default behavior should avoid forcing Kafka/Projector/DB synchronization merely to answer a display request. Preferred options are:

- read the current projected snapshot;
- use `ALLOW_STALE` for explicitly non-authoritative rankings or telemetry;
- show a last-known value with a visible syncing/not-current marker;
- combine the projected snapshot with a bounded local pending overlay when the plugin can explain the value as local/unconfirmed;
- request explicit interactive flush only when the command contract requires projected exactness.

### XP bars and progress bars

XP bars and progress bars should normally be local-first:

```text
player login/open feature
→ Query Service loads projected progression snapshot
→ plugin initializes bounded local per-player cache
→ gameplay updates the bar immediately from local cache
→ plugin emits compact ProgressionDelta/ActivityDelta batches asynchronously
→ Projector updates PostgreSQL
→ watch/query periodically reconciles local cache with projected authority
```

Rewards, purchases, premium effects, ownership changes, unique items, and other critical effects must not rely only on optimistic local UI state. They require projected-state authority, idempotency fences, startup reconciliation, or desired-state command semantics.

## 22.11 Metrics

Required metrics include:

```text
requested_flush_delay_millis
effective_flush_delay_millis
batch_records
batch_bytes
batch_age_millis
flush_reason
policy_clamp_count
policy_rejection_count
semantic_coalescing_count
physical_batch_commit_count
per_producer_batch_queue_depth
reserved_capacity_usage
```

## 22.12 Testing

Required tests:

- OR semantics: records, bytes, and time each independently trigger flush;
- low traffic flushes by time;
- high traffic flushes by records or bytes;
- out-of-range plugin requests are rejected or clamped according to policy mode;
- P0 ledger events reject semantic coalescing;
- physical batching preserves event identity and ledger rows;
- semantic coalescing is allowed only for declared aggregate event contracts;
- Agent journal group commit does not emit local acceptance before commit;
- Projector microbatch does not advance checkpoint past unresolved records;
- flush-on-shutdown/drain preserves durability and ordering;
- noisy producer cannot consume reserved P0 capacity through aggressive flush settings;
- interactive flush is optional, explicit, scoped, bounded, and never scans Kafka payloads;
- normal stats commands can answer from projected state, explicit stale mode, or local pending overlay without forcing flush;
- XP/progress bars use local-first feedback and never perform Query Service fetches per tick/action.

---

# 23. Kafka zero-loss production profile

## 23.1 Production P0 event profile

For authoritative/P0 events:

```text
broker_count = 5
replication.factor = 5
min.insync.replicas = 5
producer.acks = all
producer.enable.idempotence = true
unclean.leader.election.enable = false
auto.create.topics.enable = false
cleanup.policy = delete
```

Meaning:

- a publish is confirmed only when all five replicas are in sync;
- one unavailable broker stops new P0 confirmations;
- no automatic downgrade;
- durability is preferred over availability.

The five brokers must be on five independent hosts and storage/failure domains.

Five brokers on one host do not provide meaningful durability.

## 23.2 Retention

For ordered event topics:

```text
effective Kafka retention horizon
>
maximum projector outage
+ operator recovery window
+ local safety window
```

`retention.bytes` must either be disabled or dimensioned for worst-case throughput over that horizon.

Compaction is prohibited for ordered event topics unless a dedicated ADR proves intermediate events can be removed safely.

## 23.3 Topic protection

Production topics require infrastructure-as-code provisioning, auto-create disabled, ACL protection against deletion, versioned configuration, startup validation, drift monitoring, and alerting for ISR shrinkage, disk pressure, retention risk, and unauthorized config changes.

## 23.4 Development profile

`DEV_SINGLE_NODE` may exist only for local development and carries no host-loss protection.

It must not be used for P0 production claims.

---

# 24. Kafka delivery and replicated durability

## 24.1 Per-stream gate

`N+1` is not published before `N` in the same installation-scoped logical stream.

## 24.2 Retry authority

The journal is the durable retry authority.

Kafka client retries are bounded within one attempt.

## 24.3 Replicated transaction

After Kafka ACK under the required profile:

```text
select replicated_durable_recorded_at
BEGIN
→ update EventDeliveryState
   kafka_partition
   kafka_offset
   replication_state = CONFIRMED
   lifecycle_state = REPLICATED
   required_result = REACHED
   lifecycle_revision = previous + 1
   lifecycle_snapshot_checksum
   replicated_durable_recorded_at
COMMIT with durability barrier
→ emit latest lifecycle snapshot
```

Success for `REPLICATED_DURABLE` is emitted only after this local transaction commits.

## 24.4 DELIVERY_BLOCKED

Permanent Kafka failures cause:

```text
DELIVERY_BLOCKED
lifecycle_revision + 1
stream head blocked
event preserved
operator action required
```

No `N+1` for that stream is published until repair.

Repair is authorized, audited, leaves the envelope unchanged, increments revision, does not alter an already terminal handle, and is later observed through status lookup.

---

# 25. Payload retention responsibility

## 25.1 Local responsibility

`LOCAL_DURABLE` establishes local durability.

It does not imply permanent local payload retention.

## 25.2 Retention basis

The local payload may become deletion-eligible only when retention basis changes from:

```text
LOCAL_COPY_REQUIRED
```

to either:

```text
KAFKA_PROFILE_SUFFICIENT
```

or:

```text
REQUIRED_PROJECTORS_CONFIRMED
```

## 25.3 Kafka path

```text
LOCAL_COPY_REQUIRED
→ KAFKA_PROFILE_SUFFICIENT
→ LOCAL_PAYLOAD_REMOVED
```

Before removal, the transaction must revalidate retention revision, payload presence, envelope/payload digest invariant, Kafka profile currency, retention horizon, safety window, and policy.

If Kafka profile degrades before removal:

```text
KAFKA_PROFILE_SUFFICIENT
→ LOCAL_COPY_REQUIRED
```

with new revision and audit.

After payload removal, the system cannot pretend the local copy still exists. Degradation becomes health/operational risk, not a fake state rollback.

## 25.4 P0 projector path

For P0, preferred removal basis is:

```text
REQUIRED_PROJECTORS_CONFIRMED
```

Flow:

```text
local payload
→ Kafka RF=5 confirmed
→ projector writes immutable event archive and projection
→ projector ACK reaches Agent and is persisted
→ aggregate projection state CONFIRMED
→ retention basis REQUIRED_PROJECTORS_CONFIRMED
→ local payload eligible for removal
```

The Projector must archive immutable event data sufficient for recovery before ACK.

## 25.5 Retention transaction

```text
BEGIN
→ validate retention_revision
→ validate payload_presence = PRESENT_LOCALLY
→ validate selected retention_basis
→ validate safety window
→ validate profile/projection aggregate
→ logically delete EventPayloadBlob
→ update PayloadRetentionState
→ increment retention_revision
→ calculate retention_snapshot_checksum
COMMIT
```

Physical page reclamation may occur later through checkpoint/compaction.

---

# 26. Local journal

## 26.1 Candidate

SQLite WAL remains the initial journal candidate and is `EXPERIMENTAL` until benchmark and crash testing.

Candidate configuration:

```text
journal_mode=WAL
synchronous=FULL
foreign_keys=ON
bounded busy timeout
explicit checkpoint policy
explicit compaction policy
```

One dedicated writer thread owns the write connection.

No blocking SQLite work runs on Tokio worker threads.

## 26.2 Redis

Redis is not an authoritative durability component.

It may later be used for non-authoritative cache, hints, rate limits, or presence.

## 26.3 Logical schema groups

- immutable envelope table;
- payload blob table;
- delivery state table;
- retention state table;
- projection tracking table;
- bounded attempt summary;
- bounded recent attempts;
- producer sequence state;
- stream heads;
- retry scheduler;
- Kafka profiles;
- projection policies;
- ACK consumption records;
- operational audit;
- pruning/retention metadata.

## 26.4 Required constraints

```text
UNIQUE(installation_id, event_id)
```

```text
UNIQUE(
  installation_id,
  producer_id,
  producer_instance_id,
  producer_operation_sequence
)
```

```text
UNIQUE(
  installation_id,
  node_id,
  namespace,
  logical_stream_type,
  stream_key,
  stream_sequence
)
```

---

# 27. Projection integrity and event archive

## 27.1 Projector transaction

Checksum and schema may be validated before the transaction.

Critical sequence decisions occur inside PostgreSQL:

```text
BEGIN
→ lock/CAS projection stream state
→ validate expected sequence
→ validate existing sequence association
→ validate event-dedup association
→ insert sequence association
→ insert event-dedup association
→ write immutable event archive if required
→ apply domain projection
→ update domain version
→ update checkpoint
→ insert projection ACK outbox row when required
COMMIT
→ commit Kafka offset
```

## 27.2 Sequence association constraint

```text
UNIQUE(
  installation_id,
  projection_id,
  node_id,
  namespace,
  logical_stream_type,
  stream_key,
  stream_sequence
)
```

This ensures one sequence maps to at most one event.

## 27.3 Event dedup constraint

```text
UNIQUE(
  installation_id,
  projection_id,
  event_id
)
```

This ensures one event is applied at most once per projection.

## 27.4 Conflict rules

```text
same sequence + same event_id + same checksum
→ duplicate
```

```text
same sequence + different event_id/checksum
→ INTEGRITY_CONFLICT
```

```text
same event_id + same checksum + same sequence
→ duplicate
```

```text
same event_id + different checksum or different sequence
→ INTEGRITY_CONFLICT
```

## 27.5 Association retention

Operational associations must be retained for at least:

```text
maximum Kafka replay horizon
+ operator recovery window
+ safety margin
```

Pruning after that requires checkpoint/snapshot barrier, proof older replay is unsupported, compaction audit, retained dedup guarantee, or external archive with lookup.

Critical projectors needing indefinite history must use capacity-managed archival storage, not an unbounded hot table.

---

# 28. Projection ACK outbox and ACK consumption

## 28.1 Outbox identity

```text
installation_id
event_id
projector_id
projection_policy_id
projection_policy_version
```

## 28.2 Transactional outbox

The Projector inserts an ACK outbox row in the same PostgreSQL transaction as projection and checkpoint.

If the Projector crashes after DB commit and before ACK publish, the outbox still exists.

## 28.3 ACK publisher

```text
select pending ACK outbox
→ publish to ACK topic
→ Kafka confirm
→ mark outbox published
```

Crash after publish before mark causes duplicate ACK. The Agent deduplicates.

## 28.4 ACK transport profile

Projection ACK topics require a profile:

```text
ProjectionAckTransportProfile
- profile_id
- profile_version
- replication_factor
- min_insync_replicas
- required_acks
- cleanup_policy
- retention_ms
- retention_bytes
- maximum_supported_agent_outage
- operator_recovery_window
- ack_replay_window
- topic_deletion_policy
```

Rule:

```text
ACK topic retention horizon
>
maximum supported Agent outage
+ operator recovery window
```

For P0, use RF=5/minISR=5/acks=all or an explicitly accepted equivalent profile.

Published outbox rows are retained for at least `ack_replay_window`.

## 28.5 Agent ACK consumption

The Agent must persist ACK effects before committing ACK Kafka offset:

```text
consume ACK
→ validate installation/event/policy/projector
→ validate ACK checksum

BEGIN local journal transaction
→ deduplicate ACK identity
→ detect same identity with conflicting checksum
→ read ProjectionTrackingState
→ update required projector status
→ recompute aggregate projection state
→ increment projection_revision exactly once
→ compute projection_snapshot_checksum
→ persist ACK consumption record
→ persist ProjectionTrackingState
COMMIT with durability barrier

→ commit ACK Kafka offset
```

Crash after local commit and before offset commit:

```text
ACK redelivered
→ consumed identity exists
→ same checksum
→ duplicate
→ projection_revision does not advance again
```

## 28.6 ACK consumption retention

ACK-consumption identities must be retained for at least:

```text
ACK topic replay horizon
+ operator recovery window
+ safety margin
```

Pruning requires audit and a recovery barrier.

---

# 29. PostgreSQL durability profile

For P0 projection/archive:

- synchronous commit enabled;
- primary plus synchronous standby or stronger;
- WAL archiving;
- point-in-time recovery;
- checksums where supported;
- tested backups;
- restore drills;
- separate storage failure domains;
- alerting on replica lag and archive failure.

The Projector must not ACK P0 projection unless the PostgreSQL commit meets the required database durability profile.

---

# 30. Query Service, strict freshness, and efficient reads

## 30.1 Read path and authority

Plugins must never connect directly to PostgreSQL and must never submit arbitrary SQL.

```text
Writes:
Paper domain plugin AdapterClass/generated domain client
→ CraftRelayPaperBridge
→ CraftRelay Agent
→ journal
→ Kafka
→ Projector
→ PostgreSQL

Reads:
Paper domain plugin AdapterClass/generated domain client
→ CraftRelayPaperBridge
→ CraftRelay Query Service
→ PostgreSQL projection tables
```

The Query Service is a separate read-only process. It does not proxy through the Agent and cannot consume resources reserved for durable writes.

It reads only:

- typed projection tables;
- projection checkpoints;
- immutable event-archive metadata where explicitly required;
- no Agent SQLite/WAL tables;
- no arbitrary plugin-provided table names, columns, joins, predicates, or ordering expressions.

PostgreSQL projection tables are the authoritative read models. Kafka is the durable ordered event log. The Agent journal is the write/recovery authority. The Query Service is the controlled read gateway.

## 30.2 Exact meaning of latest

No response can remain the newest forever after delivery because another event may commit immediately afterwards.

CraftRelay therefore defines `STRICT_LATEST_COMMITTED` as a per-partition minimum-freshness guarantee, not as a fictional cluster-wide instant:

> For every partition represented in the captured barrier vector, the returned PostgreSQL snapshot includes the effects of every read-committed Kafka record whose offset is strictly lower than that partition's `required_next_offset`.

The returned PostgreSQL snapshot may include effects newer than the barrier. The barrier defines a minimum, not a maximum.

There is no total ordering between Kafka partitions and no claim of an atomic cluster-wide Kafka snapshot.

A strict query must not:

- return a cached older snapshot as current;
- silently downgrade to stale data;
- use an asynchronous PostgreSQL replica that has not proven the requirement;
- return `NOT_FOUND` before all required barrier components are reached;
- claim freshness from wall-clock age;
- treat `captured_at` as the authority of the barrier.

For continuously displayed authoritative data, a strict initial fetch is followed by a bounded watch. If the watch is detached, incomparable, invalidated, or lost, the display must refetch, close, or visibly mark itself as not current.

## 30.3 Typed QueryDefinition authority

Every query is predeclared and versioned:

```text
QueryDefinition
- query_id
- query_definition_version
- projection_id or projection_set
- projection_topology_version
- routing_version
- request schema
- response schema
- authorization policy
- consistency policy
- partition/barrier scope
- maximum partitions per barrier
- maximum result rows
- maximum result bytes
- timeout floor and ceiling
- cache policy
- watch policy
- blocked-projection policy
```

Examples:

```text
GetPlayerSnapshot
GetEconomyAccount
GetGuildSnapshot
GetLandOwnership
GetMailboxSnapshot
```

No generic SQL endpoint exists.

Core v1 queries use a Protobuf `oneof` with concrete request messages. Extensible module queries may use a registered `parameter_schema_id`, `parameter_schema_version`, and serialized typed parameters, but raw untyped bytes without a registered schema are forbidden.

## 30.4 Consistency modes

```text
STRICT_LATEST_COMMITTED
AT_LEAST_TOKEN
ALLOW_STALE
```

### STRICT_LATEST_COMMITTED

Required for:

- money;
- ownership;
- premium state;
- unique items;
- permissions;
- authoritative player state;
- gameplay decisions;
- authoritative player/admin displays.

It captures a read-committed partition barrier vector and fails closed if the required vector cannot be reached before the effective timeout.

### AT_LEAST_TOKEN

Used for read-your-write and read-after-projection.

The Query Service returns a snapshot only when the authenticated token requirements are satisfied.

For entity-scoped writes this is normally more efficient than capturing a new multi-partition strict barrier.

### ALLOW_STALE

Permitted only for explicitly non-authoritative views such as telemetry or selected rankings.

The response must be marked `STALE_ACCEPTED` and include version/barrier metadata. It must never be chosen automatically as fallback from a strict request.

## 30.5 Kafka offset types and exclusive semantics

Kafka offsets are not application sequences.

Valid Kafka offset range:

```text
0 .. INT64_MAX
```

Zero is valid for an empty partition or the initial consumer position.

Use exclusive next-offset semantics on both sides:

```text
PartitionBarrier.required_next_offset
ProjectionCheckpoint.next_offset_to_resolve
```

Barrier condition:

```text
checkpoint.next_offset_to_resolve
>=
barrier.required_next_offset
```

Example:

```text
resolved offsets: 0..9
next_offset_to_resolve = 10
required_next_offset = 10
→ reached
```

Empty partition:

```text
required_next_offset = 0
next_offset_to_resolve = 0
→ reached
```

`next_offset_to_resolve` means:

> Every Kafka offset strictly lower than this position has been resolved according to the projection policy.

Resolved for authoritative projections means one of:

- applied successfully;
- recognized as an exact valid duplicate;
- passed as a verified non-delivered/read-committed Kafka gap while advancing the consumer position under a verified Kafka-client contract.

For authoritative projections, the following do **not** satisfy strict freshness and do **not** advance authoritative completeness:

- poison event;
- schema incompatibility;
- integrity conflict;
- unresolved domain error;
- operator pause before resolution.

For v1, `QUARANTINE_AND_CONTINUE` is forbidden for projections used by authoritative reads. Such events move the checkpoint into a blocked state and cause strict queries to return `PROJECTION_BLOCKED` rather than a false `STRICT_FRESH`.

Non-authoritative analytics projections may later define quarantine-and-continue behavior by ADR, but they must not serve money, ownership, premium, permissions, inventory/unique-item, or critical player-state reads.

The checkpoint is not merely “the last applied event offset.” Kafka offsets may contain gaps, transaction/control effects, and records hidden by read-committed isolation.

## 30.6 ProjectionBarrier vector

Conceptual barrier:

```text
ProjectionBarrier
- barrier_version
- barrier_id
- installation_id
- query_id
- query_definition_version
- projection_topology_version
- routing_version
- partition_set_checksum
- captured_at
- repeated PartitionBarrier
- barrier_checksum
```

```text
PartitionBarrier
- topic
- partition
- required_next_offset
```

`captured_at` is observability metadata only.

The partition vector and its versions/checksum are authoritative.

For an entity-scoped query, the QueryDefinition maps the canonical entity/stream key to the exact relevant partition set, preferably one partition.

For aggregate queries, the barrier may include multiple partitions. Global barriers require explicit definitions, strict partition-count limits, and separate performance budgets.

If, while processing the query, any of these changes:

- QueryDefinition version;
- projection topology version;
- routing version;
- expected partition set;
- partition-set checksum;

then the barrier is invalid:

```text
BARRIER_INVALIDATED
```

The Query Service may recapture and restart within the remaining timeout; otherwise it fails explicitly.

## 30.7 Read-committed barrier capture adapter

Sprint 0 must include a compile-tested Rust spike named conceptually:

```text
BarrierCaptureAdapter
```

It must prove that the selected Rust Kafka stack can obtain the read-committed next offset/Last Stable Offset for each required `TopicPartition`.

Acceptance requirements:

- returns `required_next_offset` per partition;
- uses `READ_COMMITTED`/LSO semantics;
- does not substitute ordinary high watermarks;
- supports empty partitions;
- covers open transactions;
- covers offset gaps;
- has bounded timeout and partition count;
- does not materialize event payloads merely to capture the barrier;
- is compile-tested and integration-tested against real Kafka.

Acceptable implementation outcomes after the spike:

1. a safe public API in the selected Rust Kafka client;
2. a minimal safe wrapper over a proven librdkafka API;
3. a dedicated read-committed consumer-position mechanism that proves the required semantics;
4. selection of another Rust client with the necessary capability.

The architecture must not assume this capability without executable proof.

## 30.8 Projector checkpoint contract

Every Projector transaction updates its checkpoint in the same PostgreSQL transaction as:

- sequence association;
- event dedup association;
- immutable archive write where required;
- domain projection;
- domain version;
- ACK outbox.

Checkpoint identity:

```text
installation_id
projection_id
topic
partition
```

Checkpoint state:

```text
next_offset_to_resolve
checkpoint_revision
checkpoint_checksum
projection_topology_version
routing_version
checkpoint_state
blocked_at_offset
block_reason_code
state_revision
state_checksum
diagnostic_id
updated_at
```

`ProjectionCheckpointState`:

```text
RUNNING
BLOCKED_POISON
BLOCKED_INTEGRITY_CONFLICT
BLOCKED_GAP
PAUSED_OPERATOR
```

A checkpoint must never advance past an unresolved authoritative record according to the projection policy.

The Projector must persist enough evidence to distinguish:

- applied;
- exact duplicate;
- verified non-delivered Kafka gap;
- poison;
- schema incompatibility;
- integrity conflict;
- unresolved domain error;
- operator pause.

Query behavior:

```text
checkpoint behind + checkpoint_state = RUNNING
→ wait until timeout/deadline

checkpoint behind + blocked state
→ PROJECTION_BLOCKED
```

`INTEGRITY_CONFLICT`, poison, schema incompatibility, unresolved domain errors, and operator pauses block authoritative checkpoint advancement.

### Checkpoint-only transaction

Kafka read-committed position may advance over offsets that do not produce a visible domain record. The Projector therefore needs a checkpoint-only transaction for verified non-delivered gaps or similar non-domain advancement.

```text
BEGIN
→ verify checkpoint revision
→ verify no visible unresolved record below candidate offset
→ persist resolution evidence
→ update next_offset_to_resolve
→ increment checkpoint_revision/state_revision
→ recompute checkpoint_checksum/state_checksum
COMMIT
```

This transaction does not alter domain rows, does not increment domain version, does not create mutation references, and does not emit an ACK for a non-existent mutation. It only records verified contiguous checkpoint advancement.

## 30.9 Strict query algorithm

For `STRICT_LATEST_COMMITTED`:

```text
1. authenticate and authorize request
2. resolve exact QueryDefinition and versions
3. derive exact partition set
4. capture read-committed ProjectionBarrier vector
5. wait outside PostgreSQL, with bounded timeout, until every required checkpoint reaches its component
6. BEGIN READ ONLY REPEATABLE READ on PostgreSQL primary
7. read QueryDefinition/topology metadata and checkpoint rows in the transaction snapshot
8. verify versions, partition set, checksums, and every checkpoint requirement
9. read typed domain rows in the same transaction snapshot
10. validate result count, bytes, domain versions, and checksums
11. COMMIT/END read transaction
12. return snapshot, barrier vector, checkpoint vector, and freshness metadata
```

If a checkpoint is not reached inside the PostgreSQL snapshot, or the barrier is invalidated, the transaction is rolled back and the bounded wait/recapture logic continues only within the remaining timeout.

No Kafka wait, network retry, or watch wait occurs inside the PostgreSQL transaction.

## 30.10 PostgreSQL primary and replicas

For v1:

- strict reads use the PostgreSQL primary;
- `AT_LEAST_TOKEN` reads use the primary;
- asynchronous replicas may serve only `ALLOW_STALE` queries;
- a synchronous replica may serve strict reads only after a future ADR proves barrier visibility and snapshot semantics.

CraftRelay prefers an explicit failure over returning unproven freshness.

## 30.11 Authenticated ProjectionConsistencyToken

A checksum detects accidental corruption but does not authenticate a client-supplied token.

For v1, the Agent issues an authenticated HMAC token after it durably consumes the required Projector ACK.

Conceptual token:

```text
ProjectionConsistencyToken
- token_version
- installation_id
- issuer_agent_id
- authenticated_producer_id
- event_id
- query_id or permitted query scope
- query_definition_version
- projector_id
- projection_id
- projection_policy_id
- projection_policy_version
- projection_topology_version
- routing_version
- bounded repeated ProjectionMutationRef
- issued_at
- expires_at
- key_id
- canonical_payload_checksum
- token_mac
```

```text
ProjectionMutationRef
- projection_id
- entity_type
- entity_id
- domain_version
- optional topic
- optional partition
- optional required_next_offset
```

Rules:

- HMAC-SHA-256 baseline;
- versioned/rotatable keys;
- token bound to installation and authenticated producer;
- query/entity scope limited;
- mutation references bounded;
- server-issued expiry;
- constant-time MAC verification;
- expired, cross-scope, unknown-key, or invalid-MAC tokens rejected;
- no client-provided versions or entity IDs are trusted without token verification.

Events affecting an unbounded set use a domain-specific aggregate token, never an unbounded mutation list.

V1 uses one token per projector/projection. A query that requires multiple projections carries multiple tokens with a small maximum defined by the `QueryDefinition`. CraftRelay v1 does not create ambiguous merged multi-projector tokens.

## 30.12 Token issuance and Java retrieval flow

```text
publish event
→ replicated receipt
→ Projector applies event and writes ACK outbox with mutation references
→ ACK reaches Agent
→ Agent persists ACK consumption and ProjectionTrackingState
→ Agent issues authenticated ProjectionConsistencyToken for that projector/projection
→ Java client retrieves one or more required tokens
→ client sends AT_LEAST_TOKEN query with bounded token list
→ Query Service verifies token and waits only for its bounded requirements
```

Required Java API:

```java
CompletionStage<ProjectionConsistencyToken> awaitProjectionToken(
    UUID eventId,
    String projectorId,
    String projectionId,
    Duration timeout
);

CompletionStage<List<ProjectionConsistencyToken>> awaitProjectionTokens(
    UUID eventId,
    Collection<ProjectionRequirement> requirements,
    Duration timeout
);
```

`GetPublishStatus` may also include an optional latest token already available for the authenticated producer.

Token tracking is bounded and may return `TRACKING_DETACHED`; the durable token/status remains retrievable through status lookup while retained by policy.

## 30.13 Query protocol

Core conceptual request:

```protobuf
message QueryRequest {
  string query_id = 1;
  int32 query_definition_version = 2;
  QueryConsistency consistency = 3;
  repeated ProjectionConsistencyToken minimum_tokens = 4;
  int64 timeout_millis = 5;

  oneof query {
    GetPlayerSnapshotRequest get_player_snapshot = 10;
    GetEconomyAccountRequest get_economy_account = 11;
    GetGuildSnapshotRequest get_guild_snapshot = 12;
    GetLandOwnershipRequest get_land_ownership = 13;
    GetMailboxSnapshotRequest get_mailbox_snapshot = 14;
  }
}
```

Effective timeout:

```text
minimum of:
- requested timeout_millis
- remaining gRPC deadline
- QueryDefinition timeout ceiling
```

The server measures duration with a monotonic clock. Client wall-clock time is not authoritative.

Response metadata includes:

```text
query_id
query_definition_version
installation_id
freshness_result
projection_barrier
checkpoint_vector
domain version or mutation versions
projected_at
snapshot_checksum
served_from
```

Results:

```text
STRICT_FRESH
AT_LEAST_TOKEN_REACHED
STALE_ACCEPTED
FRESHNESS_NOT_REACHED
VERSION_NOT_REACHED
BARRIER_INVALIDATED
PROJECTION_BLOCKED
TIMEOUT
UNAVAILABLE
NOT_FOUND
QUERY_OVERLOADED
TRACKING_DETACHED
INTEGRITY_CONFLICT
```

`PROJECTION_BLOCKED` distinguishes ordinary lag from poison, integrity conflict, paused partition, or operator-required repair.

`NOT_FOUND` under strict consistency is valid only after the complete barrier vector is reached and validated in the PostgreSQL snapshot.

## 30.14 Cache dominance

The Query Service cache is not authoritative.

Cache key includes:

```text
installation_id
query_id
query_definition_version
projection/entity identity
canonical parameters
projection_topology_version
routing_version
partition_set_checksum
```

Cached value includes:

```text
typed snapshot
snapshot checksum
domain version
validated checkpoint vector
validated barrier vector
projected_at
expiry
```

A cached strict snapshot dominates a required barrier only when:

- installation matches;
- query ID and definition version match;
- topology and routing versions match;
- partition sets are exactly equal;
- for every partition, cached `next_offset_to_resolve >= required_next_offset`;
- all token/domain-version requirements are met;
- the cache entry was created from a successfully validated strict PostgreSQL snapshot.

Every cache has count, byte, TTL, per-installation, and per-query limits.

## 30.15 Bounded snapshot watching

Watches are transport optimizations, not durable truth.

Authoritative display flow:

```text
strict initial fetch
→ bounded watch
→ monotonic updates
→ strict refetch/close/not-current on detach or ambiguity
```

Two watch types exist.

### EntityVersionWatch

For a query with one total `domain_version`:

```text
lower version
→ ignore

same version + same checksum
→ duplicate

same version + different checksum
→ INTEGRITY_CONFLICT

higher version + valid snapshot
→ replace
```

### BarrierVectorWatch

For aggregates or multi-partition queries, ordering uses a checkpoint vector.

A new vector dominates an old vector when:

```text
for every partition: new >= old
and for at least one partition: new > old
```

Rules:

```text
same vector + same checksum
→ duplicate

same vector + different checksum
→ INTEGRITY_CONFLICT

new vector dominates old
→ replace

vectors incomparable
→ detach
→ strict refetch
```

Comparison also requires identical:

- installation;
- query ID/version;
- projection topology version;
- routing version;
- partition set.

Required bounds:

```text
max_active_snapshot_watches
max_watches_per_producer
max_watch_waiters
max_watch_buffer_events
max_watch_buffer_bytes
watch_ttl
watch_idle_timeout
```

After detach, a critical UI must refetch, close, or visibly indicate that the value is no longer guaranteed current.

## 30.16 Query Service resource isolation

Required limits:

```text
max_db_connections
max_pending_queries
max_barrier_waiters
max_barrier_partitions_per_query
max_query_result_rows
max_query_result_bytes
max_queries_per_producer
query_timeout_floor
query_timeout_ceiling
barrier_wait_timeout
cache_max_entries
cache_max_bytes
single_flight_max_waiters
max_token_mutation_refs
max_token_bytes
```

Queue exhaustion returns `QUERY_OVERLOADED`.

No query overload may affect the Agent or Projector.

## 30.17 Efficiency and fast paths

CraftRelay must not use a global strict barrier for every read.

Preferred strategy:

```text
entity-scoped strict fetch
+ one relevant partition whenever possible
+ AT_LEAST_TOKEN after own writes
+ bounded watch while UI is open
+ precomputed aggregates for global views
```

Fast path for a healthy entity query:

```text
resolve QueryDefinition
→ derive one partition
→ capture one required_next_offset
→ checkpoint already reaches it
→ short PostgreSQL REPEATABLE READ
→ indexed row lookup
→ Protobuf response
```

Mandatory optimizations:

- persistent Kafka and PostgreSQL clients;
- no client creation per query;
- partition derivation without scanning Kafka;
- checkpoint waiting outside DB transaction;
- short read-only transactions;
- prepared statements;
- exact indexes;
- notification-based wake-up as a hint, followed by authoritative checkpoint validation;
- safe single-flight for identical query/barrier work;
- bounded cache with formal dominance;
- precomputed/materialized aggregates;
- no polling every Minecraft tick;
- no N+1 query loops for player lists.

Suggested indexes:

```text
PRIMARY KEY (
  installation_id,
  projection_id,
  topic,
  partition
)
```

and entity-specific indexes such as:

```text
PRIMARY KEY (
  installation_id,
  entity_id
)
```

## 30.18 Performance and capacity gates

No latency number is a guarantee until measured on documented hardware.

Sprint 12 must benchmark at minimum:

```text
10 concurrent players
20 concurrent players
50 concurrent players
100 concurrent players
```

For each level, measure:

- authoritative reads per second;
- writes per second;
- strict entity-query p50/p95/p99;
- `AT_LEAST_TOKEN` p50/p95/p99;
- barrier-capture p50/p95/p99;
- checkpoint-wait latency;
- projection lag;
- PostgreSQL transaction latency;
- Kafka publish latency under RF=5/minISR=5;
- active/peak watches;
- watch-detach rate;
- timeout/overload rate;
- CPU, RAM, network, disk IO, WAL growth, Kafka disk growth;
- query partitions per request;
- cache hit rate with valid dominance;
- single-flight collapse ratio.

Benchmark scenarios:

1. steady-state healthy infrastructure;
2. burst login/player-load;
3. many players opening critical UIs simultaneously;
4. read-after-write bursts;
5. one Kafka broker slow but still in ISR;
6. Projector lag;
7. PostgreSQL pressure;
8. watch reconnect storm;
9. aggregate query load;
10. failure/repair recovery.

Capacity must be based on measured workload events per player, not merely player count.

The release profile must document:

```text
maximum supported players
maximum supported authoritative reads/s
maximum supported writes/s
maximum supported active watches
maximum supported barrier partitions/query
hardware and network assumptions
headroom target
```

Recommended production headroom is at least 2x the measured peak expected workload, with separate emergency capacity for P0 operations.

## 30.19 CraftMMO display and feedback policy

Default policy:

- authoritative player-facing values use `STRICT_LATEST_COMMITTED` for the initial fetch;
- decisions following a write use `AT_LEAST_TOKEN`;
- critical values use bounded watches while displayed;
- stale fallback is prohibited for money, ownership, premium, permissions, authoritative inventory/unique-item state, and critical player state;
- rankings and telemetry may explicitly use `ALLOW_STALE` and must be labeled;
- detached/incomparable watches trigger refetch, close, or visible not-current state;
- visual-only feedback such as XP bars, action bars, and transient progress indicators may be local-first when the domain marks the value as optimistic/local and reconciles against projected authority later.

The system must never silently display an old authoritative value as current.

For CraftMMO progression features, the recommended UX path is:

```text
Query Service initial snapshot
→ local in-memory player progression cache
→ immediate local XP/progress bar update on gameplay action
→ async batched ProgressionDelta publish
→ Projector authority in PostgreSQL
→ bounded watch/query reconciliation
```

The Query Service is used for initial fetch, menus, reconciliation, admin/debug views, and read-your-write for critical effects. It is not used as a per-action render loop.


---

# 31. Security

CraftRelay protects against unauthorized external processes, unregistered producers, accidental cross-namespace access, invalid credentials, and cross-installation identity collisions.

It does not strongly isolate a malicious plugin inside the same JVM.

Rule:

> Untrusted plugins must not share a JVM with critical CraftRelay producers.

Early baseline:

- loopback bind;
- producer authentication;
- credential loading;
- authorization;
- secret redaction;
- security logs;
- size limits;
- bounded startup timeout;
- authentication and authorization tests.

Later:

- mTLS or protected IPC;
- rotation;
- Kafka ACLs;
- PostgreSQL TLS/roles;
- signed configuration.

---

# 32. Observability

## Agent metrics

- publish attempts;
- local acceptances;
- lifecycle revisions;
- stale update ignores;
- lifecycle integrity conflicts;
- required durability completions;
- tracking detached results;
- active handles/subscriptions;
- queue depth;
- journal latency;
- disk headroom;
- Kafka pending/retrying;
- DELIVERY_BLOCKED streams;
- payload retention states;
- ACK consumption lag;
- projection revision updates;
- profile drift;
- requested/effective flush policy;
- journal batch records/bytes/age;
- flush reason;
- policy clamp/rejection count;
- per-producer batch queue depth;
- reserved capacity usage.

## Projector metrics

- consumer lag;
- projection latency;
- duplicate events;
- sequence conflicts;
- event-dedup conflicts;
- gaps;
- integrity conflicts;
- transaction retries;
- outbox pending/published/failed;
- ACK publish retries;
- archive writes;
- DB microbatch records/bytes/age;
- checkpoint-only advancement count;
- semantic coalescing/projector delta counts where applicable.

## Query metrics

- requests by consistency mode;
- barrier capture latency;
- barrier partition count;
- checkpoint wait latency;
- strict freshness success/failure;
- `AT_LEAST_TOKEN` success/failure;
- `FRESHNESS_NOT_REACHED`;
- stale responses;
- snapshot watch count/detach/conflicts;
- cache hit/miss by freshness eligibility;
- PostgreSQL primary pool usage;
- query result rows/bytes;
- overload rejections;
- circuit state.

---

# 33. Repository structure

Sprint 0 creates only the minimum useful structure:

```text
CraftRelay/
├─ Cargo.toml
├─ Cargo.lock
├─ rust-toolchain.toml
├─ deny.toml
├─ MASTER_PLAN.md
├─ PluginAdapter.md
├─ PluginUsage.md
├─ README.md
├─ AGENTS.md
├─ CONTRIBUTING.md
├─ SECURITY.md
├─ CHANGELOG.md
├─ VERSION_SOURCES.md
│
├─ proto/
│  └─ craftrelay/v1/
│
├─ crates/
│  ├─ craftrelay-domain/
│  ├─ craftrelay-protocol/
│  ├─ craftrelay-agent/
│  └─ craftrelay-testkit/
│
├─ java/
│  ├─ pom.xml
│  ├─ mvnw
│  ├─ mvnw.cmd
│  ├─ .mvn/
│  ├─ craftrelay-client-core/
│  ├─ craftrelay-paper-api/
│  ├─ craftrelay-paper-bridge/
│  ├─ craftrelay-java-integration-tests/
│  └─ integrations/
│     ├─ craftrelay-reference-domain-client/
│     └─ craftrelay-reference-paper-fixture/
│
├─ deployment/
│  └─ compose/
│
├─ scripts/
│  ├─ New-AuditBundle.ps1
│  ├─ Test-AuditBundle.ps1
│  ├─ Start-DevStack.ps1
│  └─ Stop-DevStack.ps1
│
├─ tests/
│  ├─ fixtures/
│  └─ compatibility/
│
└─ docs/
   ├─ architecture.md
   ├─ threat-model.md
   ├─ protocol.md
   ├─ canonicalization.md
   ├─ lifecycle.md
   ├─ payload-retention.md
   ├─ kafka-zero-loss-profile.md
   ├─ projector-outbox.md
   ├─ query-consistency.md
   ├─ plugin-adapter.md
   ├─ plugin-usage.md
   ├─ integration-manifest.md
   ├─ paper-threading.md
   ├─ failure-matrix.md
   ├─ testing.md
   ├─ operations.md
   └─ adrs/
```

Future crates and modules are created only when they gain real behavior.


# 34. ADR lifecycle

States:

```text
PROPOSED
EXPERIMENTAL
ACCEPTED
SUPERSEDED
REJECTED
```

## 34.1 ACCEPTED in Sprint 0

1. System boundaries and process topology.
2. Threat model and RPO=0 wording.
3. Installation-scoped identities and UUIDv7.
4. Positive signed numeric types.
5. Immutable envelope, payload blob, and revisioned mutable states.
6. Request fingerprint and envelope checksum.
7. Snapshot checksum and revision integrity.
8. Revisioned publish lifecycle and bounded Java tracking.
9. Rejection taxonomy and indeterminate transport.
10. Static NODE_LOCAL ownership.
11. Logical stream versus physical Kafka routing.
12. Single-event atomicity and no cross-publish atomicity.
13. Kafka RF=5/minISR=5 zero-loss production profile.
14. Kafka retention and topic protection.
15. Payload retention basis and removal transaction.
16. Transactional projector sequencing and event dedup constraints.
17. Transactional projection ACK outbox.
18. Agent ACK-consumption transaction before offset commit.
19. PostgreSQL durability profile for P0 projection/archive.
20. Strict query freshness and exclusive Kafka next-offset barriers.
21. Versioned barrier vectors, topology invalidation, and PostgreSQL snapshot validation.
22. Authenticated projection consistency tokens and read-your-write.
23. Bounded entity-version and barrier-vector watches.
24. Query fast paths, resource isolation, and capacity gates.
25. Batching, semantic coalescing, and flush-policy contract.
26. Audit workflow including unborn HEAD.
27. Trust boundaries.
28. One shared `CraftRelayPaperBridge` per Paper server.
29. Embedded first-party domain AdapterClasses/generated clients.
30. Logical producer sessions, service resolution, and `IntegrationManifest`.
31. Paper/Folia threading, callback, classloader, and dependency boundaries.

## 34.2 PROPOSED/EXPERIMENTAL

- SQLite final backend choice;
- monolithic versus segmented journal;
- advanced batching algorithms beyond the accepted flush/coalescing contract;
- journal compaction;
- association compaction;
- query cache implementation details;
- synchronous-replica strict-read support;
- fairness;
- SLOs;
- global ownership;
- online routing migration;
- atomic batch;
- telemetry API;
- multi-datacenter deployment.

---

# 35. Version and tool policy

`VERSION_SOURCES.md` records component, selected version, official source, release date, reason, compatibility notes, security/durability notes, and date checked.

Include Rust, Tokio, tonic/prost, SQLite binding/runtime, Kafka Rust client and its read-committed offset API, Kafka broker, PostgreSQL, PostgreSQL Rust driver/pool, Java, Maven, gRPC Java, Protobuf, Buf, OpenTelemetry, Prometheus, `cargo-audit`, and `cargo-deny`.

Rules:

- versions pinned;
- no Docker `latest`;
- `Cargo.lock` generated and left in the uncommitted working tree;
- Codex never commits;
- audit tools are never installed silently;
- missing mandatory tools fail validation.

---

# 36. Sprint plan

## Sprint 0 — Foundation and contracts

Deliver minimal Rust workspace, minimal Java reactor, `craftrelay-client-core`, `craftrelay-paper-api`, non-functional `craftrelay-paper-bridge` skeleton, `PluginAdapter.md`, `PluginUsage.md`, reference `IntegrationManifest` schema, protocol skeleton, accepted foundational ADRs, immutable/mutable state models, lifecycle and retention revisions, payload-retention contract, batching/flush-policy contract, Kafka zero-loss profile contract, ACK outbox and ACK consumption contracts, exclusive next-offset read barriers, versioned barrier vectors, authenticated per-projector consistency-token contracts, typed query/watch contracts, a compile-tested Rust `BarrierCaptureAdapter` spike against real Kafka, canonicalization spec, shared test vectors, version sources, CI, Compose skeleton, functional audit scripts, no functional journal/projector/query behavior, no functional batching engine beyond contracts/tests, and no real durable receipt.

## Sprint 1 — Protocol and Java client

Implement request/envelope/state models, metadata entries, handshake, installation scope, UUIDv7 validation, positive `int32`/`int64` validation, lifecycle snapshots/revisions/checksums, bounded `PublishHandle`, status lookup, retry/reconnect behavior, the shared Bridge transport runtime, Paper service registration, logical producer clients, one reference AdapterClass/generated domain client, concrete typed query requests, versioned `ProjectionBarrier`, authenticated per-projector `ProjectionConsistencyToken`, token retrieval API, bounded entity/barrier-vector watch interfaces, fake Agent/Query Service fixtures, and security/metrics baseline.

## Sprint 2 — Policy, admission, ownership

Implement producer registration, Bridge-to-Agent logical producer authentication, `IntegrationManifest` validation, credentials, ACL, policy resolution, ownership snapshot validation, quotas, bounded ingress, reserved P0 local capacity, and profile validation skeletons.

## Sprint 3 — Journal benchmark spike

```text
gRPC ingress
→ bounded channel
→ SQLite candidate
→ immutable envelope + payload blob + initial state transaction
→ synthetic lifecycle snapshot
```

Compare monolithic/segmented candidates, WAL checkpointing, compaction, large journals, Windows/Linux.

## Sprint 4 — Durable local journal

Implement schema, migrations, dedicated writer, atomic local acceptance transaction, sequence allocation inside transaction, CAS revisions, payload blob, retention state, dedup, status recovery, disk guard, bounded attempts, and corruption behavior.

## Sprint 5 — Kafka delivery

Implement installation-scoped routing, RF=5/minISR=5 production profile validation, per-stream gate, persistent retry, replicated transaction, DELIVERY_BLOCKED, profile drift, and repair.

## Sprint 6 — Projector/PostgreSQL/outbox/ACK consumption

Implement reference domain, transactional sequence validation, event-dedup constraint, immutable event archive, projection/domain versioning, per-partition exclusive `next_offset_to_resolve` checkpoints updated in the same transaction for visible events, checkpoint-only advancement for verified non-delivered gaps, persisted checkpoint blocked states, authoritative poison blocking, bounded mutation references, per-projector token material, ACK outbox, ACK publisher, ACK topic profile, Agent ACK consumption transaction before offset commit, authenticated token issuance, projection tracking, and duplicate ACK handling.

## Sprint 7 — Real plugin shadow mode

Existing persistence remains authoritative. One real first-party plugin integrates through its embedded AdapterClass/generated client and the shared `CraftRelayPaperBridge`. CraftRelay receives equivalent events behind a feature flag. Compare state, latency, disconnect behavior, Bridge restart, service-registration behavior, rollback, and API ergonomics.

## Sprint 8 — Query Service

Implement a separate read-only process with concrete typed query definitions, no arbitrary SQL, PostgreSQL-primary strict reads, exclusive read-committed barrier vectors, topology/routing invalidation, projection-checkpoint waiting, `STRICT_LATEST_COMMITTED`, authenticated `AT_LEAST_TOKEN`, explicit `ALLOW_STALE`, short read-only `REPEATABLE READ` validation, formal cache dominance, bounded entity-version/barrier-vector watches, timeout_millis semantics, `PROJECTION_BLOCKED`, authorization, freshness metadata, and fail-closed behavior.

## Sprint 9 — Hardening

Crash matrix, disk pressure, compaction, long Kafka outage, slow disk, blocked repair, backup/restore, soak, runbooks.

## Sprint 10 — Security/isolation

Fairness if justified, reserved capacity, mTLS/IPC, rotation, ACL, DB hardening.

## Sprint 11 — Operations

CLI, health endpoints, dashboards, alerts, packaging, signed artifacts, SBOM.

## Sprint 12 — Performance/release

Benchmarks at 10/20/50/100 concurrent players, shared-Bridge publish/query/watch fan-out benchmarks, JFR, Rust profiling, hardware/network matrix, strict-read and read-your-write SLOs, throughput/headroom limits, upgrade/rollback, 24h soak, and disaster recovery drill.

---

# 37. Testing requirements

## Unit

- UUIDv7;
- installation scoping;
- positive numeric ranges;
- canonicalization;
- request fingerprint;
- envelope checksum after sequence allocation;
- snapshot checksums;
- lifecycle revision conflicts;
- retention revision conflicts;
- projection revision conflicts;
- metadata duplicate rejection;
- rejection taxonomy;
- bounded tracking;
- payload retention transitions;
- policy resolution;
- ownership snapshot;
- quotas;
- projection-barrier canonicalization;
- consistency-token validation;
- strict freshness result rules;
- exclusive next-offset barrier semantics;
- barrier vector topology/routing invalidation;
- authenticated token MAC/scope/expiry validation;
- timeout_millis and monotonic server timeout rules;
- entity-version and barrier-vector watch comparison;
- incomparable vector detachment;
- snapshot-watch monotonicity and detachment.

## Cross-language

- Rust/Java fingerprints;
- UUID parsing;
- int32/int64 boundaries;
- lifecycle/revision vectors;
- payload digest/envelope checksum vectors;
- Kafka key vectors;
- installation-isolation vectors;
- same revision/different snapshot conflict vectors;
- projection-barrier/checkpoint vectors;
- consistency-token checksum vectors;
- query result freshness metadata vectors.

## Batching, coalescing, and flush policy

- requested flush criteria parse and canonicalize correctly;
- effective flush criteria are clamped or rejected according to policy mode;
- OR semantics: `max_records`, `max_bytes`, and `max_delay_millis` each independently trigger flush;
- low traffic flushes by time;
- high traffic flushes by records or bytes;
- semantic coalescing is forbidden for P0 ledger/ownership/premium/unique-item/security events;
- physical batching preserves per-event identity, fingerprints, lifecycle, ledger/audit rows, and deduplication;
- Projector microbatch preserves checkpoint, outbox, and token semantics;
- noisy producers cannot consume reserved P0 capacity by requesting aggressive flush settings;
- interactive flush is optional, explicit, scoped, bounded, and never scans Kafka payloads;
- normal stats/display commands do not force flush by default;
- XP/progress bars use local-first feedback and never fetch Query Service per tick/action;
- critical rewards/effects reconcile against projected authority or idempotency fences;
- `flush_reason`, requested/effective policy, and clamp/rejection diagnostics are observable.

## Journal

- atomic acceptance;
- no checksum before sequence allocation;
- crash before/after commit;
- envelope immutability;
- payload blob removal transaction;
- CAS revisions;
- same-ID retry;
- conflict;
- disk full;
- corruption;
- recovery;
- bounded audit pruning.

## Kafka

- broker offline;
- RF=5/minISR=5 fail-closed;
- ISR shrink;
- ordering;
- duplicates;
- profile drift;
- retention risk;
- DELIVERY_BLOCKED;
- repair.

## Projector

- sequence validation inside transaction;
- sequence conflict;
- event-dedup conflict;
- duplicate replay;
- gap;
- concurrent consumers/rebalance;
- DB rollback;
- event archive;
- ACK outbox;
- outbox retry;
- ACK retention profile;
- checkpoint and domain rows committed atomically;
- checkpoint never advances past an unapplied event;
- mutation-reference bounds.

## ACK consumption

- consume ACK then crash before local commit;
- local commit then crash before offset commit;
- duplicate ACK same checksum;
- duplicate ACK different checksum;
- projection revision increments once;
- ACK dedup retention.

## Query

- plugins cannot submit arbitrary SQL;
- installation scope and ACL;
- strict barrier capture for one and many partitions;
- read-committed LSO capture for empty partitions, open transactions, and offset gaps;
- ordinary high watermark is rejected as a strict-barrier substitute;
- read-committed exclusive next-offset semantics;
- checkpoint waiting;
- checkpoint/domain read in the same `REPEATABLE READ` snapshot;
- event committed before barrier is included;
- event committed after barrier may be excluded but is never misrepresented;
- authenticated `AT_LEAST_TOKEN` read-your-write;
- invalid MAC, expired token, cross-producer token, and cross-installation token rejection;
- Java token retrieval flow;
- topology/routing/partition-set barrier invalidation;
- timeout_millis bounded by transport deadline and query ceiling using server monotonic time;
- strict `NOT_FOUND` only after barrier reached;
- no automatic stale fallback;
- async replicas rejected for strict mode;
- bounded queues, barrier waiters, results, cache, and watches;
- cache barrier dominance;
- same snapshot version/different checksum conflict;
- entity-version watch ordering;
- barrier-vector dominance;
- incomparable barrier vectors detach and require strict refetch;
- watch disconnect/detach requires refetch;
- query overload does not affect Agent;
- process isolation.

## Paper Bridge and domain adapter

- Bridge may register `CraftRelayService` as `NOT_READY` when local config/manifests are valid but sidecars are unavailable;
- dependent plugins must not enable authoritative features until their integration is `READY`;
- missing Bridge causes explicit plugin integration failure, never silent direct fallback;
- `clientFor(plugin)` accepts no arbitrary producer string and resolves the registered manifest mapping;
- tests do not claim `clientFor(Plugin)` is a malicious same-JVM security boundary;
- duplicate integration claims fail startup validation;
- incompatible integration/client/manifest versions fail closed;
- one shared transport runtime serves multiple logical producers;
- each logical producer has a producer-instance ID and sequence allocator contract;
- reconnect keeps producer instance; full Bridge restart creates a new one;
- per-producer quotas and metrics remain isolated;
- reserved P0 local capacity prevents noisy producers from starving critical producers;
- Agent and Query Service channels fail independently;
- Bridge queues, handles, token waiters, query waiters, callback queues, and watches are bounded;
- AdapterClass creates canonical typed requests through manifest-issued handles;
- AdapterClass never chooses Kafka topic, retention, or effective durability;
- no domain plugin contains Kafka/PostgreSQL/journal dependencies;
- no domain plugin creates its own sidecar transport stack;
- no blocking wait occurs on Paper gameplay threads;
- callbacks that change Paper state are rescheduled through the accepted Paper/Folia execution context;
- Bridge disable performs bounded drain/detach and does not claim false completion;
- Paper-side callback execution is not claimed exactly-once unless the domain provides idempotency/reconciliation;
- classloader compatibility is tested with multiple domain plugins enabled simultaneously;
- third-party adapter-plugin exception is tested separately when implemented.

## Crash matrix

Kill processes before local commit, after local commit before client update, during lifecycle revision transition, after Kafka ACK before replicated transaction, after replicated transaction before update, after Projector DB commit before offset, after offset before ACK publish, after ACK publish before outbox mark, after Agent ACK local commit before ACK offset, after query barrier capture before checkpoint wait, after checkpoint reaches barrier before PostgreSQL read, during strict read transaction, during snapshot watch delivery, during retention deletion, during compaction, and during shutdown.

---

# 38. Codex rules

Codex must read `MASTER_PLAN.md` and ADRs, keep scope bounded, add tests, execute validations, report limitations, and leave changes uncommitted.

Codex must not:

- commit, push, tag, or publish;
- execute audit scripts;
- skip tests;
- create unbounded structures;
- calculate envelope checksum before sequence allocation;
- mix payload bytes into immutable envelope;
- mutate accepted envelope;
- omit installation scope;
- omit snapshot checksums;
- omit bounded tracking;
- weaken RF=5/minISR=5 production profile;
- trust producer ID from request;
- include transport context in fingerprint;
- add `BEST_EFFORT`;
- add Redis to durability path;
- add Kafka/JDBC or direct PostgreSQL access to Paper plugins;
- let first-party domain plugins instantiate their own gRPC channels, reconnect loops, or sidecar thread pools;
- create one adapter plugin per first-party domain plugin;
- put mining/economy/claims/premium domain logic inside `CraftRelayPaperBridge`;
- resolve producer identity from an arbitrary plugin-supplied string;
- bypass `craftrelay-paper-api` or the registered Bridge service;
- add arbitrary SQL endpoints;
- silently downgrade strict reads to stale data;
- use inclusive last-applied offsets instead of exclusive next offsets;
- substitute Kafka high watermarks for read-committed LSO;
- accept unauthenticated consistency tokens;
- trust client wall-clock deadlines;
- compare aggregate watches using one scalar version;
- serve strict reads from an unproven asynchronous replica;
- represent a detached watch as still current;
- use Kafka/Projector/Query Service as a per-tick XP bar, scoreboard, or UI render loop;
- make interactive flush mandatory for normal stats/display commands;
- let plugins choose effective flush policy, global priority, durability, retention, Kafka topic, projector policy, or reserved P0 capacity;
- block gameplay threads;
- implement required projection ACK without outbox;
- commit ACK Kafka offset before local ACK-consumption commit;
- validate projector sequence only outside DB transaction;
- use `unsafe` without ADR;
- use floating versions or Docker `latest`.

---

# 39. Codex prompt template

```text
You are working in:

<ABSOLUTE_REPOSITORY_PATH>

Authoritative documents:
- MASTER_PLAN.md
- AGENTS.md
- <RELEVANT ADR FILES>

Supporting guides when relevant:
- PluginUsage.md

Implement only Sprint <N>: <NAME>.

Objective:
<ONE BOUNDED OBJECTIVE>

In scope:
- ...

Out of scope:
- ...

Non-negotiable invariants:
- Durability and integrity over availability.
- No unbounded structures.
- No blocking IO on Paper gameplay threads.
- All persisted identities are installation-scoped.
- event_id is canonical UUIDv7 and stable across retries.
- Positive signed numeric types only.
- schema_version is positive int32.
- Producer identity comes from authentication.
- StoredEventEnvelope is immutable.
- Payload bytes are stored in EventPayloadBlob, not in the envelope.
- Envelope checksum is calculated inside the acceptance transaction after sequence allocation.
- Delivery, projection, and retention states are separate and revisioned.
- Same revision with different snapshot checksum is an integrity conflict.
- Client tracking is bounded and may detach.
- Retry never mutates the envelope.
- publish() succeeds only at required durability.
- P0 production profile is Kafka RF=5/minISR=5/acks=all.
- Accepted events are never later rejected.
- NODE_LOCAL ownership only.
- Logical stream identity is separate from Kafka routing.
- Independent publishes are never atomic.
- BEST_EFFORT is excluded.
- Permanent Kafka failures enter DELIVERY_BLOCKED.
- Projector sequence validation occurs inside PostgreSQL transaction.
- Projection has both sequence and event dedup constraints.
- Required ACKs use transactional outbox.
- Agent persists ACK consumption before committing ACK offset.
- One shared CraftRelayPaperBridge owns Paper-side transport resources.
- First-party plugins use embedded typed AdapterClasses/generated clients.
- Domain plugins never instantiate their own sidecar transport stack.
- The Bridge contains no domain logic and never chooses effective policy.
- Bridge readiness, classloader, producer-instance, and Paper/Folia callback contracts follow PluginAdapter.md.
- Requested flush criteria are OR-based and validated against CraftRelay effective policy.
- Plugins never access PostgreSQL directly.
- Query APIs are typed; arbitrary SQL is forbidden.
- Authoritative reads use strict barriers or a minimum consistency token.
- Strict reads never silently fall back to stale data.
- Query checkpoint and domain rows are validated in one PostgreSQL snapshot.
- Critical displays refetch or mark themselves non-current when watch tracking detaches.
- No silent confirmed-event loss or stale-authoritative display.

Required tests:
- ...

Required commands:
- cargo fmt --all -- --check
- cargo clippy --workspace --all-targets --all-features -- -D warnings
- cargo test --workspace --all-features --locked
- java\mvnw.cmd -B -ntp clean verify
- buf lint
- docker compose config --services
- git status --short --branch

Do not:
- commit, push, tag, or publish;
- execute scripts/New-AuditBundle.ps1;
- skip tests;
- install hidden global dependencies;
- modify unrelated modules.

Final response:

CRAFTRELAY SPRINT <N> RESULT
Status: PASS | PARTIAL | FAIL

Changed files
Implemented behavior
Zero-loss durability notes
Immutable/payload/state notes
Lifecycle/revision notes
Kafka profile notes
Projector transaction/outbox/ACK notes
Query freshness/barrier/token/watch notes
Tests
Commands
Verification
Known limitations
Audit command
Git confirmation
```

---

# 40. Sprint 0 Codex prompt

```text
You are working in:

<ABSOLUTE_REPOSITORY_PATH>

Create only CraftRelay Sprint 0: Foundation and Fundamental Contracts.

Authoritative sources:
- MASTER_PLAN.md
- PluginAdapter.md

Supporting guide:
- PluginUsage.md

Do not implement functional journal writes, Kafka publishing, PostgreSQL projection,
snapshot queries, ACK consumption, or real DurableReceipts.

Required:

1. Create minimal Rust workspace:
   - craftrelay-domain
   - craftrelay-protocol
   - craftrelay-agent
   - craftrelay-testkit

2. Create minimal Java reactor:
   - craftrelay-client-core
   - craftrelay-paper-api
   - craftrelay-paper-bridge
   - craftrelay-java-integration-tests
   - integrations/craftrelay-reference-domain-client
   - integrations/craftrelay-reference-paper-fixture

3. Add Maven Wrapper and verified Java toolchain.

4. Create shared Protobuf skeleton.

5. Pin Rust toolchain.

6. Generate Cargo.lock and leave it uncommitted.

7. Create Compose skeleton:
   - 5 Kafka brokers for production-like profile definition
   - PostgreSQL
   - Prometheus
   - optional Grafana profile

8. Create:
   - MASTER_PLAN.md
   - PluginAdapter.md
   - PluginUsage.md
   - AGENTS.md
   - VERSION_SOURCES.md
   - SECURITY.md
   - CONTRIBUTING.md
   - CHANGELOG.md
   - README.md
   - docs/canonicalization.md
   - docs/lifecycle.md
   - docs/payload-retention.md
   - docs/kafka-zero-loss-profile.md
   - docs/projector-outbox.md
   - docs/query-consistency.md
   - docs/plugin-adapter.md
   - docs/integration-manifest.md
   - docs/paper-threading.md
   - shared Rust/Java vectors for fingerprint, UUID, installation scope,
     payload digest, revision checksums, positive numeric boundaries,
     exclusive next-offset projection barriers, authenticated consistency tokens, and barrier-vector watch comparisons

9. Create only ACCEPTED ADRs listed in MASTER_PLAN.md.

10. Protocol/domain skeleton must distinguish:
    - StoredEventEnvelope
    - EventPayloadBlob
    - PayloadRetentionState
    - EventDeliveryState
    - ProjectionTrackingState
    - bounded attempt summaries
    - PublishLifecycleSnapshot
    - RequestedFlushCriteria and EffectiveFlushCriteria contract
    - OR flush semantics across records, bytes, age, and shutdown/drain
    - physical batching versus semantic coalescing contract
    - optional scoped interactive flush contract that never scans Kafka payloads
    - local-first XP/progress UI policy and Query Service reconciliation contract
    - event-class policy ceilings and plugin request clamp/rejection behavior
    - Projection ACK outbox contract
    - Agent ACK consumption contract
    - QueryDefinition and typed query contract
    - versioned ProjectionBarrier with required_next_offset
    - ProjectionCheckpoint with next_offset_to_resolve, blocked states, and checkpoint-only advancement contract
    - per-projector authenticated ProjectionConsistencyToken contract and bounded multi-token retrieval flow
    - bounded EntityVersionWatch and BarrierVectorWatch contracts
    - BarrierCaptureAdapter spike contract
    - CraftRelayPaperBridge service-registration and readiness contract
    - IntegrationManifest and logical producer-registration contract
    - reference embedded domain AdapterClass/generated client contract
    - Paper/Folia execution-context abstraction
    - classloader/API boundary contract
    - Bridge producer-instance and sequence ownership contract

11. Keep storage backend, batching, compaction, SLOs, global ownership,
    online routing migration, atomic batch, telemetry and fairness
    PROPOSED or EXPERIMENTAL.

12. Create CI:
    - Rust format
    - Clippy
    - Rust tests
    - Java verify
    - buf lint
    - Compose validation
    - shared vectors

13. Create fully functional Sprint 0 audit scripts:
    - New-AuditBundle.ps1
    - Test-AuditBundle.ps1

14. Audit scripts must support:
    - HEAD_STATE=UNBORN
    - HEAD_STATE=EXISTING

15. Create:
    - Start-DevStack.ps1
    - Stop-DevStack.ps1

16. Verify versions from official primary sources.

17. Create a compile-tested Rust BarrierCaptureAdapter spike against real Kafka that proves READ_COMMITTED/LSO next-offset capture for empty partitions, open transactions, and offset gaps.

18. Do not emit or simulate a real DurableReceipt.

Acceptance:

- cargo fmt passes.
- cargo clippy passes.
- cargo test passes.
- Java verify passes.
- buf lint passes.
- buf breaking is NOT_APPLICABLE.
- Compose validates.
- vectors match.
- same revision with different snapshot checksum is rejected.
- same event/stream names do not collide across installations.
- mutable fields are absent from StoredEventEnvelope.
- payload bytes are absent from StoredEventEnvelope.
- strict query requests cannot silently downgrade to stale.
- projection barriers use required_next_offset and checkpoints use next_offset_to_resolve.
- barrier vectors are versioned and invalidate on topology/routing/partition-set changes.
- consistency tokens are authenticated, scoped, expiring, per-projector/per-projection, and retrievable through the Java contract.
- requested flush criteria are validated against effective event-class policy.
- flush criteria use OR semantics across records, bytes, and time.
- interactive flush is explicit, optional, scoped, bounded, and never scans Kafka payloads.
- XP/progress bars are documented as local-first with async deltas and reconciliation, not Query Service fetch per action.
- physical batching preserves event identity, auditability, lifecycle, dedup, and ledger rows.
- semantic coalescing is rejected for P0 ledger/ownership/premium/unique-item/security events.
- timeout_millis uses server monotonic time and transport/query ceilings.
- entity-version and barrier-vector watches are explicitly bounded and detachable.
- the Bridge is the only first-party Paper-side transport owner.
- the reference first-party plugin uses an embedded typed AdapterClass/client, not a separate adapter plugin.
- producer identity is resolved from the registered Plugin/integration mapping, not request strings.
- `clientFor(Plugin)` is not documented or tested as a malicious same-JVM security boundary.
- the Bridge contains no domain logic and no Kafka/PostgreSQL/journal dependency.
- domain DTOs remain plugin-local and public API types remain API-owned/JDK-owned.
- manifest-issued EventContractHandle/QueryContractHandle are used instead of arbitrary schema strings/bytes.
- Bridge readiness supports NOT_READY and bounded reconnect without enabling authoritative domain features.
- Bridge logical producers define producer_instance_id and producer_operation_sequence ownership.
- Paper/Folia execution-context abstraction exists.
- Paper-side callbacks are not claimed exactly-once without idempotency/reconciliation.
- reserved P0 local capacity and per-producer Bridge limits are represented.
- the Rust BarrierCaptureAdapter spike retrieves read-committed next offsets/LSO and does not substitute high watermarks.
- audit scripts produce and validate a real Sprint 0 bundle.
- audit supports unborn HEAD.
- no floating versions/latest.
- no functional journal/Kafka/PostgreSQL implementation.
- no runtime durability claim.
- no unnecessary crates.
- no commits.

Final response:

CRAFTRELAY SPRINT 0 RESULT
Status: PASS | PARTIAL | FAIL

Changed files
Repository structure
Versions
ADRs
Protocol/canonicalization
Zero-loss durability profile
State separation
Payload retention
Lifecycle revisions
Installation scoping
Outbox and ACK contracts
Query freshness/barrier/token/watch contracts
CI
Compose
Audit scripts
Tests
Commands
Verification
Known limitations
Audit command
Git confirmation
```

---

# 41. Audit workflow

## 41.1 Human-controlled flow

```text
Codex reports PASS
→ human confirms required tools and Docker
→ human runs New-AuditBundle.ps1
→ Test-AuditBundle.ps1 validates
→ independent review
→ APPROVED FOR COMMIT or REJECTED FOR COMMIT
→ human commits only after approval
```

## 41.2 Unborn HEAD support

The first Sprint 0 audit may run before any commit exists.

Valid Git states:

```text
HEAD_STATE=UNBORN
HEAD_SYMBOLIC_REF=<ref>
```

or:

```text
HEAD_STATE=EXISTING
HEAD_COMMIT=<hash>
```

`git rev-parse --verify HEAD` may fail in `UNBORN` state without failing the audit if:

- the repository is a Git work tree;
- the state is recorded;
- all tracked and untracked files are inventoried;
- source snapshot is complete;
- SHA-256 manifest covers the bundle.

The audit cannot require a commit that the workflow forbids before approval.

## 41.3 New-AuditBundle.ps1

Must run all Sprint 0 checks, preserve exit codes, capture stdout/stderr, capture Git state including unborn HEAD, copy source, exclude secrets and build outputs, create and verify manifest, write `Validation failure`, return non-zero on validation failure, and create ZIP only after validation.

## 41.4 Test-AuditBundle.ps1

Must validate structure, manifest, hashes, summary, required logs, forbidden file absence, source snapshot, and validation consistency.

## 41.5 Commands

Rust:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
cargo deny check
cargo audit
```

Java:

```text
java\mvnw.cmd -B -ntp clean verify
```

Protocol:

```text
buf lint
buf breaking: NOT_APPLICABLE
```

Docker/Git checks are mandatory.

---

# 42. Failure matrix minimum

## Agent absent

- bounded timeout;
- plugin critical behavior does not initialize;
- no infinite wait.

## Client disconnect

- unresolved operation becomes transport-indeterminate;
- retry same event;
- reconnect/status returns latest revision.

## Tracking limit reached

- waiter returns `TRACKING_DETACHED`;
- event remains durable/pending;
- status lookup continues.

## Journal unavailable

- no acceptance;
- no receipt;
- no false persisted rejection.

## Acceptance transaction fails

- no sequence externally visible;
- no checksum emitted;
- no acceptance.

## Crash after local commit before client update

- status recovers latest snapshot;
- retry dedups.

## Kafka below RF=5/minISR=5

- P0 success not emitted;
- pending/retrying/blocked according to failure classification.

## Kafka ACK before replicated transaction

- no replicated success;
- retry safe.

## Replicated transaction before update

- status returns replicated state.

## Profile drift before payload removal

- local payload retained;
- retention basis returns to `LOCAL_COPY_REQUIRED` if not removed.

## Projector DB commit before offset

- Kafka replay;
- duplicate idempotent;
- outbox exists.

## Projector offset before ACK publish

- outbox publishes later.

## ACK publish before outbox mark

- duplicate ACK possible;
- Agent dedups.

## Agent ACK local commit before ACK offset

- ACK redelivered;
- projection revision does not advance again.

## Same revision different checksum

- integrity conflict;
- no arbitrary replacement.

## Same event different projector sequence

- integrity conflict.

## Strict query cannot reach barrier

- no stale fallback;
- return `FRESHNESS_NOT_REACHED`, `TIMEOUT`, or `UNAVAILABLE`;
- authoritative UI does not claim current data.

## Query barrier reached but database snapshot is older

- rollback read transaction;
- retry within deadline;
- never return the older snapshot as strict-fresh.

## Snapshot watch disconnects or detaches

- last snapshot is no longer represented as continuously current;
- client performs strict refetch, closes the view, or marks it not current;
- durable projection remains in PostgreSQL.

## Same snapshot version with different checksum

- query integrity conflict;
- preserve evidence;
- do not arbitrarily replace the displayed state.


## Barrier topology changes during strict query

- return `BARRIER_INVALIDATED`;
- recapture only within the remaining timeout;
- never omit a newly required partition silently.

## Barrier capture cannot prove read-committed LSO

- strict query fails closed;
- ordinary high watermark is not substituted;
- health reports barrier-capture unavailable.

## Invalid or forged consistency token

- reject without waiting on fabricated versions;
- preserve diagnostic evidence without exposing secrets;
- no cross-installation or cross-producer access.

## Aggregate watch vectors are incomparable

- detach the watch;
- do not select either snapshot arbitrarily;
- require strict refetch.

## Projection is operationally blocked

- return `PROJECTION_BLOCKED`;
- distinguish it from ordinary lag and timeout;
- do not return stale authoritative data.

## CraftRelayPaperBridge absent at plugin startup

- a hard-dependent first-party integration does not enable CraftRelay-dependent behavior;
- no direct Kafka/PostgreSQL fallback is attempted;
- startup failure is explicit and diagnosable.

## Bridge starts while sidecars are unavailable

- Bridge may register service as `NOT_READY` only after local config and manifests validate;
- bounded reconnect starts;
- domain plugins may obtain handles but must not enable authoritative features until `READY`;
- a domain plugin may self-disable after a finite configured deadline.

## Bridge loses Agent connection

- new authoritative publishes are rejected, queued only within configured bounds, or become transport-indeterminate according to API state;
- existing durable events remain owned by the Agent journal;
- no new event IDs are invented during reconnect;
- Query Service connectivity may remain available independently.

## Bridge loses Query Service connection

- writes may remain available;
- strict queries fail explicitly;
- no stale local fallback is presented as current.

## Integration manifest or producer mapping conflict

- Bridge readiness fails for the affected integration;
- no producer identity is guessed;
- unrelated valid integrations may remain available only if isolation is proven by the accepted startup policy.

## Bridge tracking detaches during plugin disable/reload

- durable work remains queryable by event ID;
- callbacks are not invoked against a disabled plugin;
- no false cancellation or false durable receipt is emitted.

## Paper-side effect callback crash/restart ambiguity

- CraftRelay does not claim exactly-once Paper-side callback execution;
- critical domains use projected-state authority, idempotency fences, startup reconciliation, or desired-state commands;
- duplicate or missing local callback effects are resolved by domain logic, not by pretending futures are durable effects.

## Batching policy misconfiguration

- out-of-range requested flush criteria are rejected or clamped according to policy mode;
- effective policy is auditable;
- P0 events do not accept semantic coalescing;
- noisy producers cannot bypass reserved capacity by lowering maxDelay or maxRecords.

## Interactive flush unavailable or timed out

- normal stats/display commands degrade gracefully with projected/cached/local-overlay data according to their declared display policy;
- critical commands do not proceed as authoritative without the required durability/freshness;
- no stale value is represented as current;
- no global flush or Kafka payload scan is attempted.

## Query overload

- Query Service degrades;
- strict requests fail explicitly;
- Agent and Projector remain unaffected.

---

# 43. Definition of Done for CraftRelay v1

CraftRelay v1 requires:

1. Agent, Projector, and Query Service are separate processes.
2. Java client does not block Paper gameplay threads.
3. Production P0 profile uses Kafka RF=5/minISR=5/acks=all.
4. Event identity is `(installation_id,event_id)`.
5. UUIDv7 timestamp is not authoritative.
6. Status lookup is authorized and non-enumerating.
7. Producer identity comes from authentication.
8. Transport context is excluded from event identity.
9. Positive signed numeric types are used.
10. `schema_version` is positive `int32`.
11. Metadata duplicates are detectable.
12. Request fingerprint is cross-language tested.
13. Stored envelope excludes payload bytes.
14. Payload bytes are stored in `EventPayloadBlob`.
15. Envelope checksum is computed after sequence allocation inside the transaction.
16. Stored envelope never mutates.
17. Delivery, projection, and retention states are separately revisioned.
18. Same revision with a different snapshot checksum is an integrity conflict.
19. Client publish tracking is bounded.
20. Reconnect/status cannot regress lifecycle state.
21. `publish()` succeeds only at required durability.
22. P0 gameplay effects use `DURABLE_BEFORE_EFFECT`.
23. Accepted events are never later represented as rejected.
24. Local and replicated state are persisted atomically.
25. Rejection and indeterminate transport are distinct.
26. Same-event retry and status lookup are proven.
27. Logical stream identity is independent from Kafka topic.
28. Runtime ownership/routing migrations are not falsely supported.
29. Independent publishes are non-atomic.
30. `BEST_EFFORT` is absent from the durable API.
31. Permanent Kafka failures enter `DELIVERY_BLOCKED`.
32. Kafka retention horizon is sufficient.
33. Topic deletion and configuration drift are guarded.
34. Payload retention uses explicit basis and presence.
35. Local payload removal is transactional and audited.
36. Disk headroom is enforced.
37. No hot operational structure is unbounded.
38. Projector sequence validation is transactional.
39. Projector has sequence and event dedup constraints.
40. Projector writes the immutable event archive before required ACK.
41. Projector checkpoint and domain changes commit in the same transaction.
42. A checkpoint never advances past an unresolved authoritative event/offset; checkpoint-only advancement is allowed only for verified non-delivered gaps with persisted evidence.
43. Required projection ACK uses a transactional outbox.
44. Agent persists ACK consumption before ACK offset commit.
45. ACK transport retention covers supported Agent outage.
46. PostgreSQL P0 profile is durable and tested.
47. Plugins never access PostgreSQL directly.
48. Query Service exposes typed APIs only; arbitrary SQL is impossible.
49. Authoritative reads use `STRICT_LATEST_COMMITTED` or authenticated `AT_LEAST_TOKEN`.
50. Kafka barriers use per-partition exclusive `required_next_offset` values.
51. Projector checkpoints use exclusive `next_offset_to_resolve` values.
52. Kafka offset zero is accepted while application sequences remain positive.
53. Barrier vectors include query-definition, topology, routing, and partition-set versions/checksums.
54. Barrier invalidation is detected and never returns a false strict result.
55. A compile-tested Rust `BarrierCaptureAdapter` proves read-committed LSO capture.
56. Empty partitions, open transactions, and offset gaps pass integration tests.
57. Ordinary Kafka high watermarks are never substituted for LSO.
58. Strict reads wait for every required projection checkpoint.
59. Strict reads validate checkpoints and domain rows in one PostgreSQL primary snapshot.
60. Strict reads never silently fall back to stale data.
61. Strict `NOT_FOUND` is returned only after the complete barrier vector is reached.
62. `ProjectionConsistencyToken` is authenticated, scoped, expiring, versioned, and bound to one projector/projection in v1.
63. Read-your-write through `AT_LEAST_TOKEN` is proven.
64. The Java client can retrieve one or more projection tokens through a bounded API.
65. Query duration uses `timeout_millis`, transport deadline, query ceiling, and server monotonic time.
66. Query caches carry exact barrier/version/checksum metadata and cannot bypass freshness.
67. Core query parameters are concretely typed in Protobuf.
68. Entity-version watches are bounded, checksummed, monotonic, and detachable.
69. Barrier-vector watches use component-wise dominance.
70. Incomparable barrier vectors trigger detach and strict refetch.
71. Critical displays refetch, close, or mark themselves not current after watch detachment.
72. `PROJECTION_BLOCKED` is distinct from ordinary lag, timeout, and unavailable states.
73. Query overload cannot affect Agent or Projector.
74. Entity-scoped strict reads use the smallest relevant partition set.
75. Read-your-write prefers tokens over unnecessary global barriers.
76. Aggregate views are precomputed or explicitly budgeted.
77. Persistent Kafka/PostgreSQL clients, prepared statements, exact indexes, and short transactions are used.
78. Capacity benchmarks cover 10, 20, 50, and 100 concurrent players.
79. Capacity is documented in operations per second and active watches, not player count alone.
80. Integrity conflicts are preserved.
81. Gaps are detected.
82. Poison policy is explicit; authoritative projections do not quarantine-and-continue while claiming strict freshness.
83. Query consistency and freshness semantics are explicit.
84. Security boundaries are realistic.
85. A real plugin passes shadow mode with strict-read comparisons.
86. Sprint 0 audit scripts are functional.
87. Audit supports unborn HEAD.
88. Independent audit passes.
89. Crash matrix passes.
90. Upgrade and rollback pass.
91. Backup and restore pass.
92. A 24-hour soak passes without continuous memory/storage growth.
93. SLOs are measured on documented hardware.
94. No unresolved silent-loss, stale-authoritative-read, performance-contract, or protocol-contract blocker remains.
95. Requested flush criteria are OR-based across records, bytes, and age.
96. Effective flush criteria are computed by CraftRelay policy and may reject or clamp plugin requests.
97. Physical batching preserves per-event identity, auditability, lifecycle, deduplication, and ledger rows.
98. Semantic coalescing is available only for explicit aggregate event contracts and is forbidden for P0 ledger/ownership/premium/unique-item/security events.
99. Noisy producers cannot consume reserved P0 capacity through aggressive requested flush settings.
100. Exactly one first-party `CraftRelayPaperBridge` owns Paper-side transport resources per server.
101. `craftrelay-paper-api` is small, stable, and used as provided/compileOnly by domain plugins.
102. First-party domain plugins use embedded typed AdapterClasses/generated clients, not separate adapter plugins.
103. The Bridge registers and removes `CraftRelayService` safely across enable/disable and supports `NOT_READY` readiness.
104. Logical producer identity is resolved from authoritative Plugin/integration registration; `clientFor(Plugin)` is not a malicious same-JVM security boundary.
105. Per-producer ACLs, quotas, metrics, diagnostics, producer-instance IDs, and sequence allocators survive shared transport multiplexing.
106. The Bridge contains no Kafka, PostgreSQL, journal, or domain-projection implementation.
107. Domain plugins contain no Kafka, PostgreSQL, journal, or independent sidecar transport stack.
108. Integration manifests, classloader/API boundaries, and generated client versions are validated before authoritative work.
109. Bridge/adapter threading, Folia/Paper execution context, callback, reconnect, disable, and classloader behavior pass Paper integration tests.
110. Paper-side callback exactly-once is not claimed unless the domain provides idempotency/reconciliation.
---

# 44. Immediate next action

1. Save this file as `MASTER_PLAN.md`.
2. Replace every earlier CraftRelay plan.
3. Save and review `PluginAdapter.md`; it is authoritative for Paper integration details.
4. Create `AGENTS.md` from these rules.
5. Send only the Sprint 0 prompt to Codex.
6. Review generated ADRs and `.proto`, including batching/flush-policy contracts.
7. Confirm `craftrelay-client-core`, `craftrelay-paper-api`, `craftrelay-paper-bridge`, `IntegrationManifest`, and reference AdapterClass contracts exist.
8. Confirm first-party domain plugins never create their own gRPC/Kafka/PostgreSQL/journal stacks.
9. Confirm Bridge readiness, classloader/API boundary, producer-instance/sequence ownership, and Paper/Folia execution-context contracts exist.
6. Confirm payload bytes are absent from `StoredEventEnvelope`.
7. Confirm sequence allocation occurs before envelope checksum.
8. Confirm RF=5/minISR=5 profile is captured.
9. Confirm ACK outbox and Agent ACK-consumption contracts exist.
10. Confirm QueryDefinition versions, exclusive next-offset barriers/checkpoints, topology invalidation, and strict freshness results exist in the protocol skeleton.
11. Confirm ProjectionConsistencyToken is authenticated, scoped, expiring, and retrievable by the Java client.
12. Confirm timeout_millis uses server monotonic time and bounded ceilings.
13. Confirm EntityVersionWatch and BarrierVectorWatch semantics, including incomparable-vector detachment.
13a. Confirm authoritative projections block on poison/schema/integrity/domain errors and persist `PROJECTION_BLOCKED` state.
13b. Confirm checkpoint-only advancement exists for verified non-delivered Kafka gaps.
13c. Confirm consistency tokens are per-projector/per-projection and queries accept bounded token lists.
13d. Confirm CraftRelay does not claim exactly-once Paper callbacks without domain idempotency/reconciliation.
14. Confirm the compile-tested Rust BarrierCaptureAdapter spike passes against real Kafka.
15. Confirm authoritative queries cannot silently use stale cache or asynchronous replicas.
16. Confirm performance benchmarks cover 10, 20, 50, and 100 concurrent players.
17. Generate a functional audit bundle.
18. Do not commit until independent review returns `APPROVED FOR COMMIT`.
