# ADR-0025: Batching, coalescing, and flush policy

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Records OR bytes OR age OR drain triggers flush. Agent policy accepts/clamps/rejects requests. Physical batching preserves identity; semantic coalescing is forbidden for P0 critical classes. Interactive flush is scoped/bounded and scans no Kafka payloads.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

