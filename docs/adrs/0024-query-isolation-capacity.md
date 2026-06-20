# ADR-0024: Query fast paths, isolation, and capacity gates

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Typed query definitions, bounded queues/pools/caches/single-flight, primary snapshot authority, monotonic deadlines, and capacity gates prevent query load from harming write durability.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

