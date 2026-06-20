# ADR-0023: Bounded entity and barrier watches

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

EntityVersionWatch and BarrierVectorWatch have event/byte/deadline bounds and explicit detach. Aggregate vectors compare component-wise, never as one scalar.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

