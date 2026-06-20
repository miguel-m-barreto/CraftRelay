# ADR-0012: Single-event atomicity and no cross-publish atomicity

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Each accepted event is locally atomic. Physical grouping does not create cross-publish atomicity. Atomic batch requires a future ADR.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

