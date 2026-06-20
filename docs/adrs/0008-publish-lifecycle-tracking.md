# ADR-0008: Revisioned publish lifecycle and bounded Java tracking

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Lifecycle is revisioned and accepted events are never rejected later. Client maps/subscriptions have count/byte/age bounds and may detach without altering durable truth.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

