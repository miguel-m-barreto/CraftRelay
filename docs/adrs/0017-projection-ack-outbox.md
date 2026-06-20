# ADR-0017: Transactional projection ACK outbox

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Required ACK records are inserted in the projection transaction and published only after commit. Direct non-outbox required ACK is forbidden.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

