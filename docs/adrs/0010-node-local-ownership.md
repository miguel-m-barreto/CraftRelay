# ADR-0010: Static NODE_LOCAL ownership

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

v1 supports static NODE_LOCAL ownership only. Global ownership and transfer are outside accepted scope.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

