# ADR-0011: Logical stream versus physical Kafka routing

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Installation-scoped logical stream identity is stable and distinct from Agent-selected topic/partition/routing version. Plugins cannot choose routing.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

