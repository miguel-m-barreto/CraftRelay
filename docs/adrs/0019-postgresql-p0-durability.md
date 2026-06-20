# ADR-0019: PostgreSQL P0 durability profile

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

P0 projection/archive requires primary durable commit, synchronous replication policy, checksums/backups/restore tests, and fail-closed operation. Sprint 0 makes no connection.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

