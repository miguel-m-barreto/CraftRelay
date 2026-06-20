# ADR-0021: Versioned vectors and PostgreSQL snapshot validation

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Barrier vectors carry topology/routing/partition-set versions and invalidate when any changes. Checkpoints and domain rows are validated in one primary snapshot.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

