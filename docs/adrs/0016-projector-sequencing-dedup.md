# ADR-0016: Transactional projector sequencing and event dedup

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Sequence validation, event dedup, domain rows, archive, associations, and checkpoint movement occur in one PostgreSQL transaction with both sequence and event constraints.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

