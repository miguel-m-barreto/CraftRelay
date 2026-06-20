# ADR-0004: Positive signed numeric types

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Application counters, revisions, sequences, schema versions, and limits use positive int32/int64. Kafka offset zero remains valid; unsigned protocol types are avoided.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

