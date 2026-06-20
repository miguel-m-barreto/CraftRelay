# ADR-0026: Audit workflow including unborn HEAD

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Evidence captures checks, stdout/stderr/exit codes, tracked/untracked source, source snapshot, HEAD_STATE UNBORN or EXISTING, exclusions, SHA-256 manifest, summary, then ZIP; validation failure remains nonzero.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

