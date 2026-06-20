# ADR-0002: Threat model and RPO=0

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

RPO=0 applies only to confirmed events inside the declared replicated storage and operational failure model. When proof is unavailable the system fails closed; total destruction of all copies is excluded.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

