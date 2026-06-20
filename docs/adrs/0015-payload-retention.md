# ADR-0015: Payload retention basis and removal transaction

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Payload removal requires proven policy basis and one transaction that validates evidence, advances retention revision/checksum, and removes only blob bytes.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

