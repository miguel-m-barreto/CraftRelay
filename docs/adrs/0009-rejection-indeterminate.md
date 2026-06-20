# ADR-0009: Rejection taxonomy and indeterminate transport

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Pre-acceptance rejection is distinct from transport ambiguity. An indeterminate caller retries the same event ID/fingerprint or performs status lookup; it never creates a new identity.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

