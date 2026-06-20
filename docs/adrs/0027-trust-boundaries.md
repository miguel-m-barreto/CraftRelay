# ADR-0027: Trust boundaries

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Producer identity comes from authentication/registration, not request strings. Same-JVM plugins are trusted; clientFor is not a sandbox. Typed handles replace arbitrary schemas/SQL.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

