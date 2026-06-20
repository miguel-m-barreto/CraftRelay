# ADR-0003: Installation-scoped identities and UUIDv7

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Every persisted identity and uniqueness key includes installation scope. Event IDs are canonical UUIDv7, client-generated once, stable across retries, with variant/version validation.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

