# ADR-0007: Snapshot checksum and revision integrity

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Mutable snapshots have positive monotonic revision and SHA-256 checksum. Equal revision/equal checksum is duplicate; equal revision/different checksum is integrity conflict; lower is stale.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

