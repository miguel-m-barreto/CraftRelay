# ADR-0005: Immutable envelope, payload blob, revisioned states

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

StoredEventEnvelope is immutable and excludes payload bytes and mutable status. EventPayloadBlob stores bytes. Delivery, projection, and retention are separate revisioned snapshots.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

