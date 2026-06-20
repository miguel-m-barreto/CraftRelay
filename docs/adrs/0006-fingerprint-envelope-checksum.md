# ADR-0006: Request fingerprint and envelope checksum

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

SHA-256 canonical request fingerprints detect retry conflicts. Envelope checksum is calculated inside acceptance after sequence/routing allocation and covers immutable persisted fields.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

