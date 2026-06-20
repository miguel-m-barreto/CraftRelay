# ADR-0030: Logical producer sessions and IntegrationManifest

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

The manifest binds canonical plugin registration to authenticated logical producer, compatible handles, versions, and bounds. Bridge owns producer instance and monotonic operation sequence.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

