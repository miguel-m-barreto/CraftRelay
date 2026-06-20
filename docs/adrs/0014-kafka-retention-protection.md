# ADR-0014: Kafka retention and topic protection

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Retention exceeds recovery responsibility, destructive operations are ACL protected, and ordered event topics are not compacted without a dedicated accepted ADR.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

