# ADR-0018: Agent ACK consumption before offset commit

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Agent deduplicates and commits local ACK consumption/state before committing the Kafka ACK offset. Redelivery is safe and bounded.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

