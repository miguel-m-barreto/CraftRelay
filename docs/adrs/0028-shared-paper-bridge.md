# ADR-0028: One shared CraftRelayPaperBridge

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Exactly one first-party Bridge per Paper server owns bounded sidecar transport resources. It contains no domain logic and no Kafka/JDBC/Redis/journal dependencies.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

