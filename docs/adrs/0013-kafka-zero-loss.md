# ADR-0013: Kafka RF=5/minISR=5 P0 profile

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

P0 production requires five brokers, RF=5, minISR=5, acks=all, idempotence, and no unclean leader election. Development Compose is not a durability claim.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

