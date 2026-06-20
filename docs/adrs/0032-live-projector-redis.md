# ADR-0032: Optional Kafka-derived Redis Live Projector

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

An optional service consumes Kafka and writes rebuildable Redis display models. PROJECTED_PLUS_LIVE/LIVE_ONLY are display-only, never prove strict/token authority; Redis failure cannot affect durability/critical paths and checkpoints remain separate.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

