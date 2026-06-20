# ADR-0001: System boundaries and process topology

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Paper domain plugins call one Bridge; Agent, Kafka, Projector/PostgreSQL, Query Service, and optional Live Projector are separate processes/failure domains. Sprint 0 supplies contracts only.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

