# ADR-0031: Paper/Folia threading, callback, classloader, dependencies

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

All APIs are async; gameplay threads never block. Paper mutations use explicit global/entity/region context. API/JDK types cross classloaders; transport/generated DTOs do not. Callback exactly-once is not claimed.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

