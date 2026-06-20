# ADR-0029: Embedded first-party AdapterClasses/generated clients

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Each first-party domain plugin embeds typed adaptation and uses the shared API. Separate adapter plugins are reserved for accepted third-party exceptions.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

