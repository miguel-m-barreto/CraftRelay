# ADR-0020: Strict freshness and exclusive next-offset barriers

- Status: ACCEPTED
- Date: 2026-06-20

## Context

CraftRelay requires this contract before functional persistence work.

## Decision

Strict capture uses read-committed LSO expressed as exclusive required_next_offset. High watermark and inclusive last-applied offsets are forbidden substitutes.

## Consequences

Violations fail closed and require a superseding ADR. Runtime implementation and operational proof remain future work.

