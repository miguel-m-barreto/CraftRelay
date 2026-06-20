# ADR-0022: Authenticated consistency tokens and read-your-write

- Status: ACCEPTED
- Date: 2026-06-20

## Context

Read-your-write queries must carry proof that the required projection has resolved the caller's
accepted event. A checksum alone detects corruption but does not authenticate client-supplied
requirements. One merged token across unrelated projectors would also create ambiguous scope and
unbounded mutation data.

## Decision

The Agent issues one versioned `ProjectionConsistencyToken` per projector and projection only after
it durably consumes the corresponding transactional projection ACK. HMAC-SHA-256 is the baseline
MAC. Verification is constant-time and keys are identified, rotatable, and server-controlled.

Every token binds all of the following authenticated claims:

- installation ID and authenticated producer ID;
- issuer Agent, originating event, projector, and projection;
- permitted query/entity scope and QueryDefinition version;
- projection policy and projection-policy version;
- projection-topology and routing versions;
- issue time, server-issued expiry, key ID, canonical payload checksum, and MAC;
- a bounded list of mutation references containing domain versions and optional exclusive
  `required_next_offset` requirements.

The Java API retrieves either one token or a small bounded list of per-projector/per-projection
tokens. Tracking may detach, but retained durable status/token lookup remains authoritative.
`AT_LEAST_TOKEN` provides read-your-write only after the Query Service authenticates every token,
validates its complete scope and versions, and waits for its bounded requirements.

Invalid MACs, unknown keys, expired tokens, cross-installation or cross-producer reuse, query/entity
scope mismatch, topology/routing incompatibility, excessive mutation references, and excessive token
counts are rejected explicitly. They never downgrade to stale or display-only reads.

## Consequences

This contract adds bounded key rotation, status retention, and token-verification responsibilities.
Violations fail closed and require a superseding ADR. Sprint 0 defines models and tests only; it does
not issue or verify real tokens.
