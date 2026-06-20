# Query consistency

`STRICT_LATEST_COMMITTED` captures read-committed exclusive `required_next_offset` values, waits for every required `next_offset_to_resolve`, then validates checkpoints and rows in one PostgreSQL primary snapshot. `AT_LEAST_TOKEN` validates authenticated, scoped, expiring per-projector tokens. Neither silently falls back. `ALLOW_STALE` is explicit.

Barrier vectors include topology, routing, and partition-set identity; any change makes old vectors incomparable. Entity and vector watches have count/byte/deadline bounds and detach explicitly. Server monotonic time and server ceilings govern timeout. Critical displays refetch, close, or mark non-current after detach. High watermarks are not strict read-committed LSO substitutes.

