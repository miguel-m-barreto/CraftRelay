# Lifecycle

Acceptance creates revision 1. Later snapshots strictly increase revision. Equal revision/equal checksum is an idempotent duplicate; equal revision/different checksum is an integrity conflict; a lower revision is stale. Accepted events are never later rejected. Java tracking is bounded and may detach without changing durable truth. `publish()` will eventually succeed only at effective required durability; Sprint 0 exposes no functional publish or DurableReceipt.

