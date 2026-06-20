# Plugin usage summary

`PluginUsage.md` is the practical baseline. Calls are asynchronous; gameplay threads never wait on network/disk or call `join/get`. Callbacks that mutate Paper return through a declared execution context. XP/progress UI is local-first, publishes bounded deltas asynchronously, and reconciles with projected state. Critical rewards require authority/idempotency/reconciliation.

