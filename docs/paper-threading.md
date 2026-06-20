# Paper and Folia threading

The API represents `GLOBAL_SERVER`, `ENTITY`, `REGION`, and `ASYNC_ONLY`. Serialization beyond small bounded work, network/disk waits, retries, reconnect, drains, and blocking completion calls are forbidden on gameplay threads. Callback delivery is at-least-once/indeterminate across failure; Paper-side exactly-once is never claimed without domain idempotency and reconciliation. Disable is bounded and detaches local tracking without cancelling accepted durable truth.

