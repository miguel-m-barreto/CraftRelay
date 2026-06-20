# Operations

Use `scripts/Start-DevStack.ps1` and `Stop-DevStack.ps1` for the non-production Compose skeleton. Production requires independently reviewed storage, broker, PostgreSQL, TLS, ACL, backup, retention, alerting, disk, and capacity configuration. Redis profile is optional. Audit generation is explicit; it is never part of normal build execution.

