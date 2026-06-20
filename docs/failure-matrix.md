# Failure matrix

| Failure | Contract outcome |
|---|---|
| Agent unavailable | Bridge remains bounded/NOT_READY; no ad-hoc storage fallback. |
| Kafka cannot prove P0 replication | No success; accepted work remains pending/blocked with evidence. |
| PostgreSQL unavailable | Delivery remains independent; projection/strict reads unavailable. |
| Query Service unavailable | Writes remain independent; no direct DB/Redis fallback. |
| Redis unavailable/rebuilding | Display live modes degrade explicitly; durability and strict paths unaffected. |
| Watch detached | Refetch, close, or mark non-current. |
| Same revision/different checksum | Integrity conflict; fail closed. |
| Client transport ambiguity | `TRANSPORT_INDETERMINATE`; retry same event ID. |

