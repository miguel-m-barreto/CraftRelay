# Architecture

Paper domain plugins use embedded typed adapters through one `CraftRelayPaperBridge`. The Bridge owns bounded shared sidecar transport in future sprints but contains no domain logic. External Agent, Kafka, Projector/PostgreSQL, Query Service, and optional Kafka-derived Live Projector are distinct failure domains. Sprint 0 implements only contracts. Durability and integrity outrank availability; NODE_LOCAL is the only accepted ownership model.

