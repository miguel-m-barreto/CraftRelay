# Kafka zero-loss production profile

P0 requires five brokers, replication factor 5, `min.insync.replicas=5`, producer `acks=all`, idempotence, unclean leader election disabled, protected topics, and retention exceeding the declared recovery window. Independent publishes are not atomic. Logical stream identity is separate from physical topic/partition routing. Development Compose proves configuration shape only and is not a production durability claim.

