# Threat model

The target is RPO=0 for each event whose effective durability is confirmed, within the declared broker/journal/database/archive failure model. It is not protection against destruction of every durable copy, malicious code in the Paper JVM, stolen host credentials, or operator bypass. The system fails closed when durability or freshness cannot be proven. Installation-scoped authentication, MAC tokens, TLS/mTLS, ACLs, least privilege, secret exclusion, and evidence integrity are required future controls.

