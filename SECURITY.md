# Security

Report vulnerabilities privately to the repository owners. Do not open public issues containing secrets or exploit details.

Sprint 0 models trusted first-party plugins in one JVM; `clientFor(Plugin)` prevents accidents but is not a malicious same-JVM security boundary. Producer identity is derived from authenticated integration registration. Plugins cannot select producer ID, policy, Kafka topic, SQL, retention, or authority. Tokens must be installation/projector/projection scoped, expiring, checksummed, and MAC-authenticated. Audit bundles exclude `.env`, keys, credentials, build output, and Git internals.

