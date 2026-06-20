# Payload retention

Payload bytes live only in `EventPayloadBlob`; the immutable envelope retains the digest and reference identity. Delivery, projection, and retention are independent revisioned states. Removal eligibility is policy-derived and requires proven Kafka retention/archive/projection responsibility. Removal is one local transaction that verifies basis, records a new retention revision/checksum, removes bytes, and preserves immutable evidence. Failure leaves payload present or integrity-blocked.

