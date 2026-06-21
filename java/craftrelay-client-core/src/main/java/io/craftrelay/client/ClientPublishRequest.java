package io.craftrelay.client;

import java.util.Objects;

/** Session-bound request; producer identity is supplied by the authenticated transport. */
public record ClientPublishRequest(EnvelopeInput envelopeInput, long producerOperationSequence) {
    public ClientPublishRequest {
        Objects.requireNonNull(envelopeInput, "envelopeInput");
        producerOperationSequence = ContractValidation.positiveInt64(
                producerOperationSequence, "producerOperationSequence");
    }
}
