package com.sandbox.policy.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * Envelope of every Kafka event (contracts/schemas/event-envelope.schema.json), snake_case on the
 * wire through Boot's mapper. Copied per service on purpose (CLAUDE.md D17): the contract is the
 * JSON Schema, not a shared class.
 */
record EventEnvelope<P>(String eventId, String eventType, String correlationId, Instant occurredAt, P payload) {

	/**
	 * The event_id is derived from the business id, so publishing the same fact again (a retry, or
	 * a re-publish after a duplicate) carries the same event_id and the consumer's inbox skips it.
	 */
	static <P> EventEnvelope<P> of(String eventType, String businessId, String correlationId, P payload) {
		String eventId = UUID.nameUUIDFromBytes((eventType + ":" + businessId).getBytes(StandardCharsets.UTF_8))
			.toString();
		return new EventEnvelope<>(eventId, eventType, correlationId, Instant.now().truncatedTo(ChronoUnit.MILLIS),
				payload);
	}

}
