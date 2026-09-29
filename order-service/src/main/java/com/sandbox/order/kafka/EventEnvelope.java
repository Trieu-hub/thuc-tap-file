package com.sandbox.order.kafka;

import java.time.Instant;

/**
 * Envelope of every Kafka event (contracts/schemas/event-envelope.schema.json), snake_case on the
 * wire through Boot's mapper. Copied per service on purpose (CLAUDE.md D17): the contract is the
 * JSON Schema, not a shared class.
 */
record EventEnvelope<P>(String eventId, String eventType, String correlationId, Instant occurredAt, P payload) {

}
