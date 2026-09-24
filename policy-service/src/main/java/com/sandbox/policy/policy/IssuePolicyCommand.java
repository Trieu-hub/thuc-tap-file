package com.sandbox.policy.policy;

/**
 * Transport-neutral issuance request. {@code eventId} is the Kafka envelope's event_id for
 * payment.recorded (Flow 2) and {@code null} for a RabbitMQ RPC request (Flow 1), which carries
 * no event_id and is deduplicated by order_id alone.
 */
public record IssuePolicyCommand(String orderId, String paymentId, String eventId) {

}
