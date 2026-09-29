package com.sandbox.policy.kafka;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.sandbox.policy.policy.IssuePolicyResult;

/**
 * Publishes {@code policy.issued} (contracts/schemas/policy-issued.schema.json) with key = order_id.
 * <p>
 * The event_id is derived from policy_id, so every publication for one policy carries the same
 * event_id and order-service's inbox applies it once, however often it is re-published.
 */
@Component
class PolicyIssuedPublisher {

	static final long SEND_TIMEOUT_MS = 5000;

	private static final Logger log = LoggerFactory.getLogger(PolicyIssuedPublisher.class);

	private final KafkaTemplate<String, String> kafka;

	private final JsonMapper jsonMapper;

	PolicyIssuedPublisher(KafkaTemplate<String, String> kafka, JsonMapper jsonMapper) {
		this.kafka = kafka;
		this.jsonMapper = jsonMapper;
	}

	/**
	 * Waits for the broker's acknowledgement (at most 5 s) and throws on failure, so the listener
	 * fails, the payment.recorded offset is not committed and the event is retried; the retry finds
	 * the policy already issued and publishes it again.
	 */
	void publish(IssuePolicyResult policy, String correlationId) {
		long start = System.nanoTime();
		EventEnvelope<Payload> event = EventEnvelope.of(KafkaConfig.POLICY_ISSUED, policy.policyId(), correlationId,
				new Payload(policy.orderId(), policy.policyId(), policy.policyNumber(), policy.issuedAt()));
		try {
			this.kafka.send(KafkaConfig.POLICY_ISSUED, policy.orderId(), this.jsonMapper.writeValueAsString(event))
				.get(SEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw publishFailed(policy, event, start, ex);
		}
		// RuntimeException too, not only Spring's KafkaException: with Kafka down, the first send after a
		// start fails while creating the producer with org.apache.kafka.common.KafkaException (DNS).
		catch (ExecutionException | TimeoutException | RuntimeException ex) {
			throw publishFailed(policy, event, start, ex);
		}
		log.atInfo()
			.addKeyValue("transport", "Kafka")
			.addKeyValue("action", "PublishPolicyIssued")
			.addKeyValue("order_id", policy.orderId())
			.addKeyValue("event_id", event.eventId())
			.addKeyValue("policy_number", policy.policyNumber())
			.addKeyValue("status", "SUCCESS")
			.addKeyValue("execution_time_ms", elapsedMs(start))
			.log("policy.issued published");
	}

	private static IllegalStateException publishFailed(IssuePolicyResult policy, EventEnvelope<Payload> event,
			long start, Exception ex) {
		log.atError()
			.addKeyValue("transport", "Kafka")
			.addKeyValue("action", "event_publish_failed")
			.addKeyValue("order_id", policy.orderId())
			.addKeyValue("event_id", event.eventId())
			.addKeyValue("status", "FAILED")
			.addKeyValue("execution_time_ms", elapsedMs(start))
			.setCause(ex)
			.log("policy.issued not published, payment.recorded will be retried");
		return new IllegalStateException("policy.issued not published for order " + policy.orderId(), ex);
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

	/** {@code payload} of policy.issued. */
	record Payload(String orderId, String policyId, String policyNumber, Instant issuedAt) {

	}

}
