package com.sandbox.order.kafka;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.sandbox.order.notification.NotificationWorker;
import com.sandbox.order.order.OrderProgressService;
import com.sandbox.order.order.TimelineEntry;
import com.sandbox.order.order.TimelineStep;
import com.sandbox.order.order.TransitionResult;

/**
 * Flow 2 transport adapter: read policy.issued, apply it with {@link OrderProgressService} (the
 * class Flow 1 uses), then run the simulated notification (D5). No business logic here: the inbox
 * claim, the forward-only status and the once-per-step timeline are the service's.
 * <p>
 * An unknown order_id throws {@code OrderNotFoundException}: the transaction and its inbox claim
 * roll back, the event is retried and finally lands in policy.issued.DLT instead of being silently
 * acknowledged. The offset is committed only after this method returns (ack-mode record).
 */
@Component
class PolicyIssuedListener {

	private static final Logger log = LoggerFactory.getLogger(PolicyIssuedListener.class);

	private final ObjectReader reader;

	private final OrderProgressService progress;

	private final NotificationWorker notifications;

	PolicyIssuedListener(JsonMapper jsonMapper, OrderProgressService progress, NotificationWorker notifications) {
		// Missing or null contract fields fail parsing (JacksonException), so the record goes to the DLT at once.
		this.reader = jsonMapper.readerFor(new TypeReference<EventEnvelope<Payload>>() {
		})
			.with(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
					DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES);
		this.progress = progress;
		this.notifications = notifications;
	}

	@KafkaListener(topics = KafkaConfig.POLICY_ISSUED)
	void onPolicyIssued(ConsumerRecord<String, String> record) {
		EventEnvelope<Payload> event = this.reader.readValue(record.value());
		// The listener thread is shared by all records, so the MDC is set and cleared per record (F30).
		MDC.put("correlation_id", event.correlationId());
		MDC.put("transport", "Kafka");
		long start = System.nanoTime();
		try {
			Payload policy = event.payload();
			// Time from issuance in policy-service to this consumer: how asynchronous the step was.
			long deliveryMs = Math.max(0, Duration.between(event.occurredAt(), Instant.now()).toMillis());
			TransitionResult result = this.progress.recordPolicyIssued(policy.orderId(), event.eventId(),
					policy.policyNumber(), new TimelineEntry(TimelineStep.POLICY_ISSUANCE, "policy-service", "Kafka",
							"SUCCESS", deliveryMs, policy.policyNumber()));
			log.atInfo()
				.addKeyValue("transport", "Kafka")
				.addKeyValue("action", "ApplyPolicyIssued")
				.addKeyValue("order_id", policy.orderId())
				.addKeyValue("event_id", event.eventId())
				.addKeyValue("status", result.status().name())
				.addKeyValue("outcome", result.outcome().name())
				.addKeyValue("policy_number", policy.policyNumber())
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("policy.issued handled");
			if (result.outcome() == TransitionResult.Outcome.APPLIED) {
				// Only when this event moved the order to ISSUED; a replay must not notify twice.
				this.notifications.policyIssued(policy.orderId(), policy.policyNumber());
			}
		}
		finally {
			MDC.remove("correlation_id");
			MDC.remove("transport");
		}
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

	/** {@code payload} of policy.issued. */
	record Payload(String orderId, String policyId, String policyNumber, Instant issuedAt) {

	}

}
