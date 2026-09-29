package com.sandbox.policy.kafka;

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

import com.sandbox.policy.policy.IssuePolicyCommand;
import com.sandbox.policy.policy.IssuePolicyResult;
import com.sandbox.policy.policy.PolicyIssuer;

/**
 * Flow 2 transport adapter: read payment.recorded, call {@link PolicyIssuer} (the class Flow 1
 * uses), publish policy.issued. No business logic here; deduplication is the issuer's.
 * <p>
 * The issuer's transaction has committed when {@code issue} returns, and the offset is committed
 * only after this method returns (ack-mode record). A crash in between redelivers the event, and
 * the inbox recognises its event_id.
 */
@Component
class PaymentRecordedListener {

	private static final Logger log = LoggerFactory.getLogger(PaymentRecordedListener.class);

	private final ObjectReader reader;

	private final PolicyIssuer issuer;

	private final PolicyIssuedPublisher events;

	PaymentRecordedListener(JsonMapper jsonMapper, PolicyIssuer issuer, PolicyIssuedPublisher events) {
		// Missing or null contract fields fail parsing (JacksonException), so the record goes to the DLT at once.
		this.reader = jsonMapper.readerFor(new TypeReference<EventEnvelope<Payload>>() {
		})
			.with(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
					DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES);
		this.issuer = issuer;
		this.events = events;
	}

	@KafkaListener(topics = KafkaConfig.PAYMENT_RECORDED)
	void onPaymentRecorded(ConsumerRecord<String, String> record) {
		EventEnvelope<Payload> event = this.reader.readValue(record.value());
		// The listener thread is shared by all records, so the MDC is set and cleared per record (F30).
		MDC.put("correlation_id", event.correlationId());
		MDC.put("transport", "Kafka");
		long start = System.nanoTime();
		try {
			IssuePolicyResult result = this.issuer.issue(
					new IssuePolicyCommand(event.payload().orderId(), event.payload().paymentId(), event.eventId()));
			log.atInfo()
				.addKeyValue("transport", "Kafka")
				.addKeyValue("action", "IssuePolicy")
				.addKeyValue("order_id", result.orderId())
				.addKeyValue("event_id", event.eventId())
				.addKeyValue("status", "ISSUED")
				.addKeyValue("outcome", result.outcome().name())
				.addKeyValue("policy_number", result.policyNumber())
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("payment.recorded handled");
			// Also for DUPLICATE_EVENT_IGNORED: the first delivery may have committed the policy and then
			// failed to publish, and only a re-publish lets the order reach ISSUED. It carries the same
			// event_id, so order-service applies it once.
			this.events.publish(result, event.correlationId());
		}
		finally {
			MDC.remove("correlation_id");
			MDC.remove("transport");
		}
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

	/** {@code payload} of payment.recorded; only order_id and payment_id are used here. */
	record Payload(String orderId, String paymentId, String partnerTransactionId, long amount, Instant recordedAt) {

	}

}
