package com.sandbox.payment.kafka;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.sandbox.payment.payment.RecordPaymentCommand;
import com.sandbox.payment.payment.RecordPaymentResult;

/**
 * Publishes {@code payment.recorded} (contracts/schemas/payment-recorded.schema.json) with key =
 * order_id, so every event of one order lands in the same partition, in order.
 * <p>
 * Known limitation, on purpose: the payment row is committed first and the event is sent after,
 * two writes that are not atomic ("dual write"). If Kafka is down at that moment the event is lost
 * and the order stays PAYMENT_RECORDED. The production fix is a Transactional Outbox (the event is
 * written in the same DB transaction and relayed later); it is out of scope here (README).
 */
@Component
public class PaymentRecordedPublisher {

	static final long SEND_TIMEOUT_MS = 5000;

	private static final Logger log = LoggerFactory.getLogger(PaymentRecordedPublisher.class);

	private final KafkaTemplate<String, String> kafka;

	private final JsonMapper jsonMapper;

	PaymentRecordedPublisher(KafkaTemplate<String, String> kafka, JsonMapper jsonMapper) {
		this.kafka = kafka;
		this.jsonMapper = jsonMapper;
	}

	/**
	 * Waits for the broker's acknowledgement (at most 5 s) so the outcome is logged with the
	 * request's correlation_id. A failure is logged, not thrown: the payment is already recorded
	 * and the gRPC response already sent.
	 */
	public void publish(RecordPaymentCommand command, RecordPaymentResult result, String correlationId) {
		long start = System.nanoTime();
		// Same payment_id → same event_id: a duplicate RecordPayment re-publishes an event policy-service already knows.
		EventEnvelope<Payload> event = EventEnvelope.of(KafkaConfig.PAYMENT_RECORDED, result.paymentId(), correlationId,
				new Payload(command.orderId(), result.paymentId(), command.partnerTransactionId(), command.amount(),
						result.recordedAt()));
		try {
			this.kafka.send(KafkaConfig.PAYMENT_RECORDED, command.orderId(), this.jsonMapper.writeValueAsString(event))
				.get(SEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			log.atInfo()
				.addKeyValue("transport", "Kafka")
				.addKeyValue("action", "PublishPaymentRecorded")
				.addKeyValue("order_id", command.orderId())
				.addKeyValue("event_id", event.eventId())
				.addKeyValue("duplicate", result.duplicate())
				.addKeyValue("status", "SUCCESS")
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("payment.recorded published");
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			publishFailed(command, event, start, ex);
		}
		// RuntimeException too, not only Spring's KafkaException: with Kafka down, the first send after a
		// start fails while creating the producer with org.apache.kafka.common.KafkaException (DNS).
		catch (ExecutionException | TimeoutException | RuntimeException ex) {
			publishFailed(command, event, start, ex);
		}
	}

	private static void publishFailed(RecordPaymentCommand command, EventEnvelope<Payload> event, long start,
			Exception ex) {
		log.atError()
			.addKeyValue("transport", "Kafka")
			.addKeyValue("action", "event_publish_failed")
			.addKeyValue("order_id", command.orderId())
			.addKeyValue("event_id", event.eventId())
			.addKeyValue("status", "FAILED")
			.addKeyValue("execution_time_ms", elapsedMs(start))
			.setCause(ex)
			.log("payment.recorded not published: the payment is recorded but the event is lost (dual write)");
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

	/** {@code payload} of payment.recorded. */
	record Payload(String orderId, String paymentId, String partnerTransactionId, long amount, Instant recordedAt) {

	}

}
