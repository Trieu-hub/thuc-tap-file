package com.sandbox.payment.rpc;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.sandbox.payment.payment.PaymentRecorder;
import com.sandbox.payment.payment.RecordPaymentResult;

/**
 * Runs the RPC path once at startup, so the first real request does not pay for class loading and
 * first-use initialisation. Measured on a fresh container: about 2.7 s per hop, close to the
 * caller's 3 s timeout, so the first order after a restart could fail for no business reason.
 * <p>
 * It runs on {@link ApplicationReadyEvent}, before Spring reports readiness, and the compose
 * healthcheck uses {@code /actuator/health/readiness}: the container only turns healthy once warm.
 * Nothing is persisted: the recorder runs inside a transaction that is rolled back.
 */
@Component
class RpcWarmUp {

	private static final Logger log = LoggerFactory.getLogger(RpcWarmUp.class);

	private final JacksonJsonMessageConverter converter;

	private final PaymentRecorder recorder;

	private final TransactionTemplate transactions;

	RpcWarmUp(JacksonJsonMessageConverter converter, PaymentRecorder recorder, TransactionTemplate transactions) {
		this.converter = converter;
		this.recorder = recorder;
		this.transactions = transactions;
	}

	@EventListener(ApplicationReadyEvent.class)
	void warmUp() {
		long start = System.nanoTime();
		try {
			// Fits the VARCHAR(32) order_id column; random so two instances warming up at once never share a key.
			String id = "WARMUP-" + UUID.randomUUID().toString().substring(0, 12);
			Message message = this.converter.toMessage(new PaymentRpcRequest(id, id, "TXN-" + id, 1),
					new MessageProperties());
			PaymentRpcRequest request = (PaymentRpcRequest) this.converter.fromMessage(message,
					ParameterizedTypeReference.forType(PaymentRpcRequest.class));
			this.transactions.executeWithoutResult((status) -> {
				RecordPaymentResult result = this.recorder.record(request.toCommand());
				this.converter.toMessage(PaymentRpcResponse.from(result), new MessageProperties());
				// The recorder joins this transaction, so its INSERT is undone too.
				status.setRollbackOnly();
			});
			log.atInfo()
				.addKeyValue("transport", "RabbitMQ")
				.addKeyValue("action", "warm_up")
				.addKeyValue("status", "SUCCESS")
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("RPC path warmed up, nothing persisted");
		}
		catch (RuntimeException ex) {
			// A failed warm-up must not stop the service: the first real request is only slower.
			log.atWarn()
				.addKeyValue("transport", "RabbitMQ")
				.addKeyValue("action", "warm_up")
				.addKeyValue("status", "FAILED")
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.setCause(ex)
				.log("RPC warm-up failed, continuing without it");
		}
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
