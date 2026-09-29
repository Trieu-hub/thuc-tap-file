package com.sandbox.payment.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import com.sandbox.payment.payment.PaymentRecorder;
import com.sandbox.payment.payment.RecordPaymentResult;

/**
 * Flow 1 transport adapter: receive the request, call {@link PaymentRecorder}, reply. No business
 * logic here, so Flow 2 (gRPC) can reuse the same recorder. The reply goes to the request's
 * reply_to with its AMQP correlation_id, both handled by Spring AMQP.
 */
@Component
class PaymentRpcListener {

	private static final Logger log = LoggerFactory.getLogger(PaymentRpcListener.class);

	private final PaymentRecorder recorder;

	PaymentRpcListener(PaymentRecorder recorder) {
		this.recorder = recorder;
	}

	@RabbitListener(queues = RabbitConfig.REQUEST_QUEUE)
	Message<PaymentRpcResponse> onRequest(PaymentRpcRequest request,
			@Header(name = RabbitConfig.CORRELATION_HEADER, required = false) String correlationId) {
		// The listener thread is shared by all messages, so the MDC must be set and cleared per message (F30).
		MDC.put("correlation_id", correlationId);
		MDC.put("transport", "RabbitMQ");
		long start = System.nanoTime();
		try {
			RecordPaymentResult result = this.recorder.record(request.toCommand());
			log.atInfo()
				.addKeyValue("transport", "RabbitMQ")
				.addKeyValue("action", "RecordPayment")
				.addKeyValue("order_id", request.orderId())
				.addKeyValue("status", result.status().name())
				.addKeyValue("duplicate", result.duplicate())
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("Payment RPC handled");
			// Echo the header so order-service can still trace a reply that arrives after its timeout.
			return MessageBuilder.withPayload(PaymentRpcResponse.from(result))
				.setHeader(RabbitConfig.CORRELATION_HEADER, correlationId)
				.build();
		}
		catch (RuntimeException ex) {
			// Rethrown: the container rejects without requeue and the broker moves the message to the DLQ.
			log.atError()
				.addKeyValue("transport", "RabbitMQ")
				.addKeyValue("action", "RecordPayment")
				.addKeyValue("order_id", request.orderId())
				.addKeyValue("status", "FAILED")
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.setCause(ex)
				.log("Payment RPC failed, message goes to the DLQ");
			throw ex;
		}
		finally {
			MDC.remove("correlation_id");
			MDC.remove("transport");
		}
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
