package com.sandbox.order.grpc;

import java.time.Instant;

import com.google.protobuf.Timestamp;
import io.grpc.health.v1.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.sandbox.contracts.payment.v1.PaymentStatus;
import com.sandbox.contracts.payment.v1.RecordPaymentRequest;
import com.sandbox.contracts.payment.v1.RecordPaymentResponse;

/**
 * Runs the gRPC client path once at startup, the Flow 2 counterpart of payment/policy's RpcWarmUp.
 * Measured on a fresh order-service container: the first RecordPayment took 1.4-2.2 s on the
 * client side (channel, Netty and HTTP/2 set-up, class loading) while payment-service needed
 * 11-29 ms, close enough to the 3 s deadline to fail a demo order for no business reason.
 * <p>
 * It opens the payment channel with a health check (no side effect, so no payment is recorded)
 * and round-trips the RecordPayment messages through Protobuf locally. It runs on
 * {@link ApplicationReadyEvent}, before Spring reports readiness, and the compose healthcheck uses
 * {@code /actuator/health/readiness}: the container only turns healthy once warm. A failure (for
 * example payment-service not running) only logs a warning.
 */
@Component
class GrpcWarmUp {

	private static final Logger log = LoggerFactory.getLogger(GrpcWarmUp.class);

	private final PaymentGrpcClient payments;

	GrpcWarmUp(PaymentGrpcClient payments) {
		this.payments = payments;
	}

	@EventListener(ApplicationReadyEvent.class)
	void warmUp() {
		long start = System.nanoTime();
		try {
			RecordPaymentRequest request = RecordPaymentRequest.newBuilder()
				.setOrderId("WARMUP")
				.setPartnerOrderId("WARMUP")
				.setPartnerTransactionId("TXN-WARMUP")
				.setAmount(1)
				.build();
			RecordPaymentRequest.parseFrom(request.toByteArray());
			RecordPaymentResponse.parseFrom(RecordPaymentResponse.newBuilder()
				.setOrderId("WARMUP")
				.setStatus(PaymentStatus.RECORDED)
				.setRecordedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
				.build()
				.toByteArray());
			HealthCheckResponse.ServingStatus status = this.payments.checkHealth();
			log.atInfo()
				.addKeyValue("transport", "gRPC")
				.addKeyValue("action", "warm_up")
				.addKeyValue("status", "SUCCESS")
				.addKeyValue("payment_health", status.name())
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("gRPC path to payment-service warmed up");
		}
		catch (Exception ex) {
			// A failed warm-up must not stop the service: the first real call is only slower.
			log.atWarn()
				.addKeyValue("transport", "gRPC")
				.addKeyValue("action", "warm_up")
				.addKeyValue("status", "FAILED")
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.setCause(ex)
				.log("gRPC warm-up failed, continuing without it");
		}
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
