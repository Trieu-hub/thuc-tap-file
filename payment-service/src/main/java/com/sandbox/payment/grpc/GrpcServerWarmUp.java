package com.sandbox.payment.grpc;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.event.EventListener;
import org.springframework.grpc.server.lifecycle.GrpcServerStartedEvent;
import org.springframework.stereotype.Component;

import com.sandbox.contracts.payment.v1.PaymentServiceGrpc;
import com.sandbox.contracts.payment.v1.RecordPaymentRequest;
import com.sandbox.contracts.payment.v1.RecordPaymentResponse;

/**
 * Runs the server side of RecordPayment once as soon as the gRPC server listens, the counterpart
 * of order-service's GrpcWarmUp. Measured after restarting payment-service alone (order-service
 * still running, so its own warm-up does not run again): the first RecordPayment took 1746 ms in
 * payment-service instead of 11-53 ms, and the order hit its 3 s deadline while the payment was
 * recorded.
 * <p>
 * It calls its own server over a real local connection (Netty, HTTP/2, interceptor, Protobuf, the
 * service adapter) with amount 0: PaymentRecorder rejects it before touching the database, and a
 * REJECTED result is never published, so nothing is stored or sent. The server starts during
 * context refresh, before readiness is reported, and the compose healthcheck uses
 * {@code /actuator/health/readiness}. A failure only logs a warning.
 */
@Component
class GrpcServerWarmUp {

	private static final Logger log = LoggerFactory.getLogger(GrpcServerWarmUp.class);

	@EventListener
	void onServerStarted(GrpcServerStartedEvent event) {
		// The event carries the real port, also when tests bind port 0.
		warmUp(event.getPort());
	}

	void warmUp(int port) {
		long start = System.nanoTime();
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
		try {
			String id = "WARMUP-" + UUID.randomUUID().toString().substring(0, 12);
			RecordPaymentResponse response = PaymentServiceGrpc.newBlockingStub(channel)
				.withDeadlineAfter(10, TimeUnit.SECONDS)
				.recordPayment(RecordPaymentRequest.newBuilder()
					.setOrderId(id)
					.setPartnerOrderId(id)
					.setPartnerTransactionId("TXN-" + id)
					.setAmount(0)
					.build());
			log.atInfo()
				.addKeyValue("transport", "gRPC")
				.addKeyValue("action", "warm_up")
				.addKeyValue("status", "SUCCESS")
				.addKeyValue("warm_up_reply", response.getStatus().name())
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("gRPC server path warmed up, nothing persisted or published");
		}
		catch (RuntimeException ex) {
			// A failed warm-up must not stop the service: the first real call is only slower.
			log.atWarn()
				.addKeyValue("transport", "gRPC")
				.addKeyValue("action", "warm_up")
				.addKeyValue("status", "FAILED")
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.setCause(ex)
				.log("gRPC server warm-up failed, continuing without it");
		}
		finally {
			channel.shutdownNow();
		}
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
