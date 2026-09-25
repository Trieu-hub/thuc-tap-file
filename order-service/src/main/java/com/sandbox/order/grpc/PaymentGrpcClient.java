package com.sandbox.order.grpc;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.stereotype.Component;

import com.sandbox.contracts.payment.v1.PaymentServiceGrpc;
import com.sandbox.contracts.payment.v1.RecordPaymentRequest;
import com.sandbox.contracts.payment.v1.RecordPaymentResponse;

/**
 * Calls {@code PaymentService.RecordPayment} on the channel named {@code payment}
 * ({@code spring.grpc.client.channel.payment.target}).
 * <p>
 * The deadline is set on every call, not once on the stub: a deadline is an absolute point in time,
 * so a stub created with one at startup would already be expired for later calls. On
 * {@code DEADLINE_EXCEEDED} or {@code UNAVAILABLE} the blocking stub throws
 * {@link io.grpc.StatusRuntimeException}, which the flow maps to a failure reason.
 */
@Component
public class PaymentGrpcClient {

	private final PaymentServiceGrpc.PaymentServiceBlockingStub stub;

	private final HealthGrpc.HealthBlockingStub health;

	private final Duration deadline;

	PaymentGrpcClient(GrpcChannelFactory channels, @Value("${sandbox.grpc.payment-deadline}") Duration deadline) {
		Channel channel = ClientInterceptors.intercept(channels.createChannel("payment"),
				new CorrelationIdClientInterceptor());
		this.stub = PaymentServiceGrpc.newBlockingStub(channel);
		// Same channel, so a health check opens the very connection RecordPayment will use.
		this.health = HealthGrpc.newBlockingStub(channel);
		this.deadline = deadline;
	}

	public RecordPaymentResponse recordPayment(RecordPaymentRequest request) {
		return this.stub.withDeadlineAfter(this.deadline.toMillis(), TimeUnit.MILLISECONDS).recordPayment(request);
	}

	/**
	 * Standard gRPC health check ({@code grpc.health.v1.Health/Check}) of payment-service. It has no
	 * side effect, unlike a RecordPayment call, which is why {@link GrpcWarmUp} uses it.
	 */
	HealthCheckResponse.ServingStatus checkHealth() {
		return this.health.withDeadlineAfter(this.deadline.toMillis(), TimeUnit.MILLISECONDS)
			.check(HealthCheckRequest.getDefaultInstance())
			.getStatus();
	}

}
