package com.sandbox.order.flow;

import java.util.Optional;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.stereotype.Service;

import com.sandbox.contracts.payment.v1.PaymentStatus;
import com.sandbox.contracts.payment.v1.RecordPaymentRequest;
import com.sandbox.contracts.payment.v1.RecordPaymentResponse;
import com.sandbox.order.grpc.PaymentGrpcClient;
import com.sandbox.order.order.NewOrder;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderProgressService;
import com.sandbox.order.order.OrderRepository;
import com.sandbox.order.order.OrderView;
import com.sandbox.order.order.TimelineEntry;
import com.sandbox.order.order.TimelineStep;

/**
 * Flow 2, synchronous part: create the order, record the payment over gRPC, and answer. The policy
 * is issued later through Kafka (payment.recorded → policy-service → policy.issued), which moves the
 * order to ISSUED; the client polls {@code GET /api/v1/orders/{id}} for it.
 * <p>
 * The order is inserted (CREATED) before the gRPC call, so a policy.issued event that arrives very
 * early always finds its order. Not {@code @Transactional}, for the same reason as Flow 1: no
 * connection is held while waiting up to the 3 s deadline.
 */
@Service
public class GrpcKafkaOrderFlow {

	static final String PAYMENT_TIMEOUT = "PAYMENT_TIMEOUT";

	static final String PAYMENT_SERVICE_UNAVAILABLE = "PAYMENT_SERVICE_UNAVAILABLE";

	static final String PAYMENT_GRPC_ERROR = "PAYMENT_GRPC_ERROR";

	private static final Logger log = LoggerFactory.getLogger(GrpcKafkaOrderFlow.class);

	private final OrderIntake intake;

	private final OrderRepository orders;

	private final OrderProgressService progress;

	private final PaymentGrpcClient payments;

	public GrpcKafkaOrderFlow(OrderIntake intake, OrderRepository orders, OrderProgressService progress,
			PaymentGrpcClient payments) {
		this.intake = intake;
		this.orders = orders;
		this.progress = progress;
		this.payments = payments;
	}

	public OrderResult place(PlaceOrderCommand command) {
		long start = System.nanoTime();
		Optional<OrderResult> replay = this.intake.replay(command.partnerOrderId(), start);
		if (replay.isPresent()) {
			return replay.get();
		}
		NewOrder order = this.intake.newOrder(command, OrderMode.GRPC_KAFKA);
		MDC.put("correlation_id", order.correlationId());
		try {
			this.intake.insert(order, start);
			recordPayment(order);
			// Usually PAYMENT_RECORDED; already ISSUED if policy.issued overtook this thread.
			OrderView result = this.orders.findById(order.orderId()).orElseThrow();
			log.atInfo()
				.addKeyValue("transport", "HTTP")
				.addKeyValue("action", "CreateOrder")
				.addKeyValue("order_id", order.orderId())
				.addKeyValue("status", result.status().name())
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("Order accepted, policy follows through Kafka");
			return new OrderResult(result, false);
		}
		finally {
			MDC.remove("correlation_id");
		}
	}

	private void recordPayment(NewOrder order) {
		long paymentStart = System.nanoTime();
		RecordPaymentResponse payment;
		try {
			payment = this.payments.recordPayment(RecordPaymentRequest.newBuilder()
				.setOrderId(order.orderId())
				.setPartnerOrderId(order.partnerOrderId())
				.setPartnerTransactionId(order.partnerTransactionId())
				.setAmount(order.amount())
				.build());
		}
		catch (StatusRuntimeException ex) {
			grpcFailure(order, ex.getStatus(), elapsedMs(paymentStart));
			return;
		}
		long paymentMs = elapsedMs(paymentStart);
		log.atInfo()
			.addKeyValue("transport", "gRPC")
			.addKeyValue("action", "RecordPayment")
			.addKeyValue("order_id", order.orderId())
			.addKeyValue("status", payment.getStatus().name())
			.addKeyValue("execution_time_ms", paymentMs)
			.log("gRPC response received");
		if (payment.getStatus() != PaymentStatus.RECORDED) {
			// Nothing will be published for a rejected payment, so the order fails here.
			String reason = payment.getRejectReason().isEmpty() ? "PAYMENT_REJECTED" : payment.getRejectReason();
			this.progress.recordFailure(order.orderId(), reason, new TimelineEntry(TimelineStep.PROCESSING_FAILED,
					"payment-service", "gRPC", "REJECTED", paymentMs, reason));
			return;
		}
		this.progress.recordPayment(order.orderId(), new TimelineEntry(TimelineStep.PAYMENT, "payment-service",
				"gRPC", "SUCCESS", paymentMs, payment.getPaymentId() + (payment.getDuplicate() ? " (duplicate)" : "")));
	}

	private void grpcFailure(NewOrder order, Status status, long waitedMs) {
		String reason = switch (status.getCode()) {
			case DEADLINE_EXCEEDED -> PAYMENT_TIMEOUT;
			case UNAVAILABLE -> PAYMENT_SERVICE_UNAVAILABLE;
			default -> PAYMENT_GRPC_ERROR;
		};
		// After a deadline the payment may still be recorded (and published) later; see README limits.
		log.atWarn()
			.addKeyValue("transport", "gRPC")
			.addKeyValue("action", "grpc_call_failed")
			.addKeyValue("order_id", order.orderId())
			.addKeyValue("status", reason)
			.addKeyValue("grpc_status", status.getCode().name())
			.addKeyValue("execution_time_ms", waitedMs)
			.log("RecordPayment failed: " + status.getDescription());
		String timelineStatus = (status.getCode() == Status.Code.DEADLINE_EXCEEDED) ? "TIMEOUT" : "FAILED";
		this.progress.recordFailure(order.orderId(), reason, new TimelineEntry(TimelineStep.PROCESSING_FAILED,
				"payment-service", "gRPC", timelineStatus, waitedMs,
				reason + " (grpc_status=" + status.getCode().name() + ")"));
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
