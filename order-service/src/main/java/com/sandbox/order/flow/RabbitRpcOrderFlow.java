package com.sandbox.order.flow;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.amqp.AmqpException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import com.sandbox.order.notification.NotificationWorker;
import com.sandbox.order.order.NewOrder;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderProgressService;
import com.sandbox.order.order.OrderRepository;
import com.sandbox.order.order.OrderView;
import com.sandbox.order.order.TimelineEntry;
import com.sandbox.order.order.TimelineStep;
import com.sandbox.order.rpc.OrderRpcClient;
import com.sandbox.order.rpc.PaymentRpcRequest;
import com.sandbox.order.rpc.PaymentRpcResponse;
import com.sandbox.order.rpc.PolicyRpcRequest;
import com.sandbox.order.rpc.PolicyRpcResponse;

/**
 * Flow 1: create the order, then RPC to payment-service, then RPC to policy-service, waiting for each
 * reply on the HTTP thread.
 * <p>
 * Deliberately not {@code @Transactional}: each database write below is its own short transaction,
 * so no pooled connection is held during the up to 2 x 3 s spent waiting for replies. Holding one
 * would let a handful of slow requests exhaust the Hikari pool.
 */
@Service
public class RabbitRpcOrderFlow {

	static final String PAYMENT_TIMEOUT = "PAYMENT_TIMEOUT";

	static final String POLICY_TIMEOUT = "POLICY_TIMEOUT";

	static final String BROKER_UNAVAILABLE = "BROKER_UNAVAILABLE";

	private static final Logger log = LoggerFactory.getLogger(RabbitRpcOrderFlow.class);

	private static final DateTimeFormatter ORDER_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

	private final OrderRepository orders;

	private final OrderProgressService progress;

	private final OrderRpcClient rpc;

	private final NotificationWorker notifications;

	public RabbitRpcOrderFlow(OrderRepository orders, OrderProgressService progress, OrderRpcClient rpc,
			NotificationWorker notifications) {
		this.orders = orders;
		this.progress = progress;
		this.rpc = rpc;
		this.notifications = notifications;
	}

	public OrderResult place(PlaceOrderCommand command) {
		long start = System.nanoTime();
		Optional<OrderView> existing = this.orders.findByPartnerOrderId(command.partnerOrderId());
		if (existing.isPresent()) {
			return replay(existing.get(), start);
		}
		NewOrder order = new NewOrder(newOrderId(), command.partnerOrderId(), command.customerName(),
				command.phone(), command.amount(), OrderMode.RABBITMQ_RPC, UUID.randomUUID().toString(),
				"TXN-" + command.partnerOrderId());
		MDC.put("correlation_id", order.correlationId());
		try {
			insert(order, start);
			run(order);
			OrderView result = this.orders.findById(order.orderId()).orElseThrow();
			log.atInfo()
				.addKeyValue("transport", "HTTP")
				.addKeyValue("action", "CreateOrder")
				.addKeyValue("order_id", order.orderId())
				.addKeyValue("status", result.status().name())
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("Order processed");
			return new OrderResult(result, false);
		}
		finally {
			MDC.remove("correlation_id");
		}
	}

	private OrderResult replay(OrderView order, long start) {
		MDC.put("correlation_id", order.correlationId());
		try {
			log.atInfo()
				.addKeyValue("transport", "HTTP")
				.addKeyValue("action", "idempotent_replay")
				.addKeyValue("order_id", order.orderId())
				.addKeyValue("status", order.status().name())
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("Duplicate partner_order_id, returning the stored order");
			return new OrderResult(order, true);
		}
		finally {
			MDC.remove("correlation_id");
		}
	}

	private void insert(NewOrder order, long start) {
		try {
			this.orders.insertCreated(order, new TimelineEntry(TimelineStep.ORDER_CREATED, "order-service", "HTTP",
					"SUCCESS", elapsedMs(start), null));
		}
		catch (DuplicateKeyException ex) {
			// The lookup above found nothing, so an identical request inserted in between (D11).
			log.atInfo()
				.addKeyValue("transport", "HTTP")
				.addKeyValue("action", "concurrent_duplicate_rejected")
				.addKeyValue("partner_order_id", order.partnerOrderId())
				.log("Identical request is already being processed");
			throw new OrderConflictException(order.partnerOrderId());
		}
	}

	private void run(NewOrder order) {
		long paymentStart = System.nanoTime();
		PaymentRpcResponse payment;
		try {
			payment = this.rpc.recordPayment(new PaymentRpcRequest(order.orderId(), order.partnerOrderId(),
					order.partnerTransactionId(), order.amount()), order.correlationId());
		}
		catch (AmqpException ex) {
			brokerFailure(order, "payment-service", paymentStart, ex);
			return;
		}
		long paymentMs = elapsedMs(paymentStart);
		if (payment == null) {
			timeout(order, "payment-service", OrderRpcClient.PAYMENT_QUEUE, PAYMENT_TIMEOUT, paymentMs);
			return;
		}
		logReply("RecordPayment", order, payment.status(), paymentMs);
		if (!payment.recorded()) {
			// Policy must only be issued after a recorded payment, so the flow stops here.
			String reason = (payment.rejectReason() != null) ? payment.rejectReason() : "PAYMENT_REJECTED";
			this.progress.recordFailure(order.orderId(), reason, new TimelineEntry(TimelineStep.PROCESSING_FAILED,
					"payment-service", "RabbitMQ", "REJECTED", paymentMs, reason));
			return;
		}
		this.progress.recordPayment(order.orderId(), new TimelineEntry(TimelineStep.PAYMENT, "payment-service",
				"RabbitMQ", "SUCCESS", paymentMs, payment.paymentId() + (payment.duplicate() ? " (duplicate)" : "")));

		long policyStart = System.nanoTime();
		PolicyRpcResponse policy;
		try {
			policy = this.rpc.issuePolicy(new PolicyRpcRequest(order.orderId(), payment.paymentId()),
					order.correlationId());
		}
		catch (AmqpException ex) {
			brokerFailure(order, "policy-service", policyStart, ex);
			return;
		}
		long policyMs = elapsedMs(policyStart);
		if (policy == null) {
			timeout(order, "policy-service", OrderRpcClient.POLICY_QUEUE, POLICY_TIMEOUT, policyMs);
			return;
		}
		logReply("IssuePolicy", order, policy.status(), policyMs);
		// eventId null: an RPC reply has no Kafka event_id to deduplicate on.
		this.progress.recordPolicyIssued(order.orderId(), null, policy.policyNumber(), new TimelineEntry(
				TimelineStep.POLICY_ISSUANCE, "policy-service", "RabbitMQ", "SUCCESS", policyMs, policy.policyNumber()));
		this.notifications.policyIssued(order.orderId(), policy.policyNumber());
	}

	private void timeout(NewOrder order, String service, String queue, String reason, long waitedMs) {
		// The request may still be processed later (e.g. a payment recorded after we gave up); see README limits.
		log.atWarn()
			.addKeyValue("transport", "RabbitMQ")
			.addKeyValue("action", "rpc_timeout")
			.addKeyValue("order_id", order.orderId())
			.addKeyValue("queue", queue)
			.addKeyValue("status", reason)
			.addKeyValue("execution_time_ms", waitedMs)
			.log("No RPC reply within the timeout");
		this.progress.recordFailure(order.orderId(), reason, new TimelineEntry(TimelineStep.PROCESSING_FAILED, service,
				"RabbitMQ", "TIMEOUT", waitedMs, reason));
	}

	private void brokerFailure(NewOrder order, String service, long start, AmqpException ex) {
		long waitedMs = elapsedMs(start);
		log.atError()
			.addKeyValue("transport", "RabbitMQ")
			.addKeyValue("action", "rpc_failed")
			.addKeyValue("order_id", order.orderId())
			.addKeyValue("status", BROKER_UNAVAILABLE)
			.addKeyValue("execution_time_ms", waitedMs)
			.setCause(ex)
			.log("RPC request could not be sent");
		this.progress.recordFailure(order.orderId(), BROKER_UNAVAILABLE, new TimelineEntry(
				TimelineStep.PROCESSING_FAILED, service, "RabbitMQ", "FAILED", waitedMs, BROKER_UNAVAILABLE));
	}

	private static void logReply(String action, NewOrder order, String status, long elapsedMs) {
		log.atInfo()
			.addKeyValue("transport", "RabbitMQ")
			.addKeyValue("action", action)
			.addKeyValue("order_id", order.orderId())
			.addKeyValue("status", status)
			.addKeyValue("execution_time_ms", elapsedMs)
			.log("RPC reply received");
	}

	/**
	 * e.g. ORD-20260924-4F7A2C1B. A random suffix needs no sequence table shared between instances; a
	 * collision would fail the insert on the primary key instead of overwriting another order.
	 */
	private static String newOrderId() {
		String random = UUID.randomUUID().toString().substring(0, 8).toUpperCase(Locale.ROOT);
		return "ORD-" + LocalDate.now(ZoneOffset.UTC).format(ORDER_DATE) + "-" + random;
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
