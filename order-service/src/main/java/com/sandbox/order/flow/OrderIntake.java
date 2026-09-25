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

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

import com.sandbox.order.order.NewOrder;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderRepository;
import com.sandbox.order.order.OrderView;
import com.sandbox.order.order.TimelineEntry;
import com.sandbox.order.order.TimelineStep;

/**
 * The first steps shared by both flows, before any transport is involved: return the stored order
 * for a repeated partner_order_id (D4), otherwise generate the identifiers and insert the order as
 * CREATED, answering 409 when an identical request won the insert (D11). Kept in one place so the
 * two flows cannot drift apart on idempotency.
 */
@Component
class OrderIntake {

	private static final Logger log = LoggerFactory.getLogger(OrderIntake.class);

	private static final DateTimeFormatter ORDER_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

	private final OrderRepository orders;

	OrderIntake(OrderRepository orders) {
		this.orders = orders;
	}

	/** The stored order as an idempotent replay, or empty when this partner_order_id is new. */
	Optional<OrderResult> replay(String partnerOrderId, long startNanos) {
		return this.orders.findByPartnerOrderId(partnerOrderId).map((order) -> replay(order, startNanos));
	}

	private OrderResult replay(OrderView order, long startNanos) {
		MDC.put("correlation_id", order.correlationId());
		try {
			log.atInfo()
				.addKeyValue("transport", "HTTP")
				.addKeyValue("action", "idempotent_replay")
				.addKeyValue("order_id", order.orderId())
				.addKeyValue("status", order.status().name())
				.addKeyValue("execution_time_ms", elapsedMs(startNanos))
				.log("Duplicate partner_order_id, returning the stored order");
			return new OrderResult(order, true);
		}
		finally {
			MDC.remove("correlation_id");
		}
	}

	NewOrder newOrder(PlaceOrderCommand command, OrderMode mode) {
		return new NewOrder(newOrderId(), command.partnerOrderId(), command.customerName(), command.phone(),
				command.amount(), mode, UUID.randomUUID().toString(), "TXN-" + command.partnerOrderId());
	}

	/** Inserts the order as CREATED with its first timeline step. */
	void insert(NewOrder order, long startNanos) {
		try {
			this.orders.insertCreated(order, new TimelineEntry(TimelineStep.ORDER_CREATED, "order-service", "HTTP",
					"SUCCESS", elapsedMs(startNanos), null));
		}
		catch (DuplicateKeyException ex) {
			// The lookup found nothing, so an identical request inserted in between (D11).
			log.atInfo()
				.addKeyValue("transport", "HTTP")
				.addKeyValue("action", "concurrent_duplicate_rejected")
				.addKeyValue("partner_order_id", order.partnerOrderId())
				.log("Identical request is already being processed");
			throw new OrderConflictException(order.partnerOrderId());
		}
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
