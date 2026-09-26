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

import com.sandbox.order.cache.IdempotencyStore;
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
 * <p>
 * Two lines of defence: the Redis key {@code idempotency:order:{partner_order_id}} (spec V.1), then
 * MySQL's {@code UNIQUE(partner_order_id)}, which alone still works when Redis is down (D12).
 */
@Component
class OrderIntake {

	private static final Logger log = LoggerFactory.getLogger(OrderIntake.class);

	private static final DateTimeFormatter ORDER_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

	private final OrderRepository orders;

	private final IdempotencyStore idempotency;

	OrderIntake(OrderRepository orders, IdempotencyStore idempotency) {
		this.orders = orders;
		this.idempotency = idempotency;
	}

	/**
	 * The stored order as an idempotent replay, or empty when this partner_order_id is new.
	 * @throws OrderConflictException when the key is claimed but its order is not written yet (D11)
	 */
	Optional<OrderResult> replay(String partnerOrderId, long startNanos) {
		Optional<String> knownOrderId = this.idempotency.find(partnerOrderId);
		if (knownOrderId.isPresent()) {
			Optional<OrderView> order = this.orders.findById(knownOrderId.get());
			if (order.isEmpty()) {
				// Claimed by an identical request whose order row is not committed yet.
				throw conflict(partnerOrderId);
			}
			return Optional.of(replay(order.get(), "REDIS", startNanos));
		}
		// No key: a new partner_order_id, a key older than 24 h, or Redis down. MySQL decides.
		return this.orders.findByPartnerOrderId(partnerOrderId).map((order) -> replay(order, "MYSQL", startNanos));
	}

	private OrderResult replay(OrderView order, String foundIn, long startNanos) {
		MDC.put("correlation_id", order.correlationId());
		try {
			log.atInfo()
				.addKeyValue("transport", "HTTP")
				.addKeyValue("action", "idempotent_replay")
				.addKeyValue("order_id", order.orderId())
				.addKeyValue("status", order.status().name())
				.addKeyValue("idempotency_source", foundIn)
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

	/**
	 * Claims the idempotency key, then inserts the order as CREATED with its first timeline step.
	 * @throws OrderConflictException when an identical request claimed or inserted first (D11)
	 */
	void insert(NewOrder order, long startNanos) {
		IdempotencyStore.Claim claim = this.idempotency.claim(order.partnerOrderId(), order.orderId());
		if (claim == IdempotencyStore.Claim.TAKEN) {
			throw conflict(order.partnerOrderId());
		}
		try {
			this.orders.insertCreated(order, new TimelineEntry(TimelineStep.ORDER_CREATED, "order-service", "HTTP",
					"SUCCESS", elapsedMs(startNanos), null));
		}
		catch (DuplicateKeyException ex) {
			// UNIQUE(partner_order_id): an identical request inserted in between without the key (D11, D12).
			release(order, claim);
			throw conflict(order.partnerOrderId());
		}
		catch (RuntimeException ex) {
			release(order, claim);
			throw ex;
		}
		if (claim == IdempotencyStore.Claim.CLAIMED) {
			this.idempotency.confirm(order.partnerOrderId());
		}
	}

	private void release(NewOrder order, IdempotencyStore.Claim claim) {
		if (claim == IdempotencyStore.Claim.CLAIMED) {
			this.idempotency.release(order.partnerOrderId(), order.orderId());
		}
	}

	private static OrderConflictException conflict(String partnerOrderId) {
		log.atInfo()
			.addKeyValue("transport", "HTTP")
			.addKeyValue("action", "concurrent_duplicate_rejected")
			.addKeyValue("partner_order_id", partnerOrderId)
			.addKeyValue("status", "CONFLICT")
			.log("Identical request is already being processed");
		return new OrderConflictException(partnerOrderId);
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
