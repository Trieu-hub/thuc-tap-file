package com.sandbox.order.flow;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
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
 * The first steps shared by both flows, before any transport is involved: refuse a repeated
 * partner_order_id with 409 (D21), otherwise generate the identifiers and insert the order as
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
	 * Refuses a repeated partner_order_id (D21): the first valid request keeps it, a later one gets
	 * 409 DUPLICATE_ORDER when all five fields match the stored order and 409
	 * DUPLICATE_ORDER_MISMATCH otherwise. Returns normally for a new partner_order_id.
	 * @throws DuplicateOrderException when an order with this partner_order_id exists
	 * @throws OrderConflictException when the key is claimed but its order is not written yet (D11)
	 */
	void rejectDuplicate(PlaceOrderCommand command, OrderMode mode, long startNanos) {
		Optional<String> knownOrderId = this.idempotency.find(command.partnerOrderId());
		if (knownOrderId.isPresent()) {
			Optional<OrderView> order = this.orders.findById(knownOrderId.get());
			if (order.isEmpty()) {
				// Claimed by a request whose order row is not committed yet: nothing to compare with.
				throw conflict(command.partnerOrderId());
			}
			throw duplicate(order.get(), command, mode, "REDIS", startNanos);
		}
		// No key: a new partner_order_id, a key older than 24 h, or Redis down. MySQL decides.
		Optional<OrderView> order = this.orders.findByPartnerOrderId(command.partnerOrderId());
		if (order.isPresent()) {
			throw duplicate(order.get(), command, mode, "MYSQL", startNanos);
		}
	}

	private DuplicateOrderException duplicate(OrderView order, PlaceOrderCommand command, OrderMode mode,
			String foundIn, long startNanos) {
		List<String> mismatched = mismatchedFields(order, command, mode);
		boolean mismatch = !mismatched.isEmpty();
		// The stored order's correlation_id, so the refusal shows up when tracing the original order.
		MDC.put("correlation_id", order.correlationId());
		try {
			var event = (mismatch ? log.atWarn() : log.atInfo()).addKeyValue("transport", "HTTP")
				.addKeyValue("action", mismatch ? "duplicate_order_mismatch" : "duplicate_order_rejected")
				.addKeyValue("order_id", order.orderId())
				.addKeyValue("status", mismatch ? "DUPLICATE_ORDER_MISMATCH" : "DUPLICATE_ORDER")
				.addKeyValue("idempotency_source", foundIn)
				.addKeyValue("execution_time_ms", elapsedMs(startNanos));
			if (mismatch) {
				// Field names only: the values are customer data.
				event = event.addKeyValue("mismatched_fields", String.join(",", mismatched));
			}
			event.log(mismatch ? "partner_order_id reused with different data, request refused"
					: "Duplicate partner_order_id, request refused");
			return new DuplicateOrderException(order, mismatch);
		}
		finally {
			MDC.remove("correlation_id");
		}
	}

	/**
	 * The request fields that differ from the stored order, compared exactly. partner_order_id is
	 * compared too: MySQL finds it case-insensitively (utf8mb4_unicode_ci), Redis does not.
	 */
	static List<String> mismatchedFields(OrderView order, PlaceOrderCommand command, OrderMode mode) {
		List<String> fields = new ArrayList<>();
		if (!order.partnerOrderId().equals(command.partnerOrderId())) {
			fields.add("partner_order_id");
		}
		if (!order.customerName().equals(command.customerName())) {
			fields.add("customer_name");
		}
		if (!order.phone().equals(command.phone())) {
			fields.add("phone");
		}
		if (order.amount() != command.amount()) {
			fields.add("amount");
		}
		if (order.mode() != mode) {
			fields.add("mode");
		}
		return fields;
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
