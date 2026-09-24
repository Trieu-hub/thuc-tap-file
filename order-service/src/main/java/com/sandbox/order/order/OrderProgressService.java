package com.sandbox.order.order;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.sandbox.order.idempotency.ConsumerInbox;

/**
 * Applies the results that come back to order-service (RPC replies, gRPC responses and the Kafka
 * {@code policy.issued} event) to an order, safely under duplicates, reordering and concurrency.
 * <ul>
 * <li>The order row is locked ({@code SELECT ... FOR UPDATE}) first, so all updates to one order
 * run one after another and cannot overwrite each other.</li>
 * <li>Status only moves forward ({@link OrderStatus#allowedFrom()}), so a late or replayed message
 * cannot move an order back.</li>
 * <li>The timeline records what happened, once per step (uk_order_timeline_order_step), even if
 * the status did not change: a payment that is confirmed after the policy is still shown.</li>
 * </ul>
 */
@Service
public class OrderProgressService {

	static final String POLICY_ISSUED_EVENT = "policy.issued";

	private static final Logger log = LoggerFactory.getLogger(OrderProgressService.class);

	private final JdbcTemplate jdbc;

	private final ConsumerInbox inbox;

	public OrderProgressService(JdbcTemplate jdbc, ConsumerInbox inbox) {
		this.jdbc = jdbc;
		this.inbox = inbox;
	}

	@Transactional
	public TransitionResult recordPayment(String orderId, TimelineEntry entry) {
		return transition(orderId, OrderStatus.PAYMENT_RECORDED, entry, null, null);
	}

	/**
	 * @param eventId event_id of the Kafka {@code policy.issued} envelope (Flow 2), or {@code null}
	 * for an RPC reply (Flow 1)
	 */
	@Transactional
	public TransitionResult recordPolicyIssued(String orderId, String eventId, String policyNumber,
			TimelineEntry entry) {
		if (eventId != null && !this.inbox.tryClaim(eventId, POLICY_ISSUED_EVENT)) {
			log.atInfo()
				.addKeyValue("action", "duplicate_event_ignored")
				.addKeyValue("event_id", eventId)
				.addKeyValue("order_id", orderId)
				.log("Event already processed, order left unchanged");
			return new TransitionResult(currentStatus(orderId), TransitionResult.Outcome.DUPLICATE_EVENT);
		}
		return transition(orderId, OrderStatus.ISSUED, entry, policyNumber, null);
	}

	@Transactional
	public TransitionResult recordFailure(String orderId, String failureReason, TimelineEntry entry) {
		return transition(orderId, OrderStatus.PROCESSING_FAILED, entry, null, failureReason);
	}

	private TransitionResult transition(String orderId, OrderStatus target, TimelineEntry entry, String policyNumber,
			String failureReason) {
		OrderStatus current = lockOrder(orderId);
		boolean applied = target.allowedFrom().contains(current);
		if (applied) {
			this.jdbc.update("""
					UPDATE orders SET status = ?, policy_number = COALESCE(?, policy_number),
					failure_reason = COALESCE(?, failure_reason), updated_at = ?
					WHERE order_id = ?""", target.name(), policyNumber, failureReason, now(), orderId);
		}
		else {
			log.atInfo()
				.addKeyValue("action", "stale_transition_ignored")
				.addKeyValue("order_id", orderId)
				.addKeyValue("status", current.name())
				.addKeyValue("target_status", target.name())
				.log("Order already past this status, status left unchanged");
		}
		appendStepOnce(orderId, entry);
		return new TransitionResult(applied ? target : current,
				applied ? TransitionResult.Outcome.APPLIED : TransitionResult.Outcome.IGNORED);
	}

	private OrderStatus lockOrder(String orderId) {
		List<String> status = this.jdbc.queryForList("SELECT status FROM orders WHERE order_id = ? FOR UPDATE",
				String.class, orderId);
		if (status.isEmpty()) {
			throw new OrderNotFoundException(orderId);
		}
		return OrderStatus.valueOf(status.get(0));
	}

	private OrderStatus currentStatus(String orderId) {
		List<String> status = this.jdbc.queryForList("SELECT status FROM orders WHERE order_id = ?", String.class,
				orderId);
		if (status.isEmpty()) {
			throw new OrderNotFoundException(orderId);
		}
		return OrderStatus.valueOf(status.get(0));
	}

	private void appendStepOnce(String orderId, TimelineEntry entry) {
		// The order row lock serialises writers of this order, so MAX(seq) + 1 cannot be taken twice.
		// FOR UPDATE makes it a locking read: it sees the latest committed rows, not an older snapshot.
		Integer seq = this.jdbc.queryForObject(
				"SELECT COALESCE(MAX(seq), 0) + 1 FROM order_timeline WHERE order_id = ? FOR UPDATE", Integer.class,
				orderId);
		try {
			this.jdbc.update("""
					INSERT INTO order_timeline (order_id, seq, step, service, transport, status, duration_ms, detail, created_at)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", orderId, seq, entry.step().name(), entry.service(),
					entry.transport(), entry.status(), entry.durationMs(), entry.detail(), now());
		}
		catch (DuplicateKeyException ex) {
			log.atInfo()
				.addKeyValue("action", "duplicate_timeline_step_ignored")
				.addKeyValue("order_id", orderId)
				.addKeyValue("step", entry.step().name())
				.log("Timeline step already recorded");
		}
	}

	private static Timestamp now() {
		return Timestamp.from(Instant.now());
	}

}
