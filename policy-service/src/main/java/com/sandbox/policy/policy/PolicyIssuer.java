package com.sandbox.policy.policy;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.sandbox.policy.idempotency.ConsumerInbox;

/**
 * Issues at most one policy per order, whatever the delivery pattern.
 * <ol>
 * <li>Same event delivered again (Kafka at-least-once): stopped by {@code consumer_inbox}.</li>
 * <li>Different event_id for the same order (payment re-published it) or a replayed RPC: stopped
 * by {@code uk_policies_order_id}; the stored policy is returned.</li>
 * <li>Two consumers racing on either case: InnoDB makes the second INSERT wait for the first
 * transaction, then it takes branch 1 or 2.</li>
 * </ol>
 * The inbox claim and the policy row commit together, so a crash in between leaves neither.
 */
@Service
public class PolicyIssuer {

	static final String PAYMENT_RECORDED_EVENT = "payment.recorded";

	static final int MAX_POLICY_NUMBER_ATTEMPTS = 5;

	private static final Logger log = LoggerFactory.getLogger(PolicyIssuer.class);

	private final JdbcTemplate jdbc;

	private final ConsumerInbox inbox;

	private final PolicyNumberGenerator policyNumbers;

	public PolicyIssuer(JdbcTemplate jdbc, ConsumerInbox inbox, PolicyNumberGenerator policyNumbers) {
		this.jdbc = jdbc;
		this.inbox = inbox;
		this.policyNumbers = policyNumbers;
	}

	@Transactional
	public IssuePolicyResult issue(IssuePolicyCommand command) {
		if (command.eventId() != null && !this.inbox.tryClaim(command.eventId(), PAYMENT_RECORDED_EVENT)) {
			log.atInfo()
				.addKeyValue("action", "duplicate_event_ignored")
				.addKeyValue("status", "IGNORED")
				.addKeyValue("event_id", command.eventId())
				.addKeyValue("order_id", command.orderId())
				.log("Event already processed, no new policy issued");
			return existingPolicy(command.orderId(), IssuePolicyResult.Outcome.DUPLICATE_EVENT_IGNORED);
		}
		for (int attempt = 1; attempt <= MAX_POLICY_NUMBER_ATTEMPTS; attempt++) {
			String policyId = "POL-" + UUID.randomUUID();
			String policyNumber = this.policyNumbers.next();
			Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
			try {
				this.jdbc.update("""
						INSERT INTO policies (policy_id, order_id, payment_id, policy_number, issued_at)
						VALUES (?, ?, ?, ?, ?)""", policyId, command.orderId(), command.paymentId(), policyNumber,
						Timestamp.from(now));
				return new IssuePolicyResult(IssuePolicyResult.Outcome.ISSUED, command.orderId(), policyId,
						policyNumber, now);
			}
			catch (DuplicateKeyException ex) {
				if (!violates(ex, "uk_policies_policy_number")) {
					// uk_policies_order_id: this order already has its policy.
					log.atInfo()
						.addKeyValue("action", "duplicate_policy_ignored")
						.addKeyValue("status", "IGNORED")
						.addKeyValue("order_id", command.orderId())
						.log("Order already has a policy, returning it");
					return existingPolicy(command.orderId(), IssuePolicyResult.Outcome.ALREADY_ISSUED);
				}
				// Only the random number collided: must not be mistaken for "already issued".
				log.atWarn()
					.addKeyValue("action", "policy_number_collision")
					.addKeyValue("status", "RETRY")
					.addKeyValue("order_id", command.orderId())
					.addKeyValue("attempt", attempt)
					.log("Generated policy number already exists, retrying");
			}
		}
		// Propagates: the transaction (and the inbox claim) rolls back, so the event is retried.
		throw new IllegalStateException("No unique policy number after " + MAX_POLICY_NUMBER_ATTEMPTS
				+ " attempts for order " + command.orderId());
	}

	private IssuePolicyResult existingPolicy(String orderId, IssuePolicyResult.Outcome outcome) {
		// Locking read: the row may have been committed by a concurrent transaction after ours began.
		List<IssuePolicyResult> rows = this.jdbc.query(
				"SELECT policy_id, policy_number, issued_at FROM policies WHERE order_id = ? FOR SHARE",
				(rs, rowNum) -> new IssuePolicyResult(outcome, orderId, rs.getString("policy_id"),
						rs.getString("policy_number"), rs.getTimestamp("issued_at").toInstant()),
				orderId);
		if (rows.isEmpty()) {
			throw new IllegalStateException("Expected an existing policy for order " + orderId);
		}
		return rows.get(0);
	}

	private static boolean violates(DuplicateKeyException ex, String constraintName) {
		String message = ex.getMostSpecificCause().getMessage();
		return message != null && message.contains(constraintName);
	}

}
