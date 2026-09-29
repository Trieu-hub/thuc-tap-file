package com.sandbox.payment.payment;

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

/**
 * Records a payment exactly once per partner_transaction_id, however many times the request is
 * delivered (RabbitMQ redelivery after a crash before ack, gRPC client retry, partner resend).
 * <p>
 * The database is the only arbiter: the INSERT is attempted first and a unique-key violation means
 * "already recorded". A SELECT-then-INSERT check would let two concurrent deliveries both see no
 * row and both charge. InnoDB makes the second INSERT wait on the first one's index lock, so the
 * loser only sees the violation once the winner has committed (or proceeds if it rolled back).
 */
@Service
public class PaymentRecorder {

	static final String INVALID_AMOUNT = "INVALID_AMOUNT";

	static final String IDEMPOTENCY_KEY_MISMATCH = "IDEMPOTENCY_KEY_MISMATCH";

	private static final Logger log = LoggerFactory.getLogger(PaymentRecorder.class);

	private final JdbcTemplate jdbc;

	public PaymentRecorder(JdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	@Transactional
	public RecordPaymentResult record(RecordPaymentCommand command) {
		if (command.amount() <= 0) {
			return RecordPaymentResult.rejected(command.orderId(), INVALID_AMOUNT);
		}
		String paymentId = "PAY-" + UUID.randomUUID();
		Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
		try {
			this.jdbc.update("""
					INSERT INTO payments (payment_id, order_id, partner_transaction_id, amount, status, created_at)
					VALUES (?, ?, ?, ?, 'RECORDED', ?)""", paymentId, command.orderId(),
					command.partnerTransactionId(), command.amount(), Timestamp.from(now));
			return RecordPaymentResult.recorded(command.orderId(), paymentId, now, false);
		}
		catch (DuplicateKeyException ex) {
			// MySQL rolls back only the failed statement, so this transaction is still usable.
			return resolveDuplicate(command);
		}
	}

	private RecordPaymentResult resolveDuplicate(RecordPaymentCommand command) {
		// Locking read: sees the latest committed row, not a snapshot taken before the winner committed.
		List<StoredPayment> existing = this.jdbc.query("""
				SELECT payment_id, order_id, partner_transaction_id, amount, created_at
				FROM payments WHERE partner_transaction_id = ? OR order_id = ? FOR SHARE""",
				(rs, rowNum) -> new StoredPayment(rs.getString("payment_id"), rs.getString("order_id"),
						rs.getString("partner_transaction_id"), rs.getLong("amount"),
						rs.getTimestamp("created_at").toInstant()),
				command.partnerTransactionId(), command.orderId());
		if (existing.isEmpty()) {
			throw new IllegalStateException("Duplicate key reported but no payment found for order " + command.orderId());
		}
		StoredPayment stored = existing.get(0);
		if (existing.size() == 1 && stored.matches(command)) {
			log.atInfo()
				.addKeyValue("action", "duplicate_payment_ignored")
				.addKeyValue("status", "IGNORED")
				.addKeyValue("order_id", command.orderId())
				.addKeyValue("payment_id", stored.paymentId())
				.log("Payment already recorded, returning the stored result");
			return RecordPaymentResult.recorded(command.orderId(), stored.paymentId(), stored.createdAt(), true);
		}
		// Same key, different content: never return someone else's payment and never charge again.
		log.atWarn()
			.addKeyValue("action", "idempotency_key_mismatch")
			.addKeyValue("status", "REJECTED")
			.addKeyValue("order_id", command.orderId())
			.addKeyValue("partner_transaction_id", command.partnerTransactionId())
			.log("Payment request reuses an idempotency key with different content");
		return RecordPaymentResult.rejected(command.orderId(), IDEMPOTENCY_KEY_MISMATCH);
	}

	private record StoredPayment(String paymentId, String orderId, String partnerTransactionId, long amount,
			Instant createdAt) {

		boolean matches(RecordPaymentCommand command) {
			return this.orderId.equals(command.orderId())
					&& this.partnerTransactionId.equals(command.partnerTransactionId())
					&& this.amount == command.amount();
		}

	}

}
