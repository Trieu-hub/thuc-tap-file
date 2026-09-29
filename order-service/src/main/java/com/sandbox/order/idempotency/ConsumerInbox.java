package com.sandbox.order.idempotency;

import java.sql.Timestamp;
import java.time.Instant;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Remembers which Kafka events this service has already processed (table {@code consumer_inbox}).
 * <p>
 * The claim must commit in the same database transaction as the business write, hence
 * {@code MANDATORY}: if the consumer crashes before commit, both roll back and the redelivered
 * event is processed again; if it crashes after commit but before the offset commit, the
 * redelivered event finds its row and is skipped. Redis cannot give this guarantee because it is
 * not part of the MySQL transaction.
 * Copied per service on purpose (CLAUDE.md D17).
 */
@Component
public class ConsumerInbox {

	private final JdbcTemplate jdbc;

	public ConsumerInbox(JdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	/**
	 * Returns {@code true} if this call claimed the event, {@code false} if it was already processed.
	 * A concurrent claim of the same event blocks until the other transaction ends.
	 */
	@Transactional(propagation = Propagation.MANDATORY)
	public boolean tryClaim(String eventId, String eventType) {
		try {
			this.jdbc.update("INSERT INTO consumer_inbox (event_id, event_type, processed_at) VALUES (?, ?, ?)",
					eventId, eventType, Timestamp.from(Instant.now()));
			return true;
		}
		catch (DuplicateKeyException ex) {
			return false;
		}
	}

}
