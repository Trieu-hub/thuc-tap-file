package com.sandbox.order.cache;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * A minimal circuit breaker for Redis (D12). With Redis down, every command waits for its 300 ms
 * timeout, and one POST makes 4-6 of them: about 1.9 s instead of 140 ms. After a failure, Redis is
 * skipped for {@code sandbox.redis.retry-after} (5 s by default) and callers fall back to MySQL at
 * once; the first command after that window is a real try again.
 * <p>
 * {@code redis_circuit_open} is logged once per outage (closed to open), and
 * {@code redis_circuit_closed} once when a command succeeds again; a failed retry only extends the
 * window. Thread-safe: the state is one {@link AtomicLong} holding the end of the window, or 0 when
 * the circuit is closed.
 */
@Component
public class RedisAvailability {

	private static final long CLOSED = 0;

	private static final Logger log = LoggerFactory.getLogger(RedisAvailability.class);

	private final Clock clock;

	private final Duration retryAfter;

	private final AtomicLong openUntil = new AtomicLong(CLOSED);

	public RedisAvailability(Clock clock, @Value("${sandbox.redis.retry-after:5s}") Duration retryAfter) {
		this.clock = clock;
		this.retryAfter = retryAfter;
	}

	/** False while inside the skip window: treat Redis as not answering without calling it. */
	public boolean shouldTry() {
		long until = this.openUntil.get();
		return until == CLOSED || this.clock.millis() >= until;
	}

	/** A Redis command failed: skip Redis for the next retry-after. */
	public void markFailure() {
		long until = this.clock.millis() + this.retryAfter.toMillis();
		long previous = this.openUntil.getAndSet(until);
		if (previous == CLOSED) {
			log.atWarn()
				.addKeyValue("action", "redis_circuit_open")
				.addKeyValue("status", "OPEN")
				.addKeyValue("retry_after_ms", this.retryAfter.toMillis())
				.log("Redis unavailable, skipping it and using MySQL only for the next " + this.retryAfter.toSeconds()
						+ " s");
		}
	}

	/** A Redis command succeeded: close the circuit if it was open. */
	public void markSuccess() {
		if (this.openUntil.get() != CLOSED && this.openUntil.getAndSet(CLOSED) != CLOSED) {
			log.atInfo()
				.addKeyValue("action", "redis_circuit_closed")
				.addKeyValue("status", "CLOSED")
				.log("Redis answers again, using it for idempotency and the read cache");
		}
	}

}
