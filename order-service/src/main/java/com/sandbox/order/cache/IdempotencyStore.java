package com.sandbox.order.cache;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Redis key {@code idempotency:order:{partner_order_id}}, value {@code order_id} (spec V.1).
 * <p>
 * Two stages: {@link #claim} sets the key with a 30 s TTL before the order row exists, and
 * {@link #confirm} extends it to 24 h once the row is committed. With 24 h from the start, a crash
 * between the two would leave a key without an order and every retry would get 409 for a day.
 * <p>
 * Redis is an optimisation, not a dependency (D12): every Redis error is logged as
 * {@code redis_unavailable} and reported as "unknown", and the caller falls back to MySQL, whose
 * {@code UNIQUE(partner_order_id)} is the second line of defence. While {@link RedisAvailability}
 * says Redis just failed, commands are not even sent (and not logged), so a request does not wait
 * for one timeout per command.
 */
@Component
public class IdempotencyStore {

	static final Duration CLAIM_TTL = Duration.ofSeconds(30);

	static final Duration KEY_TTL = Duration.ofHours(24);

	private static final Logger log = LoggerFactory.getLogger(IdempotencyStore.class);

	private final StringRedisTemplate redis;

	private final RedisAvailability availability;

	public IdempotencyStore(StringRedisTemplate redis, RedisAvailability availability) {
		this.redis = redis;
		this.availability = availability;
	}

	public enum Claim {

		/** This request owns the key now. */
		CLAIMED,

		/** Another request with the same partner_order_id owns it. */
		TAKEN,

		/** Redis did not answer; the caller relies on MySQL alone. */
		UNAVAILABLE

	}

	/** The order_id stored for this partner_order_id, or empty if there is none or Redis is down. */
	public Optional<String> find(String partnerOrderId) {
		return call("find", partnerOrderId, () -> Optional.ofNullable(this.redis.opsForValue().get(key(partnerOrderId))),
				Optional.empty());
	}

	/** {@code SET key order_id NX EX 30}: atomic, so of two identical requests only one gets CLAIMED. */
	public Claim claim(String partnerOrderId, String orderId) {
		return call("claim", partnerOrderId, () -> {
			Boolean set = this.redis.opsForValue().setIfAbsent(key(partnerOrderId), orderId, CLAIM_TTL);
			return Boolean.TRUE.equals(set) ? Claim.CLAIMED : Claim.TAKEN;
		}, Claim.UNAVAILABLE);
	}

	/** The order row is committed: keep the key for 24 h. */
	public void confirm(String partnerOrderId) {
		// If Redis fails here, the key expires after 30 s; MySQL's UNIQUE constraint still rejects a later duplicate.
		call("confirm", partnerOrderId, () -> this.redis.expire(key(partnerOrderId), KEY_TTL), null);
	}

	/** The insert failed: free the key so a retry is not answered 409, but only if it is still ours. */
	public void release(String partnerOrderId, String orderId) {
		call("release", partnerOrderId, () -> {
			if (Objects.equals(this.redis.opsForValue().get(key(partnerOrderId)), orderId)) {
				this.redis.delete(key(partnerOrderId));
			}
			return null;
		}, null);
	}

	static String key(String partnerOrderId) {
		return "idempotency:order:" + partnerOrderId;
	}

	/** Runs one Redis command, or returns the "Redis did not answer" value without calling it. */
	private <T> T call(String operation, String partnerOrderId, Supplier<T> command, T unavailable) {
		if (!this.availability.shouldTry()) {
			return unavailable;
		}
		try {
			T result = command.get();
			this.availability.markSuccess();
			return result;
		}
		catch (RuntimeException ex) {
			this.availability.markFailure();
			log.atWarn()
				.addKeyValue("action", "redis_unavailable")
				.addKeyValue("operation", "idempotency_" + operation)
				.addKeyValue("partner_order_id", partnerOrderId)
				.addKeyValue("status", "DEGRADED")
				.addKeyValue("error", ex.getClass().getSimpleName())
				.log("Redis unavailable, falling back to MySQL for idempotency");
			return unavailable;
		}
	}

}
