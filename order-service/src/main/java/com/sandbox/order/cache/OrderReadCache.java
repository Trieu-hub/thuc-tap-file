package com.sandbox.order.cache;

import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;

import com.sandbox.order.order.OrderChangedEvent;
import com.sandbox.order.order.OrderRepository;
import com.sandbox.order.order.OrderStatus;
import com.sandbox.order.order.OrderView;
import com.sandbox.order.order.TimelineStep;

/**
 * Read cache {@code order:{order_id}}, TTL 10 min, for {@code GET /api/v1/orders/{id}} (spec V.1):
 * Redis is always asked first, MySQL only on a miss.
 * <ul>
 * <li><b>Cache-aside, invalidate on write (D1):</b> only a GET writes the key; a state change deletes
 * it. The spec's diagrams write the cache when the order is issued, but then the first GET would
 * already be a HIT and the "CACHE MISS (DB)" of the first load (spec IV.3) could never be shown.</li>
 * <li><b>Only finished orders are written:</b> ISSUED with its NOTIFICATION step. A GET that reads
 * MySQL just before a change commits could otherwise store the old state after the change deleted the
 * key, and the UI would poll PAYMENT_RECORDED (or PROCESSING_FAILED, which can still become ISSUED)
 * for 10 minutes. The notification is its own transaction after ISSUED, hence it is part of the test.</li>
 * </ul>
 * Redis errors fall back to MySQL and are reported as a miss (D12).
 */
@Component
public class OrderReadCache {

	static final Duration TTL = Duration.ofMinutes(10);

	private static final Logger log = LoggerFactory.getLogger(OrderReadCache.class);

	private final StringRedisTemplate redis;

	private final JsonMapper jsonMapper;

	private final OrderRepository orders;

	public OrderReadCache(StringRedisTemplate redis, JsonMapper jsonMapper, OrderRepository orders) {
		this.redis = redis;
		this.jsonMapper = jsonMapper;
		this.orders = orders;
	}

	/** An order and where it was read from. */
	public record Read(OrderView order, boolean cacheHit) {

	}

	public Optional<Read> read(String orderId) {
		Optional<OrderView> cached = get(orderId);
		if (cached.isPresent()) {
			return Optional.of(new Read(cached.get(), true));
		}
		Optional<OrderView> stored = this.orders.findById(orderId);
		stored.filter(OrderReadCache::isFinished).ifPresent(this::put);
		return stored.map((order) -> new Read(order, false));
	}

	/** Deletes the key once the change is committed, so a GET after it cannot read the old copy. */
	@TransactionalEventListener
	void onOrderChanged(OrderChangedEvent event) {
		try {
			this.redis.delete(key(event.orderId()));
		}
		catch (RuntimeException ex) {
			// Only finished orders are cached, and the key expires after 10 min anyway.
			unavailable("evict", event.orderId(), ex);
		}
	}

	static boolean isFinished(OrderView order) {
		return order.status() == OrderStatus.ISSUED
				&& order.timeline().stream().anyMatch((step) -> step.step() == TimelineStep.NOTIFICATION);
	}

	static String key(String orderId) {
		return "order:" + orderId;
	}

	private Optional<OrderView> get(String orderId) {
		String json;
		try {
			json = this.redis.opsForValue().get(key(orderId));
		}
		catch (RuntimeException ex) {
			unavailable("get", orderId, ex);
			return Optional.empty();
		}
		if (json == null) {
			return Optional.empty();
		}
		try {
			return Optional.of(this.jsonMapper.readValue(json, OrderView.class));
		}
		catch (RuntimeException ex) {
			// A copy written by an older version of the class: treat as a miss, MySQL is the truth.
			log.atWarn()
				.addKeyValue("action", "cache_entry_unreadable")
				.addKeyValue("order_id", orderId)
				.addKeyValue("status", "IGNORED")
				.log("Cached order could not be read, reading MySQL");
			return Optional.empty();
		}
	}

	private void put(OrderView order) {
		try {
			this.redis.opsForValue().set(key(order.orderId()), this.jsonMapper.writeValueAsString(order), TTL);
		}
		catch (RuntimeException ex) {
			unavailable("put", order.orderId(), ex);
		}
	}

	private static void unavailable(String operation, String orderId, RuntimeException ex) {
		log.atWarn()
			.addKeyValue("action", "redis_unavailable")
			.addKeyValue("operation", "cache_" + operation)
			.addKeyValue("order_id", orderId)
			.addKeyValue("status", "DEGRADED")
			.addKeyValue("error", ex.getClass().getSimpleName())
			.log("Redis unavailable, reading MySQL");
	}

}
