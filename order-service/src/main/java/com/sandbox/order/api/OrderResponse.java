package com.sandbox.order.api;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderStatus;
import com.sandbox.order.order.OrderView;

/**
 * Order as returned by the API. {@code idempotent_replay} is only sent by POST (D4),
 * {@code cache_status} only by GET of one order (D1), and {@code timeline} is left out of the list
 * endpoint.
 */
public record OrderResponse(String orderId, String partnerOrderId, String customerName, String phone, long amount,
		OrderMode mode, OrderStatus status, String failureReason, String correlationId, String policyNumber,
		@JsonInclude(JsonInclude.Include.NON_NULL) Boolean idempotentReplay,
		@JsonInclude(JsonInclude.Include.NON_NULL) CacheStatus cacheStatus, Instant createdAt, Instant updatedAt,
		@JsonInclude(JsonInclude.Include.NON_NULL) List<OrderView.Step> timeline) {

	/** Where GET read the order from; the UI shows "CACHE HIT (REDIS)" / "CACHE MISS (DB)". */
	public enum CacheStatus {

		CACHE_HIT_REDIS, CACHE_MISS_DB

	}

	static OrderResponse created(OrderView order, boolean idempotentReplay) {
		return of(order, idempotentReplay, null, order.timeline());
	}

	static OrderResponse detail(OrderView order, boolean cacheHit) {
		return of(order, null, cacheHit ? CacheStatus.CACHE_HIT_REDIS : CacheStatus.CACHE_MISS_DB, order.timeline());
	}

	static OrderResponse summary(OrderView order) {
		return of(order, null, null, null);
	}

	private static OrderResponse of(OrderView order, Boolean idempotentReplay, CacheStatus cacheStatus,
			List<OrderView.Step> timeline) {
		return new OrderResponse(order.orderId(), order.partnerOrderId(), order.customerName(), order.phone(),
				order.amount(), order.mode(), order.status(), order.failureReason(), order.correlationId(),
				order.policyNumber(), idempotentReplay, cacheStatus, order.createdAt(), order.updatedAt(), timeline);
	}

}
