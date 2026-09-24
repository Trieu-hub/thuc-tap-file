package com.sandbox.order.api;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderStatus;
import com.sandbox.order.order.OrderView;

/**
 * Order as returned by the API. {@code idempotent_replay} is only sent by POST (D4) and
 * {@code timeline} is left out of the list endpoint.
 */
public record OrderResponse(String orderId, String partnerOrderId, String customerName, String phone, long amount,
		OrderMode mode, OrderStatus status, String failureReason, String correlationId, String policyNumber,
		@JsonInclude(JsonInclude.Include.NON_NULL) Boolean idempotentReplay, Instant createdAt, Instant updatedAt,
		@JsonInclude(JsonInclude.Include.NON_NULL) List<OrderView.Step> timeline) {

	static OrderResponse created(OrderView order, boolean idempotentReplay) {
		return of(order, idempotentReplay, order.timeline());
	}

	static OrderResponse detail(OrderView order) {
		return of(order, null, order.timeline());
	}

	static OrderResponse summary(OrderView order) {
		return of(order, null, null);
	}

	private static OrderResponse of(OrderView order, Boolean idempotentReplay, List<OrderView.Step> timeline) {
		return new OrderResponse(order.orderId(), order.partnerOrderId(), order.customerName(), order.phone(),
				order.amount(), order.mode(), order.status(), order.failureReason(), order.correlationId(),
				order.policyNumber(), idempotentReplay, order.createdAt(), order.updatedAt(), timeline);
	}

}
