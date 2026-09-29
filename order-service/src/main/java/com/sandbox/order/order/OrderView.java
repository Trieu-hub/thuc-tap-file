package com.sandbox.order.order;

import java.time.Instant;
import java.util.List;

/**
 * An order as read from the database. {@code timeline} is empty when read for the list endpoint.
 */
public record OrderView(String orderId, String partnerOrderId, String customerName, String phone, long amount,
		OrderMode mode, OrderStatus status, String failureReason, String correlationId, String policyNumber,
		Instant createdAt, Instant updatedAt, List<Step> timeline) {

	public record Step(int seq, TimelineStep step, String service, String transport, String status, Long durationMs,
			String detail, Instant createdAt) {

	}

}
