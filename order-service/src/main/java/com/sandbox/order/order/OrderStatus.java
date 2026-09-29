package com.sandbox.order.order;

import java.util.EnumSet;
import java.util.Set;

/**
 * Order lifecycle. Transitions only move forward: a duplicate or out-of-order message can never
 * move an order back (e.g. a late PAYMENT_RECORDED after ISSUED, or a failure after ISSUED).
 */
public enum OrderStatus {

	CREATED, PAYMENT_RECORDED, ISSUED, PROCESSING_FAILED;

	/** Statuses from which a message may move the order to {@code this}. */
	Set<OrderStatus> allowedFrom() {
		return switch (this) {
			case CREATED -> EnumSet.noneOf(OrderStatus.class);
			case PAYMENT_RECORDED -> EnumSet.of(CREATED);
			// PROCESSING_FAILED -> ISSUED: the order timed out but the policy really was issued
			// afterwards (Flow 2); showing FAILED for an existing policy would be wrong.
			case ISSUED -> EnumSet.of(CREATED, PAYMENT_RECORDED, PROCESSING_FAILED);
			case PROCESSING_FAILED -> EnumSet.of(CREATED, PAYMENT_RECORDED);
		};
	}

}
