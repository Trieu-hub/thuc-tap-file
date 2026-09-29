package com.sandbox.order.flow;

import com.sandbox.order.order.OrderStatus;
import com.sandbox.order.order.OrderView;

/**
 * The partner_order_id already has an order (D21): the first valid request wins and every later one
 * is refused with 409. {@link #mismatch()} is true when the later request differs from the stored
 * order in any of the five request fields, i.e. the partner reused the id for a different order.
 */
public class DuplicateOrderException extends RuntimeException {

	private final String orderId;

	private final OrderStatus orderStatus;

	private final boolean mismatch;

	public DuplicateOrderException(OrderView stored, boolean mismatch) {
		super(mismatch
				? "partner_order_id " + stored.partnerOrderId() + " is already used by another order with different data"
				: "Order with partner_order_id " + stored.partnerOrderId() + " already exists");
		this.orderId = stored.orderId();
		this.orderStatus = stored.status();
		this.mismatch = mismatch;
	}

	public String orderId() {
		return this.orderId;
	}

	public OrderStatus orderStatus() {
		return this.orderStatus;
	}

	public boolean mismatch() {
		return this.mismatch;
	}

}
