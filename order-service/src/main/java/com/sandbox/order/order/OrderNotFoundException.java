package com.sandbox.order.order;

/**
 * Thrown for a message about an unknown order. It propagates so the transaction (including the
 * inbox claim) rolls back and the broker redelivers the message, instead of marking it processed.
 */
public class OrderNotFoundException extends RuntimeException {

	public OrderNotFoundException(String orderId) {
		super("Order not found: " + orderId);
	}

}
