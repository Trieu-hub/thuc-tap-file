package com.sandbox.order.flow;

/**
 * Two identical requests raced: the other one inserted the order first, and its result is not
 * final yet. Mapped to 409 so the caller retries instead of getting a half-built answer (D11).
 */
public class OrderConflictException extends RuntimeException {

	public OrderConflictException(String partnerOrderId) {
		super("Order with partner_order_id " + partnerOrderId + " is being processed, retry later");
	}

}
