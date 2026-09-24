package com.sandbox.order.order;

/**
 * Outcome of applying one message to an order.
 * @param status the order status after the call
 * @param outcome whether the message changed the order
 */
public record TransitionResult(OrderStatus status, Outcome outcome) {

	public enum Outcome {

		/** Status changed. */
		APPLIED,

		/** Status left as is: the message is stale or a replay (its timeline step is still recorded once). */
		IGNORED,

		/** This exact event_id was processed before; nothing was touched. */
		DUPLICATE_EVENT

	}

}
