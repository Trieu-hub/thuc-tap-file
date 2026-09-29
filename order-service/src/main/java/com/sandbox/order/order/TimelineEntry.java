package com.sandbox.order.order;

/** One row of {@code order_timeline}, as reported by the service that ran the step. */
public record TimelineEntry(TimelineStep step, String service, String transport, String status, Long durationMs,
		String detail) {

}
