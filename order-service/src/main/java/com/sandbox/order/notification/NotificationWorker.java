package com.sandbox.order.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

import com.sandbox.order.order.OrderProgressService;
import com.sandbox.order.order.TimelineEntry;
import com.sandbox.order.order.TimelineStep;

/**
 * Simulated notification step (D5): no SMS is sent, no extra service or broker is involved. It only
 * logs and records the timeline step so the demo shows the full chain.
 */
@Component
public class NotificationWorker {

	private static final Logger log = LoggerFactory.getLogger(NotificationWorker.class);

	private final OrderProgressService progress;

	public NotificationWorker(OrderProgressService progress) {
		this.progress = progress;
	}

	public void policyIssued(String orderId, String policyNumber) {
		long start = System.nanoTime();
		log.atInfo()
			.addKeyValue("transport", "IN_PROCESS")
			.addKeyValue("action", "SendNotification")
			.addKeyValue("order_id", orderId)
			.addKeyValue("status", "SUCCESS")
			.addKeyValue("policy_number", policyNumber)
			.log("SMS sent (simulated)");
		this.progress.recordStep(orderId, new TimelineEntry(TimelineStep.NOTIFICATION, "notification-worker",
				"IN_PROCESS", "SUCCESS", (System.nanoTime() - start) / 1_000_000, "SMS sent (simulated)"));
	}

}
