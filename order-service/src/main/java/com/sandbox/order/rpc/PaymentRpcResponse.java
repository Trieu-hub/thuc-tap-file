package com.sandbox.order.rpc;

import java.time.Instant;

/** contracts/schemas/payment-rpc-response.schema.json. {@code status} is RECORDED or REJECTED. */
public record PaymentRpcResponse(String orderId, String status, String paymentId, String rejectReason,
		boolean duplicate, Instant recordedAt) {

	public boolean recorded() {
		return "RECORDED".equals(this.status);
	}

}
