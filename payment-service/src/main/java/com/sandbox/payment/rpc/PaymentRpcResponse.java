package com.sandbox.payment.rpc;

import java.time.Instant;

import com.sandbox.payment.payment.RecordPaymentResult;

/** Reply body: contracts/schemas/payment-rpc-response.schema.json. */
record PaymentRpcResponse(String orderId, String status, String paymentId, String rejectReason, boolean duplicate,
		Instant recordedAt) {

	static PaymentRpcResponse from(RecordPaymentResult result) {
		return new PaymentRpcResponse(result.orderId(), result.status().name(), result.paymentId(),
				result.rejectReason(), result.duplicate(), result.recordedAt());
	}

}
