package com.sandbox.payment.payment;

import java.time.Instant;

/**
 * Mirrors {@code RecordPaymentResponse} in payment.proto. {@code duplicate} is true when the
 * partner_transaction_id was already recorded and the stored payment is returned unchanged.
 */
public record RecordPaymentResult(String orderId, PaymentStatus status, String paymentId, String rejectReason,
		boolean duplicate, Instant recordedAt) {

	static RecordPaymentResult recorded(String orderId, String paymentId, Instant recordedAt, boolean duplicate) {
		return new RecordPaymentResult(orderId, PaymentStatus.RECORDED, paymentId, null, duplicate, recordedAt);
	}

	static RecordPaymentResult rejected(String orderId, String reason) {
		return new RecordPaymentResult(orderId, PaymentStatus.REJECTED, null, reason, false, null);
	}

}
