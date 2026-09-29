package com.sandbox.payment.rpc;

import com.sandbox.payment.payment.RecordPaymentCommand;

/** Body of a {@code payment.rpc.request} message: contracts/schemas/payment-rpc-request.schema.json. */
record PaymentRpcRequest(String orderId, String partnerOrderId, String partnerTransactionId, long amount) {

	RecordPaymentCommand toCommand() {
		return new RecordPaymentCommand(this.orderId, this.partnerOrderId, this.partnerTransactionId, this.amount);
	}

}
