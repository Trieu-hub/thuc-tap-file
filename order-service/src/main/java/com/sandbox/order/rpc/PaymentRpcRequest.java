package com.sandbox.order.rpc;

/** contracts/schemas/payment-rpc-request.schema.json. */
public record PaymentRpcRequest(String orderId, String partnerOrderId, String partnerTransactionId, long amount) {

}
