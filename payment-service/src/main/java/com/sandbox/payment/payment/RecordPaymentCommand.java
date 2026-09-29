package com.sandbox.payment.payment;

/**
 * Transport-neutral payment request: the RabbitMQ RPC listener (Flow 1) and the gRPC
 * {@code RecordPayment} method (Flow 2) both map their message to this command.
 */
public record RecordPaymentCommand(String orderId, String partnerOrderId, String partnerTransactionId, long amount) {

}
