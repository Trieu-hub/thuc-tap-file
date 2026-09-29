package com.sandbox.order.flow;

/** Validated input of {@code POST /api/v1/orders}, independent of the HTTP layer. */
public record PlaceOrderCommand(String partnerOrderId, String customerName, String phone, long amount) {

}
