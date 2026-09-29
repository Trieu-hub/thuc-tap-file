package com.sandbox.order.order;

/**
 * Published by {@link OrderProgressService} inside the transaction that changed an order (status or
 * timeline). Listeners that must only see committed data use {@code @TransactionalEventListener}.
 */
public record OrderChangedEvent(String orderId) {

}
