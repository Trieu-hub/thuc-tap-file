package com.sandbox.order.flow;

import com.sandbox.order.order.OrderView;

/**
 * @param order the order as stored after the flow ran
 * @param idempotentReplay true when the partner_order_id already existed and the stored order is
 * returned without running anything again (D4)
 */
public record OrderResult(OrderView order, boolean idempotentReplay) {

}
