package com.sandbox.order.order;

/** Values of {@code orders.mode}: which transport runs the order (CLAUDE.md section 7). */
public enum OrderMode {

	RABBITMQ_RPC, GRPC_KAFKA

}
