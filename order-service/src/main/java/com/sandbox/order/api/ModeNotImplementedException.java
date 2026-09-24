package com.sandbox.order.api;

import com.sandbox.order.order.OrderMode;

/** GRPC_KAFKA is a valid mode but Flow 2 is only built on Day 3; answered with 501. */
class ModeNotImplementedException extends RuntimeException {

	ModeNotImplementedException(OrderMode mode) {
		super("mode " + mode + " is not implemented yet");
	}

}
