package com.sandbox.order.api;

import java.util.List;

import jakarta.validation.Valid;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sandbox.order.flow.OrderResult;
import com.sandbox.order.flow.RabbitRpcOrderFlow;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderNotFoundException;
import com.sandbox.order.order.OrderRepository;

@RestController
@RequestMapping("/api/v1/orders")
class OrderController {

	private static final int LIST_LIMIT = 100;

	private final RabbitRpcOrderFlow rabbitRpcFlow;

	private final OrderRepository orders;

	OrderController(RabbitRpcOrderFlow rabbitRpcFlow, OrderRepository orders) {
		this.rabbitRpcFlow = rabbitRpcFlow;
		this.orders = orders;
	}

	/** 200 with ISSUED or PROCESSING_FAILED; also 200 for a duplicate partner_order_id (D4). */
	@PostMapping
	OrderResponse create(@Valid @RequestBody CreateOrderRequest request) {
		if (request.mode() != OrderMode.RABBITMQ_RPC) {
			throw new ModeNotImplementedException(request.mode());
		}
		OrderResult result = this.rabbitRpcFlow.place(request.toCommand());
		return OrderResponse.created(result.order(), result.idempotentReplay());
	}

	/** Reads the database directly; the Redis read cache (D1) comes on Day 4. */
	@GetMapping("/{orderId}")
	OrderResponse get(@PathVariable String orderId) {
		return this.orders.findById(orderId)
			.map(OrderResponse::detail)
			.orElseThrow(() -> new OrderNotFoundException(orderId));
	}

	@GetMapping
	List<OrderResponse> list() {
		return this.orders.findLatest(LIST_LIMIT).stream().map(OrderResponse::summary).toList();
	}

}
