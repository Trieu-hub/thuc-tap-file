package com.sandbox.order.api;

import java.util.List;

import jakarta.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sandbox.order.flow.GrpcKafkaOrderFlow;
import com.sandbox.order.flow.OrderResult;
import com.sandbox.order.flow.RabbitRpcOrderFlow;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderNotFoundException;
import com.sandbox.order.order.OrderRepository;
import com.sandbox.order.order.OrderStatus;

@RestController
@RequestMapping("/api/v1/orders")
class OrderController {

	private static final int LIST_LIMIT = 100;

	private final RabbitRpcOrderFlow rabbitRpcFlow;

	private final GrpcKafkaOrderFlow grpcKafkaFlow;

	private final OrderRepository orders;

	OrderController(RabbitRpcOrderFlow rabbitRpcFlow, GrpcKafkaOrderFlow grpcKafkaFlow, OrderRepository orders) {
		this.rabbitRpcFlow = rabbitRpcFlow;
		this.grpcKafkaFlow = grpcKafkaFlow;
		this.orders = orders;
	}

	/**
	 * RABBITMQ_RPC: 200 with ISSUED or PROCESSING_FAILED. GRPC_KAFKA: 202 once the payment is recorded
	 * (the policy follows asynchronously), 200 with PROCESSING_FAILED when the gRPC call failed. A
	 * duplicate partner_order_id is always 200 with the stored order (D4).
	 */
	@PostMapping
	ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
		if (request.mode() == OrderMode.RABBITMQ_RPC) {
			OrderResult result = this.rabbitRpcFlow.place(request.toCommand());
			return ResponseEntity.ok(OrderResponse.created(result.order(), result.idempotentReplay()));
		}
		OrderResult result = this.grpcKafkaFlow.place(request.toCommand());
		// 202 = accepted, still in progress: the client polls GET /api/v1/orders/{id} until ISSUED.
		boolean accepted = !result.idempotentReplay() && result.order().status() != OrderStatus.PROCESSING_FAILED;
		return ResponseEntity.status(accepted ? HttpStatus.ACCEPTED : HttpStatus.OK)
			.body(OrderResponse.created(result.order(), result.idempotentReplay()));
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
