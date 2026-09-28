package com.sandbox.order.api;

import java.util.List;

import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sandbox.order.cache.OrderReadCache;
import com.sandbox.order.flow.GrpcKafkaOrderFlow;
import com.sandbox.order.flow.RabbitRpcOrderFlow;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderNotFoundException;
import com.sandbox.order.order.OrderRepository;
import com.sandbox.order.order.OrderStatus;
import com.sandbox.order.order.OrderView;

@RestController
@RequestMapping("/api/v1/orders")
class OrderController {

	static final String CORRELATION_HEADER = "X-Correlation-Id";

	private static final int LIST_LIMIT = 100;

	private static final Logger log = LoggerFactory.getLogger(OrderController.class);

	private final RabbitRpcOrderFlow rabbitRpcFlow;

	private final GrpcKafkaOrderFlow grpcKafkaFlow;

	private final OrderRepository orders;

	private final OrderReadCache cache;

	OrderController(RabbitRpcOrderFlow rabbitRpcFlow, GrpcKafkaOrderFlow grpcKafkaFlow, OrderRepository orders,
			OrderReadCache cache) {
		this.rabbitRpcFlow = rabbitRpcFlow;
		this.grpcKafkaFlow = grpcKafkaFlow;
		this.orders = orders;
		this.cache = cache;
	}

	/**
	 * RABBITMQ_RPC: 200 with ISSUED or PROCESSING_FAILED. GRPC_KAFKA: 202 once the payment is recorded
	 * (the policy follows asynchronously), 200 with PROCESSING_FAILED when the gRPC call failed. A
	 * repeated partner_order_id is refused with 409 by {@link ApiExceptionHandler} (D21).
	 */
	@PostMapping
	ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
		if (request.mode() == OrderMode.RABBITMQ_RPC) {
			return ResponseEntity.ok(OrderResponse.created(this.rabbitRpcFlow.place(request.toCommand())));
		}
		OrderView order = this.grpcKafkaFlow.place(request.toCommand());
		// 202 = accepted, still in progress: the client polls GET /api/v1/orders/{id} until ISSUED.
		boolean accepted = order.status() != OrderStatus.PROCESSING_FAILED;
		return ResponseEntity.status(accepted ? HttpStatus.ACCEPTED : HttpStatus.OK).body(OrderResponse.created(order));
	}

	/**
	 * Redis first, MySQL on a miss (spec V.1, D1). The order's correlation_id is only known once the
	 * order is read, so it is put in the MDC and the X-Correlation-Id header here, not in a filter.
	 */
	@GetMapping("/{orderId}")
	ResponseEntity<OrderResponse> get(@PathVariable String orderId) {
		long start = System.nanoTime();
		OrderReadCache.Read read = this.cache.read(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
		MDC.put("correlation_id", read.order().correlationId());
		try {
			log.atInfo()
				.addKeyValue("transport", "HTTP")
				.addKeyValue("action", "GetOrder")
				.addKeyValue("order_id", orderId)
				.addKeyValue("status", read.order().status().name())
				.addKeyValue("cache_hit", read.cacheHit())
				.addKeyValue("execution_time_ms", (System.nanoTime() - start) / 1_000_000)
				.log(read.cacheHit() ? "Order read from Redis" : "Order read from MySQL");
			return ResponseEntity.ok()
				.header(CORRELATION_HEADER, read.order().correlationId())
				.body(OrderResponse.detail(read.order(), read.cacheHit()));
		}
		finally {
			MDC.remove("correlation_id");
		}
	}

	@GetMapping
	List<OrderResponse> list() {
		return this.orders.findLatest(LIST_LIMIT).stream().map(OrderResponse::summary).toList();
	}

}
