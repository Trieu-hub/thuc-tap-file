package com.sandbox.order.api;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.sandbox.order.flow.GrpcKafkaOrderFlow;
import com.sandbox.order.flow.OrderResult;
import com.sandbox.order.flow.RabbitRpcOrderFlow;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderRepository;
import com.sandbox.order.order.OrderStatus;
import com.sandbox.order.order.OrderView;

import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** HTTP contract only: nothing reaches the flow unless the body is valid. No Docker needed. */
@WebMvcTest(OrderController.class)
class OrderControllerValidationTests {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private RabbitRpcOrderFlow flow;

	@MockitoBean
	private GrpcKafkaOrderFlow grpcKafkaFlow;

	@MockitoBean
	private OrderRepository orders;

	@Test
	void invalidFieldsAreListedInSnakeCase() throws Exception {
		this.mockMvc
			.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content("""
					{"partner_order_id":"","customer_name":"Nguyen Van A","phone":"abc","amount":0,"mode":"RABBITMQ_RPC"}"""))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.error").value("VALIDATION_FAILED"))
			.andExpect(jsonPath("$.errors[*].field").value(contains("amount", "partner_order_id", "phone")));
		verify(this.flow, never()).place(any());
	}

	@Test
	void missingFieldsAreRejected() throws Exception {
		this.mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content("{}"))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.errors[*].field").value(
					contains("amount", "customer_name", "mode", "partner_order_id", "phone")));
	}

	@Test
	void unknownModeIsRejected() throws Exception {
		this.mockMvc
			.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content("""
					{"partner_order_id":"P-1","customer_name":"A","phone":"0901234567","amount":500000,"mode":"SOAP"}"""))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.errors[0].field").value("mode"));
		verify(this.flow, never()).place(any());
	}

	@Test
	void grpcKafkaModeGoesToFlow2AndIsAcceptedWith202() throws Exception {
		given(this.grpcKafkaFlow.place(any()))
			.willReturn(new OrderResult(order(OrderStatus.PAYMENT_RECORDED), false));

		this.mockMvc
			.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content("""
					{"partner_order_id":"P-1","customer_name":"A","phone":"0901234567","amount":500000,"mode":"GRPC_KAFKA"}"""))
			.andExpect(status().isAccepted())
			.andExpect(jsonPath("$.status").value("PAYMENT_RECORDED"));
		verify(this.flow, never()).place(any());
	}

	@Test
	void grpcKafkaFailureOrReplayIsAnsweredWith200() throws Exception {
		String body = """
				{"partner_order_id":"P-1","customer_name":"A","phone":"0901234567","amount":500000,"mode":"GRPC_KAFKA"}""";
		given(this.grpcKafkaFlow.place(any()))
			.willReturn(new OrderResult(order(OrderStatus.PROCESSING_FAILED), false))
			.willReturn(new OrderResult(order(OrderStatus.PAYMENT_RECORDED), true));

		this.mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(body))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.status").value("PROCESSING_FAILED"));
		this.mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(body))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.idempotent_replay").value(true));
	}

	@Test
	void unknownOrderIsNotFound() throws Exception {
		given(this.orders.findById(any())).willReturn(Optional.empty());

		this.mockMvc.perform(get("/api/v1/orders/ORD-404"))
			.andExpect(status().isNotFound())
			.andExpect(jsonPath("$.error").value("ORDER_NOT_FOUND"));
	}

	private static OrderView order(OrderStatus status) {
		Instant now = Instant.now();
		return new OrderView("ORD-1", "P-1", "A", "0901234567", 500_000, OrderMode.GRPC_KAFKA, status, null, "corr-1",
				null, now, now, List.of());
	}

}
