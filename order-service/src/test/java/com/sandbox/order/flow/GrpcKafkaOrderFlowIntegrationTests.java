package com.sandbox.order.flow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mysql.MySQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.sandbox.contracts.payment.v1.PaymentServiceGrpc;
import com.sandbox.contracts.payment.v1.PaymentStatus;
import com.sandbox.contracts.payment.v1.RecordPaymentRequest;
import com.sandbox.contracts.payment.v1.RecordPaymentResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Flow 2 from order-service's side, over real HTTP, a real gRPC (HTTP/2) connection and a real Kafka
 * 4.2 (topic auto-creation off). payment-service and policy-service run in their own JVMs, so a fake
 * gRPC server built from the same payment.proto stands in for payment (RECORDED, REJECTED, an error
 * status, or 4 s late), and the test itself produces policy.issued as policy-service would. MySQL 8.4
 * via Testcontainers; skipped without Docker. The real three-service run is the README's end-to-end check.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
		properties = { "spring.kafka.listener.auto-startup=true", "spring.kafka.admin.auto-create=true" })
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
// Close the context with the class: its consumer would otherwise keep polling the stopped container.
@DirtiesContext
class GrpcKafkaOrderFlowIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Container
	@ServiceConnection
	static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.1")
		.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

	static final FakePayment payment = new FakePayment();

	static final Server paymentServer = start(payment);

	private final HttpClient http = HttpClient.newHttpClient();

	@LocalServerPort
	private int port;

	@Autowired
	private JsonMapper jsonMapper;

	@Autowired
	private JdbcTemplate jdbc;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaListenerEndpointRegistry listeners;

	@DynamicPropertySource
	static void paymentTarget(DynamicPropertyRegistry registry) {
		registry.add("spring.grpc.client.channel.payment.target", () -> "static://localhost:" + paymentServer.getPort());
	}

	@AfterAll
	static void stopPaymentServer() {
		paymentServer.shutdownNow();
	}

	@BeforeEach
	void reset() {
		this.jdbc.update("DELETE FROM order_timeline");
		this.jdbc.update("DELETE FROM orders");
		payment.reset();
	}

	@Test
	void recordedPaymentIsAcceptedWith202AndCorrelationIdTravelsInMetadata(CapturedOutput output) throws Exception {
		HttpResponse<String> response = post(order("P-GRPC-OK"));

		assertThat(response.statusCode()).isEqualTo(202);
		JsonNode body = json(response);
		String correlationId = body.path("correlation_id").asString();
		assertThat(body.path("status").asString()).isEqualTo("PAYMENT_RECORDED");
		assertThat(body.path("idempotent_replay").asBoolean()).isFalse();
		assertThat(steps(body)).containsExactly("ORDER_CREATED", "PAYMENT");
		assertThat(body.path("timeline").get(1).path("transport").asString()).isEqualTo("gRPC");
		assertThat(payment.correlationIds).containsExactly(correlationId);
		assertThat(output.getOut().lines())
			.anyMatch((line) -> line.contains("\"transport\":\"gRPC\"") && line.contains("\"action\":\"RecordPayment\"")
					&& line.contains("\"correlation_id\":\"" + correlationId + "\""));
	}

	@Test
	void acceptedOrderReachesIssuedWhenPolicyIssuedArrives(CapturedOutput output) throws Exception {
		await().atMost(Duration.ofSeconds(30))
			.until(() -> this.listeners.getListenerContainers()
				.stream()
				.allMatch((container) -> container.getAssignedPartitions() != null
						&& !container.getAssignedPartitions().isEmpty()));

		HttpResponse<String> response = post(order("P-E2E"));
		assertThat(response.statusCode()).isEqualTo(202);
		JsonNode accepted = json(response);
		String orderId = accepted.path("order_id").asString();
		String correlationId = accepted.path("correlation_id").asString();
		assertThat(accepted.path("status").asString()).isEqualTo("PAYMENT_RECORDED");

		// policy-service's part: it received payment.recorded and publishes policy.issued, key = order_id.
		this.kafkaTemplate.send("policy.issued", orderId, """
				{"event_id":"%s","event_type":"policy.issued","correlation_id":"%s","occurred_at":"%s",
				 "payload":{"order_id":"%s","policy_id":"POL-E2E","policy_number":"ACBI-2026-424242","issued_at":"%s"}}"""
			.formatted(UUID.randomUUID(), correlationId, Instant.now(), orderId, Instant.now()))
			.get();

		// What the UI does: poll GET until the order is final.
		JsonNode issued = await().atMost(Duration.ofSeconds(15))
			.pollInterval(Duration.ofMillis(200))
			.until(() -> json(get("/api/v1/orders/" + orderId)),
					(order) -> "ISSUED".equals(order.path("status").asString())
							&& steps(order).contains("NOTIFICATION"));
		assertThat(issued.path("policy_number").asString()).isEqualTo("ACBI-2026-424242");
		assertThat(steps(issued)).containsExactly("ORDER_CREATED", "PAYMENT", "POLICY_ISSUANCE", "NOTIFICATION");
		assertThat(issued.path("timeline").get(1).path("transport").asString()).isEqualTo("gRPC");
		assertThat(issued.path("timeline").get(2).path("transport").asString()).isEqualTo("Kafka");
		// One correlation_id across the HTTP, gRPC and Kafka hops of order-service, and sent to payment.
		assertThat(payment.correlationIds).containsExactly(correlationId);
		for (String transport : List.of("HTTP", "gRPC", "Kafka")) {
			assertThat(output.getOut().lines()).as(transport)
				.anyMatch((line) -> line.contains("\"transport\":\"" + transport + "\"")
						&& line.contains("\"correlation_id\":\"" + correlationId + "\""));
		}
	}

	@Test
	void slowPaymentHitsDeadlineAndFailsOrderWithinThreeAndAHalfSeconds(CapturedOutput output) throws Exception {
		payment.delayMs = 4000;

		Instant start = Instant.now();
		HttpResponse<String> response = post(order("P-GRPC-SLOW"));
		Duration took = Duration.between(start, Instant.now());

		assertThat(took).as("waits the 3 s deadline, then answers without hanging")
			.isBetween(Duration.ofMillis(2900), Duration.ofMillis(3500));
		assertThat(response.statusCode()).isEqualTo(200);
		JsonNode body = json(response);
		assertThat(body.path("status").asString()).isEqualTo("PROCESSING_FAILED");
		assertThat(body.path("failure_reason").asString()).isEqualTo(GrpcKafkaOrderFlow.PAYMENT_TIMEOUT);
		assertThat(steps(body)).containsExactly("ORDER_CREATED", "PROCESSING_FAILED");
		assertThat(body.path("timeline").get(1).path("detail").asString()).contains("grpc_status=DEADLINE_EXCEEDED");
		assertThat(output).contains("\"action\":\"grpc_call_failed\"", "\"grpc_status\":\"DEADLINE_EXCEEDED\"");
	}

	@Test
	void rejectedPaymentFailsOrderWithPaymentReason() throws Exception {
		payment.rejectReason = "INVALID_AMOUNT";

		HttpResponse<String> response = post(order("P-GRPC-REJECT"));

		assertThat(response.statusCode()).isEqualTo(200);
		JsonNode body = json(response);
		assertThat(body.path("status").asString()).isEqualTo("PROCESSING_FAILED");
		assertThat(body.path("failure_reason").asString()).isEqualTo("INVALID_AMOUNT");
		assertThat(steps(body)).containsExactly("ORDER_CREATED", "PROCESSING_FAILED");
	}

	@Test
	void otherGrpcErrorFailsOrderWithGenericReasonAndOriginalCode() throws Exception {
		payment.error = Status.INTERNAL;

		JsonNode body = json(post(order("P-GRPC-ERROR")));

		assertThat(body.path("status").asString()).isEqualTo("PROCESSING_FAILED");
		assertThat(body.path("failure_reason").asString()).isEqualTo(GrpcKafkaOrderFlow.PAYMENT_GRPC_ERROR);
		assertThat(body.path("timeline").get(1).path("detail").asString()).contains("grpc_status=INTERNAL");
	}

	@Test
	void duplicatePartnerOrderIdReturnsStoredOrderWith200WithoutNewGrpcCall() throws Exception {
		JsonNode first = json(post(order("P-GRPC-DUP")));
		HttpResponse<String> second = post(order("P-GRPC-DUP"));

		assertThat(second.statusCode()).isEqualTo(200);
		JsonNode replay = json(second);
		assertThat(replay.path("order_id").asString()).isEqualTo(first.path("order_id").asString());
		assertThat(replay.path("idempotent_replay").asBoolean()).isTrue();
		assertThat(payment.calls).hasValue(1);
	}

	private static String order(String partnerOrderId) {
		return """
				{"partner_order_id":"%s","customer_name":"Nguyễn Văn A","phone":"0901234567","amount":500000,"mode":"GRPC_KAFKA"}"""
			.formatted(partnerOrderId);
	}

	private HttpResponse<String> post(String body) throws Exception {
		return this.http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + this.port + "/api/v1/orders"))
			.header("Content-Type", "application/json")
			.POST(HttpRequest.BodyPublishers.ofString(body))
			.build(), HttpResponse.BodyHandlers.ofString());
	}

	private HttpResponse<String> get(String path) throws Exception {
		return this.http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + this.port + path)).GET().build(),
				HttpResponse.BodyHandlers.ofString());
	}

	private JsonNode json(HttpResponse<String> response) {
		return this.jsonMapper.readTree(response.body());
	}

	private static List<String> steps(JsonNode order) {
		return order.path("timeline").valueStream().map((step) -> step.path("step").asString()).toList();
	}

	private static Server start(FakePayment payment) {
		try {
			return ServerBuilder.forPort(0).addService(ServerInterceptors.intercept(payment, payment)).build().start();
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

	@TestConfiguration(proxyBeanMethods = false)
	static class Topics {

		/** Declared by policy-service in the real system; the test produces to it. */
		@Bean
		NewTopic policyIssuedTopic() {
			return TopicBuilder.name("policy.issued").partitions(3).replicas(1).build();
		}

	}

	/** Stand-in for payment-service: same generated service base class, configurable answer. */
	static class FakePayment extends PaymentServiceGrpc.PaymentServiceImplBase implements ServerInterceptor {

		static final Metadata.Key<String> CORRELATION_ID = Metadata.Key.of("x-correlation-id",
				Metadata.ASCII_STRING_MARSHALLER);

		final AtomicInteger calls = new AtomicInteger();

		final List<String> correlationIds = new CopyOnWriteArrayList<>();

		volatile long delayMs;

		volatile String rejectReason;

		volatile Status error;

		void reset() {
			this.calls.set(0);
			this.correlationIds.clear();
			this.delayMs = 0;
			this.rejectReason = null;
			this.error = null;
		}

		@Override
		public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
				ServerCallHandler<Q, R> next) {
			this.correlationIds.add(headers.get(CORRELATION_ID));
			return next.startCall(call, headers);
		}

		@Override
		public void recordPayment(RecordPaymentRequest request, StreamObserver<RecordPaymentResponse> observer) {
			this.calls.incrementAndGet();
			if (this.delayMs > 0) {
				try {
					Thread.sleep(this.delayMs);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
			if (this.error != null) {
				observer.onError(this.error.asRuntimeException());
				return;
			}
			RecordPaymentResponse.Builder response = RecordPaymentResponse.newBuilder().setOrderId(request.getOrderId());
			if (this.rejectReason != null) {
				response.setStatus(PaymentStatus.REJECTED).setRejectReason(this.rejectReason);
			}
			else {
				response.setStatus(PaymentStatus.RECORDED).setPaymentId("PAY-" + request.getOrderId());
			}
			observer.onNext(response.build());
			observer.onCompleted();
		}

	}

}
