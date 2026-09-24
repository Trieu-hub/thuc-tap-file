package com.sandbox.order.flow;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;

import com.sandbox.order.rpc.OrderRpcClient;
import com.sandbox.order.support.Concurrently;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Flow 1 through a real RabbitMQ 4.3 and MySQL 8.4, over real HTTP.
 * <p>
 * payment-service and policy-service run in their own JVMs, so this test stands in for them with
 * fake listeners on the real queue names that speak the JSON contract. They can be made to reply
 * RECORDED/ISSUED, REJECTED, or 4 s late. The real three-service run is the end-to-end check in
 * the README. Skipped without Docker.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
// Close the context with the class: its listeners would otherwise keep reconnecting to the stopped container.
@DirtiesContext
class RabbitRpcOrderFlowIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Container
	@ServiceConnection
	static RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:4.3-management");

	private final HttpClient http = HttpClient.newHttpClient();

	@LocalServerPort
	private int port;

	@Autowired
	private FakeBackend payment;

	@Autowired
	private FakeBackend policy;

	@Autowired
	private JsonMapper jsonMapper;

	@Autowired
	private JdbcTemplate jdbc;

	@BeforeEach
	void reset() {
		this.jdbc.update("DELETE FROM order_timeline");
		this.jdbc.update("DELETE FROM orders");
		this.payment.reset();
		this.policy.reset();
	}

	@Test
	void happyPathIssuesPolicyAndPropagatesCorrelationId(CapturedOutput output) throws Exception {
		HttpResponse<String> response = post(order("P-HAPPY"));

		assertThat(response.statusCode()).isEqualTo(200);
		JsonNode body = json(response);
		String correlationId = body.path("correlation_id").asString();
		assertThat(body.path("order_id").asString()).matches("ORD-\\d{8}-[0-9A-F]{8}");
		assertThat(body.path("status").asString()).isEqualTo("ISSUED");
		assertThat(body.path("policy_number").asString()).startsWith("ACBI-");
		assertThat(body.path("idempotent_replay").asBoolean()).isFalse();
		assertThat(steps(body)).containsExactly("ORDER_CREATED", "PAYMENT", "POLICY_ISSUANCE", "NOTIFICATION");
		assertThat(body.path("timeline").valueStream()).allMatch((step) -> step.path("duration_ms").isNumber());
		// The business id reached both services in the x-correlation-id header, not only in the response.
		assertThat(this.payment.correlationIds).containsExactly(correlationId);
		assertThat(this.policy.correlationIds).containsExactly(correlationId);
		assertThat(output).contains("\"correlation_id\":\"" + correlationId + "\"", "\"action\":\"CreateOrder\"");

		JsonNode stored = json(get("/api/v1/orders/" + body.path("order_id").asString()));
		assertThat(stored.path("status").asString()).isEqualTo("ISSUED");
		assertThat(steps(stored)).hasSize(4);
		assertThat(json(get("/api/v1/orders")).get(0).path("order_id").asString())
			.isEqualTo(body.path("order_id").asString());
	}

	@Test
	void paymentTimeoutFailsOrderWithinThreeAndAHalfSeconds(CapturedOutput output) throws Exception {
		this.payment.delayMs = 4000;

		Instant start = Instant.now();
		HttpResponse<String> response = post(order("P-SLOW"));
		Duration took = Duration.between(start, Instant.now());

		assertThat(took).as("waits the full 3 s reply timeout, then answers without hanging")
			.isBetween(Duration.ofMillis(2900), Duration.ofMillis(3500));
		JsonNode body = json(response);
		assertThat(response.statusCode()).isEqualTo(200);
		assertThat(body.path("status").asString()).isEqualTo("PROCESSING_FAILED");
		assertThat(body.path("failure_reason").asString()).isEqualTo(RabbitRpcOrderFlow.PAYMENT_TIMEOUT);
		assertThat(steps(body)).containsExactly("ORDER_CREATED", "PROCESSING_FAILED");
		assertThat(this.policy.calls).hasValue(0);
		assertThat(output).contains("\"action\":\"rpc_timeout\"");
		// The fake payment still answers at ~4 s; that reply must be dropped and logged, not applied.
		String correlationId = body.path("correlation_id").asString();
		await().atMost(Duration.ofSeconds(5))
			.untilAsserted(() -> assertThat(output.getOut().lines())
				.anyMatch((line) -> line.contains("\"action\":\"late_reply_ignored\"")
						&& line.contains("\"correlation_id\":\"" + correlationId + "\"")));
		assertThat(json(get("/api/v1/orders/" + body.path("order_id").asString())).path("status").asString())
			.isEqualTo("PROCESSING_FAILED");
	}

	@Test
	void policyTimeoutFailsOrderAfterPaymentWasRecorded() throws Exception {
		this.policy.delayMs = 4000;

		JsonNode body = json(post(order("P-POLICY-SLOW")));

		assertThat(body.path("status").asString()).isEqualTo("PROCESSING_FAILED");
		assertThat(body.path("failure_reason").asString()).isEqualTo(RabbitRpcOrderFlow.POLICY_TIMEOUT);
		assertThat(steps(body)).containsExactly("ORDER_CREATED", "PAYMENT", "PROCESSING_FAILED");
	}

	@Test
	void rejectedPaymentFailsOrderWithoutCallingPolicy() throws Exception {
		this.payment.rejectReason = "INVALID_AMOUNT";

		JsonNode body = json(post(order("P-REJECT")));

		assertThat(body.path("status").asString()).isEqualTo("PROCESSING_FAILED");
		assertThat(body.path("failure_reason").asString()).isEqualTo("INVALID_AMOUNT");
		assertThat(steps(body)).containsExactly("ORDER_CREATED", "PROCESSING_FAILED");
		assertThat(this.policy.calls).hasValue(0);
	}

	@Test
	void duplicatePartnerOrderIdReturnsOriginalOrderWithoutNewRpc() throws Exception {
		JsonNode first = json(post(order("P-DUP")));
		HttpResponse<String> second = post(order("P-DUP"));

		assertThat(second.statusCode()).isEqualTo(200);
		JsonNode replay = json(second);
		assertThat(replay.path("order_id").asString()).isEqualTo(first.path("order_id").asString());
		assertThat(replay.path("correlation_id").asString()).isEqualTo(first.path("correlation_id").asString());
		assertThat(replay.path("status").asString()).isEqualTo("ISSUED");
		assertThat(replay.path("idempotent_replay").asBoolean()).isTrue();
		assertThat(this.payment.calls).hasValue(1);
		assertThat(this.policy.calls).hasValue(1);
	}

	@Test
	void concurrentDuplicatesCreateOneOrderAndLosersGetConflict() throws Exception {
		List<Integer> statuses = Concurrently.run(4, () -> post(order("P-RACE")).statusCode());

		assertThat(statuses).allMatch((status) -> status == 200 || status == 409).contains(200);
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class)).isEqualTo(1);
		assertThat(this.payment.calls).hasValue(1);
	}

	private static String order(String partnerOrderId) {
		return """
				{"partner_order_id":"%s","customer_name":"Nguyễn Văn A","phone":"0901234567","amount":500000,"mode":"RABBITMQ_RPC"}"""
			.formatted(partnerOrderId);
	}

	private HttpResponse<String> post(String body) throws Exception {
		return this.http.send(HttpRequest.newBuilder(uri("/api/v1/orders"))
			.header("Content-Type", "application/json")
			.POST(HttpRequest.BodyPublishers.ofString(body))
			.build(), HttpResponse.BodyHandlers.ofString());
	}

	private HttpResponse<String> get(String path) throws Exception {
		return this.http.send(HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
	}

	private URI uri(String path) {
		return URI.create("http://localhost:" + this.port + path);
	}

	private JsonNode json(HttpResponse<String> response) {
		return this.jsonMapper.readTree(response.body());
	}

	private static List<String> steps(JsonNode order) {
		return order.path("timeline").valueStream().map((step) -> step.path("step").asString()).toList();
	}

	/** Stand-in for payment-service or policy-service: records calls, replies per the JSON contract. */
	static class FakeBackend {

		final AtomicInteger calls = new AtomicInteger();

		final List<String> correlationIds = new CopyOnWriteArrayList<>();

		volatile long delayMs;

		volatile String rejectReason;

		void reset() {
			this.calls.set(0);
			this.correlationIds.clear();
			this.delayMs = 0;
			this.rejectReason = null;
		}

		Message<Map<String, Object>> reply(Map<String, Object> body, String correlationId) {
			return MessageBuilder.withPayload(body).setHeader(OrderRpcClient.CORRELATION_HEADER, correlationId).build();
		}

		void receive(String correlationId) throws InterruptedException {
			this.calls.incrementAndGet();
			this.correlationIds.add(correlationId);
			if (this.delayMs > 0) {
				Thread.sleep(this.delayMs);
			}
		}

	}

	@TestConfiguration(proxyBeanMethods = false)
	static class FakeBackends {

		/** Same queue arguments as the real consumers declare, so both can share a broker. */
		@Bean
		Declarables rpcQueues() {
			return new Declarables(requestQueue(OrderRpcClient.PAYMENT_QUEUE), requestQueue(OrderRpcClient.POLICY_QUEUE));
		}

		private static Queue requestQueue(String name) {
			return QueueBuilder.durable(name).ttl(3000).deadLetterExchange("sandbox.dlx").build();
		}

		@Bean
		FakeBackend payment() {
			return new FakeBackend();
		}

		@Bean
		FakeBackend policy() {
			return new FakeBackend();
		}

		@Bean
		FakeListeners fakeListeners(FakeBackend payment, FakeBackend policy) {
			return new FakeListeners(payment, policy);
		}

	}

	static class FakeListeners {

		private final FakeBackend payment;

		private final FakeBackend policy;

		FakeListeners(FakeBackend payment, FakeBackend policy) {
			this.payment = payment;
			this.policy = policy;
		}

		// concurrency 2: a deliberately slow reply must not block the next test's request.
		@RabbitListener(queues = OrderRpcClient.PAYMENT_QUEUE, concurrency = "2")
		Message<Map<String, Object>> payment(Map<String, Object> request,
				@Header(name = OrderRpcClient.CORRELATION_HEADER, required = false) String correlationId)
				throws InterruptedException {
			this.payment.receive(correlationId);
			Object orderId = request.get("order_id");
			if (this.payment.rejectReason != null) {
				return this.payment.reply(Map.of("order_id", orderId, "status", "REJECTED", "reject_reason",
						this.payment.rejectReason, "duplicate", false), correlationId);
			}
			return this.payment.reply(Map.of("order_id", orderId, "status", "RECORDED", "payment_id", "PAY-" + orderId,
					"duplicate", false, "recorded_at", Instant.now().toString()), correlationId);
		}

		@RabbitListener(queues = OrderRpcClient.POLICY_QUEUE, concurrency = "2")
		Message<Map<String, Object>> policy(Map<String, Object> request,
				@Header(name = OrderRpcClient.CORRELATION_HEADER, required = false) String correlationId)
				throws InterruptedException {
			this.policy.receive(correlationId);
			String policyNumber = "ACBI-2026-%06d".formatted(ThreadLocalRandom.current().nextInt(1_000_000));
			return this.policy.reply(Map.of("order_id", request.get("order_id"), "status", "ISSUED", "policy_id",
					"POL-1", "policy_number", policyNumber, "duplicate", false, "issued_at", Instant.now().toString()),
					correlationId);
		}

	}

}
