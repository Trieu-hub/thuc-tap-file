package com.sandbox.order.cache;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.sandbox.order.order.NewOrder;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderProgressService;
import com.sandbox.order.order.OrderRepository;
import com.sandbox.order.order.TimelineEntry;
import com.sandbox.order.order.TimelineStep;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code GET /api/v1/orders/{id}} over real HTTP with the read cache on a real Redis 7.4 and MySQL
 * 8.4. Orders are put in each state with the real {@link OrderProgressService}, as the flows do.
 * Skipped without Docker.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
class OrderReadCacheIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Container
	@ServiceConnection(name = "redis")
	static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

	private final HttpClient http = HttpClient.newHttpClient();

	@LocalServerPort
	private int port;

	@Autowired
	private OrderRepository orders;

	@Autowired
	private OrderProgressService progress;

	@Autowired
	private StringRedisTemplate redisTemplate;

	@Autowired
	private JsonMapper jsonMapper;

	@BeforeEach
	void flushRedis() {
		this.redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
	}

	@Test
	void finishedOrderIsReadFromMysqlFirstThenFromRedis(CapturedOutput output) throws Exception {
		String orderId = createOrder("corr-cache-1");
		payAndIssue(orderId);
		notifySent(orderId);

		HttpResponse<String> first = get(orderId);
		HttpResponse<String> second = get(orderId);

		assertThat(json(first).path("cache_status").asString()).isEqualTo("CACHE_MISS_DB");
		assertThat(json(second).path("cache_status").asString()).isEqualTo("CACHE_HIT_REDIS");
		assertThat(json(second).path("status").asString()).isEqualTo("ISSUED");
		assertThat(steps(json(second))).containsExactly("ORDER_CREATED", "PAYMENT", "POLICY_ISSUANCE", "NOTIFICATION");
		assertThat(this.redisTemplate.getExpire("order:" + orderId, TimeUnit.SECONDS)).isBetween(590L, 600L);
		assertThat(second.headers().firstValue("X-Correlation-Id")).hasValue("corr-cache-1");
		assertThat(output.getOut().lines()).anyMatch((line) -> line.contains("\"action\":\"GetOrder\"")
				&& line.contains("\"cache_hit\":false") && line.contains("\"correlation_id\":\"corr-cache-1\""));
		assertThat(output.getOut().lines()).anyMatch((line) -> line.contains("\"action\":\"GetOrder\"")
				&& line.contains("\"cache_hit\":true") && line.contains("\"transport\":\"HTTP\""));
	}

	@Test
	void orderStillInProgressIsNeverCached() throws Exception {
		String orderId = createOrder("corr-cache-2");
		this.progress.recordPayment(orderId, step(TimelineStep.PAYMENT, "gRPC"));

		assertThat(json(get(orderId)).path("cache_status").asString()).isEqualTo("CACHE_MISS_DB");
		assertThat(json(get(orderId)).path("cache_status").asString()).isEqualTo("CACHE_MISS_DB");
		assertThat(this.redisTemplate.hasKey("order:" + orderId)).isFalse();
	}

	@Test
	void issuedButNotYetNotifiedIsNotCached() throws Exception {
		String orderId = createOrder("corr-cache-3");
		payAndIssue(orderId);

		get(orderId);

		assertThat(this.redisTemplate.hasKey("order:" + orderId))
			.as("the NOTIFICATION step is still to come in its own transaction").isFalse();
	}

	@Test
	void failedOrderThatIsIssuedLaterIsShownIssuedAtOnce() throws Exception {
		String orderId = createOrder("corr-cache-4");
		this.progress.recordFailure(orderId, "PAYMENT_TIMEOUT",
				step(TimelineStep.PROCESSING_FAILED, "gRPC"));
		assertThat(json(get(orderId)).path("status").asString()).isEqualTo("PROCESSING_FAILED");
		assertThat(json(get(orderId)).path("status").asString()).isEqualTo("PROCESSING_FAILED");

		// policy.issued arrives after the deadline: FAILED -> ISSUED (D13).
		this.progress.recordPolicyIssued(orderId, UUID.randomUUID().toString(), "ACBI-2026-000004",
				step(TimelineStep.POLICY_ISSUANCE, "Kafka"));
		notifySent(orderId);

		JsonNode now = json(get(orderId));
		assertThat(now.path("status").asString()).isEqualTo("ISSUED");
		assertThat(now.path("cache_status").asString()).isEqualTo("CACHE_MISS_DB");
		assertThat(json(get(orderId)).path("cache_status").asString()).isEqualTo("CACHE_HIT_REDIS");
	}

	@Test
	void changeAfterCachingDeletesTheKey() throws Exception {
		// policy.issued overtook the gRPC thread: the order is finished before PAYMENT is written.
		String orderId = createOrder("corr-cache-5");
		this.progress.recordPolicyIssued(orderId, UUID.randomUUID().toString(), "ACBI-2026-000005",
				step(TimelineStep.POLICY_ISSUANCE, "Kafka"));
		notifySent(orderId);
		get(orderId);
		assertThat(json(get(orderId)).path("cache_status").asString()).isEqualTo("CACHE_HIT_REDIS");

		this.progress.recordPayment(orderId, step(TimelineStep.PAYMENT, "gRPC"));

		JsonNode after = json(get(orderId));
		assertThat(after.path("cache_status").asString()).isEqualTo("CACHE_MISS_DB");
		assertThat(steps(after)).contains("PAYMENT");
	}

	private String createOrder(String correlationId) {
		String suffix = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
		String orderId = "ORD-20260926-" + suffix;
		this.orders.insertCreated(new NewOrder(orderId, "P-" + suffix, "Nguyễn Văn A", "0901234567", 500_000,
				OrderMode.GRPC_KAFKA, correlationId, "TXN-P-" + suffix),
				new TimelineEntry(TimelineStep.ORDER_CREATED, "order-service", "HTTP", "SUCCESS", 1L, null));
		return orderId;
	}

	private void payAndIssue(String orderId) {
		this.progress.recordPayment(orderId, step(TimelineStep.PAYMENT, "gRPC"));
		this.progress.recordPolicyIssued(orderId, UUID.randomUUID().toString(), "ACBI-2026-000001",
				step(TimelineStep.POLICY_ISSUANCE, "Kafka"));
	}

	private void notifySent(String orderId) {
		this.progress.recordStep(orderId, new TimelineEntry(TimelineStep.NOTIFICATION, "notification-worker",
				"IN_PROCESS", "SUCCESS", 0L, "SMS sent (simulated)"));
	}

	private static TimelineEntry step(TimelineStep step, String transport) {
		return new TimelineEntry(step, "test", transport, "SUCCESS", 5L, null);
	}

	private HttpResponse<String> get(String orderId) throws Exception {
		return this.http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + this.port + "/api/v1/orders/" + orderId))
			.GET()
			.build(), HttpResponse.BodyHandlers.ofString());
	}

	private JsonNode json(HttpResponse<String> response) {
		return this.jsonMapper.readTree(response.body());
	}

	private static List<String> steps(JsonNode order) {
		return order.path("timeline").valueStream().map((step) -> step.path("step").asString()).toList();
	}

}
