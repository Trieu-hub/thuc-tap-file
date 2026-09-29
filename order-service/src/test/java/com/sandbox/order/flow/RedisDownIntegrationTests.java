package com.sandbox.order.flow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterAll;
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
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.sandbox.order.cache.RedisAvailability;
import com.sandbox.order.order.NewOrder;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.rpc.OrderRpcClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Redis is down the way a stopped container is: a server accepts the TCP connection and never
 * answers, so every Redis command waits for its 300 ms timeout (a refused port would fail in about
 * 1 ms and hide the problem). Orders must still be created and duplicates recognised through MySQL,
 * the circuit breaker must stop later requests from paying the timeouts again, and the service must
 * stay healthy (D12). Flow 1 runs against fake payment and policy RPC listeners on a real RabbitMQ;
 * MySQL 8.4 and RabbitMQ 4.3 via Testcontainers; skipped without Docker.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
		properties = "management.endpoint.health.show-components=always")
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
// Close the context with the class: its listeners would otherwise keep reconnecting to the stopped container.
@DirtiesContext
class RedisDownIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Container
	@ServiceConnection
	static RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:4.3-management");

	static final SilentServer redis = new SilentServer();

	private final HttpClient http = HttpClient.newHttpClient();

	@Autowired
	private OrderIntake intake;

	@Autowired
	private RedisAvailability availability;

	@Autowired
	private JsonMapper jsonMapper;

	@LocalServerPort
	private int port;

	@DynamicPropertySource
	static void deadRedis(DynamicPropertyRegistry registry) {
		registry.add("spring.data.redis.host", () -> "localhost");
		registry.add("spring.data.redis.port", redis::port);
	}

	@AfterAll
	static void stopRedis() {
		redis.close();
	}

	@BeforeEach
	void closeCircuit() {
		// Each test starts as if Redis had just been seen alive, so it observes the first failure itself.
		this.availability.markSuccess();
	}

	@Test
	void ordersAreCreatedAndDuplicatesRecognisedThroughMysql(CapturedOutput output) {
		PlaceOrderCommand command = new PlaceOrderCommand("P-NO-REDIS-" + suffix(), "A", "0901234567", 500_000);
		NewOrder order = this.intake.newOrder(command, OrderMode.GRPC_KAFKA);

		Instant start = Instant.now();
		this.intake.rejectDuplicate(command, OrderMode.GRPC_KAFKA, System.nanoTime());
		this.intake.insert(order, System.nanoTime());
		assertThatExceptionOfType(DuplicateOrderException.class)
			.isThrownBy(() -> this.intake.rejectDuplicate(command, OrderMode.GRPC_KAFKA, System.nanoTime()))
			.satisfies((ex) -> assertThat(ex.orderId()).isEqualTo(order.orderId()));

		assertThat(Duration.between(start, Instant.now())).as("Redis errors do not make requests hang")
			.isLessThan(Duration.ofSeconds(3));
		assertThat(output).contains("\"action\":\"redis_unavailable\"", "\"idempotency_source\":\"MYSQL\"");
	}

	@Test
	void afterTheFirstFailureRequestsSkipRedisAndStayFast(CapturedOutput output) throws Exception {
		Response first = post("P-CB1-" + suffix());
		Response second = post("P-CB2-" + suffix());
		String thirdPartner = "P-CB3-" + suffix();
		Response third = post(thirdPartner);
		Response duplicate = post(thirdPartner);

		assertThat(List.of(first, second, third)).allSatisfy((response) -> {
			assertThat(response.code()).isEqualTo(200);
			assertThat(response.body().path("status").asString()).isEqualTo("ISSUED");
		});
		assertThat(second.took()).as("second POST within 5 s: Redis skipped").isLessThan(Duration.ofMillis(500));
		assertThat(third.took()).as("third POST within 5 s: Redis skipped").isLessThan(Duration.ofMillis(500));
		assertThat(duplicate.code()).as("MySQL still rejects the duplicate").isEqualTo(409);
		assertThat(duplicate.body().path("error").asString()).isEqualTo("DUPLICATE_ORDER");
		assertThat(duplicate.body().path("order_id").asString()).isEqualTo(third.body().path("order_id").asString());
		assertThat(count(output, "\"action\":\"redis_circuit_open\"")).as("one opening for the whole outage").isEqualTo(1);
		assertThat(count(output, "\"action\":\"redis_unavailable\"")).as("only the command that failed is logged")
			.isEqualTo(1);
		System.out.printf("POST with Redis down: first %d ms, second %d ms, third %d ms%n", first.took().toMillis(),
				second.took().toMillis(), third.took().toMillis());
	}

	@Test
	void healthAndReadinessStayUp() throws Exception {
		for (String path : new String[] { "/actuator/health", "/actuator/health/readiness" }) {
			HttpResponse<String> response = this.http.send(
					HttpRequest.newBuilder(URI.create("http://localhost:" + this.port + path)).GET().build(),
					HttpResponse.BodyHandlers.ofString());

			assertThat(response.statusCode()).as(path + " " + response.body()).isEqualTo(200);
			assertThat(response.body()).as(path).contains("\"status\":\"UP\"").doesNotContain("redis");
		}
	}

	private Response post(String partnerOrderId) throws Exception {
		Instant start = Instant.now();
		HttpResponse<String> response = this.http.send(HttpRequest
			.newBuilder(URI.create("http://localhost:" + this.port + "/api/v1/orders"))
			.header("Content-Type", "application/json")
			.POST(HttpRequest.BodyPublishers.ofString("""
					{"partner_order_id":"%s","customer_name":"A","phone":"0901234567","amount":500000,"mode":"RABBITMQ_RPC"}"""
				.formatted(partnerOrderId)))
			.build(), HttpResponse.BodyHandlers.ofString());
		return new Response(response.statusCode(), this.jsonMapper.readTree(response.body()),
				Duration.between(start, Instant.now()));
	}

	private static long count(CapturedOutput output, String text) {
		return output.getAll().lines().filter((line) -> line.contains(text)).count();
	}

	private static String suffix() {
		return UUID.randomUUID().toString().substring(0, 8);
	}

	record Response(int code, JsonNode body, Duration took) {

	}

	/** Accepts TCP connections and never answers, like a Redis that is gone but whose address still routes. */
	static final class SilentServer implements AutoCloseable {

		private final ServerSocket socket;

		private final List<Socket> accepted = new CopyOnWriteArrayList<>();

		SilentServer() {
			try {
				this.socket = new ServerSocket(0);
			}
			catch (IOException ex) {
				throw new UncheckedIOException(ex);
			}
			Thread acceptor = new Thread(() -> {
				while (!this.socket.isClosed()) {
					try {
						this.accepted.add(this.socket.accept());
					}
					catch (IOException ex) {
						return;
					}
				}
			}, "silent-redis");
			acceptor.setDaemon(true);
			acceptor.start();
		}

		int port() {
			return this.socket.getLocalPort();
		}

		@Override
		public void close() {
			try {
				this.socket.close();
				for (Socket connection : this.accepted) {
					connection.close();
				}
			}
			catch (IOException ex) {
				// best effort at the end of the test class
			}
		}

	}

	@TestConfiguration(proxyBeanMethods = false)
	static class FakeBackends {

		/** Same queue arguments as the real consumers declare. */
		@Bean
		Declarables rpcQueues() {
			return new Declarables(requestQueue(OrderRpcClient.PAYMENT_QUEUE), requestQueue(OrderRpcClient.POLICY_QUEUE));
		}

		private static Queue requestQueue(String name) {
			return QueueBuilder.durable(name).ttl(3000).deadLetterExchange("sandbox.dlx").build();
		}

		@Bean
		FakeListeners fakeListeners() {
			return new FakeListeners();
		}

	}

	/** Stand-ins for payment-service and policy-service that always succeed. */
	static class FakeListeners {

		@RabbitListener(queues = OrderRpcClient.PAYMENT_QUEUE)
		Message<Map<String, Object>> payment(Map<String, Object> request,
				@Header(name = OrderRpcClient.CORRELATION_HEADER, required = false) String correlationId) {
			Object orderId = request.get("order_id");
			return reply(Map.of("order_id", orderId, "status", "RECORDED", "payment_id", "PAY-" + orderId, "duplicate",
					false, "recorded_at", Instant.now().toString()), correlationId);
		}

		@RabbitListener(queues = OrderRpcClient.POLICY_QUEUE)
		Message<Map<String, Object>> policy(Map<String, Object> request,
				@Header(name = OrderRpcClient.CORRELATION_HEADER, required = false) String correlationId) {
			return reply(Map.of("order_id", request.get("order_id"), "status", "ISSUED", "policy_id", "POL-1",
					"policy_number", "ACBI-2026-000001", "duplicate", false, "issued_at", Instant.now().toString()),
					correlationId);
		}

		private static Message<Map<String, Object>> reply(Map<String, Object> body, String correlationId) {
			return MessageBuilder.withPayload(body).setHeader(OrderRpcClient.CORRELATION_HEADER, correlationId).build();
		}

	}

}
