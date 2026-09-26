package com.sandbox.order.flow;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;

import com.sandbox.order.order.NewOrder;
import com.sandbox.order.order.OrderMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Redis is down (nothing listens on the configured port, see src/test/resources/application.properties):
 * orders are still created and duplicates still recognised through MySQL, without waiting long, and
 * the service stays healthy (D12). MySQL 8.4 via Testcontainers; skipped without Docker.
 */
// No RabbitMQ in this test: its health indicator is off so that the overall status reflects Redis only.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
		properties = { "management.endpoint.health.show-components=always", "management.health.rabbit.enabled=false" })
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
class RedisDownIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Autowired
	private OrderIntake intake;

	@LocalServerPort
	private int port;

	@Test
	void ordersAreCreatedAndDuplicatesRecognisedThroughMysql(CapturedOutput output) {
		NewOrder order = this.intake.newOrder(new PlaceOrderCommand("P-NO-REDIS", "A", "0901234567", 500_000),
				OrderMode.GRPC_KAFKA);

		Instant start = Instant.now();
		assertThat(this.intake.replay("P-NO-REDIS", System.nanoTime())).isEmpty();
		this.intake.insert(order, System.nanoTime());
		assertThat(this.intake.replay("P-NO-REDIS", System.nanoTime()))
			.hasValueSatisfying((result) -> assertThat(result.order().orderId()).isEqualTo(order.orderId()));

		assertThat(Duration.between(start, Instant.now())).as("Redis errors do not make requests hang")
			.isLessThan(Duration.ofSeconds(3));
		assertThat(output).contains("\"action\":\"redis_unavailable\"", "\"idempotency_source\":\"MYSQL\"");
	}

	@Test
	void healthAndReadinessStayUp() throws Exception {
		HttpClient http = HttpClient.newHttpClient();
		for (String path : new String[] { "/actuator/health", "/actuator/health/readiness" }) {
			HttpResponse<String> response = http.send(
					HttpRequest.newBuilder(URI.create("http://localhost:" + this.port + path)).GET().build(),
					HttpResponse.BodyHandlers.ofString());

			assertThat(response.statusCode()).as(path + " " + response.body()).isEqualTo(200);
			assertThat(response.body()).as(path).contains("\"status\":\"UP\"").doesNotContain("redis");
		}
	}

}
