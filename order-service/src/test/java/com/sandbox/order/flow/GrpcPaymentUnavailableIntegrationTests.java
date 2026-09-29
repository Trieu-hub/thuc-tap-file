package com.sandbox.order.flow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * payment-service is down: nothing listens on the gRPC target port. The call must fail fast with
 * UNAVAILABLE instead of hanging, and the order must end PROCESSING_FAILED. A class of its own
 * because the channel target is fixed per application context.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
class GrpcPaymentUnavailableIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@LocalServerPort
	private int port;

	@Autowired
	private JsonMapper jsonMapper;

	@DynamicPropertySource
	static void deadPaymentTarget(DynamicPropertyRegistry registry) {
		registry.add("spring.grpc.client.channel.payment.target", () -> "static://localhost:" + closedPort());
	}

	@Test
	void paymentServiceDownFailsOrderWithoutHanging(CapturedOutput output) throws Exception {
		Instant start = Instant.now();
		HttpResponse<String> response = HttpClient.newHttpClient()
			.send(HttpRequest.newBuilder(URI.create("http://localhost:" + this.port + "/api/v1/orders"))
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString("""
						{"partner_order_id":"P-DOWN","customer_name":"A","phone":"0901234567","amount":500000,"mode":"GRPC_KAFKA"}"""))
				.build(), HttpResponse.BodyHandlers.ofString());
		Duration took = Duration.between(start, Instant.now());

		assertThat(took).as("connection refused is reported at once, well before the deadline")
			.isLessThan(Duration.ofMillis(3500));
		assertThat(response.statusCode()).isEqualTo(200);
		JsonNode body = this.jsonMapper.readTree(response.body());
		assertThat(body.path("status").asString()).isEqualTo("PROCESSING_FAILED");
		assertThat(body.path("failure_reason").asString()).isEqualTo(GrpcKafkaOrderFlow.PAYMENT_SERVICE_UNAVAILABLE);
		assertThat(body.path("timeline").get(1).path("detail").asString()).contains("grpc_status=UNAVAILABLE");
		assertThat(output).contains("\"grpc_status\":\"UNAVAILABLE\"");
	}

	/** A port that was free a moment ago and has nothing bound to it now. */
	private static int closedPort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

}
