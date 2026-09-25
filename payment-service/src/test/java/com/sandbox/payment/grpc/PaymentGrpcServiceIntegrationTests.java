package com.sandbox.payment.grpc;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
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
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.event.EventListener;
import org.springframework.grpc.server.lifecycle.GrpcServerStartedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.annotation.DirtiesContext;

import com.sandbox.contracts.payment.v1.PaymentServiceGrpc;
import com.sandbox.contracts.payment.v1.PaymentStatus;
import com.sandbox.contracts.payment.v1.RecordPaymentRequest;
import com.sandbox.contracts.payment.v1.RecordPaymentResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Calls the real gRPC server over a real HTTP/2 connection with the stubs generated from
 * payment.proto, the same ones order-service uses, and reads what it publishes on payment.recorded
 * from a real Kafka 4.2 (topic auto-creation off, as in compose). MySQL 8.4 via Testcontainers;
 * skipped without Docker. The RabbitMQ listener is not started: Flow 1 is not involved.
 */
@SpringBootTest(properties = { "spring.rabbitmq.listener.simple.auto-startup=false",
		"spring.kafka.listener.auto-startup=true", "spring.kafka.admin.auto-create=true" })
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
// Close the context with the class: the probe consumer would otherwise keep polling the stopped container.
@DirtiesContext
class PaymentGrpcServiceIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Container
	@ServiceConnection
	static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.1")
		.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

	@Autowired
	private GrpcPort grpcPort;

	@Autowired
	private Probe probe;

	@Autowired
	private KafkaListenerEndpointRegistry listeners;

	@Autowired
	private JsonMapper jsonMapper;

	@Autowired
	private JdbcTemplate jdbc;

	private ManagedChannel channel;

	@BeforeEach
	void setUp() {
		this.jdbc.update("DELETE FROM payments");
		this.probe.records.clear();
		this.channel = ManagedChannelBuilder.forAddress("localhost", this.grpcPort.port).usePlaintext().build();
		await().atMost(Duration.ofSeconds(30))
			.until(() -> this.listeners.getListenerContainers()
				.stream()
				.allMatch((container) -> container.getAssignedPartitions() != null
						&& !container.getAssignedPartitions().isEmpty()));
	}

	@AfterEach
	void closeChannel() throws InterruptedException {
		this.channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
	}

	@Test
	void recordsPaymentAndLogsCorrelationIdFromMetadata(CapturedOutput output) {
		RecordPaymentResponse response = stub("corr-grpc-1").recordPayment(request("ORD-1", 500_000));

		assertThat(response.getStatus()).isEqualTo(PaymentStatus.RECORDED);
		assertThat(response.getPaymentId()).startsWith("PAY-");
		assertThat(response.getDuplicate()).isFalse();
		assertThat(response.hasRecordedAt()).isTrue();
		assertThat(output).contains("\"correlation_id\":\"corr-grpc-1\"", "\"transport\":\"gRPC\"",
				"\"action\":\"RecordPayment\"", "\"execution_time_ms\":");
	}

	@Test
	void recordedPaymentIsPublishedWithOrderIdKeyAndEnvelope() {
		RecordPaymentResponse response = stub("corr-event-1").recordPayment(request("ORD-1", 500_000));

		ConsumerRecord<String, String> record = awaitEvents("ORD-1", 1).get(0);
		assertThat(record.key()).isEqualTo("ORD-1");
		JsonNode event = this.jsonMapper.readTree(record.value());
		assertThat(event.path("event_id").asString()).matches("[0-9a-f-]{36}");
		assertThat(event.path("event_type").asString()).isEqualTo("payment.recorded");
		assertThat(event.path("correlation_id").asString()).isEqualTo("corr-event-1");
		assertThat(event.path("occurred_at").asString()).endsWith("Z");
		JsonNode payload = event.path("payload");
		assertThat(payload.path("order_id").asString()).isEqualTo("ORD-1");
		assertThat(payload.path("payment_id").asString()).isEqualTo(response.getPaymentId());
		assertThat(payload.path("partner_transaction_id").asString()).isEqualTo("TXN-P-ORD-1");
		assertThat(payload.path("amount").isIntegralNumber()).isTrue();
		assertThat(payload.path("amount").asLong()).isEqualTo(500_000);
		assertThat(payload.path("recorded_at").asString()).endsWith("Z");
		assertThat(record.headers().lastHeader("__TypeId__")).as("no Java class name on the wire").isNull();
	}

	@Test
	void repeatedRequestReturnsSamePaymentAsDuplicateAndRepublishesSameEvent() {
		RecordPaymentResponse first = stub("corr-1").recordPayment(request("ORD-1", 500_000));
		RecordPaymentResponse second = stub("corr-1").recordPayment(request("ORD-1", 500_000));

		assertThat(second.getStatus()).isEqualTo(PaymentStatus.RECORDED);
		assertThat(second.getDuplicate()).isTrue();
		assertThat(second.getPaymentId()).isEqualTo(first.getPaymentId());
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM payments", Integer.class)).isEqualTo(1);
		// Published again so a lost first event can still be delivered; same event_id, so policy-service dedupes.
		List<ConsumerRecord<String, String>> events = awaitEvents("ORD-1", 2);
		assertThat(events).extracting((record) -> this.jsonMapper.readTree(record.value()).path("event_id").asString())
			.containsOnly(this.jsonMapper.readTree(events.get(0).value()).path("event_id").asString());
	}

	@Test
	void invalidAmountIsRejectedAndNothingIsPublished() {
		RecordPaymentResponse response = stub("corr-1").recordPayment(request("ORD-1", 0));
		// A later valid payment for another order proves the probe is reading; ORD-1 must not appear.
		stub("corr-2").recordPayment(request("ORD-2", 500_000));

		assertThat(response.getStatus()).isEqualTo(PaymentStatus.REJECTED);
		assertThat(response.getRejectReason()).isEqualTo("INVALID_AMOUNT");
		assertThat(response.getPaymentId()).isEmpty();
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM payments WHERE order_id = 'ORD-1'", Integer.class))
			.isZero();
		awaitEvents("ORD-2", 1);
		assertThat(this.probe.records).noneMatch((record) -> "ORD-1".equals(record.key()));
	}

	private List<ConsumerRecord<String, String>> awaitEvents(String orderId, int count) {
		return await().atMost(Duration.ofSeconds(15))
			.until(() -> this.probe.records.stream().filter((record) -> orderId.equals(record.key())).toList(),
					(records) -> records.size() >= count);
	}

	private PaymentServiceGrpc.PaymentServiceBlockingStub stub(String correlationId) {
		Metadata headers = new Metadata();
		headers.put(CorrelationIdServerInterceptor.CORRELATION_ID, correlationId);
		return PaymentServiceGrpc.newBlockingStub(this.channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
			.withDeadlineAfter(3, TimeUnit.SECONDS);
	}

	private static RecordPaymentRequest request(String orderId, long amount) {
		return RecordPaymentRequest.newBuilder()
			.setOrderId(orderId)
			.setPartnerOrderId("P-" + orderId)
			.setPartnerTransactionId("TXN-P-" + orderId)
			.setAmount(amount)
			.build();
	}

	/** The server binds a free port in tests (src/test/resources/application.properties). */
	@TestConfiguration
	static class GrpcPort {

		volatile int port;

		@EventListener
		void onStarted(GrpcServerStartedEvent event) {
			this.port = event.getPort();
		}

	}

	/** Reads payment.recorded the way policy-service would, with its own consumer group. */
	@TestConfiguration
	static class Probe {

		final List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();

		@KafkaListener(topics = "payment.recorded", groupId = "payment-test-probe")
		void onRecord(ConsumerRecord<String, String> record) {
			this.records.add(record);
		}

	}

}
