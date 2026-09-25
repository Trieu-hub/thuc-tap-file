package com.sandbox.order.kafka;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mysql.MySQLContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

import com.sandbox.order.order.NewOrder;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderProgressService;
import com.sandbox.order.order.OrderRepository;
import com.sandbox.order.order.TimelineEntry;
import com.sandbox.order.order.TimelineStep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Flow 2, order-service leg: policy.issued events produced as raw JSON (the test plays
 * policy-service) on a real Kafka 4.2 with topic auto-creation off, applied to orders in MySQL 8.4.
 * Covers redelivery, an event that overtakes the gRPC response, and an unknown order. Skipped
 * without Docker.
 */
@SpringBootTest(properties = { "spring.kafka.listener.auto-startup=true", "spring.kafka.admin.auto-create=true" })
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
// Close the context with the class: its consumers would otherwise keep polling the stopped container.
@DirtiesContext
class PolicyIssuedListenerIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Container
	@ServiceConnection
	static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.1")
		.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaListenerEndpointRegistry listeners;

	@Autowired
	private OrderRepository orders;

	@Autowired
	private OrderProgressService progress;

	@Autowired
	private DeadLetterProbe deadLetters;

	@Autowired
	private JdbcTemplate jdbc;

	@BeforeEach
	void waitForConsumers() {
		await().atMost(Duration.ofSeconds(30))
			.until(() -> this.listeners.getListenerContainers()
				.stream()
				.allMatch((container) -> container.getAssignedPartitions() != null
						&& !container.getAssignedPartitions().isEmpty()));
	}

	@Test
	void policyIssuedMovesOrderToIssuedAndNotifies(CapturedOutput output) throws Exception {
		String orderId = createOrder();
		this.progress.recordPayment(orderId, paymentStep());

		send(policyIssued(UUID.randomUUID().toString(), orderId, "corr-order-1"));

		awaitStatus(orderId, "ISSUED");
		await().atMost(Duration.ofSeconds(5))
			.untilAsserted(() -> assertThat(steps(orderId)).containsExactly("ORDER_CREATED", "PAYMENT",
					"POLICY_ISSUANCE", "NOTIFICATION"));
		assertThat(this.jdbc.queryForObject("SELECT policy_number FROM orders WHERE order_id = ?", String.class, orderId))
			.isEqualTo("ACBI-2026-000001");
		assertThat(output.getOut().lines())
			.anyMatch((line) -> line.contains("\"transport\":\"Kafka\"") && line.contains("\"action\":\"ApplyPolicyIssued\"")
					&& line.contains("\"correlation_id\":\"corr-order-1\""));
	}

	@Test
	void republishedPolicyIssuedIsIgnoredAndAddsNoTimelineStep(CapturedOutput output) throws Exception {
		String orderId = createOrder();
		this.progress.recordPayment(orderId, paymentStep());
		String eventId = UUID.randomUUID().toString();
		String event = policyIssued(eventId, orderId, "corr-order-dup");

		send(event);
		awaitStatus(orderId, "ISSUED");
		send(event);

		await().atMost(Duration.ofSeconds(10))
			.until(() -> output.getOut()
				.lines()
				.anyMatch((line) -> line.contains("\"action\":\"duplicate_event_ignored\"")
						&& line.contains("\"event_id\":\"" + eventId + "\"")));
		assertThat(steps(orderId)).containsExactly("ORDER_CREATED", "PAYMENT", "POLICY_ISSUANCE", "NOTIFICATION");
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM consumer_inbox WHERE event_id = ?", Integer.class,
				eventId)).isEqualTo(1);
	}

	@Test
	void policyIssuedBeforePaymentRecordedStillEndsIssued() throws Exception {
		String orderId = createOrder();

		// The event overtakes the gRPC thread: the order is still CREATED when it arrives.
		send(policyIssued(UUID.randomUUID().toString(), orderId, "corr-early"));
		awaitStatus(orderId, "ISSUED");
		// Then the gRPC thread records its payment: the status must not move back.
		this.progress.recordPayment(orderId, paymentStep());

		assertThat(status(orderId)).isEqualTo("ISSUED");
		assertThat(steps(orderId)).contains("ORDER_CREATED", "POLICY_ISSUANCE", "PAYMENT").doesNotHaveDuplicates();
	}

	@Test
	void eventForUnknownOrderIsNotMarkedProcessedAndEndsInDeadLetterTopic(CapturedOutput output) throws Exception {
		String orderId = "ORD-MISSING-" + UUID.randomUUID().toString().substring(0, 8);
		String eventId = UUID.randomUUID().toString();

		send(policyIssued(eventId, orderId, "corr-missing"));

		ConsumerRecord<String, String> dead = await().atMost(Duration.ofSeconds(20))
			.until(() -> this.deadLetters.records.stream().filter((record) -> orderId.equals(record.key())).toList(),
					(records) -> !records.isEmpty())
			.get(0);
		assertThat(dead.value()).contains(eventId);
		// Rolled back with the failed transaction, so a later redelivery would be processed, not skipped.
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM consumer_inbox WHERE event_id = ?", Integer.class,
				eventId)).isZero();
		assertThat(output).contains("\"action\":\"event_dead_lettered\"", "Order not found: " + orderId);
	}

	private String createOrder() {
		String suffix = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
		String orderId = "ORD-20260925-" + suffix;
		this.orders.insertCreated(new NewOrder(orderId, "P-" + suffix, "Nguyen Van A", "0901234567", 500_000,
				OrderMode.GRPC_KAFKA, "corr-" + suffix, "TXN-P-" + suffix),
				new TimelineEntry(TimelineStep.ORDER_CREATED, "order-service", "HTTP", "SUCCESS", 1L, null));
		return orderId;
	}

	private static TimelineEntry paymentStep() {
		return new TimelineEntry(TimelineStep.PAYMENT, "payment-service", "gRPC", "SUCCESS", 5L, "PAY-1");
	}

	private void send(String event) throws Exception {
		String orderId = event.replaceAll("(?s).*\"order_id\":\"([^\"]+)\".*", "$1");
		this.kafkaTemplate.send(KafkaConfig.POLICY_ISSUED, orderId, event).get(5, TimeUnit.SECONDS);
	}

	private void awaitStatus(String orderId, String status) {
		await().atMost(Duration.ofSeconds(15)).until(() -> status.equals(status(orderId)));
	}

	private String status(String orderId) {
		return this.jdbc.queryForObject("SELECT status FROM orders WHERE order_id = ?", String.class, orderId);
	}

	private List<String> steps(String orderId) {
		return this.jdbc.queryForList("SELECT step FROM order_timeline WHERE order_id = ? ORDER BY seq", String.class,
				orderId);
	}

	private static String policyIssued(String eventId, String orderId, String correlationId) {
		return """
				{"event_id":"%s","event_type":"policy.issued","correlation_id":"%s","occurred_at":"2026-09-25T09:15:00.123Z",
				 "payload":{"order_id":"%s","policy_id":"POL-%s","policy_number":"ACBI-2026-000001",
				            "issued_at":"2026-09-25T09:15:00.100Z"}}"""
			.formatted(eventId, correlationId, orderId, orderId);
	}

	@TestConfiguration(proxyBeanMethods = false)
	static class ProbeConfig {

		/** Declared by policy-service in the real system; the test needs it to produce to. */
		@Bean
		NewTopic policyIssuedTopic() {
			return TopicBuilder.name(KafkaConfig.POLICY_ISSUED).partitions(3).replicas(1).build();
		}

		@Bean
		DeadLetterProbe deadLetterProbe() {
			return new DeadLetterProbe();
		}

	}

	static class DeadLetterProbe {

		final List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();

		@KafkaListener(topics = KafkaConfig.POLICY_ISSUED + ".DLT", groupId = "order-test-probe")
		void onRecord(ConsumerRecord<String, String> record) {
			this.records.add(record);
		}

	}

}
