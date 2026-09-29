package com.sandbox.policy.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Flow 2, policy-service leg, through a real Kafka 4.2 (same image as compose, topic auto-creation
 * off) and MySQL 8.4. The test plays payment-service by producing raw JSON on payment.recorded, and
 * reads policy.issued and payment.recorded.DLT with its own consumer group. Skipped without Docker.
 */
@SpringBootTest(properties = { "spring.rabbitmq.listener.simple.auto-startup=false",
		"spring.kafka.listener.auto-startup=true", "spring.kafka.admin.auto-create=true" })
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
// Close the context with the class: its consumers would otherwise keep polling the stopped container.
@DirtiesContext
class PaymentRecordedListenerIntegrationTests {

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
	private KafkaAdmin kafkaAdmin;

	@Autowired
	private Probe probe;

	@Autowired
	private JsonMapper jsonMapper;

	@Autowired
	private JdbcTemplate jdbc;

	@BeforeEach
	void waitForConsumers() {
		// Otherwise the first test would also measure the consumer group joining.
		await().atMost(Duration.ofSeconds(30))
			.until(() -> this.listeners.getListenerContainers()
				.stream()
				.allMatch((container) -> container.getAssignedPartitions() != null
						&& !container.getAssignedPartitions().isEmpty()));
	}

	@Test
	void issuesPolicyAndPublishesPolicyIssuedWithOrderIdKeyAndCorrelationId(CapturedOutput output) throws Exception {
		String orderId = newOrderId();

		send(paymentRecorded(UUID.randomUUID().toString(), orderId, "corr-kafka-1"));

		ConsumerRecord<String, String> issued = awaitPolicyIssued(orderId, 1).get(0);
		assertThat(issued.key()).isEqualTo(orderId);
		JsonNode event = this.jsonMapper.readTree(issued.value());
		assertThat(event.path("event_type").asString()).isEqualTo("policy.issued");
		assertThat(event.path("correlation_id").asString()).isEqualTo("corr-kafka-1");
		assertThat(event.path("occurred_at").asString()).endsWith("Z");
		assertThat(event.path("payload").path("policy_number").asString()).matches("ACBI-\\d{4}-\\d{6}");
		assertThat(event.path("payload").path("policy_id").asString())
			.isEqualTo(this.jdbc.queryForObject("SELECT policy_id FROM policies WHERE order_id = ?", String.class, orderId));
		assertThat(output.getOut().lines())
			.anyMatch((line) -> line.contains("\"transport\":\"Kafka\"") && line.contains("\"action\":\"IssuePolicy\"")
					&& line.contains("\"correlation_id\":\"corr-kafka-1\""));
	}

	@Test
	void sameEventDeliveredTwiceIssuesOnePolicyAndLogsDuplicate(CapturedOutput output) throws Exception {
		String orderId = newOrderId();
		String eventId = UUID.randomUUID().toString();
		String event = paymentRecorded(eventId, orderId, "corr-dup");

		send(event);
		awaitPolicyIssued(orderId, 1);
		send(event);

		// The duplicate is not processed again but policy.issued is re-published, with the same event_id.
		List<ConsumerRecord<String, String>> issued = awaitPolicyIssued(orderId, 2);
		assertThat(issued).extracting((record) -> this.jsonMapper.readTree(record.value()).path("event_id").asString())
			.containsOnly(this.jsonMapper.readTree(issued.get(0).value()).path("event_id").asString());
		assertThat(policyCount(orderId)).isEqualTo(1);
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM consumer_inbox WHERE event_id = ?", Integer.class,
				eventId)).isEqualTo(1);
		assertThat(output.getOut().lines())
			.anyMatch((line) -> line.contains("\"action\":\"duplicate_event_ignored\"")
					&& line.contains("\"event_id\":\"" + eventId + "\""));
	}

	@Test
	void newEventIdForSameOrderStillIssuesOnePolicy() throws Exception {
		String orderId = newOrderId();

		send(paymentRecorded(UUID.randomUUID().toString(), orderId, "corr-new-id"));
		awaitPolicyIssued(orderId, 1);
		send(paymentRecorded(UUID.randomUUID().toString(), orderId, "corr-new-id"));

		List<ConsumerRecord<String, String>> issued = awaitPolicyIssued(orderId, 2);
		assertThat(policyCount(orderId)).isEqualTo(1);
		// Both publications describe the same policy, so order-service's inbox sees one event.
		assertThat(issued).extracting((record) -> this.jsonMapper.readTree(record.value()).path("event_id").asString())
			.containsOnly(this.jsonMapper.readTree(issued.get(0).value()).path("event_id").asString());
	}

	@Test
	void unreadableEventGoesToDeadLetterTopicWithoutRetries(CapturedOutput output) throws Exception {
		String key = newOrderId();

		Instant sent = Instant.now();
		int partition = this.kafkaTemplate.send(KafkaConfig.PAYMENT_RECORDED, key, "{not json")
			.get(5, TimeUnit.SECONDS)
			.getRecordMetadata()
			.partition();

		ConsumerRecord<String, String> dead = await().atMost(Duration.ofSeconds(10))
			.until(() -> this.probe.deadLetters.stream().filter((record) -> key.equals(record.key())).findFirst(),
					Optional::isPresent)
			.get();
		// 3 retries 1 s apart would take at least 3 s; an unreadable record is not retried.
		assertThat(Duration.between(sent, Instant.now())).isLessThan(Duration.ofMillis(2500));
		assertThat(dead.value()).isEqualTo("{not json");
		assertThat(dead.partition()).as("same partition as the original").isEqualTo(partition);
		assertThat(output).contains("\"action\":\"event_dead_lettered\"");
	}

	@Test
	void topicsAreDeclaredWithThreePartitions() {
		Map<String, TopicDescription> topics = this.kafkaAdmin.describeTopics(KafkaConfig.POLICY_ISSUED,
				KafkaConfig.PAYMENT_RECORDED + ".DLT");

		assertThat(topics.values()).allSatisfy((topic) -> assertThat(topic.partitions()).hasSize(3));
	}

	private void send(String event) throws Exception {
		String orderId = this.jsonMapper.readTree(event).path("payload").path("order_id").asString();
		this.kafkaTemplate.send(KafkaConfig.PAYMENT_RECORDED, orderId, event).get(5, TimeUnit.SECONDS);
	}

	private List<ConsumerRecord<String, String>> awaitPolicyIssued(String orderId, int count) {
		return await().atMost(Duration.ofSeconds(15))
			.until(() -> this.probe.policyIssued.stream().filter((record) -> orderId.equals(record.key())).toList(),
					(records) -> records.size() >= count);
	}

	private int policyCount(String orderId) {
		return this.jdbc.queryForObject("SELECT COUNT(*) FROM policies WHERE order_id = ?", Integer.class, orderId);
	}

	private static String newOrderId() {
		return "ORD-TEST-" + UUID.randomUUID().toString().substring(0, 8);
	}

	private static String paymentRecorded(String eventId, String orderId, String correlationId) {
		return """
				{"event_id":"%s","event_type":"payment.recorded","correlation_id":"%s","occurred_at":"2026-09-25T09:15:00.123Z",
				 "payload":{"order_id":"%s","payment_id":"PAY-%s","partner_transaction_id":"TXN-P-%s","amount":500000,
				            "recorded_at":"2026-09-25T09:15:00.100Z"}}"""
			.formatted(eventId, correlationId, orderId, orderId, orderId);
	}

	@TestConfiguration(proxyBeanMethods = false)
	static class ProbeConfig {

		/** Declared by payment-service in the real system; the test needs it to produce to. */
		@Bean
		NewTopic paymentRecordedTopic() {
			return TopicBuilder.name(KafkaConfig.PAYMENT_RECORDED).partitions(3).replicas(1).build();
		}

		@Bean
		Probe probe() {
			return new Probe();
		}

	}

	/** Reads what policy-service wrote, with its own consumer group so it does not steal records. */
	static class Probe {

		final List<ConsumerRecord<String, String>> policyIssued = new CopyOnWriteArrayList<>();

		final List<ConsumerRecord<String, String>> deadLetters = new CopyOnWriteArrayList<>();

		@KafkaListener(topics = { KafkaConfig.POLICY_ISSUED, KafkaConfig.PAYMENT_RECORDED + ".DLT" },
				groupId = "policy-test-probe")
		void onRecord(ConsumerRecord<String, String> record) {
			(record.topic().endsWith(".DLT") ? this.deadLetters : this.policyIssued).add(record);
		}

	}

}
