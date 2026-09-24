package com.sandbox.payment.rpc;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import com.sandbox.payment.payment.PaymentRecorder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Drives the real listener through a real broker (RabbitMQ 4.3) with raw JSON bodies, the way
 * order-service sends them: no shared Java class, only the JSON contract. Skipped without Docker.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
class PaymentRpcListenerIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Container
	@ServiceConnection
	static RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:4.3-management");

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private AmqpAdmin amqpAdmin;

	@Autowired
	private RabbitListenerEndpointRegistry listeners;

	@Autowired
	private JsonMapper jsonMapper;

	@Autowired
	private JdbcTemplate jdbc;

	@MockitoSpyBean
	private PaymentRecorder recorder;

	@BeforeEach
	void clean() {
		this.jdbc.update("DELETE FROM payments");
		this.amqpAdmin.purgeQueue(RabbitConfig.DEAD_LETTER_QUEUE, false);
	}

	@Test
	void recordsPaymentAndRepliesWithCorrelationHeader(CapturedOutput output) {
		Message reply = this.rabbitTemplate.sendAndReceive("", RabbitConfig.REQUEST_QUEUE,
				request(validBody("ORD-1", 500_000), "corr-test-1"));

		assertThat(reply).as("reply routed back through reply_to").isNotNull();
		JsonNode body = json(reply);
		assertThat(body.path("status").asString()).isEqualTo("RECORDED");
		assertThat(body.path("payment_id").asString()).startsWith("PAY-");
		assertThat(body.path("duplicate").asBoolean()).isFalse();
		assertThat((String) reply.getMessageProperties().getHeader(RabbitConfig.CORRELATION_HEADER))
			.isEqualTo("corr-test-1");
		assertThat(output).contains("\"correlation_id\":\"corr-test-1\"", "\"transport\":\"RabbitMQ\"",
				"\"action\":\"RecordPayment\"", "\"execution_time_ms\":");
	}

	@Test
	void repeatedRequestReturnsSamePaymentAsDuplicate() {
		JsonNode first = json(this.rabbitTemplate.sendAndReceive("", RabbitConfig.REQUEST_QUEUE,
				request(validBody("ORD-1", 500_000), "corr-1")));
		JsonNode second = json(this.rabbitTemplate.sendAndReceive("", RabbitConfig.REQUEST_QUEUE,
				request(validBody("ORD-1", 500_000), "corr-1")));

		assertThat(second.path("duplicate").asBoolean()).isTrue();
		assertThat(second.path("payment_id").asString()).isEqualTo(first.path("payment_id").asString());
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM payments", Integer.class)).isEqualTo(1);
	}

	@Test
	void invalidAmountIsRejected() {
		JsonNode body = json(this.rabbitTemplate.sendAndReceive("", RabbitConfig.REQUEST_QUEUE,
				request(validBody("ORD-1", 0), "corr-1")));

		assertThat(body.path("status").asString()).isEqualTo("REJECTED");
		assertThat(body.path("reject_reason").asString()).isEqualTo("INVALID_AMOUNT");
		assertThat(body.path("payment_id").isNull()).isTrue();
	}

	@Test
	void requestNotConsumedWithinTtlExpiresIntoDlq() {
		this.listeners.stop();
		try {
			this.rabbitTemplate.send("", RabbitConfig.REQUEST_QUEUE, request(validBody("ORD-1", 500_000), "corr-1"));

			Message dead = this.rabbitTemplate.receive(RabbitConfig.DEAD_LETTER_QUEUE, 10_000);

			assertThat(dead).as("expired after x-message-ttl=3000").isNotNull();
			assertThat(deathReason(dead)).isEqualTo("expired");
		}
		finally {
			this.listeners.start();
		}
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM payments", Integer.class))
			.as("an expired request is never processed late")
			.isZero();
	}

	@Test
	void listenerExceptionSendsMessageToDlqWithoutRequeue() {
		doThrow(new IllegalStateException("simulated failure")).when(this.recorder).record(any());

		this.rabbitTemplate.send("", RabbitConfig.REQUEST_QUEUE, request(validBody("ORD-1", 500_000), "corr-1"));

		Message dead = this.rabbitTemplate.receive(RabbitConfig.DEAD_LETTER_QUEUE, 10_000);
		assertThat(dead).isNotNull();
		assertThat(deathReason(dead)).isEqualTo("rejected");
		// Requeue would make the listener spin on the same message; it must have run exactly once.
		await().during(Duration.ofSeconds(1))
			.atMost(Duration.ofSeconds(3))
			.untilAsserted(() -> verify(this.recorder, times(1)).record(any()));
	}

	@Test
	void unparseableMessageGoesToDlq() {
		this.rabbitTemplate.send("", RabbitConfig.REQUEST_QUEUE, request("{not json", "corr-1"));

		Message dead = this.rabbitTemplate.receive(RabbitConfig.DEAD_LETTER_QUEUE, 10_000);

		assertThat(dead).isNotNull();
		assertThat(new String(dead.getBody(), StandardCharsets.UTF_8)).isEqualTo("{not json");
		verify(this.recorder, times(0)).record(any());
	}

	private static String validBody(String orderId, long amount) {
		return """
				{"order_id":"%s","partner_order_id":"P-1","partner_transaction_id":"TXN-P-1","amount":%d}"""
			.formatted(orderId, amount);
	}

	private static Message request(String json, String correlationId) {
		return MessageBuilder.withBody(json.getBytes(StandardCharsets.UTF_8))
			.setContentType("application/json")
			.setHeader(RabbitConfig.CORRELATION_HEADER, correlationId)
			.build();
	}

	private JsonNode json(Message message) {
		assertThat(message).isNotNull();
		return this.jsonMapper.readTree(message.getBody());
	}

	private static Object deathReason(Message message) {
		List<Map<String, ?>> deaths = message.getMessageProperties().getXDeathHeader();
		assertThat(deaths).isNotEmpty();
		return deaths.get(0).get("reason");
	}

}
