package com.sandbox.policy.rpc;

import java.nio.charset.StandardCharsets;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the real listener through a real broker (RabbitMQ 4.3) with raw JSON bodies, the way
 * order-service sends them. Skipped without Docker.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
// Close the context with the class: its listeners would otherwise keep reconnecting to the stopped container.
@DirtiesContext
class PolicyRpcListenerIntegrationTests {

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
	private JsonMapper jsonMapper;

	@Autowired
	private JdbcTemplate jdbc;

	@Autowired
	private RpcWarmUp warmUp;

	@BeforeEach
	void clean() {
		this.jdbc.update("DELETE FROM policies");
		this.amqpAdmin.purgeQueue(RabbitConfig.DEAD_LETTER_QUEUE, false);
	}

	@Test
	void issuesPolicyAndRepliesWithCorrelationHeader(CapturedOutput output) {
		Message reply = this.rabbitTemplate.sendAndReceive("", RabbitConfig.REQUEST_QUEUE,
				request(body("ORD-1"), "corr-test-1"));

		assertThat(reply).as("reply routed back through reply_to").isNotNull();
		JsonNode body = json(reply);
		assertThat(body.path("status").asString()).isEqualTo("ISSUED");
		assertThat(body.path("policy_number").asString()).matches("ACBI-\\d{4}-\\d{6}");
		assertThat(body.path("duplicate").asBoolean()).isFalse();
		assertThat((String) reply.getMessageProperties().getHeader(RabbitConfig.CORRELATION_HEADER))
			.isEqualTo("corr-test-1");
		assertThat(output).contains("\"correlation_id\":\"corr-test-1\"", "\"transport\":\"RabbitMQ\"",
				"\"action\":\"IssuePolicy\"", "\"execution_time_ms\":");
	}

	@Test
	void repeatedRequestReturnsSamePolicyAsDuplicate() {
		JsonNode first = json(
				this.rabbitTemplate.sendAndReceive("", RabbitConfig.REQUEST_QUEUE, request(body("ORD-1"), "corr-1")));
		JsonNode second = json(
				this.rabbitTemplate.sendAndReceive("", RabbitConfig.REQUEST_QUEUE, request(body("ORD-1"), "corr-1")));

		assertThat(second.path("duplicate").asBoolean()).isTrue();
		assertThat(second.path("policy_number").asString()).isEqualTo(first.path("policy_number").asString());
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM policies", Integer.class)).isEqualTo(1);
	}

	@Test
	void unparseableMessageGoesToDlq() {
		this.rabbitTemplate.send("", RabbitConfig.REQUEST_QUEUE, request("{not json", "corr-1"));

		Message dead = this.rabbitTemplate.receive(RabbitConfig.DEAD_LETTER_QUEUE, 10_000);

		assertThat(dead).isNotNull();
		List<Map<String, ?>> deaths = dead.getMessageProperties().getXDeathHeader();
		assertThat(deaths).isNotEmpty();
		assertThat(deaths.get(0).get("reason")).isEqualTo("rejected");
	}

	@Test
	void warmUpIssuesNothing(CapturedOutput output) {
		this.warmUp.warmUp();

		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM policies", Integer.class)).isZero();
		assertThat(output).contains("\"action\":\"warm_up\"", "\"status\":\"SUCCESS\"");
	}

	private static String body(String orderId) {
		return """
				{"order_id":"%s","payment_id":"PAY-1"}""".formatted(orderId);
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

}
