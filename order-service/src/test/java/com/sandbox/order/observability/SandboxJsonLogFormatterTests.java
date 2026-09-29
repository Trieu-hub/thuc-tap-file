package com.sandbox.order.observability;

import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.event.KeyValuePair;

import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

class SandboxJsonLogFormatterTests {

	private final SandboxJsonLogFormatter formatter = new SandboxJsonLogFormatter(
			new MockEnvironment().withProperty("spring.application.name", "order-service"));

	@Test
	void writesSpecFieldsFromMdcAndKeyValuePairs() {
		LoggingEvent event = new LoggingEvent("fqcn", new LoggerContext().getLogger("test"), Level.INFO,
				"payment recorded", null, null);
		event.setMDCPropertyMap(Map.of("correlation_id", "corr-123"));
		event.addKeyValuePair(new KeyValuePair("transport", "gRPC"));
		event.addKeyValuePair(new KeyValuePair("execution_time_ms", 35));
		event.addKeyValuePair(new KeyValuePair("cache_hit", false));

		String json = this.formatter.format(event);

		assertThat(json).startsWith("{").endsWith("}\n");
		assertThat(json).contains("\"service\":\"order-service\"", "\"log_level\":\"INFO\"",
				"\"correlation_id\":\"corr-123\"", "\"transport\":\"gRPC\"", "\"execution_time_ms\":35",
				"\"cache_hit\":false", "\"message\":\"payment recorded\"", "\"timestamp\":\"");
	}

	@Test
	void keyValuePairOverridesTheSameMdcKeyInsteadOfRepeatingIt() {
		LoggingEvent event = new LoggingEvent("fqcn", new LoggerContext().getLogger("test"), Level.INFO,
				"reply received", null, null);
		// The adapter put transport=HTTP in the MDC; this log line is about a RabbitMQ reply.
		event.setMDCPropertyMap(Map.of("transport", "HTTP", "correlation_id", "corr-123"));
		event.addKeyValuePair(new KeyValuePair("transport", "RabbitMQ"));

		String json = this.formatter.format(event);

		assertThat(json).contains("\"transport\":\"RabbitMQ\"", "\"correlation_id\":\"corr-123\"")
			.doesNotContain("\"transport\":\"HTTP\"");
		assertThat(json.split("\"transport\"", -1)).as("one transport key").hasSize(2);
	}

}
