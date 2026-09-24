package com.sandbox.payment.observability;

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
			new MockEnvironment().withProperty("spring.application.name", "payment-service"));

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
		assertThat(json).contains("\"service\":\"payment-service\"", "\"log_level\":\"INFO\"",
				"\"correlation_id\":\"corr-123\"", "\"transport\":\"gRPC\"", "\"execution_time_ms\":35",
				"\"cache_hit\":false", "\"message\":\"payment recorded\"", "\"timestamp\":\"");
	}

}
