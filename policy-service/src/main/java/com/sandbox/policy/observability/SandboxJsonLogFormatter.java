package com.sandbox.policy.observability;

import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import org.slf4j.event.KeyValuePair;

import org.springframework.boot.json.JsonWriter;
import org.springframework.boot.logging.structured.StructuredLogFormatter;
import org.springframework.core.env.Environment;

/**
 * Writes one JSON object per log line with the field names required by the spec
 * (timestamp, service, log_level, correlation_id, transport, action, order_id, status,
 * execution_time_ms, cache_hit). Spec fields are not hard-coded here: they come from the
 * MDC (e.g. correlation_id) or from SLF4J key-value pairs, e.g.
 * {@code log.atInfo().addKeyValue("action", "RecordPayment").addKeyValue("execution_time_ms", 35).log("...")}.
 * Enabled by {@code logging.structured.format.console} in application.yml.
 * Copied per service on purpose (CLAUDE.md D17).
 */
public class SandboxJsonLogFormatter implements StructuredLogFormatter<ILoggingEvent> {

	private final JsonWriter<ILoggingEvent> writer;

	public SandboxJsonLogFormatter(Environment environment) {
		String service = environment.getProperty("spring.application.name", "unknown");
		this.writer = JsonWriter.<ILoggingEvent>of((members) -> {
			members.add("timestamp", (event) -> event.getInstant().truncatedTo(ChronoUnit.MILLIS).toString());
			members.add("service", service);
			members.add("log_level", (event) -> event.getLevel().toString());
			members.add("logger", ILoggingEvent::getLoggerName);
			members.add("message", ILoggingEvent::getFormattedMessage);
			// One map, key-value pairs over MDC: an adapter puts "transport" in the MDC and a log line may
			// set it again; writing both would repeat the key, which ConvertFrom-Json (PowerShell) rejects.
			members.from(SandboxJsonLogFormatter::fields).whenNotEmpty().usingPairs(Map::forEach);
			members.add("exception", ILoggingEvent::getThrowableProxy)
				.whenNotNull()
				.as(ThrowableProxyUtil::asString);
		}).withNewLineAtEnd();
	}

	@Override
	public String format(ILoggingEvent event) {
		return this.writer.writeToString(event);
	}

	private static Map<String, Object> fields(ILoggingEvent event) {
		Map<String, Object> fields = new LinkedHashMap<>(event.getMDCPropertyMap());
		if (event.getKeyValuePairs() != null) {
			for (KeyValuePair pair : event.getKeyValuePairs()) {
				fields.put(pair.key, pair.value);
			}
		}
		return fields;
	}

}
