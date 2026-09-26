package com.sandbox.order.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.boot.amqp.autoconfigure.RabbitTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * order-service only sends RPC requests; it declares no queues because the consumers own them
 * (payment-service, policy-service). Replies use Direct Reply-to ({@code amq.rabbitmq.reply-to}),
 * RabbitTemplate's default, so no temporary reply queue is created.
 */
@Configuration(proxyBeanMethods = false)
class RabbitConfig {

	private static final Logger log = LoggerFactory.getLogger(RabbitConfig.class);

	/** JSON, not Java serialization; reuses Boot's mapper so the snake_case setting applies. */
	@Bean
	JacksonJsonMessageConverter jacksonJsonMessageConverter(JsonMapper jsonMapper) {
		return new JacksonJsonMessageConverter(jsonMapper);
	}

	/**
	 * A reply that arrives after the 3 s timeout finds no pending request: RabbitTemplate drops it and
	 * throws, and that error lands here. Logging it as {@code late_reply_ignored} makes the drop
	 * visible instead of assumed (D7).
	 */
	@Bean
	RabbitTemplateCustomizer lateReplyLogging() {
		return (template) -> template.setReplyErrorHandler((error) -> {
			Message reply = (error instanceof ListenerExecutionFailedException failed) ? failed.getFailedMessage()
					: null;
			String correlationId = (reply != null)
					? reply.getMessageProperties().getHeader(OrderRpcClient.CORRELATION_HEADER) : null;
			// The reply container thread has no MDC of its own; take the id from the echoed header.
			MDC.put("correlation_id", correlationId);
			try {
				log.atWarn()
					.addKeyValue("transport", "RabbitMQ")
					.addKeyValue("action", "late_reply_ignored")
					.addKeyValue("status", "IGNORED")
					.log("RPC reply arrived after the timeout and was discarded");
			}
			finally {
				MDC.remove("correlation_id");
			}
		});
	}

}
