package com.sandbox.payment.rpc;

import tools.jackson.databind.json.JsonMapper;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declares the queue this service consumes (the consumer owns its queue) and its dead-letter path.
 * <p>
 * A request that sits in the queue longer than the caller's 3 s reply timeout expires instead of
 * being processed for nobody (D7); expired and rejected messages are dead-lettered through
 * {@code sandbox.dlx} with their original routing key, so they land in {@code payment.rpc.request.dlq}
 * where they can be inspected in the RabbitMQ UI.
 */
@Configuration(proxyBeanMethods = false)
class RabbitConfig {

	static final String REQUEST_QUEUE = "payment.rpc.request";

	static final String DEAD_LETTER_QUEUE = REQUEST_QUEUE + ".dlq";

	static final String DEAD_LETTER_EXCHANGE = "sandbox.dlx";

	/** Business correlation id set by order-service; not the AMQP correlation_id, which RabbitTemplate owns. */
	static final String CORRELATION_HEADER = "x-correlation-id";

	static final int REQUEST_TTL_MS = 3000;

	@Bean
	Declarables paymentRpcTopology() {
		Queue requests = QueueBuilder.durable(REQUEST_QUEUE)
			.ttl(REQUEST_TTL_MS)
			.deadLetterExchange(DEAD_LETTER_EXCHANGE)
			.build();
		Queue deadLetters = QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
		DirectExchange dlx = new DirectExchange(DEAD_LETTER_EXCHANGE);
		Binding dlqBinding = BindingBuilder.bind(deadLetters).to(dlx).with(REQUEST_QUEUE);
		return new Declarables(requests, deadLetters, dlx, dlqBinding);
	}

	/** JSON, not Java serialization; reuses Boot's mapper so the snake_case setting applies. */
	@Bean
	JacksonJsonMessageConverter jacksonJsonMessageConverter(JsonMapper jsonMapper) {
		return new JacksonJsonMessageConverter(jsonMapper);
	}

}
