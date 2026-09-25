package com.sandbox.policy.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Topics and error handling of this service. The broker does not auto-create topics (F23), so each
 * service declares the topics it produces: policy.issued, and payment.recorded.DLT because the
 * dead-letter recoverer of this consumer is the one writing to it.
 * <p>
 * A record that keeps failing is retried 3 times, 1 s apart, then copied to {@code <topic>.DLT} on
 * the same partition (hence 3 partitions there too) and its offset committed, so one bad record
 * cannot block its partition (F19). A record that is not valid JSON for the contract will never
 * succeed, so it goes to the DLT at once. Boot applies this error handler to the listener factory.
 */
@Configuration(proxyBeanMethods = false)
class KafkaConfig {

	static final String PAYMENT_RECORDED = "payment.recorded";

	static final String POLICY_ISSUED = "policy.issued";

	static final int PARTITIONS = 3;

	private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

	@Bean
	NewTopic policyIssuedTopic() {
		return TopicBuilder.name(POLICY_ISSUED).partitions(PARTITIONS).replicas(1).build();
	}

	@Bean
	NewTopic paymentRecordedDeadLetterTopic() {
		return TopicBuilder.name(PAYMENT_RECORDED + ".DLT").partitions(PARTITIONS).replicas(1).build();
	}

	@Bean
	DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<?, ?> kafkaTemplate) {
		// Explicit name: Spring Kafka 4 defaults to "<topic>-dlt". Same partition as the original record.
		DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(kafkaTemplate,
				(record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
		DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
			log.atError()
				.addKeyValue("transport", "Kafka")
				.addKeyValue("action", "event_dead_lettered")
				.addKeyValue("topic", record.topic())
				.addKeyValue("partition", record.partition())
				.addKeyValue("offset", record.offset())
				.addKeyValue("status", "DLT")
				.setCause(ex)
				.log("Event could not be processed, sent to " + record.topic() + ".DLT");
			deadLetters.accept(record, ex);
		}, new FixedBackOff(1000, 3));
		handler.addNotRetryableExceptions(JacksonException.class);
		return handler;
	}

}
