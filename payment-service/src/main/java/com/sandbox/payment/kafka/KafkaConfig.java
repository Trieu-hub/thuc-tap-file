package com.sandbox.payment.kafka;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * The broker does not auto-create topics (F23), so each service declares the topics it produces.
 * payment-service only produces payment.recorded; it consumes nothing from Kafka.
 */
@Configuration(proxyBeanMethods = false)
class KafkaConfig {

	static final String PAYMENT_RECORDED = "payment.recorded";

	static final int PARTITIONS = 3;

	@Bean
	NewTopic paymentRecordedTopic() {
		return TopicBuilder.name(PAYMENT_RECORDED).partitions(PARTITIONS).replicas(1).build();
	}

}
