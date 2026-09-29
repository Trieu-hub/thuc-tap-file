package com.sandbox.payment.kafka;

import java.time.Instant;

import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;

import com.sandbox.payment.payment.PaymentStatus;
import com.sandbox.payment.payment.RecordPaymentCommand;
import com.sandbox.payment.payment.RecordPaymentResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Regression, found in the Day 3 demo: with Kafka stopped, the first publish after a payment-service
 * start fails while the producer is created, with Kafka's own {@code KafkaException} (not Spring's).
 * It escaped the publisher, so {@code event_publish_failed} was never logged. No Docker needed.
 */
@ExtendWith(OutputCaptureExtension.class)
class PaymentRecordedPublisherTests {

	@Test
	void producerThatCannotBeCreatedIsLoggedAsPublishFailedAndNotThrown(CapturedOutput output) {
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
		given(kafka.send(anyString(), anyString(), anyString()))
			.willThrow(new KafkaException("Failed to construct kafka producer"));
		PaymentRecordedPublisher publisher = new PaymentRecordedPublisher(kafka, JsonMapper.builder().build());

		assertThatNoException().isThrownBy(() -> publisher.publish(
				new RecordPaymentCommand("ORD-1", "P-1", "TXN-P-1", 500_000),
				new RecordPaymentResult("ORD-1", PaymentStatus.RECORDED, "PAY-1", null, false, Instant.now()),
				"corr-1"));
		assertThat(output).contains("event_publish_failed", "ORD-1", "Failed to construct kafka producer");
	}

}
