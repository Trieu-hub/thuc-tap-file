package com.sandbox.policy.kafka;

import java.time.Instant;

import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;

import com.sandbox.policy.policy.IssuePolicyResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Regression: Kafka's own {@code KafkaException} (thrown when the producer cannot be created) must
 * be logged as {@code event_publish_failed} and rethrown, so the payment.recorded offset is not
 * committed and the event is retried. No Docker needed.
 */
@ExtendWith(OutputCaptureExtension.class)
class PolicyIssuedPublisherTests {

	@Test
	void producerThatCannotBeCreatedIsLoggedAndRethrown(CapturedOutput output) {
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
		given(kafka.send(anyString(), anyString(), anyString()))
			.willThrow(new KafkaException("Failed to construct kafka producer"));
		PolicyIssuedPublisher publisher = new PolicyIssuedPublisher(kafka, JsonMapper.builder().build());

		assertThatIllegalStateException()
			.isThrownBy(() -> publisher.publish(new IssuePolicyResult(IssuePolicyResult.Outcome.ISSUED, "ORD-1",
					"POL-1", "ACBI-2026-000001", Instant.now()), "corr-1"))
			.withCauseInstanceOf(KafkaException.class);
		assertThat(output).contains("event_publish_failed", "ORD-1");
	}

}
