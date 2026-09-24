package com.sandbox.policy.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import com.sandbox.policy.policy.IssuePolicyResult;
import com.sandbox.policy.policy.PolicyIssuer;

/**
 * Flow 1 transport adapter: receive the request, call {@link PolicyIssuer}, reply. No business
 * logic here, so Flow 2 (Kafka) can reuse the same issuer.
 */
@Component
class PolicyRpcListener {

	private static final Logger log = LoggerFactory.getLogger(PolicyRpcListener.class);

	private final PolicyIssuer issuer;

	PolicyRpcListener(PolicyIssuer issuer) {
		this.issuer = issuer;
	}

	@RabbitListener(queues = RabbitConfig.REQUEST_QUEUE)
	Message<PolicyRpcResponse> onRequest(PolicyRpcRequest request,
			@Header(name = RabbitConfig.CORRELATION_HEADER, required = false) String correlationId) {
		// The listener thread is shared by all messages, so the MDC must be set and cleared per message (F30).
		MDC.put("correlation_id", correlationId);
		long start = System.nanoTime();
		try {
			IssuePolicyResult result = this.issuer.issue(request.toCommand());
			log.atInfo()
				.addKeyValue("transport", "RabbitMQ")
				.addKeyValue("action", "IssuePolicy")
				.addKeyValue("order_id", request.orderId())
				.addKeyValue("status", "ISSUED")
				.addKeyValue("outcome", result.outcome().name())
				.addKeyValue("policy_number", result.policyNumber())
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("Policy RPC handled");
			// Echo the header so order-service can still trace a reply that arrives after its timeout.
			return MessageBuilder.withPayload(PolicyRpcResponse.from(result))
				.setHeader(RabbitConfig.CORRELATION_HEADER, correlationId)
				.build();
		}
		catch (RuntimeException ex) {
			// Rethrown: the container rejects without requeue and the broker moves the message to the DLQ.
			log.atError()
				.addKeyValue("transport", "RabbitMQ")
				.addKeyValue("action", "IssuePolicy")
				.addKeyValue("order_id", request.orderId())
				.addKeyValue("status", "FAILED")
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.setCause(ex)
				.log("Policy RPC failed, message goes to the DLQ");
			throw ex;
		}
		finally {
			MDC.remove("correlation_id");
		}
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
