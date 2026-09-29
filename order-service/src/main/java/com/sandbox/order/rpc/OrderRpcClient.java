package com.sandbox.order.rpc;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;

/**
 * Sends the two Flow 1 RPC requests and waits for the reply.
 * <p>
 * RabbitTemplate sets reply_to (Direct Reply-to) and the AMQP correlation_id itself and matches the
 * reply to the pending request by it. The business correlation id is a separate header,
 * {@code x-correlation-id}, so the two are never mixed up.
 * <p>
 * Returns {@code null} when no reply arrives within {@code spring.rabbitmq.template.reply-timeout}
 * (3000 ms): RabbitTemplate does not throw on timeout (D7), so callers must treat {@code null} as a
 * timeout. A broker that cannot be reached throws {@link org.springframework.amqp.AmqpException}.
 */
@Component
public class OrderRpcClient {

	public static final String PAYMENT_QUEUE = "payment.rpc.request";

	public static final String POLICY_QUEUE = "policy.rpc.request";

	public static final String CORRELATION_HEADER = "x-correlation-id";

	private final RabbitTemplate rabbitTemplate;

	public OrderRpcClient(RabbitTemplate rabbitTemplate) {
		this.rabbitTemplate = rabbitTemplate;
	}

	public PaymentRpcResponse recordPayment(PaymentRpcRequest request, String correlationId) {
		return call(PAYMENT_QUEUE, request, correlationId, new ParameterizedTypeReference<>() {
		});
	}

	public PolicyRpcResponse issuePolicy(PolicyRpcRequest request, String correlationId) {
		return call(POLICY_QUEUE, request, correlationId, new ParameterizedTypeReference<>() {
		});
	}

	private <T> T call(String queue, Object request, String correlationId, ParameterizedTypeReference<T> replyType) {
		// Default exchange: the routing key is the queue name. The explicit reply type makes the
		// converter ignore the __TypeId__ header, which names a class that only exists in the other service.
		return this.rabbitTemplate.convertSendAndReceiveAsType("", queue, request, (message) -> {
			message.getMessageProperties().setHeader(CORRELATION_HEADER, correlationId);
			return message;
		}, replyType);
	}

}
