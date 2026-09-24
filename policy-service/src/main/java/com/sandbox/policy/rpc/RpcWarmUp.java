package com.sandbox.policy.rpc;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.sandbox.policy.policy.IssuePolicyResult;
import com.sandbox.policy.policy.PolicyIssuer;

/**
 * Runs the RPC path once at startup, so the first real request does not pay for class loading and
 * first-use initialisation. Measured on a fresh container: about 2.7 s per hop, close to the
 * caller's 3 s timeout, so the first order after a restart could fail for no business reason.
 * <p>
 * It runs on {@link ApplicationReadyEvent}, before Spring reports readiness, and the compose
 * healthcheck uses {@code /actuator/health/readiness}: the container only turns healthy once warm.
 * Nothing is persisted: the issuer runs inside a transaction that is rolled back.
 * Copied from payment-service on purpose (CLAUDE.md D17).
 */
@Component
class RpcWarmUp {

	private static final Logger log = LoggerFactory.getLogger(RpcWarmUp.class);

	private final JacksonJsonMessageConverter converter;

	private final PolicyIssuer issuer;

	private final TransactionTemplate transactions;

	RpcWarmUp(JacksonJsonMessageConverter converter, PolicyIssuer issuer, TransactionTemplate transactions) {
		this.converter = converter;
		this.issuer = issuer;
		this.transactions = transactions;
	}

	@EventListener(ApplicationReadyEvent.class)
	void warmUp() {
		long start = System.nanoTime();
		try {
			// Fits the VARCHAR(32) order_id column; random so two instances warming up at once never share a key.
			String id = "WARMUP-" + UUID.randomUUID().toString().substring(0, 12);
			Message message = this.converter.toMessage(new PolicyRpcRequest(id, id), new MessageProperties());
			PolicyRpcRequest request = (PolicyRpcRequest) this.converter.fromMessage(message,
					ParameterizedTypeReference.forType(PolicyRpcRequest.class));
			this.transactions.executeWithoutResult((status) -> {
				IssuePolicyResult result = this.issuer.issue(request.toCommand());
				this.converter.toMessage(PolicyRpcResponse.from(result), new MessageProperties());
				// The issuer joins this transaction, so its INSERT is undone too.
				status.setRollbackOnly();
			});
			log.atInfo()
				.addKeyValue("action", "warm_up")
				.addKeyValue("status", "SUCCESS")
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.log("RPC path warmed up, nothing persisted");
		}
		catch (RuntimeException ex) {
			// A failed warm-up must not stop the service: the first real request is only slower.
			log.atWarn()
				.addKeyValue("action", "warm_up")
				.addKeyValue("status", "FAILED")
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.setCause(ex)
				.log("RPC warm-up failed, continuing without it");
		}
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
