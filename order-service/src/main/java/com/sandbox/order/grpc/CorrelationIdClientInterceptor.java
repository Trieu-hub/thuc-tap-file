package com.sandbox.order.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.slf4j.MDC;

/**
 * Sends the business correlation id of the current request as gRPC metadata {@code x-correlation-id},
 * the gRPC counterpart of the RabbitMQ header used in Flow 1. It reads the MDC, which the flow sets on
 * the calling thread; {@code start} runs on that thread for a blocking stub.
 */
class CorrelationIdClientInterceptor implements ClientInterceptor {

	static final Metadata.Key<String> CORRELATION_ID = Metadata.Key.of("x-correlation-id",
			Metadata.ASCII_STRING_MARSHALLER);

	@Override
	public <Q, R> ClientCall<Q, R> interceptCall(MethodDescriptor<Q, R> method, CallOptions callOptions, Channel next) {
		return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {

			@Override
			public void start(Listener<R> responseListener, Metadata headers) {
				String correlationId = MDC.get("correlation_id");
				if (correlationId != null) {
					headers.put(CORRELATION_ID, correlationId);
				}
				super.start(responseListener, headers);
			}

		};
	}

}
