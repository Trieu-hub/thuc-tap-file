package com.sandbox.payment.grpc;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.MDC;

import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.stereotype.Component;

/**
 * Copies the business correlation id from gRPC metadata {@code x-correlation-id} into the MDC,
 * together with {@code transport=gRPC}.
 * <p>
 * gRPC may run the callbacks of one call on different executor threads, so the MDC is set around
 * each callback and cleared in {@code finally} (F30); for a unary call the service method runs
 * inside {@code onHalfClose}.
 */
@Component
@GlobalServerInterceptor
class CorrelationIdServerInterceptor implements ServerInterceptor {

	static final Metadata.Key<String> CORRELATION_ID = Metadata.Key.of("x-correlation-id",
			Metadata.ASCII_STRING_MARSHALLER);

	@Override
	public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
			ServerCallHandler<Q, R> next) {
		String correlationId = headers.get(CORRELATION_ID);
		ServerCall.Listener<Q> delegate;
		MDC.put("correlation_id", correlationId);
		MDC.put("transport", "gRPC");
		try {
			delegate = next.startCall(call, headers);
		}
		finally {
			MDC.remove("correlation_id");
			MDC.remove("transport");
		}
		return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {

			@Override
			public void onMessage(Q message) {
				withCorrelationId(correlationId, () -> super.onMessage(message));
			}

			@Override
			public void onHalfClose() {
				withCorrelationId(correlationId, super::onHalfClose);
			}

			@Override
			public void onCancel() {
				withCorrelationId(correlationId, super::onCancel);
			}

			@Override
			public void onComplete() {
				withCorrelationId(correlationId, super::onComplete);
			}

		};
	}

	private static void withCorrelationId(String correlationId, Runnable action) {
		MDC.put("correlation_id", correlationId);
		MDC.put("transport", "gRPC");
		try {
			action.run();
		}
		finally {
			MDC.remove("correlation_id");
			MDC.remove("transport");
		}
	}

}
