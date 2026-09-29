package com.sandbox.order.observability;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;

import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Puts {@code transport=HTTP} in the MDC for the whole request, so every log line written while
 * serving it (including the transport-neutral business classes) carries the transport, like the
 * RabbitMQ, gRPC and Kafka adapters do for theirs.
 * <p>
 * It deliberately does not set {@code correlation_id}: a POST creates it in the flow, and a GET only
 * knows it once the order has been read. The MDC is per thread and the thread is pooled, hence the
 * {@code finally}.
 */
@Component
class HttpTransportFilter extends OncePerRequestFilter {

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
			throws ServletException, IOException {
		MDC.put("transport", "HTTP");
		try {
			chain.doFilter(request, response);
		}
		finally {
			MDC.remove("transport");
		}
	}

}
