package com.sandbox.order.grpc;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.grpc.client.ChannelBuilderOptions;
import org.springframework.grpc.client.GrpcChannelFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The warm-up against a real gRPC server that serves the standard health service (as
 * payment-service does), and against a port where nothing listens. No Docker needed. The log is
 * JSON once another test has started Spring, plain text otherwise, so only the values are checked.
 */
@ExtendWith(OutputCaptureExtension.class)
class GrpcWarmUpTests {

	private final List<ManagedChannel> channels = new ArrayList<>();

	private Server server;

	@AfterEach
	void stop() {
		this.channels.forEach(ManagedChannel::shutdownNow);
		if (this.server != null) {
			this.server.shutdownNow();
		}
	}

	@Test
	void healthyPaymentServiceIsReportedServing(CapturedOutput output) throws IOException {
		this.server = ServerBuilder.forPort(0).addService(new HealthStatusManager().getHealthService()).build().start();

		new GrpcWarmUp(client(this.server.getPort())).warmUp();

		assertThat(output).contains("warm_up", "SUCCESS", "SERVING");
	}

	@Test
	void unreachablePaymentServiceOnlyLogsAWarning(CapturedOutput output) throws IOException {
		int closedPort;
		try (ServerSocket socket = new ServerSocket(0)) {
			closedPort = socket.getLocalPort();
		}

		new GrpcWarmUp(client(closedPort)).warmUp();

		assertThat(output).contains("warm_up", "FAILED", "UNAVAILABLE");
	}

	private PaymentGrpcClient client(int port) {
		GrpcChannelFactory factory = new GrpcChannelFactory() {

			@Override
			public boolean supports(String target) {
				return true;
			}

			@Override
			public ManagedChannel createChannel(String target, ChannelBuilderOptions options) {
				ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
				GrpcWarmUpTests.this.channels.add(channel);
				return channel;
			}

		};
		return new PaymentGrpcClient(factory, Duration.ofSeconds(3));
	}

}
