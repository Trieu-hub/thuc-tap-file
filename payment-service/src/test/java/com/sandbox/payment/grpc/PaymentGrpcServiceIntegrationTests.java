package com.sandbox.payment.grpc;

import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.event.EventListener;
import org.springframework.grpc.server.lifecycle.GrpcServerStartedEvent;
import org.springframework.jdbc.core.JdbcTemplate;

import com.sandbox.contracts.payment.v1.PaymentServiceGrpc;
import com.sandbox.contracts.payment.v1.PaymentStatus;
import com.sandbox.contracts.payment.v1.RecordPaymentRequest;
import com.sandbox.contracts.payment.v1.RecordPaymentResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Calls the real gRPC server over a real HTTP/2 connection with the stubs generated from
 * payment.proto, the same ones order-service uses. MySQL 8.4 via Testcontainers; skipped without
 * Docker. The RabbitMQ listener is not started: Flow 1 is not involved.
 */
@SpringBootTest(properties = "spring.rabbitmq.listener.simple.auto-startup=false")
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
class PaymentGrpcServiceIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Autowired
	private GrpcPort grpcPort;

	@Autowired
	private JdbcTemplate jdbc;

	private ManagedChannel channel;

	@BeforeEach
	void setUp() {
		this.jdbc.update("DELETE FROM payments");
		this.channel = ManagedChannelBuilder.forAddress("localhost", this.grpcPort.port).usePlaintext().build();
	}

	@AfterEach
	void closeChannel() throws InterruptedException {
		this.channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
	}

	@Test
	void recordsPaymentAndLogsCorrelationIdFromMetadata(CapturedOutput output) {
		RecordPaymentResponse response = stub("corr-grpc-1").recordPayment(request("ORD-1", 500_000));

		assertThat(response.getStatus()).isEqualTo(PaymentStatus.RECORDED);
		assertThat(response.getPaymentId()).startsWith("PAY-");
		assertThat(response.getDuplicate()).isFalse();
		assertThat(response.hasRecordedAt()).isTrue();
		assertThat(output).contains("\"correlation_id\":\"corr-grpc-1\"", "\"transport\":\"gRPC\"",
				"\"action\":\"RecordPayment\"", "\"execution_time_ms\":");
	}

	@Test
	void repeatedRequestReturnsSamePaymentAsDuplicate() {
		RecordPaymentResponse first = stub("corr-1").recordPayment(request("ORD-1", 500_000));
		RecordPaymentResponse second = stub("corr-1").recordPayment(request("ORD-1", 500_000));

		assertThat(second.getStatus()).isEqualTo(PaymentStatus.RECORDED);
		assertThat(second.getDuplicate()).isTrue();
		assertThat(second.getPaymentId()).isEqualTo(first.getPaymentId());
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM payments", Integer.class)).isEqualTo(1);
	}

	@Test
	void invalidAmountIsRejected() {
		RecordPaymentResponse response = stub("corr-1").recordPayment(request("ORD-1", 0));

		assertThat(response.getStatus()).isEqualTo(PaymentStatus.REJECTED);
		assertThat(response.getRejectReason()).isEqualTo("INVALID_AMOUNT");
		assertThat(response.getPaymentId()).isEmpty();
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM payments", Integer.class)).isZero();
	}

	private PaymentServiceGrpc.PaymentServiceBlockingStub stub(String correlationId) {
		Metadata headers = new Metadata();
		headers.put(CorrelationIdServerInterceptor.CORRELATION_ID, correlationId);
		return PaymentServiceGrpc.newBlockingStub(this.channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
			.withDeadlineAfter(3, TimeUnit.SECONDS);
	}

	private static RecordPaymentRequest request(String orderId, long amount) {
		return RecordPaymentRequest.newBuilder()
			.setOrderId(orderId)
			.setPartnerOrderId("P-" + orderId)
			.setPartnerTransactionId("TXN-P-" + orderId)
			.setAmount(amount)
			.build();
	}

	/** The server binds a free port in tests (src/test/resources/application.properties). */
	@TestConfiguration
	static class GrpcPort {

		volatile int port;

		@EventListener
		void onStarted(GrpcServerStartedEvent event) {
			this.port = event.getPort();
		}

	}

}
