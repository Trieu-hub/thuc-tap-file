package com.sandbox.order.order;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;

import com.sandbox.order.order.TransitionResult.Outcome;
import com.sandbox.order.support.Concurrently;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Runs against a real MySQL 8.4 (Testcontainers) because the guarantees under test are InnoDB's:
 * row locks and unique-index locking between concurrent transactions. Skipped when Docker is not
 * available.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class OrderProgressServiceIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	private static final TimelineEntry PAYMENT = new TimelineEntry(TimelineStep.PAYMENT, "payment-service", "gRPC",
			"SUCCESS", 45L, null);

	private static final TimelineEntry POLICY = new TimelineEntry(TimelineStep.POLICY_ISSUANCE, "policy-service",
			"Kafka", "SUCCESS", null, "ACBI-2026-000001");

	private static final TimelineEntry FAILURE = new TimelineEntry(TimelineStep.PROCESSING_FAILED, "order-service",
			"gRPC", "FAILED", 3000L, "PAYMENT_TIMEOUT");

	@Autowired
	private OrderProgressService progress;

	@Autowired
	private JdbcTemplate jdbc;

	@BeforeEach
	void createOrder() {
		this.jdbc.update("DELETE FROM order_timeline");
		this.jdbc.update("DELETE FROM orders");
		this.jdbc.update("DELETE FROM consumer_inbox");
		this.jdbc.update("""
				INSERT INTO orders (order_id, partner_order_id, customer_name, phone, amount, mode, status,
				correlation_id, partner_transaction_id, created_at, updated_at)
				VALUES ('ORD-1', 'P-1', 'Nguyen Van A', '0901234567', 500000, 'GRPC_KAFKA', 'CREATED',
				'corr-1', 'TXN-P-1', NOW(3), NOW(3))""");
	}

	@Test
	void happyPathMovesOrderToIssued() {
		assertThat(this.progress.recordPayment("ORD-1", PAYMENT).outcome()).isEqualTo(Outcome.APPLIED);
		TransitionResult issued = this.progress.recordPolicyIssued("ORD-1", "evt-1", "ACBI-2026-000001", POLICY);

		assertThat(issued).isEqualTo(new TransitionResult(OrderStatus.ISSUED, Outcome.APPLIED));
		assertThat(status()).isEqualTo("ISSUED");
		assertThat(this.jdbc.queryForObject("SELECT policy_number FROM orders WHERE order_id = 'ORD-1'",
				String.class))
			.isEqualTo("ACBI-2026-000001");
		assertThat(steps()).containsExactly("PAYMENT", "POLICY_ISSUANCE");
	}

	@Test
	void redeliveredPolicyIssuedEventIsIgnored() {
		this.progress.recordPolicyIssued("ORD-1", "evt-1", "ACBI-2026-000001", POLICY);

		TransitionResult second = this.progress.recordPolicyIssued("ORD-1", "evt-1", "ACBI-2026-000001", POLICY);

		assertThat(second).isEqualTo(new TransitionResult(OrderStatus.ISSUED, Outcome.DUPLICATE_EVENT));
		assertThat(steps()).containsExactly("POLICY_ISSUANCE");
	}

	@Test
	void replayedRpcReplyAddsNoSecondTimelineStep() {
		this.progress.recordPayment("ORD-1", PAYMENT);

		TransitionResult replay = this.progress.recordPayment("ORD-1", PAYMENT);

		assertThat(replay).isEqualTo(new TransitionResult(OrderStatus.PAYMENT_RECORDED, Outcome.IGNORED));
		assertThat(steps()).containsExactly("PAYMENT");
	}

	@Test
	void paymentConfirmedAfterPolicyDoesNotMoveOrderBack() {
		// Flow 2: payment publishes payment.recorded before its gRPC response reaches order-service,
		// so policy.issued can be consumed first.
		this.progress.recordPolicyIssued("ORD-1", "evt-1", "ACBI-2026-000001", POLICY);

		TransitionResult late = this.progress.recordPayment("ORD-1", PAYMENT);

		assertThat(late).isEqualTo(new TransitionResult(OrderStatus.ISSUED, Outcome.IGNORED));
		assertThat(status()).isEqualTo("ISSUED");
		assertThat(steps()).containsExactly("POLICY_ISSUANCE", "PAYMENT");
	}

	@Test
	void failureAfterIssuedDoesNotOverwriteStatus() {
		this.progress.recordPolicyIssued("ORD-1", "evt-1", "ACBI-2026-000001", POLICY);

		TransitionResult failure = this.progress.recordFailure("ORD-1", "PAYMENT_TIMEOUT", FAILURE);

		assertThat(failure.status()).isEqualTo(OrderStatus.ISSUED);
		assertThat(this.jdbc.queryForObject("SELECT failure_reason FROM orders WHERE order_id = 'ORD-1'",
				String.class))
			.isNull();
	}

	@Test
	void policyIssuedAfterTimeoutRepairsOrder() {
		this.progress.recordFailure("ORD-1", "PAYMENT_TIMEOUT", FAILURE);

		TransitionResult issued = this.progress.recordPolicyIssued("ORD-1", "evt-1", "ACBI-2026-000001", POLICY);

		assertThat(issued).isEqualTo(new TransitionResult(OrderStatus.ISSUED, Outcome.APPLIED));
	}

	@Test
	void eventForUnknownOrderIsNotMarkedProcessed() {
		assertThatExceptionOfType(OrderNotFoundException.class)
			.isThrownBy(() -> this.progress.recordPolicyIssued("ORD-404", "evt-9", "ACBI-2026-000009", POLICY));

		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM consumer_inbox WHERE event_id = 'evt-9'",
				Integer.class))
			.as("rolled back so the broker redelivers it")
			.isZero();
	}

	@Test
	void concurrentDuplicateEventsUpdateOrderOnce() throws Exception {
		List<TransitionResult> results = Concurrently.run(8,
				() -> this.progress.recordPolicyIssued("ORD-1", "evt-1", "ACBI-2026-000001", POLICY));

		assertThat(results).filteredOn((result) -> result.outcome() == Outcome.APPLIED).hasSize(1);
		assertThat(results).extracting(TransitionResult::status).containsOnly(OrderStatus.ISSUED);
		assertThat(steps()).containsExactly("POLICY_ISSUANCE");
	}

	@Test
	void concurrentMessagesForSameOrderKeepTimelineConsistent() throws Exception {
		AtomicInteger turn = new AtomicInteger();
		Concurrently.run(8, () -> switch (turn.getAndIncrement() % 3) {
			case 0 -> this.progress.recordPayment("ORD-1", PAYMENT);
			case 1 -> this.progress.recordPolicyIssued("ORD-1", "evt-" + turn.get(), "ACBI-2026-000001", POLICY);
			default -> this.progress.recordFailure("ORD-1", "PAYMENT_TIMEOUT", FAILURE);
		});

		assertThat(status()).isEqualTo("ISSUED");
		assertThat(steps()).containsExactlyInAnyOrder("PAYMENT", "POLICY_ISSUANCE", "PROCESSING_FAILED");
		assertThat(this.jdbc.queryForList("SELECT seq FROM order_timeline WHERE order_id = 'ORD-1' ORDER BY seq",
				Integer.class))
			.containsExactly(1, 2, 3);
	}

	private String status() {
		return this.jdbc.queryForObject("SELECT status FROM orders WHERE order_id = 'ORD-1'", String.class);
	}

	private List<String> steps() {
		return this.jdbc.queryForList("SELECT step FROM order_timeline WHERE order_id = 'ORD-1' ORDER BY seq",
				String.class);
	}

}
