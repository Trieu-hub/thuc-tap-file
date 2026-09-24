package com.sandbox.payment.payment;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;

import com.sandbox.payment.support.Concurrently;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs against a real MySQL 8.4 (Testcontainers) because the guarantees under test are InnoDB's:
 * unique-index locking between concurrent transactions. Skipped when Docker is not available.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class PaymentRecorderIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Autowired
	private PaymentRecorder recorder;

	@Autowired
	private JdbcTemplate jdbc;

	@BeforeEach
	void cleanTables() {
		this.jdbc.update("DELETE FROM payments");
	}

	@Test
	void recordsNewPayment() {
		RecordPaymentResult result = this.recorder.record(command("ORD-1", 500_000));

		assertThat(result.status()).isEqualTo(PaymentStatus.RECORDED);
		assertThat(result.duplicate()).isFalse();
		assertThat(result.paymentId()).startsWith("PAY-");
		assertThat(paymentCount("ORD-1")).isEqualTo(1);
	}

	@Test
	void redeliveredRequestReturnsStoredPaymentWithoutChargingAgain() {
		RecordPaymentResult first = this.recorder.record(command("ORD-1", 500_000));
		RecordPaymentResult second = this.recorder.record(command("ORD-1", 500_000));

		assertThat(second.status()).isEqualTo(PaymentStatus.RECORDED);
		assertThat(second.duplicate()).isTrue();
		assertThat(second.paymentId()).isEqualTo(first.paymentId());
		assertThat(second.recordedAt()).isEqualTo(first.recordedAt());
		assertThat(paymentCount("ORD-1")).isEqualTo(1);
	}

	@Test
	void sameTransactionIdWithDifferentAmountIsRejected() {
		this.recorder.record(command("ORD-1", 500_000));

		RecordPaymentResult result = this.recorder.record(command("ORD-1", 900_000));

		assertThat(result.status()).isEqualTo(PaymentStatus.REJECTED);
		assertThat(result.rejectReason()).isEqualTo(PaymentRecorder.IDEMPOTENCY_KEY_MISMATCH);
		assertThat(this.jdbc.queryForObject("SELECT amount FROM payments WHERE order_id = 'ORD-1'", Long.class))
			.isEqualTo(500_000);
	}

	@Test
	void secondTransactionIdForSameOrderIsRejected() {
		this.recorder.record(command("ORD-1", 500_000));

		RecordPaymentResult result = this.recorder
			.record(new RecordPaymentCommand("ORD-1", "P-1", "TXN-OTHER", 500_000));

		assertThat(result.status()).isEqualTo(PaymentStatus.REJECTED);
		assertThat(result.rejectReason()).isEqualTo(PaymentRecorder.IDEMPOTENCY_KEY_MISMATCH);
		assertThat(paymentCount("ORD-1")).isEqualTo(1);
	}

	@Test
	void invalidAmountIsRejectedWithoutStoringAnything() {
		RecordPaymentResult result = this.recorder.record(command("ORD-1", 0));

		assertThat(result.status()).isEqualTo(PaymentStatus.REJECTED);
		assertThat(result.rejectReason()).isEqualTo(PaymentRecorder.INVALID_AMOUNT);
		assertThat(paymentCount("ORD-1")).isZero();
	}

	@Test
	void concurrentDeliveriesOfSameRequestChargeExactlyOnce() throws Exception {
		List<RecordPaymentResult> results = Concurrently.run(8, () -> this.recorder.record(command("ORD-1", 500_000)));

		assertThat(paymentCount("ORD-1")).isEqualTo(1);
		assertThat(results).allMatch((result) -> result.status() == PaymentStatus.RECORDED);
		assertThat(results).filteredOn((result) -> !result.duplicate()).hasSize(1);
		assertThat(results).extracting(RecordPaymentResult::paymentId).containsOnly(results.get(0).paymentId());
	}

	private RecordPaymentCommand command(String orderId, long amount) {
		return new RecordPaymentCommand(orderId, "P-1", "TXN-P-1", amount);
	}

	private int paymentCount(String orderId) {
		return this.jdbc.queryForObject("SELECT COUNT(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
	}

}
