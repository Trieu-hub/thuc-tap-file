package com.sandbox.order.flow;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import com.sandbox.order.cache.IdempotencyStore;
import com.sandbox.order.order.NewOrder;
import com.sandbox.order.order.OrderMode;
import com.sandbox.order.order.OrderStatus;
import com.sandbox.order.support.Concurrently;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * The idempotency key {@code idempotency:order:{partner_order_id}} on a real Redis 7.4 (same image as
 * compose) and MySQL 8.4, through {@link OrderIntake}, which both flows use. Skipped without Docker.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
class RedisIdempotencyIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Container
	@ServiceConnection(name = "redis")
	static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

	@Autowired
	private OrderIntake intake;

	@Autowired
	private IdempotencyStore idempotency;

	@Autowired
	private StringRedisTemplate redisTemplate;

	@Autowired
	private JdbcTemplate jdbc;

	@BeforeEach
	void clean() {
		this.jdbc.update("DELETE FROM order_timeline");
		this.jdbc.update("DELETE FROM orders");
		this.redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
	}

	@Test
	void newOrderStoresItsOrderIdUnderTheKeyFor24Hours() {
		NewOrder order = newOrder("P-NEW");

		this.intake.insert(order, System.nanoTime());

		assertThat(this.redisTemplate.opsForValue().get("idempotency:order:P-NEW")).isEqualTo(order.orderId());
		assertThat(this.redisTemplate.getExpire("idempotency:order:P-NEW", TimeUnit.SECONDS))
			.as("24 h once the order row is committed").isBetween(86_300L, 86_400L);
	}

	@Test
	void newPartnerOrderIdIsNotADuplicate() {
		this.intake.rejectDuplicate(command("P-FRESH"), OrderMode.GRPC_KAFKA, System.nanoTime());
	}

	@Test
	void identicalRepeatIsRefusedFromTheRedisKeyAndPointsToTheStoredOrder(CapturedOutput output) {
		NewOrder order = newOrder("P-DUP");
		this.intake.insert(order, System.nanoTime());

		assertThatExceptionOfType(DuplicateOrderException.class)
			.isThrownBy(() -> this.intake.rejectDuplicate(command("P-DUP"), OrderMode.GRPC_KAFKA, System.nanoTime()))
			.satisfies((ex) -> {
				assertThat(ex.mismatch()).isFalse();
				assertThat(ex.orderId()).isEqualTo(order.orderId());
				assertThat(ex.orderStatus()).isEqualTo(OrderStatus.CREATED);
			});
		assertThat(output).contains("\"action\":\"duplicate_order_rejected\"", "\"idempotency_source\":\"REDIS\"");
	}

	@Test
	void repeatWithAnyDifferentFieldIsAMismatch(CapturedOutput output) {
		this.intake.insert(newOrder("P-MIS"), System.nanoTime());
		PlaceOrderCommand same = command("P-MIS");
		List<PlaceOrderCommand> changed = List.of(
				new PlaceOrderCommand("P-MIS", "Nguyễn Văn B", same.phone(), same.amount()),
				new PlaceOrderCommand("P-MIS", same.customerName(), "0909999999", same.amount()),
				new PlaceOrderCommand("P-MIS", same.customerName(), same.phone(), 999_000));

		for (PlaceOrderCommand command : changed) {
			assertThatExceptionOfType(DuplicateOrderException.class)
				.isThrownBy(() -> this.intake.rejectDuplicate(command, OrderMode.GRPC_KAFKA, System.nanoTime()))
				.satisfies((ex) -> assertThat(ex.mismatch()).as(command.toString()).isTrue());
		}
		assertThatExceptionOfType(DuplicateOrderException.class).as("other mode")
			.isThrownBy(() -> this.intake.rejectDuplicate(same, OrderMode.RABBITMQ_RPC, System.nanoTime()))
			.satisfies((ex) -> assertThat(ex.mismatch()).isTrue());
		assertThat(output).contains("\"mismatched_fields\":\"customer_name\"", "\"mismatched_fields\":\"phone\"",
				"\"mismatched_fields\":\"amount\"", "\"mismatched_fields\":\"mode\"")
			.as("customer data is not logged").doesNotContain("0909999999", "Nguyễn Văn B");
	}

	@Test
	void partnerOrderIdInOtherCaseFoundByMysqlIsAMismatch() {
		// Redis keys are case-sensitive, MySQL's utf8mb4_unicode_ci UNIQUE index is not.
		this.intake.insert(newOrder("P-CASE"), System.nanoTime());

		assertThatExceptionOfType(DuplicateOrderException.class)
			.isThrownBy(() -> this.intake.rejectDuplicate(command("p-case"), OrderMode.GRPC_KAFKA, System.nanoTime()))
			.satisfies((ex) -> assertThat(ex.mismatch()).isTrue());
	}

	@Test
	void keyWithoutOrderRowMeansAnIdenticalRequestIsInFlight() {
		// What a request holds between its claim and its insert: the key, with the 30 s TTL, and no row.
		this.redisTemplate.opsForValue().set("idempotency:order:P-FLIGHT", "ORD-IN-FLIGHT");

		assertThatExceptionOfType(OrderConflictException.class)
			.isThrownBy(() -> this.intake.rejectDuplicate(command("P-FLIGHT"), OrderMode.GRPC_KAFKA, System.nanoTime()));
	}

	@Test
	void claimExpiresAfter30SecondsIfTheOrderIsNeverWritten() {
		// A request that crashes between claim and insert: its key must not block the partner for 24 h.
		assertThat(this.idempotency.claim("P-CRASH", "ORD-CRASH")).isEqualTo(IdempotencyStore.Claim.CLAIMED);

		assertThat(this.redisTemplate.getExpire("idempotency:order:P-CRASH", TimeUnit.SECONDS)).isBetween(1L, 30L);
		assertThat(this.idempotency.claim("P-CRASH", "ORD-OTHER")).isEqualTo(IdempotencyStore.Claim.TAKEN);
	}

	@Test
	void concurrentIdenticalRequestsCreateOneOrderAndTheOthersGetConflict() throws Exception {
		List<String> outcomes = Concurrently.run(4, () -> {
			try {
				this.intake.insert(newOrder("P-RACE"), System.nanoTime());
				return "CREATED";
			}
			catch (OrderConflictException ex) {
				return "CONFLICT";
			}
		});

		assertThat(outcomes).containsOnlyOnce("CREATED").containsOnly("CREATED", "CONFLICT");
		assertThat(this.jdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class)).isEqualTo(1);
	}

	@Test
	void failedInsertReleasesTheKeySoARetryIsNotRejected() {
		NewOrder first = newOrder("P-RETRY");
		this.intake.insert(first, System.nanoTime());
		// Simulate "key lost" (e.g. Redis flushed): the row exists but the key does not.
		this.redisTemplate.delete("idempotency:order:P-RETRY");

		assertThatExceptionOfType(OrderConflictException.class)
			.isThrownBy(() -> this.intake.insert(newOrder("P-RETRY"), System.nanoTime()));
		assertThat(this.redisTemplate.hasKey("idempotency:order:P-RETRY"))
			.as("the losing claim was released").isFalse();
		// And the partner_order_id is still recognised through MySQL.
		assertThatExceptionOfType(DuplicateOrderException.class)
			.isThrownBy(() -> this.intake.rejectDuplicate(command("P-RETRY"), OrderMode.GRPC_KAFKA, System.nanoTime()))
			.satisfies((ex) -> assertThat(ex.orderId()).isEqualTo(first.orderId()));
	}

	private NewOrder newOrder(String partnerOrderId) {
		return this.intake.newOrder(command(partnerOrderId), OrderMode.GRPC_KAFKA);
	}

	private static PlaceOrderCommand command(String partnerOrderId) {
		return new PlaceOrderCommand(partnerOrderId, "Nguyễn Văn A", "0901234567", 500_000);
	}

}
