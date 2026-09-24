package com.sandbox.policy.policy;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import com.sandbox.policy.policy.IssuePolicyResult.Outcome;
import com.sandbox.policy.support.Concurrently;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Runs against a real MySQL 8.4 (Testcontainers) because the guarantees under test are InnoDB's:
 * unique-index locking between concurrent transactions. Skipped when Docker is not available.
 * The RabbitMQ listener is not started: these tests call the issuer directly and need no broker.
 */
@SpringBootTest(properties = "spring.rabbitmq.listener.simple.auto-startup=false")
@Testcontainers(disabledWithoutDocker = true)
class PolicyIssuerIntegrationTests {

	@Container
	@ServiceConnection
	static MySQLContainer mysql = new MySQLContainer("mysql:8.4");

	@Autowired
	private PolicyIssuer issuer;

	@Autowired
	private ScriptedPolicyNumberGenerator policyNumbers;

	@Autowired
	private JdbcTemplate jdbc;

	@BeforeEach
	void cleanTables() {
		this.jdbc.update("DELETE FROM policies");
		this.jdbc.update("DELETE FROM consumer_inbox");
		this.policyNumbers.scripted.clear();
	}

	@Test
	void issuesPolicyAndRecordsEventInInbox() {
		IssuePolicyResult result = this.issuer.issue(new IssuePolicyCommand("ORD-1", "PAY-1", "evt-1"));

		assertThat(result.outcome()).isEqualTo(Outcome.ISSUED);
		assertThat(result.policyNumber()).matches("ACBI-\\d{4}-\\d{6}");
		assertThat(policyCount("ORD-1")).isEqualTo(1);
		assertThat(inboxCount("evt-1")).isEqualTo(1);
	}

	@Test
	void redeliveredEventIsIgnoredAndReturnsSamePolicy() {
		IssuePolicyResult first = this.issuer.issue(new IssuePolicyCommand("ORD-1", "PAY-1", "evt-1"));
		IssuePolicyResult second = this.issuer.issue(new IssuePolicyCommand("ORD-1", "PAY-1", "evt-1"));

		assertThat(second.outcome()).isEqualTo(Outcome.DUPLICATE_EVENT_IGNORED);
		assertThat(second.policyNumber()).isEqualTo(first.policyNumber());
		assertThat(policyCount("ORD-1")).isEqualTo(1);
	}

	@Test
	void newEventIdForSameOrderDoesNotIssueSecondPolicy() {
		IssuePolicyResult first = this.issuer.issue(new IssuePolicyCommand("ORD-1", "PAY-1", "evt-1"));
		IssuePolicyResult second = this.issuer.issue(new IssuePolicyCommand("ORD-1", "PAY-1", "evt-2"));

		assertThat(second.outcome()).isEqualTo(Outcome.ALREADY_ISSUED);
		assertThat(second.policyId()).isEqualTo(first.policyId());
		assertThat(policyCount("ORD-1")).isEqualTo(1);
		assertThat(inboxCount("evt-2")).as("evt-2 is handled, a redelivery of it is skipped").isEqualTo(1);
	}

	@Test
	void replayedRpcRequestWithoutEventIdReturnsSamePolicy() {
		IssuePolicyResult first = this.issuer.issue(new IssuePolicyCommand("ORD-1", "PAY-1", null));
		IssuePolicyResult second = this.issuer.issue(new IssuePolicyCommand("ORD-1", "PAY-1", null));

		assertThat(first.outcome()).isEqualTo(Outcome.ISSUED);
		assertThat(second.outcome()).isEqualTo(Outcome.ALREADY_ISSUED);
		assertThat(second.policyNumber()).isEqualTo(first.policyNumber());
		assertThat(policyCount("ORD-1")).isEqualTo(1);
	}

	@Test
	void policyNumberCollisionIsRetriedNotMistakenForDuplicateOrder() {
		this.policyNumbers.scripted.add("ACBI-2026-000001");
		this.issuer.issue(new IssuePolicyCommand("ORD-1", "PAY-1", "evt-1"));
		this.policyNumbers.scripted.add("ACBI-2026-000001");
		this.policyNumbers.scripted.add("ACBI-2026-000002");

		IssuePolicyResult result = this.issuer.issue(new IssuePolicyCommand("ORD-2", "PAY-2", "evt-2"));

		assertThat(result.outcome()).isEqualTo(Outcome.ISSUED);
		assertThat(result.policyNumber()).isEqualTo("ACBI-2026-000002");
		assertThat(policyCount("ORD-2")).isEqualTo(1);
	}

	@Test
	void failedIssuanceRollsBackInboxClaimSoRedeliveryCanSucceed() {
		this.policyNumbers.scripted.add("ACBI-2026-000001");
		this.issuer.issue(new IssuePolicyCommand("ORD-1", "PAY-1", "evt-1"));
		for (int i = 0; i < PolicyIssuer.MAX_POLICY_NUMBER_ATTEMPTS; i++) {
			this.policyNumbers.scripted.add("ACBI-2026-000001");
		}

		assertThatIllegalStateException()
			.isThrownBy(() -> this.issuer.issue(new IssuePolicyCommand("ORD-2", "PAY-2", "evt-2")));
		assertThat(inboxCount("evt-2")).as("claim must not survive a failed transaction").isZero();

		IssuePolicyResult redelivered = this.issuer.issue(new IssuePolicyCommand("ORD-2", "PAY-2", "evt-2"));
		assertThat(redelivered.outcome()).isEqualTo(Outcome.ISSUED);
	}

	@Test
	void concurrentDeliveriesOfSameEventIssueExactlyOnePolicy() throws Exception {
		List<IssuePolicyResult> results = Concurrently.run(8,
				() -> this.issuer.issue(new IssuePolicyCommand("ORD-1", "PAY-1", "evt-1")));

		assertThat(policyCount("ORD-1")).isEqualTo(1);
		assertThat(results).filteredOn((result) -> result.outcome() == Outcome.ISSUED).hasSize(1);
		assertThat(results).extracting(IssuePolicyResult::policyNumber).containsOnly(results.get(0).policyNumber());
	}

	@Test
	void concurrentEventsWithDifferentIdsForSameOrderIssueExactlyOnePolicy() throws Exception {
		AtomicInteger eventSeq = new AtomicInteger();
		List<IssuePolicyResult> results = Concurrently.run(8, () -> this.issuer
			.issue(new IssuePolicyCommand("ORD-1", "PAY-1", "evt-" + eventSeq.incrementAndGet())));

		assertThat(policyCount("ORD-1")).isEqualTo(1);
		assertThat(results).filteredOn((result) -> result.outcome() == Outcome.ISSUED).hasSize(1);
		assertThat(results).extracting(IssuePolicyResult::policyNumber).containsOnly(results.get(0).policyNumber());
	}

	private int policyCount(String orderId) {
		return this.jdbc.queryForObject("SELECT COUNT(*) FROM policies WHERE order_id = ?", Integer.class, orderId);
	}

	private int inboxCount(String eventId) {
		return this.jdbc.queryForObject("SELECT COUNT(*) FROM consumer_inbox WHERE event_id = ?", Integer.class,
				eventId);
	}

	/** Returns queued numbers first, to force collisions; random numbers otherwise. */
	static class ScriptedPolicyNumberGenerator implements PolicyNumberGenerator {

		final Queue<String> scripted = new ConcurrentLinkedQueue<>();

		private final PolicyNumberGenerator fallback = new RandomPolicyNumberGenerator();

		@Override
		public String next() {
			String number = this.scripted.poll();
			return (number != null) ? number : this.fallback.next();
		}

	}

	@TestConfiguration
	static class Config {

		@Bean
		@Primary
		ScriptedPolicyNumberGenerator scriptedPolicyNumberGenerator() {
			return new ScriptedPolicyNumberGenerator();
		}

	}

}
