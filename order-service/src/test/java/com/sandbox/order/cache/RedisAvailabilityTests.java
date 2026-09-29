package com.sandbox.order.cache;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Redis circuit breaker with a clock the test moves by hand: no sleeping, no Docker. The log is
 * JSON once another test has started Spring and plain text otherwise, so only values are checked.
 */
@ExtendWith(OutputCaptureExtension.class)
class RedisAvailabilityTests {

	private final MutableClock clock = new MutableClock(Instant.parse("2026-09-27T10:00:00Z"));

	private final RedisAvailability availability = new RedisAvailability(this.clock, Duration.ofSeconds(5));

	@Test
	void closedCircuitTriesRedis() {
		assertThat(this.availability.shouldTry()).isTrue();
	}

	@Test
	void afterAFailureRedisIsSkippedForExactlyFiveSeconds() {
		this.availability.markFailure();

		assertThat(this.availability.shouldTry()).isFalse();
		this.clock.advance(Duration.ofMillis(4_999));
		assertThat(this.availability.shouldTry()).as("4.999 s later").isFalse();
		this.clock.advance(Duration.ofMillis(1));
		assertThat(this.availability.shouldTry()).as("5 s later: try again").isTrue();
	}

	@Test
	void failedRetryStartsANewWindow() {
		this.availability.markFailure();
		this.clock.advance(Duration.ofSeconds(5));
		assertThat(this.availability.shouldTry()).isTrue();

		this.availability.markFailure();

		assertThat(this.availability.shouldTry()).isFalse();
		this.clock.advance(Duration.ofSeconds(5));
		assertThat(this.availability.shouldTry()).isTrue();
	}

	@Test
	void successClosesTheCircuit(CapturedOutput output) {
		this.availability.markFailure();
		this.clock.advance(Duration.ofSeconds(5));

		this.availability.markSuccess();
		this.availability.markSuccess();

		assertThat(this.availability.shouldTry()).isTrue();
		assertThat(count(output, "redis_circuit_closed")).as("logged once, on the transition").isEqualTo(1);
	}

	@Test
	void successWhileClosedLogsNothing(CapturedOutput output) {
		this.availability.markSuccess();

		assertThat(output.getAll()).doesNotContain("redis_circuit");
	}

	@Test
	void manyConsecutiveFailuresLogTheOpeningOnce(CapturedOutput output) {
		for (int i = 0; i < 10; i++) {
			this.availability.markFailure();
			this.clock.advance(Duration.ofSeconds(2));
		}

		assertThat(count(output, "redis_circuit_open")).isEqualTo(1);
	}

	@Test
	void secondOutageIsLoggedAgain(CapturedOutput output) {
		this.availability.markFailure();
		this.availability.markSuccess();
		this.availability.markFailure();

		assertThat(count(output, "redis_circuit_open")).isEqualTo(2);
		assertThat(count(output, "redis_circuit_closed")).isEqualTo(1);
	}

	private static long count(CapturedOutput output, String action) {
		return output.getAll().lines().filter((line) -> line.contains(action)).count();
	}

	private static final class MutableClock extends Clock {

		private Instant now;

		MutableClock(Instant now) {
			this.now = now;
		}

		void advance(Duration duration) {
			this.now = this.now.plus(duration);
		}

		@Override
		public Instant instant() {
			return this.now;
		}

		@Override
		public ZoneId getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(ZoneId zone) {
			return this;
		}

	}

}
