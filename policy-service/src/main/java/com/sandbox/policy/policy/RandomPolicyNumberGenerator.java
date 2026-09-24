package com.sandbox.policy.policy;

import java.time.Year;
import java.time.ZoneOffset;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.stereotype.Component;

/**
 * Simulated e-GCN number, e.g. ACBI-2026-048213. Random numbers can collide:
 * {@link PolicyIssuer} retries on a uk_policies_policy_number violation.
 */
@Component
class RandomPolicyNumberGenerator implements PolicyNumberGenerator {

	@Override
	public String next() {
		return "ACBI-%d-%06d".formatted(Year.now(ZoneOffset.UTC).getValue(),
				ThreadLocalRandom.current().nextInt(1_000_000));
	}

}
