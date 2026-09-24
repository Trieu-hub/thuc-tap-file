package com.sandbox.policy.policy;

import java.time.Instant;

/**
 * {@code outcome} tells the caller whether a policy was created now. Only {@link Outcome#ISSUED}
 * is new; for the other two the stored policy is returned so a replayed request gets the same
 * answer as the first one.
 */
public record IssuePolicyResult(Outcome outcome, String orderId, String policyId, String policyNumber,
		Instant issuedAt) {

	public enum Outcome {

		/** A new policy was created by this call. */
		ISSUED,

		/** The order already had a policy (e.g. a replayed RPC or a new event_id for the same payment). */
		ALREADY_ISSUED,

		/** This exact event_id was processed before. */
		DUPLICATE_EVENT_IGNORED

	}

}
