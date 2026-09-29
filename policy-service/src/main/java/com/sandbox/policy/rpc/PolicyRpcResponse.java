package com.sandbox.policy.rpc;

import java.time.Instant;

import com.sandbox.policy.policy.IssuePolicyResult;

/** Reply body: contracts/schemas/policy-rpc-response.schema.json. */
record PolicyRpcResponse(String orderId, String status, String policyId, String policyNumber, boolean duplicate,
		Instant issuedAt) {

	/** Every outcome carries a policy (new or stored), so the reply status is always ISSUED. */
	static PolicyRpcResponse from(IssuePolicyResult result) {
		return new PolicyRpcResponse(result.orderId(), "ISSUED", result.policyId(), result.policyNumber(),
				result.outcome() != IssuePolicyResult.Outcome.ISSUED, result.issuedAt());
	}

}
