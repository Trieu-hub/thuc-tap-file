package com.sandbox.policy.rpc;

import com.sandbox.policy.policy.IssuePolicyCommand;

/** Body of a {@code policy.rpc.request} message: contracts/schemas/policy-rpc-request.schema.json. */
record PolicyRpcRequest(String orderId, String paymentId) {

	/** An RPC request has no event_id: PolicyIssuer dedupes it on order_id alone. */
	IssuePolicyCommand toCommand() {
		return new IssuePolicyCommand(this.orderId, this.paymentId, null);
	}

}
