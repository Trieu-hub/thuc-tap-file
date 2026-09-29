package com.sandbox.order.rpc;

import java.time.Instant;

/** contracts/schemas/policy-rpc-response.schema.json. */
public record PolicyRpcResponse(String orderId, String status, String policyId, String policyNumber,
		boolean duplicate, Instant issuedAt) {

}
