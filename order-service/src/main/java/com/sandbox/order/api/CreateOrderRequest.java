package com.sandbox.order.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import com.sandbox.order.flow.PlaceOrderCommand;
import com.sandbox.order.order.OrderMode;

/**
 * Body of {@code POST /api/v1/orders} (snake_case on the wire). Sizes match the orders columns, so
 * a value that passes validation always fits. An unknown {@code mode} fails JSON parsing and is
 * reported as a 400 by {@link ApiExceptionHandler}.
 */
public record CreateOrderRequest(
		@NotBlank @Size(max = 64) String partnerOrderId,
		@NotBlank @Size(max = 255) String customerName,
		@NotBlank @Pattern(regexp = "\\+?[0-9]{8,15}", message = "must be 8-15 digits, optionally starting with +") String phone,
		// VND has no minor unit: an integer amount (D18).
		@NotNull @Positive Long amount,
		@NotNull OrderMode mode) {

	PlaceOrderCommand toCommand() {
		return new PlaceOrderCommand(this.partnerOrderId, this.customerName, this.phone, this.amount);
	}

}
