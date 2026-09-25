package com.sandbox.order.api;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.core.JacksonException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.sandbox.order.flow.OrderConflictException;
import com.sandbox.order.order.OrderNotFoundException;

/** One JSON error shape for every API error, with the offending fields named as on the wire. */
@RestControllerAdvice
class ApiExceptionHandler {

	@ExceptionHandler(MethodArgumentNotValidException.class)
	ResponseEntity<ApiError> invalidBody(MethodArgumentNotValidException ex) {
		List<FieldViolation> violations = ex.getBindingResult()
			.getFieldErrors()
			.stream()
			.map((error) -> new FieldViolation(toSnakeCase(error.getField()), error.getDefaultMessage()))
			.sorted(Comparator.comparing(FieldViolation::field))
			.collect(Collectors.toList());
		return error(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", "Request body is invalid", violations);
	}

	/** Malformed JSON, or a value of the wrong type such as an unknown {@code mode}. */
	@ExceptionHandler(HttpMessageNotReadableException.class)
	ResponseEntity<ApiError> unreadableBody(HttpMessageNotReadableException ex) {
		List<FieldViolation> violations = List.of();
		if (ex.getCause() instanceof JacksonException jackson && !jackson.getPath().isEmpty()) {
			// The path already uses the JSON (snake_case) property names.
			String field = jackson.getPath().get(jackson.getPath().size() - 1).getPropertyName();
			violations = List.of(new FieldViolation(field, "has an invalid value"));
		}
		return error(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", "Request body could not be read", violations);
	}

	@ExceptionHandler(OrderConflictException.class)
	ResponseEntity<ApiError> conflict(OrderConflictException ex) {
		return error(HttpStatus.CONFLICT, "ORDER_IN_PROGRESS", ex.getMessage(), List.of());
	}

	@ExceptionHandler(OrderNotFoundException.class)
	ResponseEntity<ApiError> notFound(OrderNotFoundException ex) {
		return error(HttpStatus.NOT_FOUND, "ORDER_NOT_FOUND", ex.getMessage(), List.of());
	}

	private static ResponseEntity<ApiError> error(HttpStatus status, String code, String message,
			List<FieldViolation> violations) {
		return ResponseEntity.status(status).body(new ApiError(status.value(), code, message, violations));
	}

	/** Bean Validation reports Java names (partnerOrderId); clients sent partner_order_id. */
	private static String toSnakeCase(String javaName) {
		return javaName.replaceAll("([A-Z])", "_$1").toLowerCase(Locale.ROOT);
	}

	record ApiError(int status, String error, String message,
			@JsonInclude(JsonInclude.Include.NON_EMPTY) List<FieldViolation> errors) {

	}

	record FieldViolation(String field, String message) {

	}

}
