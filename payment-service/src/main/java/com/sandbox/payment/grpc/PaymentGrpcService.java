package com.sandbox.payment.grpc;

import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.stereotype.Service;

import com.sandbox.contracts.payment.v1.PaymentServiceGrpc;
import com.sandbox.contracts.payment.v1.PaymentStatus;
import com.sandbox.contracts.payment.v1.RecordPaymentRequest;
import com.sandbox.contracts.payment.v1.RecordPaymentResponse;
import com.sandbox.payment.kafka.PaymentRecordedPublisher;
import com.sandbox.payment.payment.PaymentRecorder;
import com.sandbox.payment.payment.RecordPaymentCommand;
import com.sandbox.payment.payment.RecordPaymentResult;

/**
 * Flow 2 transport adapter for {@code PaymentService.RecordPayment}: map the Protobuf request to a
 * command, call {@link PaymentRecorder} (the same class Flow 1 uses), map the result back. No
 * business logic here. The correlation id is already in the MDC ({@link CorrelationIdServerInterceptor}).
 */
@Service
class PaymentGrpcService extends PaymentServiceGrpc.PaymentServiceImplBase {

	private static final Logger log = LoggerFactory.getLogger(PaymentGrpcService.class);

	private final PaymentRecorder recorder;

	private final PaymentRecordedPublisher events;

	PaymentGrpcService(PaymentRecorder recorder, PaymentRecordedPublisher events) {
		this.recorder = recorder;
		this.events = events;
	}

	@Override
	public void recordPayment(RecordPaymentRequest request, StreamObserver<RecordPaymentResponse> responseObserver) {
		long start = System.nanoTime();
		RecordPaymentCommand command = new RecordPaymentCommand(request.getOrderId(), request.getPartnerOrderId(),
				request.getPartnerTransactionId(), request.getAmount());
		RecordPaymentResult result;
		try {
			result = this.recorder.record(command);
		}
		catch (RuntimeException ex) {
			log.atError()
				.addKeyValue("transport", "gRPC")
				.addKeyValue("action", "RecordPayment")
				.addKeyValue("order_id", request.getOrderId())
				.addKeyValue("status", "FAILED")
				.addKeyValue("execution_time_ms", elapsedMs(start))
				.setCause(ex)
				.log("RecordPayment failed");
			// INTERNAL instead of the default UNKNOWN, without leaking the exception text to the caller.
			responseObserver.onError(Status.INTERNAL.withDescription("payment could not be recorded").asRuntimeException());
			return;
		}
		log.atInfo()
			.addKeyValue("transport", "gRPC")
			.addKeyValue("action", "RecordPayment")
			.addKeyValue("order_id", request.getOrderId())
			.addKeyValue("status", result.status().name())
			.addKeyValue("duplicate", result.duplicate())
			.addKeyValue("execution_time_ms", elapsedMs(start))
			.log("RecordPayment handled");
		responseObserver.onNext(toResponse(result));
		responseObserver.onCompleted();
		// record() has returned, so its transaction is committed: the event never announces a payment
		// that could still roll back. Sent after the response, so a slow or absent Kafka cannot push
		// order-service past its deadline for a payment that is already recorded. A duplicate is
		// published again (same event_id): policy-service deduplicates it.
		if (result.status() == com.sandbox.payment.payment.PaymentStatus.RECORDED) {
			this.events.publish(command, result, MDC.get("correlation_id"));
		}
	}

	private static RecordPaymentResponse toResponse(RecordPaymentResult result) {
		// Protobuf strings cannot be null: an absent value is the empty string (payment.proto).
		RecordPaymentResponse.Builder response = RecordPaymentResponse.newBuilder()
			.setOrderId(result.orderId())
			.setStatus(PaymentStatus.valueOf(result.status().name()))
			.setDuplicate(result.duplicate());
		if (result.paymentId() != null) {
			response.setPaymentId(result.paymentId());
		}
		if (result.rejectReason() != null) {
			response.setRejectReason(result.rejectReason());
		}
		if (result.recordedAt() != null) {
			response.setRecordedAt(Timestamp.newBuilder()
				.setSeconds(result.recordedAt().getEpochSecond())
				.setNanos(result.recordedAt().getNano()));
		}
		return response.build();
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
