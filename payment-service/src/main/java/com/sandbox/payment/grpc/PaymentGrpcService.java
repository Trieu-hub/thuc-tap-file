package com.sandbox.payment.grpc;

import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Service;

import com.sandbox.contracts.payment.v1.PaymentServiceGrpc;
import com.sandbox.contracts.payment.v1.PaymentStatus;
import com.sandbox.contracts.payment.v1.RecordPaymentRequest;
import com.sandbox.contracts.payment.v1.RecordPaymentResponse;
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

	PaymentGrpcService(PaymentRecorder recorder) {
		this.recorder = recorder;
	}

	@Override
	public void recordPayment(RecordPaymentRequest request, StreamObserver<RecordPaymentResponse> responseObserver) {
		long start = System.nanoTime();
		RecordPaymentResult result;
		try {
			result = this.recorder.record(new RecordPaymentCommand(request.getOrderId(), request.getPartnerOrderId(),
					request.getPartnerTransactionId(), request.getAmount()));
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
