package com.sandbox.order.order;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Plain JDBC access to {@code orders} and {@code order_timeline}, like {@link OrderProgressService}.
 * Every method is its own short transaction: Flow 1 waits up to 2 x 3 s for RPC replies and must
 * not hold a pooled connection while it waits.
 */
@Repository
public class OrderRepository {

	private static final String ORDER_COLUMNS = """
			order_id, partner_order_id, customer_name, phone, amount, mode, status, failure_reason,
			correlation_id, policy_number, created_at, updated_at""";

	private final JdbcTemplate jdbc;

	public OrderRepository(JdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	/**
	 * Inserts the order as CREATED together with its first timeline step.
	 * @throws org.springframework.dao.DuplicateKeyException if {@code partner_order_id} is taken, e.g.
	 * by a concurrent identical request that inserted first (uk_orders_partner_order_id)
	 */
	@Transactional
	public void insertCreated(NewOrder order, TimelineEntry created) {
		Timestamp now = Timestamp.from(Instant.now());
		this.jdbc.update("""
				INSERT INTO orders (order_id, partner_order_id, customer_name, phone, amount, mode, status,
				correlation_id, partner_transaction_id, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", order.orderId(), order.partnerOrderId(),
				order.customerName(), order.phone(), order.amount(), order.mode().name(), OrderStatus.CREATED.name(),
				order.correlationId(), order.partnerTransactionId(), now, now);
		this.jdbc.update("""
				INSERT INTO order_timeline (order_id, seq, step, service, transport, status, duration_ms, detail, created_at)
				VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?)""", order.orderId(), created.step().name(), created.service(),
				created.transport(), created.status(), created.durationMs(), created.detail(), now);
	}

	@Transactional(readOnly = true)
	public Optional<OrderView> findById(String orderId) {
		return findOne("SELECT " + ORDER_COLUMNS + " FROM orders WHERE order_id = ?", orderId);
	}

	@Transactional(readOnly = true)
	public Optional<OrderView> findByPartnerOrderId(String partnerOrderId) {
		return findOne("SELECT " + ORDER_COLUMNS + " FROM orders WHERE partner_order_id = ?", partnerOrderId);
	}

	/** Newest first (D6). Capped so the demo list cannot grow into an unbounded response. */
	@Transactional(readOnly = true)
	public List<OrderView> findLatest(int limit) {
		return this.jdbc.query("SELECT " + ORDER_COLUMNS + " FROM orders ORDER BY created_at DESC, order_id DESC LIMIT ?",
				(rs, rowNum) -> mapOrder(rs, List.of()), limit);
	}

	private Optional<OrderView> findOne(String sql, String key) {
		List<OrderView> orders = this.jdbc.query(sql, (rs, rowNum) -> mapOrder(rs, List.of()), key);
		if (orders.isEmpty()) {
			return Optional.empty();
		}
		OrderView order = orders.get(0);
		List<OrderView.Step> timeline = this.jdbc.query("""
				SELECT seq, step, service, transport, status, duration_ms, detail, created_at
				FROM order_timeline WHERE order_id = ? ORDER BY seq""", (rs, rowNum) -> new OrderView.Step(
				rs.getInt("seq"), TimelineStep.valueOf(rs.getString("step")), rs.getString("service"),
				rs.getString("transport"), rs.getString("status"), rs.getObject("duration_ms", Long.class),
				rs.getString("detail"), rs.getTimestamp("created_at").toInstant()), order.orderId());
		return Optional.of(mapOrder(order, timeline));
	}

	private static OrderView mapOrder(ResultSet rs, List<OrderView.Step> timeline) throws SQLException {
		return new OrderView(rs.getString("order_id"), rs.getString("partner_order_id"), rs.getString("customer_name"),
				rs.getString("phone"), rs.getLong("amount"), OrderMode.valueOf(rs.getString("mode")),
				OrderStatus.valueOf(rs.getString("status")), rs.getString("failure_reason"),
				rs.getString("correlation_id"), rs.getString("policy_number"),
				rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant(), timeline);
	}

	private static OrderView mapOrder(OrderView order, List<OrderView.Step> timeline) {
		return new OrderView(order.orderId(), order.partnerOrderId(), order.customerName(), order.phone(),
				order.amount(), order.mode(), order.status(), order.failureReason(), order.correlationId(),
				order.policyNumber(), order.createdAt(), order.updatedAt(), timeline);
	}

}
