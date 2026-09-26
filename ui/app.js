'use strict';

// Plain JS, no build step (D15). All data from the API is shown with textContent, never innerHTML,
// so a customer name such as <img onerror=...> is displayed as text and never executed.

const API = '/api/v1/orders';
const POLL_INTERVAL_MS = 500;
const POLL_LIMIT_MS = 30000; // D14: stop polling after 30 s and point to the correlation_id
const STEPS = [
    { key: 'ORDER_CREATED', label: '[Tạo đơn]' },
    { key: 'PAYMENT', label: '[Thanh toán]' },
    { key: 'POLICY_ISSUANCE', label: '[Phát hành e-GCN]' },
    { key: 'NOTIFICATION', label: '[Thông báo]' },
];
const HTTP_TEXT = { 200: 'OK', 202: 'Accepted', 400: 'Bad Request', 404: 'Not Found', 409: 'Conflict' };
const CACHE_TEXT = { CACHE_HIT_REDIS: 'CACHE HIT (REDIS)', CACHE_MISS_DB: 'CACHE MISS (DB)' };

const $ = (id) => document.getElementById(id);
const form = $('order-form');
let lastBody = null;
let pollRun = 0;

function el(tag, text, className) {
    const node = document.createElement(tag);
    if (text !== undefined && text !== null) {
        node.textContent = String(text);
    }
    if (className) {
        node.className = className;
    }
    return node;
}

function newPartnerOrderId() {
    const now = new Date();
    const pad = (n) => String(n).padStart(2, '0');
    return 'UI-' + now.getFullYear() + pad(now.getMonth() + 1) + pad(now.getDate()) + '-' + pad(now.getHours())
        + pad(now.getMinutes()) + pad(now.getSeconds());
}

function readForm() {
    const data = new FormData(form);
    return {
        partner_order_id: String(data.get('partner_order_id')).trim(),
        customer_name: String(data.get('customer_name')).trim(),
        phone: String(data.get('phone')).trim(),
        amount: Number(data.get('amount')),
        mode: String(data.get('mode')),
    };
}

function setBusy(busy) {
    $('submit').disabled = busy;
    $('resend').disabled = busy || lastBody === null;
}

function showErrors(lines) {
    const list = $('form-errors');
    list.replaceChildren(...lines.map((line) => el('li', line)));
    list.hidden = lines.length === 0;
}

function setPill(id, text, kind) {
    const pill = $(id);
    pill.textContent = text;
    pill.className = 'pill' + (kind ? ' ' + kind : '');
    pill.hidden = !text;
}

async function createOrder(body) {
    pollRun++;
    showErrors([]);
    setBusy(true);
    const start = performance.now();
    try {
        const response = await fetch(API, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const elapsed = Math.round(performance.now() - start);
        const data = await response.json().catch(() => null);
        if (response.status === 400) {
            const errors = (data && data.errors) || [];
            showErrors(errors.length ? errors.map((e) => e.field + ': ' + e.message) : ['Dữ liệu không hợp lệ']);
            return;
        }
        if (response.status === 409) {
            showErrors(['409 Conflict: một request giống hệt đang được xử lý, thử lại sau giây lát.']);
            return;
        }
        if (!response.ok || !data) {
            showErrors(['Lỗi ' + response.status + ' từ Order Service']);
            return;
        }
        showResult();
        setPill('http', 'POST → ' + response.status + ' ' + (HTTP_TEXT[response.status] || '') + ' · ' + elapsed + ' ms',
            response.status === 202 ? 'info' : 'ok');
        $('replay').hidden = !data.idempotent_replay;
        renderOrder(data);
        // The cache label always comes from a GET, never from the POST response.
        setPill('cache-status', 'đang đọc…');
        history.replaceState(null, '', '#order=' + encodeURIComponent(data.order_id));
        const order = await loadOrder(data.order_id);
        if (order && !isFinal(order)) {
            await poll(data.order_id);
        }
    }
    catch (error) {
        showErrors(['Không gọi được Order Service: ' + error.message]);
    }
    finally {
        setBusy(false);
        loadRecent();
    }
}

async function loadOrder(orderId) {
    const response = await fetch(API + '/' + encodeURIComponent(orderId), { cache: 'no-store' });
    if (response.status === 404) {
        showErrors(['Không tìm thấy đơn ' + orderId]);
        return null;
    }
    const order = await response.json();
    showResult();
    renderOrder(order);
    setPill('cache-status', CACHE_TEXT[order.cache_status] || order.cache_status,
        order.cache_status === 'CACHE_HIT_REDIS' ? 'ok' : 'info');
    return order;
}

function isFinal(order) {
    if (order.status === 'PROCESSING_FAILED') {
        return true;
    }
    return order.status === 'ISSUED' && (order.timeline || []).some((step) => step.step === 'NOTIFICATION');
}

async function poll(orderId) {
    const run = ++pollRun;
    const start = performance.now();
    while (run === pollRun) {
        const waited = performance.now() - start;
        if (waited > POLL_LIMIT_MS) {
            setPill('polling', 'Chưa có kết quả sau 30 giây. Truy vết bằng Correlation ID bên dưới.', 'warn');
            return;
        }
        setPill('polling', 'Đang chờ Kafka… ' + (waited / 1000).toFixed(1) + ' s', 'info');
        await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
        const order = await loadOrder(orderId);
        if (!order || isFinal(order)) {
            setPill('polling', order ? 'Polling xong sau ' + ((performance.now() - start) / 1000).toFixed(1) + ' s' : '',
                'ok');
            return;
        }
    }
}

function showResult() {
    $('empty').hidden = true;
    $('result').hidden = false;
}

function renderOrder(order) {
    $('order-id').textContent = order.order_id;
    $('customer').textContent = order.customer_name + ' · ' + order.phone + ' · '
        + Number(order.amount).toLocaleString('vi-VN') + ' VND · ' + order.partner_order_id;
    const status = $('status');
    status.textContent = order.status;
    status.className = 'status ' + order.status;
    $('failure').textContent = order.failure_reason ? '(' + order.failure_reason + ')' : '';
    $('policy-number').textContent = order.policy_number || '—';
    $('correlation-id').textContent = order.correlation_id;
    $('log-command').textContent = "docker compose logs --no-log-prefix order-service payment-service policy-service"
        + " | Select-String '" + order.correlation_id + "' | ForEach-Object { $_.Line | ConvertFrom-Json }"
        + " | Sort-Object timestamp | Select-Object timestamp, service, transport, action, status, execution_time_ms"
        + " | Format-Table -AutoSize";
    renderTimeline(order.timeline || []);
}

function renderTimeline(timeline) {
    const byStep = new Map(timeline.map((step) => [step.step, step]));
    const items = STEPS.map((def) => stepItem(def.label, byStep.get(def.key)));
    const failed = byStep.get('PROCESSING_FAILED');
    if (failed) {
        items.push(stepItem('[Thất bại]', failed, 'failed'));
    }
    $('timeline').replaceChildren(...items);
}

function stepItem(label, step, forcedClass) {
    if (!step) {
        const pending = el('li', null, 'pending');
        pending.append(el('div', label + ' → chưa chạy', 'title'));
        return pending;
    }
    const item = el('li', null, forcedClass || (step.status === 'SUCCESS' ? 'done' : 'failed'));
    item.append(el('div', label + ' → ' + step.service + ' → ' + step.status, 'title'));
    const parts = ['via ' + step.transport];
    if (step.duration_ms !== null && step.duration_ms !== undefined) {
        parts.push('duration: ' + step.duration_ms + ' ms');
    }
    if (step.detail) {
        parts.push(step.detail);
    }
    item.append(el('div', parts.join(' · '), 'meta'));
    return item;
}

async function loadRecent() {
    try {
        const response = await fetch(API, { cache: 'no-store' });
        const orders = response.ok ? await response.json() : [];
        const rows = orders.slice(0, 10).map((order) => {
            const row = el('tr');
            row.append(el('td', order.order_id), el('td', order.partner_order_id),
                el('td', order.mode === 'GRPC_KAFKA' ? 'gRPC + Kafka' : 'RabbitMQ RPC'), el('td', order.status),
                el('td', new Date(order.created_at).toLocaleString('vi-VN')));
            row.addEventListener('click', () => openOrder(order.order_id));
            return row;
        });
        $('recent').replaceChildren(...rows);
    }
    catch (error) {
        $('recent').replaceChildren();
    }
}

async function openOrder(orderId) {
    pollRun++;
    showErrors([]);
    $('http').hidden = true;
    $('replay').hidden = true;
    $('polling').hidden = true;
    history.replaceState(null, '', '#order=' + encodeURIComponent(orderId));
    const order = await loadOrder(orderId);
    if (order && !isFinal(order) && order.mode === 'GRPC_KAFKA') {
        await poll(orderId);
    }
}

function copy(text) {
    if (navigator.clipboard) {
        navigator.clipboard.writeText(text).catch(() => {});
    }
}

form.addEventListener('submit', (event) => {
    event.preventDefault();
    lastBody = readForm();
    form.elements.partner_order_id.value = newPartnerOrderId();
    createOrder(lastBody);
});
$('resend').addEventListener('click', () => {
    if (lastBody) {
        createOrder(lastBody);
    }
});
$('reload').addEventListener('click', () => {
    const orderId = $('order-id').textContent;
    if (orderId) {
        loadOrder(orderId);
    }
});
$('copy-cid').addEventListener('click', () => copy($('correlation-id').textContent));
$('copy-log').addEventListener('click', () => copy($('log-command').textContent));
$('refresh-list').addEventListener('click', loadRecent);

form.elements.partner_order_id.value = newPartnerOrderId();
loadRecent();
// F5 on #order=... reloads the same order: its second read is the CACHE HIT (REDIS) of spec IV.3.
const fromHash = new URLSearchParams(location.hash.slice(1)).get('order');
if (fromHash) {
    openOrder(fromHash);
}
