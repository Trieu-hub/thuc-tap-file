# Insurance Sandbox: script demo nghiệm thu (Windows PowerShell 5.1).
#
# Nạp một lần trong cửa sổ PowerShell, đứng ở thư mục gốc của repo:
#   . .\scripts\demo.ps1          dấu chấm ở đầu là bắt buộc: nạp các hàm vào cửa sổ hiện tại
#   demo                          danh sách các bước
#   demo 3                        chạy bước 3
#   demo 0 -Reset                 chuẩn bị, XÓA toàn bộ dữ liệu cũ (docker compose down -v) trước khi bật
#   demo all                      tập dượt: chạy lần lượt các bước chính, dừng chờ Enter sau mỗi bước
#   demo all -IncludeOptional     như trên, thêm bước 10 (tắt Redis) và 11 (đo tải)
#
# Kịch bản và lời nói đầy đủ: docs/demo-guide.md. Mọi lệnh ở đây chỉ gọi API, docker compose và giao
# diện quản trị có sẵn; script không sửa code hay cấu hình.

$script:DemoRoot = Split-Path -Parent $PSScriptRoot
$script:Api = 'http://localhost:8080/api/v1/orders'
$script:Last = @{}

$script:Steps = [ordered]@{
    0  = 'Chuẩn bị: bật sandbox, kiểm tra healthy, đơn mồi, mở giao diện'
    1  = 'Giới thiệu: mục tiêu, kiến trúc, 2 luồng'
    2  = 'Khởi động bằng 1 lệnh: docker compose ps, readiness'
    3  = 'Luồng 1 RabbitMQ RPC: 200 ISSUED, timeline, Correlation ID, CACHE MISS rồi HIT'
    4  = 'Luồng 2 gRPC + Kafka: 202, polling, ISSUED'
    5  = 'Chặn request trùng: 409 DUPLICATE_ORDER, 409 DUPLICATE_ORDER_MISMATCH, trùng đồng thời'
    6  = 'Truy vết log theo correlation_id qua 3 service, key Redis và TTL'
    7  = 'Ngoại lệ: tắt Payment, timeout 3 s, request hết hạn vào DLQ, bật lại, đơn mồi'
    8  = 'Ngoại lệ: message hỏng (poison) vào payment.rpc.request.dlq'
    9  = 'Kafka: gửi lại event 3 lần vẫn 1 hợp đồng, message hỏng vào DLT'
    10 = '(Tùy chọn) Tắt Redis: vẫn tạo đơn, vẫn chặn trùng, health UP'
    11 = '(Phụ lục) Đo tải 20 và 100 đơn đồng thời cho 2 luồng'
    12 = 'Hỏi đáp: 5 câu nghiệm thu và bằng chứng trong repo'
}

# ---------------------------------------------------------------- helpers

function Write-Title($n, $text) {
    Write-Host ''
    Write-Host ('=' * 78) -ForegroundColor DarkGray
    Write-Host ("BƯỚC {0}: {1}" -f $n, $text) -ForegroundColor Yellow
    Write-Host ('=' * 78) -ForegroundColor DarkGray
}
function Say($text) { Write-Host "  TRẢ LỜI : $text" -ForegroundColor Cyan }
function Ui($text) { Write-Host "  TRÊN UI : $text" -ForegroundColor Magenta }
function Expect($lines) {
    Write-Host '  MONG ĐỢI:' -ForegroundColor Green
    foreach ($l in @($lines)) { Write-Host "    - $l" -ForegroundColor Green }
}
function Cmd($text) { Write-Host "  PS> $text" -ForegroundColor DarkGray }
function Warn($text) { Write-Host "  LƯU Ý   : $text" -ForegroundColor DarkYellow }

function New-PartnerId($prefix) {
    return '{0}-{1}-{2}' -f $prefix, (Get-Date -Format 'HHmmss'), (Get-Random -Minimum 10 -Maximum 99)
}

function Get-EnvValue($name) {
    $line = Get-Content (Join-Path $script:DemoRoot '.env') | Where-Object { $_ -match "^$name=" } | Select-Object -First 1
    return ($line -replace "^$name=", '')
}

function Send-Order {
    param([string]$Partner, [string]$Mode = 'RABBITMQ_RPC', [long]$Amount = 500000,
          [string]$Name = 'Nguyen Van A', [string]$Phone = '0901234567')
    $body = '{"partner_order_id":"' + $Partner + '","customer_name":"' + $Name + '","phone":"' + $Phone +
            '","amount":' + $Amount + ',"mode":"' + $Mode + '"}'
    $sw = [Diagnostics.Stopwatch]::StartNew()
    try {
        $r = Invoke-WebRequest -UseBasicParsing -Method Post -Uri $script:Api -ContentType 'application/json' -Body $body
        $code = [int]$r.StatusCode; $content = $r.Content
    }
    catch {
        $resp = $_.Exception.Response
        if ($null -eq $resp) { throw }
        $code = [int]$resp.StatusCode
        $content = (New-Object IO.StreamReader($resp.GetResponseStream())).ReadToEnd()
    }
    return [pscustomobject]@{ Code = $code; Ms = $sw.ElapsedMilliseconds; Body = ($content | ConvertFrom-Json) }
}

function Get-Order($orderId) {
    $r = Invoke-WebRequest -UseBasicParsing -Uri "$script:Api/$orderId"
    return [pscustomobject]@{ Body = ($r.Content | ConvertFrom-Json); CorrelationHeader = $r.Headers['X-Correlation-Id'] }
}

function Show-Timeline($order) {
    $order.timeline | Format-Table seq, step, service, transport, status, duration_ms, detail -AutoSize | Out-String -Width 200 | Write-Host
}

function Invoke-Sql($sql) {
    $sql | docker compose exec -T mysql sh -c 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -t 2>/dev/null'
}

function Get-QueueDepth($queue) {
    $line = docker compose exec -T rabbitmq rabbitmqctl list_queues name messages 2>$null |
        Where-Object { $_ -match ('^' + [regex]::Escape($queue) + '\s') }
    return [int](($line -split '\s+')[1])
}

function Show-DlqMessages($queue, $auth) {
    # Management API "get" with ackmode ack_requeue_true only peeks: every message stays in the queue.
    # The oldest message comes first, so the newest ones are at the end of the list.
    $count = [Math]::Min([Math]::Max((Get-QueueDepth $queue), 1), 500)
    $body = '{"count":' + $count + ',"ackmode":"ack_requeue_true","encoding":"auto","truncate":200}'
    # Assign first, then pipe: piping Invoke-RestMethod directly can hand the whole JSON array on as ONE object.
    $response = Invoke-RestMethod -Method Post -Uri "http://localhost:15672/api/queues/%2F/$queue/get" -Headers $auth -ContentType 'application/json' -Body $body
    $msgs = @($response | Where-Object { $_ })
    $rows = @(foreach ($m in $msgs) {
        $death = $null
        if ($m.properties.headers) { $death = $m.properties.headers.'x-death' }
        $reason = '?'
        if ($death) { $reason = @($death)[0].reason }
        [pscustomobject]@{ Reason = $reason; Payload = [string]$m.payload }
    })
    Write-Host ("  {0}: {1} message" -f $queue, $rows.Count)
    foreach ($g in ($rows | Group-Object Reason)) { Write-Host ("    reason={0}: {1}" -f $g.Name, $g.Count) }
    Write-Host '  3 message mới nhất (cũ hơn ở trên):'
    foreach ($r in ($rows | Select-Object -Last 3)) {
        $t = $r.Payload
        if ($t.Length -gt 90) { $t = $t.Substring(0, 90) + '...' }
        Write-Host ("    {0,-9} | {1}" -f $r.Reason, $t)
    }
}

function Get-TopicSize($topic) {
    $total = 0
    docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $topic 2>$null |
        ForEach-Object { $total += [long](($_ -split ':')[-1]) }
    return $total
}

function Wait-Final($orderId, [int]$limitMs = 30000) {
    $sw = [Diagnostics.Stopwatch]::StartNew()
    do {
        Start-Sleep -Milliseconds 300
        $o = (Get-Order $orderId).Body
    } until ($o.status -in 'ISSUED', 'PROCESSING_FAILED' -or $sw.ElapsedMilliseconds -gt $limitMs)
    return [pscustomobject]@{ Order = $o; Ms = $sw.ElapsedMilliseconds }
}

function Send-WarmUp {
    $w = Send-Order -Partner (New-PartnerId 'WARM') -Mode 'RABBITMQ_RPC'
    Write-Host ("  Đơn mồi Luồng 1: {0} {1} sau {2} ms" -f $w.Code, $w.Body.status, $w.Ms)
}

function Test-Readiness {
    foreach ($port in 8080, 8081, 8082) {
        try { $s = (Invoke-RestMethod "http://localhost:$port/actuator/health/readiness").status } catch { $s = 'KHÔNG TRẢ LỜI' }
        Write-Host ("  :{0} readiness = {1}" -f $port, $s)
    }
}

# ---------------------------------------------------------------- steps

function Step-0([switch]$Reset, [switch]$Build, [switch]$NoBrowser) {
    Write-Title 0 $script:Steps[0]
    docker info *> $null
    if ($LASTEXITCODE -ne 0) { Warn 'Docker Desktop chưa chạy. Mở Docker Desktop, chờ biểu tượng xanh rồi chạy lại: demo 0'; return }
    if ($Reset) {
        Warn 'Đang xóa container và volume (MySQL, dữ liệu Kafka và Redis trong container): docker compose down -v'
        Cmd 'docker compose down -v'; docker compose down -v 2>&1 | Select-Object -Last 1 | Write-Host
    }
    if ($Build) { Cmd 'docker compose up --build -d --wait'; docker compose up --build -d --wait 2>&1 | Select-Object -Last 1 | Write-Host }
    else { Cmd 'docker compose up -d --wait'; docker compose up -d --wait 2>&1 | Select-Object -Last 1 | Write-Host }
    Cmd 'docker compose ps'; docker compose ps --format 'table {{.Service}}\t{{.Status}}' | Write-Host
    Test-Readiness
    Send-WarmUp
    $w2 = Send-Order -Partner (New-PartnerId 'WARM') -Mode 'GRPC_KAFKA'
    Write-Host ("  Đơn mồi Luồng 2: {0} {1} sau {2} ms" -f $w2.Code, $w2.Body.status, $w2.Ms)
    if (-not $NoBrowser) {
        Start-Process 'http://localhost:3000'
        Start-Process 'http://localhost:15672'
    }
    Expect @('Docker Compose: 7 service đều (healthy), ui Up.', 'Readiness 8080, 8081, 8082 đều UP.', '2 đơn mồi: Luồng 1 = 200 ISSUED, Luồng 2 = 202 PAYMENT_RECORDED.')
    if ($NoBrowser) { Warn 'Mở tay: UI http://localhost:3000 và RabbitMQ http://localhost:15672 (user/pass trong .env).' }
    else { Warn 'Trang UI và RabbitMQ (user/pass trong .env) vừa được mở.' }
    Warn 'Nếu UI từng mở trước đó: bấm Ctrl+F5 một lần để trình duyệt tải app.js mới.'
    Warn 'Kafka UI (tùy chọn, tốn thêm RAM): docker compose --profile tools up -d kafka-ui  rồi mở http://localhost:8090'
}

function Step-1 {
    Write-Title 1 $script:Steps[1]
    Write-Host @'
    Web UI (nginx :3000) --/api--> Order Service :8080 --+-- Luồng 1: RabbitMQ RPC --> Payment :8081 / Policy :8082
                                        |                +-- Luồng 2: gRPC --> Payment :9090 --Kafka--> Policy --Kafka--> Order
                                        +-- Redis (chống trùng + cache đọc)
                                        +-- MySQL (order_db | payment_db | policy_db, mỗi service một schema)
'@
    Expect @('Đề: đối tác tạo đơn, ghi nhận thanh toán, phát hành hợp đồng, thông báo, cập nhật UI.', '3 service (Order, Payment, Policy) + UI + RabbitMQ + Kafka + Redis + log JSON có correlation_id.', 'Luồng 1 RabbitMQ RPC: HTTP chờ reply nên là ĐỒNG BỘ dù đi qua broker.', 'Luồng 2: gRPC tới Payment rồi trả 202; phát hành chạy BẤT ĐỒNG BỘ qua Kafka.', 'Khác đề có chủ đích (docs/design-decisions.md): Java 21 + Spring Boot (D9), MySQL (D10), cache xóa khi đổi trạng thái (D1), chặn trùng 409 theo lead (D21).')
}

function Step-2 {
    Write-Title 2 $script:Steps[2]
    Cmd 'docker compose ps'; docker compose ps --format 'table {{.Service}}\t{{.Status}}' | Write-Host
    Test-Readiness
    Expect @('1 lệnh docker compose up dựng cả hệ thống (thử từ bản clone sạch: 157 s).', '7 service (healthy), ui Up.', 'Readiness 8080, 8081, 8082 đều UP: mỗi service tự chạy thử (warm-up) rồi mới báo sẵn sàng, nên request đầu không bị chậm.')
}

function Step-3 {
    Write-Title 3 $script:Steps[3]
    Ui 'Chọn "Luồng 1: RabbitMQ RPC", bấm [Tạo Đơn & Phát Hành]. Xem timeline 4 bước "via RabbitMQ", Correlation ID, CACHE MISS (DB). Bấm F5: CACHE HIT (REDIS).'
    $p = New-PartnerId 'DEMO-L1'
    Cmd "POST $script:Api  mode=RABBITMQ_RPC partner_order_id=$p"
    $r = Send-Order -Partner $p -Mode 'RABBITMQ_RPC'
    Write-Host ("  HTTP {0} sau {1} ms | status={2} | policy_number={3} | correlation_id={4}" -f $r.Code, $r.Ms, $r.Body.status, $r.Body.policy_number, $r.Body.correlation_id)
    Show-Timeline $r.Body
    $g1 = Get-Order $r.Body.order_id; $g2 = Get-Order $r.Body.order_id
    Write-Host ("  GET lần 1: {0} | GET lần 2: {1} | header X-Correlation-Id={2}" -f $g1.Body.cache_status, $g2.Body.cache_status, $g2.CorrelationHeader)
    $script:Last = @{ OrderId = $r.Body.order_id; CorrelationId = $r.Body.correlation_id; Partner = $p }
    Expect @('HTTP 200 + ISSUED + số hợp đồng ACBI-2026-xxxxxx: request phải chờ xong cả Payment và Policy mới trả về (đồng bộ).', 'Timeline 4 bước, Payment và Policy đi via RabbitMQ.', 'GET lần 1 = CACHE_MISS_DB, lần 2 = CACHE_HIT_REDIS.', 'reply_to + correlation_id ghép reply với request; timeout mỗi bước 3 s.')
}

function Step-4 {
    Write-Title 4 $script:Steps[4]
    Ui 'Chọn "Luồng 2: gRPC + Kafka", bấm [Tạo Đơn & Phát Hành]. Thấy 202, "Đang chờ Kafka…", rồi ISSUED; Thanh toán via gRPC, Phát hành via Kafka.'
    $p = New-PartnerId 'DEMO-L2'
    Cmd "POST $script:Api  mode=GRPC_KAFKA partner_order_id=$p"
    $r = Send-Order -Partner $p -Mode 'GRPC_KAFKA'
    Write-Host ("  HTTP {0} sau {1} ms | status={2} | correlation_id={3}" -f $r.Code, $r.Ms, $r.Body.status, $r.Body.correlation_id)
    $f = Wait-Final $r.Body.order_id
    Write-Host ("  Polling: {0} sau {1} ms, policy_number={2}" -f $f.Order.status, $f.Ms, $f.Order.policy_number)
    Show-Timeline $f.Order
    $script:Last = @{ OrderId = $r.Body.order_id; CorrelationId = $r.Body.correlation_id; Partner = $p }
    Expect @('HTTP 202 PAYMENT_RECORDED ngay (~100 ms): Order chỉ chờ gRPC tới Payment, không chờ Policy.', 'Polling: ISSUED sau khoảng 0,3–1 s.', 'Timeline: PAYMENT via gRPC, POLICY_ISSUANCE via Kafka.', 'Đường đi: payment.recorded -> Policy -> policy.issued -> Order.')
}

function Step-5 {
    Write-Title 5 $script:Steps[5]
    Ui 'Sau khi tạo một đơn, bấm [Gửi lại Request trùng]: POST → 409 Conflict, nhãn "Request trùng, đã chặn (409 DUPLICATE_ORDER)", bên dưới là đơn gốc.'
    $p = New-PartnerId 'DEMO-DUP'
    $first = Send-Order -Partner $p -Mode 'RABBITMQ_RPC'
    Write-Host ("  Request đầu              : {0} {1} order_id={2}" -f $first.Code, $first.Body.status, $first.Body.order_id)
    $same = Send-Order -Partner $p -Mode 'RABBITMQ_RPC'
    Write-Host ("  Gửi lại y hệt            : {0} {1} order_id={2} order_status={3}" -f $same.Code, $same.Body.error, $same.Body.order_id, $same.Body.order_status)
    $other = Send-Order -Partner $p -Mode 'RABBITMQ_RPC' -Amount 999000
    Write-Host ("  Cùng mã, khác số tiền    : {0} {1} (có order_id: {2})" -f $other.Code, $other.Body.error, ($null -ne $other.Body.order_id))
    Add-Type -AssemblyName System.Net.Http
    $client = New-Object System.Net.Http.HttpClient
    $p2 = New-PartnerId 'DEMO-RACE'
    $body = '{"partner_order_id":"' + $p2 + '","customer_name":"Nguyen Van A","phone":"0901234567","amount":500000,"mode":"RABBITMQ_RPC"}'
    $tasks = 1..6 | ForEach-Object { $client.PostAsync($script:Api, (New-Object System.Net.Http.StringContent($body, [Text.Encoding]::UTF8, 'application/json'))) }
    [Threading.Tasks.Task]::WaitAll($tasks)
    $summary = $tasks | ForEach-Object { $j = $_.Result.Content.ReadAsStringAsync().Result | ConvertFrom-Json; ('{0} {1}' -f [int]$_.Result.StatusCode, $j.error).Trim() } |
        Group-Object | ForEach-Object { '{0} x [{1}]' -f $_.Count, $_.Name }
    Write-Host ("  6 request giống hệt cùng lúc: {0}" -f ($summary -join ', '))
    Invoke-Sql ("SELECT o.partner_order_id, COUNT(*) AS orders, (SELECT COUNT(*) FROM payment_db.payments p WHERE p.partner_transaction_id = CONCAT('TXN-', o.partner_order_id)) AS payments " +
        "FROM order_db.orders o WHERE o.partner_order_id IN ('$p', '$p2') GROUP BY o.partner_order_id;") | Write-Host
    Expect @('Request đầu: 200 ISSUED (request hợp lệ đầu tiên được giữ).', 'Gửi lại y hệt: 409 DUPLICATE_ORDER, có order_id của đơn gốc.', 'Cùng mã, khác số tiền: 409 DUPLICATE_ORDER_MISMATCH, không có order_id.', '6 request cùng lúc: 1 x 200 + 5 x 409 (ORDER_IN_PROGRESS hoặc DUPLICATE_ORDER).', 'SQL: mỗi mã chỉ 1 đơn, 1 thanh toán.', 'Hai lớp chặn: Redis SET NX (24 h), rồi UNIQUE(partner_order_id) trong MySQL.')
}

function Step-6 {
    Write-Title 6 $script:Steps[6]
    if (-not $script:Last.CorrelationId) { Warn 'Chưa có đơn nào trong phiên này, tạo 1 đơn Luồng 2 trước.'; Step-4 }
    $cid = $script:Last.CorrelationId
    Ui 'Trên UI: mở "Truy vết log của đơn này qua 3 service" để copy đúng lệnh bên dưới.'
    Cmd "docker compose logs --no-log-prefix order-service payment-service policy-service | Select-String '$cid' | ..."
    docker compose logs --no-log-prefix order-service payment-service policy-service |
        Select-String $cid | ForEach-Object { $_.Line | ConvertFrom-Json } | Sort-Object timestamp |
        Select-Object timestamp, service, transport, action, status, execution_time_ms | Format-Table -AutoSize | Out-String -Width 200 | Write-Host
    Cmd "docker compose exec redis redis-cli TTL idempotency:order:$($script:Last.Partner)"
    Write-Host ("  TTL idempotency:order:{0} = {1} s" -f $script:Last.Partner, (docker compose exec -T redis redis-cli TTL "idempotency:order:$($script:Last.Partner)"))
    $null = Get-Order $script:Last.OrderId
    Write-Host ("  TTL order:{0} = {1} s" -f $script:Last.OrderId, (docker compose exec -T redis redis-cli TTL "order:$($script:Last.OrderId)"))
    Expect @('Cùng 1 correlation_id ở cả order, payment, policy.', 'Đi qua HTTP, gRPC (hoặc header RabbitMQ) và envelope Kafka.', 'TTL key chống trùng ~86400 s, key cache ~600 s.')
}

function Step-7 {
    Write-Title 7 $script:Steps[7]
    $dlq = 'payment.rpc.request.dlq'
    $before = Get-QueueDepth $dlq
    Write-Host "  $dlq trước: $before"
    Cmd 'docker compose stop payment-service'; docker compose stop payment-service 2>&1 | Select-Object -Last 1 | Write-Host
    try {
        $l1 = Send-Order -Partner (New-PartnerId 'DEMO-TIMEOUT-L1') -Mode 'RABBITMQ_RPC'
        Write-Host ("  Luồng 1 khi Payment tắt: HTTP {0} sau {1} ms | {2} | {3}" -f $l1.Code, $l1.Ms, $l1.Body.status, $l1.Body.failure_reason)
        Start-Sleep -Seconds 2
        Write-Host ("  {0} sau: {1}" -f $dlq, (Get-QueueDepth $dlq))
        $l2 = Send-Order -Partner (New-PartnerId 'DEMO-TIMEOUT-L2') -Mode 'GRPC_KAFKA'
        $failed = $l2.Body.timeline | Where-Object step -eq 'PROCESSING_FAILED'
        Write-Host ("  Luồng 2 khi Payment tắt: HTTP {0} sau {1} ms | {2} | {3}" -f $l2.Code, $l2.Ms, $l2.Body.status, $failed.detail)
    }
    finally {
        Cmd 'docker compose up -d --wait payment-service'; docker compose up -d --wait payment-service 2>&1 | Select-Object -Last 1 | Write-Host
        Send-WarmUp
    }
    Expect @('Luồng 1: 200 PROCESSING_FAILED / PAYMENT_TIMEOUT sau ~3 s: HTTP không treo.', 'DLQ tăng đúng 1: request nằm quá TTL 3 s nên bị chuyển sang DLQ.')
    Expect @('Luồng 2: PROCESSING_FAILED, PAYMENT_SERVICE_UNAVAILABLE (nhanh) hoặc PAYMENT_TIMEOUT (~3 s). Cả hai đều đúng.', 'Bật lại Payment, đơn mồi ISSUED.', 'Giới hạn đã biết: Payment xử lý xong ngay sau timeout thì tiền đã trừ nhưng đơn báo lỗi (cần đối soát).')
    Warn 'Đơn mồi: request đầu tiên sau khi một service khởi động lại có thể mất gần 3 s, nên gửi 1 đơn trước khi demo tiếp.'
}

function Step-8 {
    Write-Title 8 $script:Steps[8]
    $dlq = 'payment.rpc.request.dlq'
    $user = Get-EnvValue 'RABBITMQ_USER'; $pass = Get-EnvValue 'RABBITMQ_PASSWORD'
    $auth = @{ Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${user}:${pass}")) }
    $before = Get-QueueDepth $dlq
    Write-Host "  $dlq trước: $before"
    $publish = '{"properties":{"content_type":"application/json","headers":{"x-correlation-id":"demo-poison"}},"routing_key":"payment.rpc.request","payload":"{not json","payload_encoding":"string"}'
    Cmd 'POST http://localhost:15672/api/exchanges/%2F/amq.default/publish  routing_key=payment.rpc.request payload="{not json"'
    $r = Invoke-RestMethod -Method Post -Uri 'http://localhost:15672/api/exchanges/%2F/amq.default/publish' -Headers $auth -ContentType 'application/json' -Body $publish
    Write-Host "  routed = $($r.routed)"
    Start-Sleep -Seconds 2
    Write-Host ("  {0} sau: {1}" -f $dlq, (Get-QueueDepth $dlq))
    docker compose logs --no-log-prefix --since 1m payment-service | Select-String 'Fatal message conversion error' | Select-Object -Last 1 |
        ForEach-Object { $o = $_.Line | ConvertFrom-Json; Write-Host ('  log payment-service: {0} {1}' -f $o.log_level, $o.message.Substring(0, [Math]::Min(110, $o.message.Length))) }
    Show-DlqMessages $dlq $auth
    Ui 'Trên RabbitMQ UI (http://localhost:15672) > Queues > payment.rpc.request.dlq > Get messages: đặt Messages = 10 (không phải 1: Get messages trả message CŨ NHẤT trước, tức message hết hạn của bước 7), giữ Ack mode "Nack message requeue true", kéo xuống message CUỐI để thấy payload "{not json".'
    Expect @('routed = True; DLQ tăng đúng 1.', 'Log WARN "Fatal message conversion error; message will be reject"; payment-service vẫn chạy bình thường.', 'Không requeue (default-requeue-rejected: false) nên message hỏng không làm kẹt queue.')
    Expect @('DLQ có 2 loại: expired = quá TTL 3 s (bước 7); rejected = listener từ chối vì không đọc được ("{not json", bước 8).')
}

function Step-9 {
    Write-Title 9 $script:Steps[9]
    $p = New-PartnerId 'DEMO-REPLAY'
    $a = Send-Order -Partner $p -Mode 'GRPC_KAFKA'; $id = $a.Body.order_id
    $f = Wait-Final $id
    Write-Host "  order_id=$id status=$($f.Order.status)"
    Cmd "kafka-console-consumer.sh --topic payment.recorded --from-beginning ... | Select-String $id"
    $ev = (docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic payment.recorded --from-beginning --formatter-property print.key=true --formatter-property 'key.separator=|' --timeout-ms 5000 2>$null | Select-String $id | Select-Object -First 1).Line
    Write-Host "  event: $ev"
    Cmd 'gửi lại event đó 3 lần (awk bỏ BOM mà PowerShell 5.1 chèn vào dòng đầu)'
    $ev, $ev, $ev | docker compose exec -T kafka sh -c "awk 'NR==1{sub(/^\357\273\277/,e)} {sub(/\r$/,e); print}' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payment.recorded --reader-property parse.key=true --reader-property key.separator='|'"
    Start-Sleep -Seconds 5
    docker compose logs --no-log-prefix --since 1m policy-service order-service | Select-String $id | ForEach-Object { $_.Line | ConvertFrom-Json } |
        Where-Object action -eq 'duplicate_event_ignored' | Sort-Object timestamp | Select-Object timestamp, service, action, order_id | Format-Table -AutoSize | Out-String -Width 200 | Write-Host
    Invoke-Sql "SELECT COUNT(*) AS policies FROM policy_db.policies WHERE order_id = '$id';" | Write-Host
    Write-Host ("  timeline: " + ((Get-Order $id).Body.timeline.step -join ', '))
    $dltBefore = Get-TopicSize 'payment.recorded.DLT'
    $key = New-PartnerId 'GARBAGE'
    Cmd "`"$key|{not json`" | kafka-console-producer.sh --topic payment.recorded ..."
    "$key|{not json" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payment.recorded --reader-property parse.key=true --reader-property key.separator="|"
    Start-Sleep -Seconds 3
    Write-Host ("  payment.recorded.DLT: {0} -> {1} message" -f $dltBefore, (Get-TopicSize 'payment.recorded.DLT'))
    Expect @('Gửi lại cùng event 3 lần: duplicate_event_ignored 3 lần ở policy-service và 3 lần ở order-service.', 'policies = 1, timeline vẫn 4 bước.', 'Vì sao: consumer_inbox (event_id) + UNIQUE(order_id) ghi cùng transaction; offset commit sau khi DB xong.', 'Message hỏng: payment.recorded.DLT tăng đúng 1.')
}

function Step-10 {
    Write-Title 10 $script:Steps[10]
    Cmd 'docker compose stop redis'; docker compose stop redis 2>&1 | Select-Object -Last 1 | Write-Host
    try {
        $p = New-PartnerId 'DEMO-NOREDIS'
        $a = Send-Order -Partner $p -Mode 'RABBITMQ_RPC'
        Write-Host ("  Tạo đơn khi Redis tắt: {0} {1} sau {2} ms" -f $a.Code, $a.Body.status, $a.Ms)
        $d = Send-Order -Partner $p -Mode 'RABBITMQ_RPC'
        Write-Host ("  Gửi lại y hệt: {0} {1}" -f $d.Code, $d.Body.error)
        Write-Host ("  /actuator/health: " + (Invoke-RestMethod http://localhost:8080/actuator/health).status)
        docker compose logs --no-log-prefix --since 1m order-service | Select-String 'redis_circuit_open|redis_unavailable|duplicate_order' |
            ForEach-Object { $o = $_.Line | ConvertFrom-Json; "  log: {0} {1}" -f $o.action, $o.idempotency_source } | Write-Host
    }
    finally {
        Cmd 'docker compose up -d --wait redis'; docker compose up -d --wait redis 2>&1 | Select-Object -Last 1 | Write-Host
    }
    Expect @('Redis tắt vẫn tạo đơn: 200 ISSUED.', 'Gửi lại y hệt vẫn 409 DUPLICATE_ORDER (MySQL chặn, idempotency_source=MYSQL).', 'Health UP; log redis_circuit_open 1 lần: bỏ qua Redis 5 s rồi dùng MySQL.')
}

function Step-11 {
    Write-Title 11 $script:Steps[11]
    Warn 'Phụ lục: 100 đơn Luồng 1 cùng lúc có thể tạo nhiều đơn PAYMENT_TIMEOUT ĐÃ BỊ TRỪ TIỀN (3 lần đo: 51/100, 0/100, 0/100). Chỉ chạy khi được hỏi.'
    Add-Type -AssemblyName System.Net.Http
    $client = New-Object System.Net.Http.HttpClient; $client.Timeout = [TimeSpan]::FromSeconds(90)
    foreach ($n in 20, 100) {
        foreach ($mode in 'RABBITMQ_RPC', 'GRPC_KAFKA') {
            $tag = New-PartnerId "LOAD$n-$mode"
            $sw = [Diagnostics.Stopwatch]::StartNew()
            $tasks = 1..$n | ForEach-Object {
                $b = '{"partner_order_id":"' + $tag + '-' + $_ + '","customer_name":"A","phone":"0901234567","amount":500000,"mode":"' + $mode + '"}'
                $client.PostAsync($script:Api, (New-Object System.Net.Http.StringContent($b, [Text.Encoding]::UTF8, 'application/json')))
            }
            $done = @{}
            while ($done.Count -lt $n) { for ($i = 0; $i -lt $n; $i++) { if (-not $done.ContainsKey($i) -and $tasks[$i].IsCompleted) { $done[$i] = $sw.ElapsedMilliseconds } }; Start-Sleep -Milliseconds 10 }
            $ms = @($done.Values | Sort-Object)
            $res = @($tasks | ForEach-Object { $_.Result.Content.ReadAsStringAsync().Result | ConvertFrom-Json })
            $status = ($res | Group-Object status | ForEach-Object { '{0}:{1}' -f $_.Name, $_.Count }) -join ' '
            $reasons = (@($res | Where-Object failure_reason) | Group-Object failure_reason | ForEach-Object { '{0}:{1}' -f $_.Name, $_.Count }) -join ' '
            Write-Host ("  {0,3} x {1,-12}: {2} | {3} | p50={4} ms max={5} ms" -f $n, $mode, $status, $reasons, $ms[[int]($n / 2)], $ms[-1])
            if ($mode -eq 'GRPC_KAFKA') {
                # The 202s only mean "payment recorded": count how many orders Kafka has carried to ISSUED.
                $wait = [Diagnostics.Stopwatch]::StartNew()
                do {
                    Start-Sleep -Seconds 1
                    $issued = [int](("SELECT COUNT(*) FROM order_db.orders WHERE partner_order_id LIKE '$tag-%' AND status = 'ISSUED';" |
                        docker compose exec -T mysql sh -c 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N 2>/dev/null') | Select-Object -Last 1)
                } until ($issued -ge $n -or $wait.Elapsed.TotalSeconds -gt 60)
                Write-Host ("        Luồng 2 qua Kafka: {0}/{1} ISSUED sau {2:N0} s" -f $issued, $n, $wait.Elapsed.TotalSeconds)
            }
        }
    }
    Expect @('20 đơn: cả 2 luồng đều ISSUED hết.', '100 đơn Luồng 1: chậm rõ (p50 ~2–4,5 s so với ~90 ms khi gửi lẻ), có thể có PAYMENT_TIMEOUT tùy máy (đo 28/9: 51/100, 0/100, 0/100).', '100 đơn Luồng 2: 202 hết, phát hành đủ 100/100.', 'Vì sao: mỗi queue RPC có 1 consumer nên request xếp hàng, quá 3 s là timeout; Kafka giữ event và xử lý dần.')
}

function Step-12 {
    Write-Title 12 $script:Steps[12]
    $qa = @(
        @('1. Vì sao Luồng 1 qua broker mà vẫn đồng bộ? Khác Luồng 2 thế nào?',
          'Thread HTTP chờ reply (convertSendAndReceive, timeout 3 s) cho từng bước; Luồng 2 trả 202 sau gRPC, phần còn lại chạy qua event Kafka.',
          'RabbitRpcOrderFlow.java, GrpcKafkaOrderFlow.java, README bảng so sánh; demo 3, 4, 11'),
        @('2. reply_to và correlation_id làm gì? Payment xử lý 10 s thì sao?',
          'reply_to = nơi Payment gửi trả lời (Direct Reply-to); correlation_id ghép reply với request. Payment 10 s: Order timeout sau 3 s, trả PROCESSING_FAILED, reply muộn bị bỏ và log late_reply_ignored, request quá TTL vào DLQ; tiền có thể đã bị trừ (giới hạn đã biết).',
          'OrderRpcClient.java, application.yml (reply-timeout 3000), RabbitConfig (late_reply_ignored), test paymentTimeoutFailsOrderWithinThreeAndAHalfSeconds; demo 7'),
        @('3. Vì sao gRPC thay cho REST? HTTP/2 và Protobuf được gì?',
          'Hợp đồng kiểu mạnh sinh code cho cả 2 bên (không lệch field), message nhị phân nhỏ, kết nối HTTP/2 dùng lại, deadline có sẵn cho từng lời gọi.',
          'contracts/payment.proto, PaymentGrpcClient.java (withDeadlineAfter 3000 ms), module contracts dùng chung (D17)'),
        @('4. Consumer sập sau khi ghi DB nhưng chưa commit offset: làm sao không tạo 2 hợp đồng?',
          'Event được gửi lại (at-least-once). Policy ghi consumer_inbox (event_id) và hợp đồng trong cùng transaction, có UNIQUE(order_id): lần 2 bị chặn, log duplicate_event_ignored.',
          'policy V1__init.sql (uk_policies_order_id, consumer_inbox), PolicyIssuer.java, ack-mode record, PaymentRecordedListenerIntegrationTests; demo 9'),
        @('5. Redis dùng ở đâu? Truy vết một request lỗi qua 3 service thế nào?',
          'Key chống trùng idempotency:order:{partner_order_id} (24 h) và cache đọc order:{order_id} (10 phút, GET đọc Redis trước). Truy vết: grep correlation_id trong log JSON của 3 service.',
          'IdempotencyStore.java, OrderReadCache.java, README "Demo trên Web UI"; demo 3, 5, 6')
    )
    foreach ($q in $qa) {
        Write-Host ''
        Write-Host ('  ' + $q[0]) -ForegroundColor Yellow
        Say $q[1]
        Write-Host ('  BẰNG CHỨNG: ' + $q[2]) -ForegroundColor DarkGray
    }
}

# ---------------------------------------------------------------- entry point

function demo {
    param([string]$Step, [switch]$Reset, [switch]$Build, [switch]$NoBrowser, [switch]$IncludeOptional, [switch]$NoPause)
    Push-Location $script:DemoRoot
    try {
        if (-not $Step) {
            Write-Host 'Các bước (chạy: demo <số>, hoặc demo all để tập dượt):' -ForegroundColor Yellow
            foreach ($k in $script:Steps.Keys) { Write-Host ('  {0,2}  {1}' -f $k, $script:Steps[$k]) }
            Write-Host '  demo 0 -Reset xóa dữ liệu cũ trước khi bật; demo 0 -Build build lại image; demo 0 -NoBrowser không mở trình duyệt.' -ForegroundColor DarkGray
            return
        }
        if ($Step -eq 'all') {
            $order = @(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
            if ($IncludeOptional) { $order += 10, 11 }
            $order += 12
            foreach ($n in $order) {
                if ($n -eq 0) { Step-0 -Reset:$Reset -Build:$Build -NoBrowser:$NoBrowser } else { & "Step-$n" }
                if (-not $NoPause -and $n -ne $order[-1]) {
                    $answer = Read-Host "`nEnter: sang bước tiếp theo | q: dừng"
                    if ($answer -eq 'q') { break }
                }
            }
            return
        }
        $n = [int]$Step
        if (-not $script:Steps.Contains($n)) { Write-Host "Không có bước $Step. Gõ demo để xem danh sách." -ForegroundColor Red; return }
        if ($n -eq 0) { Step-0 -Reset:$Reset -Build:$Build -NoBrowser:$NoBrowser } else { & "Step-$n" }
    }
    finally {
        Pop-Location
    }
}

Write-Host 'Đã nạp script demo. Gõ: demo' -ForegroundColor Green
