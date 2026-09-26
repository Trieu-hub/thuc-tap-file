# Runs the full Maven build with all tests, the reliable way (Windows PowerShell 5.1+).
#
#   powershell -ExecutionPolicy Bypass -File scripts\run-tests.ps1
#
# Why a script: the integration tests start their own MySQL, RabbitMQ and Kafka with
# Testcontainers. With the docker compose sandbox running at the same time, Docker Desktop is
# short of CPU and memory, and a timing test (3 s gRPC deadline answered within 2.9-3.5 s) failed
# once with a 3.8 s stall in MySQL. So the sandbox is stopped first and started again at the end
# if it was running. The full output goes to target\run-tests.log; only the summary is printed.

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$log = Join-Path $root 'target\run-tests.log'
New-Item -ItemType Directory -Force (Split-Path $log) | Out-Null

$wasRunning = [bool](docker compose ps --status running --quiet)
if ($wasRunning) {
	Write-Host 'Stopping the docker compose sandbox during the tests...'
	docker compose stop 2>$null | Out-Null
}

Write-Host "Running mvn -B package (about 8-9 min). Full log: $log"
$watch = [Diagnostics.Stopwatch]::StartNew()
mvn -B -ntp package *> $log
$exitCode = $LASTEXITCODE

Select-String -Path $log -Pattern '^\[INFO\] Building (\S+)', '^\[(INFO|ERROR|WARNING)\] Tests run: \d+, Failures: \d+, Errors: \d+, Skipped: \d+$', '<<< (FAILURE|ERROR)!', '^\[INFO\] BUILD \w+' |
	ForEach-Object { $_.Line }
Write-Host ('Finished in {0:N1} min, exit code {1}' -f $watch.Elapsed.TotalMinutes, $exitCode)

if ($wasRunning) {
	Write-Host 'Starting the sandbox again...'
	docker compose up -d --wait 2>$null | Out-Null
	docker compose ps --format '{{.Service}}  {{.Status}}'
}
exit $exitCode
