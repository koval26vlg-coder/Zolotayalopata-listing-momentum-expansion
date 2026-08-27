param(
    [string]$PlanPath = "",
    [switch]$PreflightOnly,
    [switch]$Status,
    [switch]$Json,
    [switch]$VisibleWorker,
    [switch]$ScheduledTick
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$defaultPlanPath = Join-Path $repoRoot "docs\plans\slow-liquidity-listing-momentum-forward-expansion-planonly-20260826-v14.json"
if (-not $PlanPath) { $PlanPath = $defaultPlanPath }
$gateChecker = Join-Path $repoRoot "tools\check_active_run_gate.ps1"
$monitorPy = Join-Path $repoRoot "trading_mvp\src\slow_liquidity_listing_momentum_forward_expansion_monitor.py"
$claimPath = Join-Path $repoRoot "docs\agent-log\active-market-data-writer-claim.json"
$claimArchiveDir = Join-Path $repoRoot "docs\agent-log\global-writer-claim-archive"
$claimManagerPy = Join-Path $repoRoot "trading_mvp\src\global_market_writer_claim.py"
$legacyClaimPath = Join-Path $repoRoot "docs\agent-log\active-market-data-writer-expansion-claim.json"
$launchRecordPath = Join-Path $repoRoot "docs\agent-log\run-gates\listing_momentum_forward_expansion.launch.json"
$pythonHandoffDir = Join-Path $repoRoot "docs\agent-log\run-gates\python-worker-handoffs"
$automationId = "zolotyaylopata-listing-momentum-forward-expansion"

function Resolve-PythonExecutable {
    $candidates = [System.Collections.Generic.List[string]]::new()
    if ($env:PYTHON_EXE) { $candidates.Add($env:PYTHON_EXE) }
    foreach ($commandName in @("python.exe", "python", "py.exe")) {
        $command = Get-Command $commandName -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($command -and $command.Source) { $candidates.Add($command.Source) }
    }
    $candidates.Add("C:\Program Files\Python313\python.exe")
    $candidates.Add("C:\Users\koval\AppData\Local\Programs\Python\Python313\python.exe")
    foreach ($candidate in $candidates | Select-Object -Unique) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) { return $candidate }
    }
    throw "Python executable not found; set PYTHON_EXE or install Python 3.13"
}

function Get-FileSha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-TextSha256([string]$Value) {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Value)
    $hasher = [System.Security.Cryptography.SHA256]::Create()
    try { return [Convert]::ToHexString($hasher.ComputeHash($bytes)).ToLowerInvariant() } finally { $hasher.Dispose() }
}

function Read-JsonFile([string]$Path) {
    return Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
}

function Write-JsonFile([string]$Path, $Payload) {
    $dir = Split-Path -Parent $Path
    if ($dir -and -not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    $Payload | ConvertTo-Json -Depth 16 | Set-Content -LiteralPath $Path -Encoding UTF8
}

function New-SpotPythonWorkerHandoffReceipt($Plan, [string]$TickId) {
    $handoffToken = [guid]::NewGuid().ToString("N").ToLowerInvariant()
    $claimToken = [guid]::NewGuid().ToString("N").ToLowerInvariant()
    $receiptPath = Join-Path $pythonHandoffDir ($TickId + ".json")
    $wrapperStartedAt = (Get-Process -Id $PID -ErrorAction Stop).StartTime.ToUniversalTime().ToString("o")
    $tickOutput = [System.IO.Path]::GetFullPath((Join-Path ([string]$Plan.tick.tick_output_root) $TickId))
    Write-JsonFile -Path $receiptPath -Payload ([ordered]@{
        schema = "trading_mvp_market_data_worker_handoff_v1"
        status = "ISSUED"
        project = "trading_mvp"
        automation_id = $automationId
        attempt_id = $TickId
        plan_hash = [string]$Plan.plan_hash
        wrapper_pid = $PID
        wrapper_process_started_at_utc = $wrapperStartedAt
        handoff_token_sha256 = Get-TextSha256 $handoffToken
        claim_run_id = ([string]$Plan.plan_id + "__" + $TickId)
        claim_owner_kind = "listing_momentum_forward_expansion_monitor_tick"
        claim_owner_pid = $null
        claim_owner_process_started_at_utc = $null
        claim_ownership_token_sha256 = Get-TextSha256 $claimToken
        claim_output_namespace = $tickOutput
        claim_must_exist = $false
        issued_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    })
    return [ordered]@{ handoff_token = $handoffToken; claim_token = $claimToken; receipt_path = $receiptPath }
}

function Resolve-CanonicalWriterClaim([switch]$AllowRecovery) {
    if (-not (Test-Path -LiteralPath $claimPath)) {
        return [ordered]@{ status = "ABSENT"; reason = "claim_absent" }
    }
    if (-not $AllowRecovery) {
        return [ordered]@{ status = "PRESERVED"; reason = "recovery_not_authorized_for_read_only_preflight" }
    }
    try {
        $pythonExe = Resolve-PythonExecutable
        $output = & $pythonExe $claimManagerPy recover-stale --path $claimPath --archive-dir $claimArchiveDir 2>&1 | Out-String
        if ($LASTEXITCODE -ne 0) { throw "recover-stale exited $LASTEXITCODE`: $($output.Trim())" }
        $result = $output.Trim() | ConvertFrom-Json -DateKind String
        if ([string]$result.schema -ne "trading_mvp_global_market_writer_claim_recovery_v1") { throw "recovery schema mismatch" }
        if ([string]$result.status -notin @("ABSENT", "LIVE_PRESERVED", "STALE_RECOVERED", "BLOCKED")) { throw "unexpected recovery status" }
        return $result
    } catch {
        return [ordered]@{ status = "BLOCKED"; reason = ("claim_recovery_failed: " + $_.Exception.Message) }
    }
}

function Invoke-ActiveRunGateCheck {
    try {
        $gateText = & pwsh -NoProfile -ExecutionPolicy Bypass -File $gateChecker -Json 2>&1 | Out-String
        $gateExitCode = $LASTEXITCODE
        if ($gateExitCode -ne 0) { throw "checker exited $gateExitCode" }
        if ([string]::IsNullOrWhiteSpace($gateText)) { throw "checker returned empty output" }
        $gate = $gateText.Trim() | ConvertFrom-Json -DateKind String
        if (-not ($gate.PSObject.Properties.Name -contains "status")) { throw "authoritative status is missing" }
        $authoritativeStatus = [string]$gate.status
        if ($authoritativeStatus -cne "READY_FOR_POSTPROCESS") {
            return [ordered]@{ ok = $false; reason = "active_run_gate_$authoritativeStatus"; payload = $gate }
        }
        return [ordered]@{ ok = $true; reason = $null; payload = $gate }
    } catch {
        return [ordered]@{ ok = $false; reason = "active_run_gate_invalid: $($_.Exception.Message)"; payload = $null }
    }
}

function Invoke-Preflight([switch]$AllowClaimRecovery) {
    if (-not (Test-Path -LiteralPath $PlanPath)) { throw "Plan file not found: $PlanPath" }
    $plan = Read-JsonFile -Path $PlanPath
    $pythonExe = Resolve-PythonExecutable
    $planCheckOutput = & $pythonExe $monitorPy --plan $PlanPath --plan-check 2>&1 | Out-String
    $planCheckExitCode = $LASTEXITCODE
    if ($planCheckExitCode -ne 0) { throw "Plan validator exited $planCheckExitCode`: $($planCheckOutput.Trim())" }
    if ([string]::IsNullOrWhiteSpace($planCheckOutput)) { throw "Plan validator returned empty output" }
    $planCheck = $planCheckOutput.Trim() | ConvertFrom-Json -ErrorAction Stop
    foreach ($required in @("status", "plan_hash", "max_runtime_sec", "tick_output_root")) {
        if (-not ($planCheck.PSObject.Properties.Name -contains $required)) { throw "Plan validator payload missing $required" }
    }
    if ([string]$planCheck.status -cne "PLAN_OK") { throw "Plan validator status is not PLAN_OK" }
    if ([string]$planCheck.plan_hash -cne [string]$plan.plan_hash) { throw "Plan validator plan_hash mismatch" }
    $reasons = [System.Collections.Generic.List[string]]::new()
    if ([string]$planCheck.status -ne "PLAN_OK") { $reasons.Add("plan_check_not_ok") }
    $gateCheck = Invoke-ActiveRunGateCheck
    $gate = $gateCheck.payload
    if (-not [bool]$gateCheck.ok) { $reasons.Add([string]$gateCheck.reason) }
    $claimRecovery = Resolve-CanonicalWriterClaim -AllowRecovery:($AllowClaimRecovery -and $reasons.Count -eq 0)
    if ([string]$claimRecovery.status -notin @("ABSENT", "STALE_RECOVERED") -or (Test-Path -LiteralPath $claimPath)) { $reasons.Add("expansion_writer_claim_exists") }
    if (Test-Path -LiteralPath $legacyClaimPath) { $reasons.Add("legacy_expansion_writer_claim_exists") }
    return [ordered]@{
        ok = ($reasons.Count -eq 0)
        reasons = $reasons
        plan_id = [string]$plan.plan_id
        plan_hash = [string]$plan.plan_hash
        plan_file_sha256 = Get-FileSha256 $PlanPath
        plan_check_status = [string]$planCheck.status
        gate_status = [string]$gate.gate_status
        gate_authoritative_status = [string]$gate.status
        max_runtime_sec = [int]$plan.tick.max_runtime_sec
        tick_output_root = [string]$plan.tick.tick_output_root
        global_writer_claim_recovery = $claimRecovery
        status_command = "pwsh -NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`" -Status -Json"
    }
}

if ($VisibleWorker -and (-not $ScheduledTick -or $PreflightOnly -or $Status)) {
    $payload = [ordered]@{
        ok = $false
        status = "BLOCKED"
        reasons = @("VisibleWorker_requires_exclusive_ScheduledTick_execution")
    }
    if ($Json) { $payload | ConvertTo-Json -Depth 6 } else {
        Write-Host "VisibleWorker requires exclusive ScheduledTick execution" -ForegroundColor Red
    }
    exit 1
}

if ($Status) {
    $pythonExe = Resolve-PythonExecutable
    $statusOutput = & $pythonExe $monitorPy --status 2>&1 | Out-String
    if ($Json) { $statusOutput.Trim() } else {
        Write-Host "=== Listing Momentum expansion monitor status ===" -ForegroundColor Cyan
        Write-Host $statusOutput.Trim()
    }
    exit 0
}

if (-not $ScheduledTick -and -not $PreflightOnly) {
    $payload = [ordered]@{
        ok = $false
        status = "BLOCKED"
        reasons = @("ScheduledTick_required_for_execution")
    }
    if ($Json) { $payload | ConvertTo-Json -Depth 6 } else {
        Write-Host "expansion execution requires ScheduledTick handoff" -ForegroundColor Red
    }
    exit 1
}

if ($VisibleWorker) {
    $preflight = Invoke-Preflight -AllowClaimRecovery:$ScheduledTick
    if (-not $preflight.ok) {
        if ($Json) { $preflight | ConvertTo-Json -Depth 8 } else {
            Write-Host "expansion visible worker preflight failed:" -ForegroundColor Red
            Write-Host ($preflight.reasons -join ", ")
        }
        exit 1
    }
    $plan = Read-JsonFile -Path $PlanPath
    $tickId = "expansion_tick_" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffffffZ")
    $pythonHandoff = New-SpotPythonWorkerHandoffReceipt -Plan $plan -TickId $tickId
    $started = (Get-Date).ToUniversalTime().ToString("o")
    Write-JsonFile -Path $launchRecordPath -Payload ([ordered]@{
        schema = "trading_mvp_listing_momentum_forward_expansion_launch_v1"
        status = "RUNNING"
        run_id = [string]$plan.plan_id
        visible_terminal_pid = $PID
        started_at_utc = $started
        command = "python trading_mvp/src/slow_liquidity_listing_momentum_forward_expansion_monitor.py --plan `"$PlanPath`" --tick --confirmed-visible-tick"
        cwd = $repoRoot
        plan_path = $PlanPath
        plan_hash = [string]$plan.plan_hash
        plan_file_sha256 = Get-FileSha256 $PlanPath
        output_root = [string]$plan.tick.tick_output_root
        state_path = [string]$plan.tick.state_path
        expected_duration_sec = [int]$plan.tick.max_runtime_sec
        research_only = $true
        public_data_only = $true
    })
    Write-Host "=== Listing Momentum expansion tick (visible) ===" -ForegroundColor Cyan
    Write-Host ("plan_hash: " + [string]$plan.plan_hash)
    Write-Host ("expected max runtime: " + [int]$plan.tick.max_runtime_sec + " sec")
    $env:PYTHONIOENCODING = "utf-8"
    $env:PYTHONUTF8 = "1"
    $pythonExe = Resolve-PythonExecutable
    $exitCode = 1
    $workerError = $null
    try {
        & $pythonExe $monitorPy --plan $PlanPath --tick --confirmed-visible-tick --tick-id $tickId --worker-handoff-token ([string]$pythonHandoff.handoff_token) --claim-ownership-token ([string]$pythonHandoff.claim_token) --plan-hash ([string]$plan.plan_hash)
        $exitCode = [int]$LASTEXITCODE
    } catch {
        $workerError = $_.Exception.Message
        $exitCode = 1
    }
    $launch = Read-JsonFile -Path $launchRecordPath
    $launch.status = if ($exitCode -eq 0) { "COMPLETE" } else { "FAILED" }
    $launch | Add-Member -NotePropertyName finished_at_utc -NotePropertyValue (Get-Date).ToUniversalTime().ToString("o") -Force
    $launch | Add-Member -NotePropertyName tick_exit_code -NotePropertyValue $exitCode -Force
    if ($workerError) {
        $launch | Add-Member -NotePropertyName error -NotePropertyValue $workerError -Force
    }
    Write-JsonFile -Path $launchRecordPath -Payload $launch
    Write-Host ("tick exit code: " + $exitCode)
    exit $exitCode
}

$preflight = Invoke-Preflight -AllowClaimRecovery:$ScheduledTick
if ($PreflightOnly) {
    if ($Json) { $preflight | ConvertTo-Json -Depth 8 } else {
        Write-Host "=== Listing Momentum expansion tick preflight ===" -ForegroundColor Cyan
        Write-Host ("ok: " + $preflight.ok)
        Write-Host ("gate: " + $preflight.gate_status)
        if ($preflight.reasons.Count -gt 0) { Write-Host ("reasons: " + ($preflight.reasons -join ", ")) }
    }
    exit 0
}

if (-not $preflight.ok) {
    if ($Json) { $preflight | ConvertTo-Json -Depth 8 } else {
        Write-Host "expansion tick preflight failed:" -ForegroundColor Red
        Write-Host ($preflight.reasons -join ", ")
    }
    exit 1
}

$pwshExe = (Get-Process -Id $PID).Path
$childArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`" -VisibleWorker -ScheduledTick -PlanPath `"$PlanPath`""
$terminal = Start-Process -FilePath $pwshExe -ArgumentList $childArgs -WorkingDirectory $repoRoot -WindowStyle Normal -PassThru
$payload = [ordered]@{
    status = "VISIBLE_TERMINAL_LAUNCHED"
    run_id = $preflight.plan_id
    visible_terminal_pid = $terminal.Id
    plan_hash = $preflight.plan_hash
    expected_duration_sec = $preflight.max_runtime_sec
    output_root = $preflight.tick_output_root
    status_command = $preflight.status_command
    launch_record_path = $launchRecordPath
}
if ($Json) { $payload | ConvertTo-Json -Depth 8 } else {
    Write-Host ("visible expansion tick terminal launched (pid " + $terminal.Id + ")") -ForegroundColor Green
    Write-Host ("status: " + $preflight.status_command)
}
exit 0
