param(
    [string]$PlanPath = "",
    [switch]$Status,
    [switch]$Reconcile,
    [switch]$DryRun,
    [switch]$Tick,
    [switch]$ScheduledTick,
    [switch]$Json
)

# Visible launcher for the listing-expansion automation wrapper.
#
# Two audiences, and they want opposite things. A person running this by hand wants to
# watch the tick happen and get its result. The due coordinator wants to be told in one
# breath whether a worker is already alive, whether the tick is not due after all, or
# whether one may be started - and if started, the pid of a visible process it can wait
# on. It then waits for that pid itself and checks the state the worker left behind.
#
# So -ScheduledTick answers and returns; it does not run the tick in this process. Every
# other switch runs here, where the person can see it.
#
# The script stays thin on purpose. Its predecessor carried eleven hundred lines that
# reimplemented scheduler state, claim ownership, liveness and the cadence ladder - all
# of which now live in tested Python bound by the wrapper's PlanOnly file. The one thing
# expressed here and nowhere else is the coordinator's handshake, because it is a
# property of the caller rather than of the automation.

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runner = Join-Path $repoRoot "trading_mvp\src\listing_expansion_automation.py"
$defaultPlanPath = Join-Path $repoRoot "docs\plans\listing-momentum-expansion-automation-planonly-20260826-v4.json"
if (-not $PlanPath) { $PlanPath = $defaultPlanPath }

if (-not (Test-Path -LiteralPath $runner)) { throw "expansion automation runner not found: $runner" }
if (-not (Test-Path -LiteralPath $PlanPath)) { throw "expansion automation plan not found: $PlanPath" }

function Resolve-PythonExecutable {
    # An interpreter taken from PATH is chosen by whatever happens to be earliest on it,
    # which is not a decision this automation should delegate (CWE-426). PYTHON_EXE is
    # honoured because the scheduler sets it deliberately; everything else is absolute.
    if ($env:PYTHON_EXE -and (Test-Path -LiteralPath $env:PYTHON_EXE)) { return $env:PYTHON_EXE }
    foreach ($candidate in @(
        "C:\Program Files\Python313\python.exe",
        "C:\Users\koval\AppData\Local\Programs\Python\Python313\python.exe"
    )) {
        if (Test-Path -LiteralPath $candidate) { return $candidate }
    }
    throw "Python executable not found; set PYTHON_EXE to an absolute path"
}

$selected = @($Status, $Reconcile, $DryRun, $Tick, $ScheduledTick) | Where-Object { $_ }
if ($selected.Count -ne 1) {
    throw "choose exactly one of -Status, -Reconcile, -DryRun, -Tick, -ScheduledTick"
}

$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"
$pythonExe = Resolve-PythonExecutable

if ($ScheduledTick) {
    # Ask the wrapper what may be done, without letting the asking do it. The probe
    # starts nothing and creates no claim; that is the whole reason it exists rather
    # than this script reading the state files itself.
    $probeRaw = & $pythonExe $runner --plan $PlanPath --launch-probe 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        Write-Output (([ordered]@{
            status = "PROBE_FAILED"
            reason = $probeRaw.Trim()
        }) | ConvertTo-Json -Depth 6 -Compress)
        exit 1
    }
    try { $probe = $probeRaw | ConvertFrom-Json -ErrorAction Stop } catch {
        Write-Output (([ordered]@{ status = "PROBE_UNREADABLE"; reason = $probeRaw.Trim() }) |
            ConvertTo-Json -Depth 6 -Compress)
        exit 1
    }

    switch ([string]$probe.verdict) {
        "NOT_DUE" {
            Write-Output (([ordered]@{
                status = "NOT_DUE"
                plan_id = [string]$probe.plan_id
                next_interval_at_utc = [string]$probe.next_interval_at_utc
                execution_performed = $false
            }) | ConvertTo-Json -Depth 6 -Compress)
            exit 0
        }
        "ALREADY_RUNNING" {
            Write-Output (([ordered]@{
                status = "ALREADY_RUNNING"
                worker_pid = [int]$probe.worker_pid
                plan_id = [string]$probe.plan_id
                execution_performed = $false
            }) | ConvertTo-Json -Depth 6 -Compress)
            exit 0
        }
        "UNRESOLVED" {
            # An unbound handoff is a question for a person, not a branch to pick. A
            # non-zero exit is the only answer that cannot be mistaken for permission.
            Write-Output (([ordered]@{
                status = "CLAIM_UNRESOLVED"
                attempt_id = [string]$probe.attempt_id
                plan_id = [string]$probe.plan_id
                execution_performed = $false
            }) | ConvertTo-Json -Depth 6 -Compress)
            exit 1
        }
        "LAUNCH" { }
        default {
            Write-Output (([ordered]@{
                status = "UNRECOGNISED_PROBE_VERDICT"
                verdict = [string]$probe.verdict
            }) | ConvertTo-Json -Depth 6 -Compress)
            exit 1
        }
    }

    # A visible window, because the discipline here is that collection is watchable while
    # it happens. The coordinator waits on this pid and then reads the state the worker
    # wrote, so this process must return now rather than when the tick finishes.
    $worker = Start-Process -FilePath $pythonExe `
        -ArgumentList @($runner, "--plan", $PlanPath, "--tick") `
        -WorkingDirectory $repoRoot -WindowStyle Normal -PassThru
    if (-not $worker -or -not $worker.Id) {
        Write-Output (([ordered]@{ status = "WORKER_NOT_STARTED" }) | ConvertTo-Json -Compress)
        exit 1
    }
    Write-Output (([ordered]@{
        status = "VISIBLE_TERMINAL_LAUNCHED"
        visible_terminal_pid = [int]$worker.Id
        plan_id = [string]$probe.plan_id
        plan_hash = [string]$probe.plan_hash
        plan_path = $PlanPath
        started_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        execution_performed = $true
    }) | ConvertTo-Json -Depth 6 -Compress)
    exit 0
}

$action = if ($Status) { "--status" }
    elseif ($Reconcile) { "--reconcile" }
    elseif ($DryRun) { "--dry-run" }
    else { "--tick" }

if (-not $Json) {
    Write-Host "=== Listing expansion automation (visible) ===" -ForegroundColor Cyan
    Write-Host ("plan:   " + $PlanPath)
    Write-Host ("action: " + $action)
    Write-Host ""
}

Push-Location $repoRoot
try {
    & $pythonExe $runner --plan $PlanPath $action
    $exitCode = [int]$LASTEXITCODE
} finally {
    Pop-Location
}

if (-not $Json) {
    Write-Host ""
    if ($exitCode -eq 0) {
        Write-Host "wrapper exited 0" -ForegroundColor Green
    } else {
        # A non-zero exit here means the wake was refused or left open on purpose.
        # It is not the same as a failed tick, and it must not be retried by hand
        # without reading the reason the wrapper printed.
        Write-Host ("wrapper exited " + $exitCode) -ForegroundColor Yellow
    }
}

exit $exitCode
