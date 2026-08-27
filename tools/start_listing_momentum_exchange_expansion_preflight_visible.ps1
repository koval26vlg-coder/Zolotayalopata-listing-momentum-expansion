param(
    [string]$OutputPath = "",
    [switch]$Check,
    [switch]$Json
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if (-not $OutputPath) {
    $OutputPath = Join-Path $repoRoot "exports\trading-mvp\analysis\listing_momentum_exchange_expansion_preflight_20260817.json"
}
$modulePath = Join-Path $repoRoot "trading_mvp\src\listing_momentum_exchange_expansion.py"
$gateChecker = Join-Path $repoRoot "tools\check_active_run_gate.ps1"

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

$gate = & pwsh -NoProfile -ExecutionPolicy Bypass -File $gateChecker -Json | ConvertFrom-Json
if ([string]$gate.gate_status -in @("RUNNING", "STOPPED_INCOMPLETE")) {
    $blocked = [ordered]@{
        status = "BLOCKED_ACTIVE_RUN_GATE"
        gate_status = [string]$gate.gate_status
        output_path = $OutputPath
    }
    if ($Json) { $blocked | ConvertTo-Json -Depth 6 } else { $blocked | Format-List }
    exit 3
}

$pythonExe = Resolve-PythonExecutable
$args = @($modulePath)
if ($Check) {
    $args += @("--check", "--output", $OutputPath)
} else {
    $args += @("--output", $OutputPath)
}
$output = & $pythonExe @args 2>&1 | Out-String
$exitCode = $LASTEXITCODE
if ($Json) {
    $output.Trim()
} else {
    Write-Host "=== Listing Momentum expansion public preflight (visible) ===" -ForegroundColor Cyan
    Write-Host ("gate: " + [string]$gate.gate_status)
    Write-Host $output.Trim()
}
exit $exitCode
