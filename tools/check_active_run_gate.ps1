param(
    [string]$GatePath = "C:\Users\koval\Documents\ZolotyayLopata\docs\agent-log\active-run-gate.json",
    [switch]$Json,
    [switch]$OfflineWork,
    [string[]]$ReadResourcePath = @(),
    [string[]]$WriteResourcePath = @(),
    [ValidateRange(2, 50)][int]$StableReadAttempts = 6,
    [ValidateRange(0, 1000)][int]$StableReadDelayMs = 50
)

$ErrorActionPreference = "Stop"

function Read-StableJsonFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][int]$Attempts,
        [Parameter(Mandatory = $true)][int]$DelayMs
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "JSON file not found: $Path"
    }

    $strictUtf8 = [System.Text.UTF8Encoding]::new($false, $true)
    $previousHash = $null
    $lastError = "file did not produce two consecutive matching reads"

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $bytes = [System.IO.File]::ReadAllBytes($Path)
            if ($bytes.Length -eq 0) {
                throw "file is empty"
            }

            $raw = $strictUtf8.GetString($bytes)
            if ($raw.Length -gt 0 -and $raw[0] -eq [char]0xFEFF) {
                $raw = $raw.Substring(1)
            }
            $value = ConvertFrom-Json -InputObject $raw -ErrorAction Stop

            $sha256 = [System.Security.Cryptography.SHA256]::Create()
            try {
                $hash = [System.Convert]::ToHexString($sha256.ComputeHash($bytes)).ToLowerInvariant()
            } finally {
                $sha256.Dispose()
            }

            if ($previousHash -and $hash -eq $previousHash) {
                return [pscustomobject]@{
                    value = $value
                    sha256 = $hash
                    attempts = $attempt
                }
            }

            $previousHash = $hash
            $lastError = "valid JSON changed before a confirming read"
        } catch {
            $lastError = $_.Exception.Message
            $previousHash = $null
        }

        if ($attempt -lt $Attempts -and $DelayMs -gt 0) {
            [System.Threading.Thread]::Sleep($DelayMs)
        }
    }

    throw "stable JSON unavailable after $Attempts attempts; last error: $lastError"
}

function Assert-RunStateRecord {
    param(
        [Parameter(Mandatory = $true)]$Record,
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string[]]$AllowedSchemas
    )

    foreach ($field in @("schema", "project", "run_id", "status")) {
        $property = $Record.PSObject.Properties[$field]
        if (
            $null -eq $property -or
            -not ($property.Value -is [string]) -or
            [string]::IsNullOrWhiteSpace([string]$property.Value)
        ) {
            throw "$Label.$field must be a non-empty string"
        }
    }

    if ($AllowedSchemas -notcontains [string]$Record.schema) {
        throw "$Label.schema is unsupported: $($Record.schema)"
    }
    if ([string]$Record.project -ne "trading_mvp") {
        throw "$Label.project must be trading_mvp"
    }
    if (@("RUNNING", "READY_FOR_POSTPROCESS", "STOPPED_INCOMPLETE", "REJECTED_INCOMPLETE") -notcontains [string]$Record.status) {
        throw "$Label.status is unsupported: $($Record.status)"
    }
}

function Get-RunStateTimestamp {
    param(
        [Parameter(Mandatory = $true)]$Record,
        [Parameter(Mandatory = $true)][string]$Path
    )

    foreach ($field in @("updated_at", "completed_at", "created_at", "started_at")) {
        $property = $Record.PSObject.Properties[$field]
        if ($null -eq $property -or [string]::IsNullOrWhiteSpace([string]$property.Value)) {
            continue
        }
        $parsed = [DateTimeOffset]::MinValue
        if ([DateTimeOffset]::TryParse([string]$property.Value, [ref]$parsed)) {
            return $parsed.ToUniversalTime()
        }
    }

    return [DateTimeOffset](Get-Item -LiteralPath $Path).LastWriteTimeUtc
}

function Resolve-LaunchRecordRunType {
    param(
        [Parameter(Mandatory = $true)]$Record,
        [Parameter(Mandatory = $true)]$Gate
    )

    if (
        [string]$Record.run_id -ne [string]$Gate.run_id -or
        [string]::IsNullOrWhiteSpace([string]$Record.manifest_path)
    ) {
        throw "Current launch record identity mismatch."
    }

    $schema = [string]$Record.schema
    if ($schema -eq "active_run_launch_record_v1") {
        if (
            [string]$Record.project -ne [string]$Gate.project -or
            [string]::IsNullOrWhiteSpace([string]$Record.run_type)
        ) {
            throw "Current launch record identity mismatch."
        }
        return [string]$Record.run_type
    }

    if ($schema -eq "trading_mvp_slow_liquidity_recollect_launch_v1") {
        $allowedStatuses = @(
            "VISIBLE_WORKER_CLAIMED",
            "PREFLIGHT_PASSED",
            "GLOBAL_WRITER_CLAIMED",
            "RUNNING",
            "COMPLETE",
            "STOPPED_INCOMPLETE"
        )
        $terminalOwner = $Record.PSObject.Properties["terminal_ownership_verified"]
        $retryAuthorized = $Record.PSObject.Properties["retry_authorized"]
        if (
            $allowedStatuses -notcontains [string]$Record.status -or
            $null -eq $terminalOwner -or
            -not ($terminalOwner.Value -is [bool]) -or
            $terminalOwner.Value -ne $true -or
            $null -eq $retryAuthorized -or
            -not ($retryAuthorized.Value -is [bool]) -or
            $retryAuthorized.Value -ne $false -or
            [string]$Record.plan_file_sha256 -notmatch "^[0-9a-f]{64}$" -or
            [string]$Record.plan_hash -notmatch "^[0-9a-f]{64}$" -or
            [string]$Record.approval_receipt_sha256 -notmatch "^[0-9a-f]{64}$"
        ) {
            throw "Current launch record identity mismatch."
        }
        return "slow_liquidity_history_recollect"
    }

    throw "Current launch record identity mismatch."
}

function Set-ObjectProperty {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        $Value
    )

    if ($Object -is [System.Collections.IDictionary]) {
        $Object[$Name] = $Value
    } elseif ($Object.PSObject.Properties.Name -contains $Name) {
        $Object.$Name = $Value
    } else {
        $Object | Add-Member -NotePropertyName $Name -NotePropertyValue $Value
    }
}

function Remove-ObjectProperty {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($Object -is [System.Collections.IDictionary]) {
        [void]$Object.Remove($Name)
    } elseif ($Object.PSObject.Properties.Name -contains $Name) {
        [void]$Object.PSObject.Properties.Remove($Name)
    }
}

function Get-ExistingProcessIds {
    param([object[]]$ProcessIds)

    $existing = @()
    foreach ($pidValue in $ProcessIds) {
        if ($null -eq $pidValue) { continue }
        $process = Get-Process -Id ([int]$pidValue) -ErrorAction SilentlyContinue
        if ($process) {
            $existing += [int]$pidValue
        }
    }
    return $existing
}

function Get-RepoRootFromGatePath {
    param([string]$Path)

    $resolved = (Resolve-Path -LiteralPath $Path).Path
    $agentLogDir = Split-Path -Parent $resolved
    $docsDir = Split-Path -Parent $agentLogDir
    return (Split-Path -Parent $docsDir)
}

function ConvertTo-CanonicalResourcePath {
    param([Parameter(Mandatory = $true)][string]$Path)

    if ([string]::IsNullOrWhiteSpace($Path)) {
        return $null
    }
    try {
        $full = [System.IO.Path]::GetFullPath($Path)
    } catch {
        return $null
    }
    $separator = [System.IO.Path]::DirectorySeparatorChar
    $root = [System.IO.Path]::GetPathRoot($full)
    if ($full.Length -gt $root.Length) {
        $full = $full.TrimEnd([char[]]@('\', '/'))
    }
    return $full.Replace([System.IO.Path]::AltDirectorySeparatorChar, $separator).ToLowerInvariant()
}

function Test-ResourcePathOverlap {
    param(
        [Parameter(Mandatory = $true)][string]$Left,
        [Parameter(Mandatory = $true)][string]$Right
    )

    $leftPath = ConvertTo-CanonicalResourcePath -Path $Left
    $rightPath = ConvertTo-CanonicalResourcePath -Path $Right
    if (-not $leftPath -or -not $rightPath) {
        return $true
    }
    if ($leftPath -eq $rightPath) {
        return $true
    }
    $separator = [string][System.IO.Path]::DirectorySeparatorChar
    return $leftPath.StartsWith($rightPath + $separator, [System.StringComparison]::OrdinalIgnoreCase) -or
        $rightPath.StartsWith($leftPath + $separator, [System.StringComparison]::OrdinalIgnoreCase)
}

function Get-ProtectedResourceRoots {
    param($Gate)

    $paths = @()
    if ($Gate.output -and $Gate.output.PSObject.Properties.Name -contains "path") {
        $paths += [string]$Gate.output.path
    } elseif ($Gate.PSObject.Properties.Name -contains "output_path" -and $Gate.output_path) {
        $paths += [string]$Gate.output_path
    }
    if ($Gate.PSObject.Properties.Name -contains "manifest_path" -and $Gate.manifest_path) {
        $paths += [string]$Gate.manifest_path
    }
    $parents = @()
    foreach ($path in $paths) {
        $canonical = ConvertTo-CanonicalResourcePath -Path $path
        if (-not $canonical) { continue }
        $parent = Split-Path -Parent $canonical
        if ($parent) {
            $parents += $parent
        } else {
            $parents += $canonical
        }
    }
    return @($parents | Sort-Object -Unique)
}

function Get-OfflineScopeDecision {
    param(
        [string]$Status,
        $Gate,
        [string]$PointerError,
        [string[]]$ReadPaths,
        [string[]]$WritePaths
    )

    if ($Status -ne "RUNNING") {
        return [pscustomobject]@{
            allowed = $true
            decision = "ALLOW_GATE_NOT_RUNNING"
            protected_roots = @()
            conflicts = @()
        }
    }
    if ($PointerError) {
        return [pscustomobject]@{
            allowed = $false
            decision = "DENY_INVALID_ACTIVE_POINTER"
            protected_roots = @()
            conflicts = @([pscustomobject]@{ access = "scope"; path = $null; protected_root = $null; reason = $PointerError })
        }
    }
    $protectedRoots = @(Get-ProtectedResourceRoots -Gate $Gate)
    if ($protectedRoots.Count -eq 0 -or (($ReadPaths.Count + $WritePaths.Count) -eq 0)) {
        return [pscustomobject]@{
            allowed = $false
            decision = "DENY_UNDECLARED_OR_UNKNOWN_SCOPE"
            protected_roots = $protectedRoots
            conflicts = @()
        }
    }
    $conflicts = @()
    foreach ($entry in @(
        @($ReadPaths | ForEach-Object { [pscustomobject]@{ access = "read"; path = $_ } }) +
        @($WritePaths | ForEach-Object { [pscustomobject]@{ access = "write"; path = $_ } })
    )) {
        $canonical = ConvertTo-CanonicalResourcePath -Path ([string]$entry.path)
        if (-not $canonical) {
            $conflicts += [pscustomobject]@{
                access = [string]$entry.access
                path = [string]$entry.path
                protected_root = $null
                reason = "invalid_path"
            }
            continue
        }
        foreach ($root in $protectedRoots) {
            if (Test-ResourcePathOverlap -Left $canonical -Right $root) {
                $conflicts += [pscustomobject]@{
                    access = [string]$entry.access
                    path = $canonical
                    protected_root = $root
                    reason = "resource_overlap"
                }
            }
        }
    }
    return [pscustomobject]@{
        allowed = ($conflicts.Count -eq 0)
        decision = if ($conflicts.Count -eq 0) { "ALLOW_DISJOINT_OFFLINE_WORK" } else { "DENY_RESOURCE_OVERLAP" }
        protected_roots = $protectedRoots
        conflicts = $conflicts
    }
}

function Find-FundingPostprocessBlock {
    param(
        [string]$RepoRoot,
        [string]$ManifestPath,
        [string]$OutputPath
    )

    $fundingDir = Join-Path $RepoRoot "exports\trading-mvp\funding"
    if (-not (Test-Path -LiteralPath $fundingDir)) {
        return $null
    }

    $candidates = Get-ChildItem -LiteralPath $fundingDir -Filter "funding_final_review_*.json" -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending
    foreach ($candidate in $candidates) {
        try {
            $review = Get-Content -Raw -LiteralPath $candidate.FullName | ConvertFrom-Json
        } catch {
            continue
        }

        $reviewManifest = [string]$review.manifest
        $reviewInput = [string]$review.input
        if ($ManifestPath -and $reviewManifest -and ($reviewManifest -ne $ManifestPath)) {
            continue
        }
        if ($OutputPath -and $reviewInput -and ($reviewInput -ne $OutputPath)) {
            continue
        }

        $readinessReasons = @()
        if ($null -ne $review.collect_status -and $null -ne $review.collect_status.readiness -and $null -ne $review.collect_status.readiness.reasons) {
            $readinessReasons = @($review.collect_status.readiness.reasons)
        }
        $dataQualityReasons = @()
        if ($null -ne $review.collect_status -and $null -ne $review.collect_status.data_quality -and $null -ne $review.collect_status.data_quality.reasons) {
            $dataQualityReasons = @($review.collect_status.data_quality.reasons)
        }

        if (([bool]$review.ok -eq $false) -or ([string]$review.status -eq "not_ready_for_postprocess") -or ($readinessReasons.Count -gt 0) -or ($dataQualityReasons.Count -gt 0)) {
            return [pscustomobject]@{
                path = $candidate.FullName
                ok = [bool]$review.ok
                status = [string]$review.status
                ready_for_postprocess = if ($null -ne $review.collect_status) { [bool]$review.collect_status.ready_for_postprocess } else { $null }
                readiness_reasons = $readinessReasons
                data_quality_reasons = $dataQualityReasons
                min_rows_per_cycle = if ($null -ne $review.collect_status -and $null -ne $review.collect_status.data_quality -and $null -ne $review.collect_status.data_quality.metrics) { $review.collect_status.data_quality.metrics.min_rows_per_cycle } else { $null }
            }
        }
    }
    return $null
}

function ConvertTo-IntOrDefault {
    param(
        [object]$Value,
        [int]$Default = 0
    )

    if ($null -eq $Value) {
        return $Default
    }
    if ($Value -is [array]) {
        return [int]$Value.Count
    }
    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        $total = 0
        foreach ($property in $Value.PSObject.Properties) {
            $total += ConvertTo-IntOrDefault -Value $property.Value -Default 0
        }
        return [int]$total
    }

    try {
        return [int]$Value
    } catch {
        return $Default
    }
}

function ConvertTo-DoubleOrDefault {
    param(
        [object]$Value,
        [double]$Default = 0.0
    )

    if ($null -eq $Value) {
        return $Default
    }
    try {
        return [double]$Value
    } catch {
        return $Default
    }
}

function Get-OutputSummary {
    param(
        [string]$Path,
        [datetime]$Now,
        [Nullable[long]]$KnownLineCount = $null
    )

    if (-not $Path -or -not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    $item = Get-Item -LiteralPath $Path
    if ($item.PSIsContainer) {
        $files = @(Get-ChildItem -LiteralPath $Path -File -ErrorAction SilentlyContinue)
        $lastWrite = if ($files.Count -gt 0) {
            @($files | Sort-Object LastWriteTime -Descending | Select-Object -First 1)[0].LastWriteTime
        } else {
            $item.LastWriteTime
        }
        $bytes = 0
        foreach ($file in $files) {
            $bytes += [int64]$file.Length
        }
        return [pscustomobject]@{
            path = $Path
            kind = "directory"
            bytes = $bytes
            file_count = $files.Count
            line_count = $null
            last_write = $lastWrite.ToString("yyyy-MM-dd HH:mm:ss zzz")
            last_write_age_sec = [Math]::Round(($Now - $lastWrite).TotalSeconds, 1)
        }
    }

    $lineCount = if ($null -ne $KnownLineCount) {
        [long]$KnownLineCount
    } else {
        (Get-Content -LiteralPath $Path | Measure-Object -Line).Lines
    }
    return [pscustomobject]@{
        path = $Path
        kind = "file"
        bytes = $item.Length
        file_count = $null
        line_count = $lineCount
        last_write = $item.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss zzz")
        last_write_age_sec = [Math]::Round(($Now - $item.LastWriteTime).TotalSeconds, 1)
    }
}

function Get-ManifestRows {
    param(
        [object]$Manifest,
        [object]$Output
    )

    if ($Manifest) {
        foreach ($field in @("rows", "rows_total", "total_events", "ohlcv_rows", "row_count", "total_rows", "normalized_rows")) {
            if ($null -ne $Manifest.PSObject.Properties[$field] -and $null -ne $Manifest.$field) {
                return ConvertTo-IntOrDefault -Value $Manifest.$field -Default 0
            }
        }
    }

    if ($Output -and $null -ne $Output.line_count) {
        return ConvertTo-IntOrDefault -Value $Output.line_count -Default 0
    }

    return 0
}

function Test-ExpectedOutputsComplete {
    param([object]$Gate)

    if ($null -eq $Gate -or $null -eq $Gate.PSObject.Properties["expected_outputs"]) {
        return $false
    }

    $expected = $Gate.expected_outputs
    if (
        $null -ne $Gate.PSObject.Properties["expected_outputs_complete"] -and
        [bool]$Gate.expected_outputs_complete
    ) {
        $paths = @(
            $expected.PSObject.Properties |
                ForEach-Object { [string]$_.Value } |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )
        if ($paths.Count -eq 0) {
            return $false
        }
        foreach ($path in $paths) {
            if (-not (Test-Path -LiteralPath $path)) {
                return $false
            }
            $item = Get-Item -LiteralPath $path
            if ($item.PSIsContainer) {
                if (@(Get-ChildItem -LiteralPath $path -File -ErrorAction SilentlyContinue).Count -eq 0) {
                    return $false
                }
            } elseif ($item.Length -le 0) {
                return $false
            }
        }
        return $true
    }

    foreach ($name in @("event_quality", "event_slice", "event_validation", "ws_grid", "sweep_gate", "validation_summary")) {
        if ($null -eq $expected.PSObject.Properties[$name]) {
            return $false
        }
        $path = [string]$expected.$name
        if ([string]::IsNullOrWhiteSpace($path) -or -not (Test-Path -LiteralPath $path)) {
            return $false
        }
        $item = Get-Item -LiteralPath $path
        if ($item.PSIsContainer -or $item.Length -le 0) {
            return $false
        }
    }

    try {
        $validation = Get-Content -Raw -LiteralPath ([string]$expected.validation_summary) | ConvertFrom-Json
    } catch {
        return $false
    }

    if ($null -eq $validation.PSObject.Properties["ok"] -or -not [bool]$validation.ok) {
        return $false
    }
    return $true
}

try {
    $gateRead = Read-StableJsonFile `
        -Path $GatePath `
        -Attempts $StableReadAttempts `
        -DelayMs $StableReadDelayMs
    Assert-RunStateRecord `
        -Record $gateRead.value `
        -Label "active_run_gate" `
        -AllowedSchemas @("active_run_gate_v1", "active_run_gate_v2")
} catch {
    throw "ACTIVE_RUN_GATE_UNSTABLE_OR_INVALID: $($_.Exception.Message)"
}

$gate = $gateRead.value
$legacyGateRunType = if ($gate.PSObject.Properties.Name -contains "run_type") {
    [string]$gate.run_type
} else {
    $null
}
$gateSource = "legacy_active_run_gate"
$pointerError = $null
$pointerReplacedLegacyGate = $false
$agentLogDir = Split-Path -Parent (Resolve-Path -LiteralPath $GatePath).Path
$currentRunPointerPath = Join-Path $agentLogDir "current-run.json"
$currentRunPointer = $null
$currentRunPointerRead = $null
if (Test-Path -LiteralPath $currentRunPointerPath) {
    try {
        $currentRunPointerRead = Read-StableJsonFile `
            -Path $currentRunPointerPath `
            -Attempts $StableReadAttempts `
            -DelayMs $StableReadDelayMs
        Assert-RunStateRecord `
            -Record $currentRunPointerRead.value `
            -Label "current_run_pointer" `
            -AllowedSchemas @("active_run_pointer_v1")
        $candidatePointer = $currentRunPointerRead.value
        $currentRunPointer = $candidatePointer
        if ([string]$candidatePointer.run_id -ne [string]$gate.run_id) {
            $pointerTimestamp = Get-RunStateTimestamp `
                -Record $candidatePointer -Path $currentRunPointerPath
            $gateTimestamp = Get-RunStateTimestamp -Record $gate -Path $GatePath
            if ($pointerTimestamp -ge $gateTimestamp) {
                # Never inherit fields from another run. A stale terminal pointer
                # cannot override a newer active-run gate.
                $gate = $candidatePointer | ConvertTo-Json -Depth 16 | ConvertFrom-Json
                $pointerReplacedLegacyGate = $true
            } else {
                $currentRunPointer = $null
                $pointerError = "stale_current_run_pointer_ignored"
                $gateSource = "newer_active_run_gate"
            }
        } else {
            foreach ($field in @(
                "run_id",
                "status",
                "manifest_path",
                "collector_pid",
                "monitor_pid",
                "process_ids",
                "output",
                "updated_at",
                "launch_record_path"
            )) {
                if ($null -ne $candidatePointer.PSObject.Properties[$field]) {
                    Set-ObjectProperty -Object $gate -Name $field -Value $candidatePointer.$field
                }
            }
        }
        if ($currentRunPointer -and $candidatePointer.output -and $candidatePointer.output.PSObject.Properties.Name -contains "path") {
            Set-ObjectProperty -Object $gate -Name "output_path" -Value ([string]$candidatePointer.output.path)
        }
        if ($currentRunPointer) {
            $gateSource = "current_run_pointer"
        }
    } catch {
        throw "ACTIVE_RUN_POINTER_UNSTABLE_OR_INVALID: $($_.Exception.Message)"
    }
}

$launchRecord = $null
$launchRecordError = $null
$staleRunMetadataIgnored = @()
$launchRecordPath = if (
    $currentRunPointer -and
    $currentRunPointer.PSObject.Properties.Name -contains "launch_record_path" -and
    $currentRunPointer.launch_record_path
) {
    [string]$currentRunPointer.launch_record_path
} elseif (
    $gate.PSObject.Properties.Name -contains "launch_record_path" -and
    $gate.launch_record_path
) {
    [string]$gate.launch_record_path
} else {
    $null
}
if ($launchRecordPath) {
    try {
        if (-not (Test-Path -LiteralPath $launchRecordPath -PathType Leaf)) {
            throw "Current launch record is missing: $launchRecordPath"
        }
        $candidateLaunchRecord = (
            Get-Content -Raw -LiteralPath $launchRecordPath | ConvertFrom-Json
        )
        $launchRunType = Resolve-LaunchRecordRunType `
            -Record $candidateLaunchRecord -Gate $gate
        if (
            $candidateLaunchRecord.manifest_path -and
            $gate.manifest_path -and
            (ConvertTo-CanonicalResourcePath -Path ([string]$candidateLaunchRecord.manifest_path)) -ne
            (ConvertTo-CanonicalResourcePath -Path ([string]$gate.manifest_path))
        ) {
            throw "Current launch record manifest path mismatch."
        }
        $launchRecord = $candidateLaunchRecord
        if ($launchRunType) {
            if (
                -not $pointerReplacedLegacyGate -and
                $legacyGateRunType -ne $launchRunType
            ) {
                # The visible wrapper reused the shared gate object. Keep current
                # safety/schedule fields, but never inherit another run type's
                # completion artifacts or operational hints.
                $staleRunMetadataIgnored = @(
                    "expected_outputs",
                    "expected_outputs_complete",
                    "purpose",
                    "readiness_output_path",
                    "command_after_explicit_approval",
                    "requires_explicit_user_approval_for_actual_collect",
                    "requires_explicit_user_approval_for_public_probe",
                    "last_spot_perp_basis_public_probe_output_path",
                    "last_spot_perp_basis_public_probe_decision",
                    "last_spot_perp_basis_public_probe_confirmed",
                    "resume_command",
                    "durable_status_command",
                    "state_path",
                    "alert_path",
                    "notification_required"
                )
                foreach ($field in $staleRunMetadataIgnored) {
                    Remove-ObjectProperty -Object $gate -Name $field
                }
            }
            Set-ObjectProperty -Object $gate -Name "run_type" -Value $launchRunType
        }
    } catch {
        $launchRecordError = $_.Exception.Message
        foreach ($field in @("expected_outputs", "expected_outputs_complete")) {
            if ($gate.PSObject.Properties.Name -contains $field) {
                $staleRunMetadataIgnored += $field
                Remove-ObjectProperty -Object $gate -Name $field
            }
        }
    }
}
$repoRoot = Get-RepoRootFromGatePath -Path $GatePath
$manifestPath = [string]$gate.manifest_path
$now = Get-Date

$manifest = $null
if ($manifestPath -and (Test-Path -LiteralPath $manifestPath)) {
    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
}

$manifestOutputPath = $null
if ($manifest) {
    foreach ($field in @("snapshots_path", "output_path", "path")) {
        if ($null -ne $manifest.PSObject.Properties[$field] -and -not [string]::IsNullOrWhiteSpace([string]$manifest.$field)) {
            $manifestOutputPath = [string]$manifest.$field
            break
        }
    }
}
$nestedOutputPath = if ($null -ne $gate.PSObject.Properties["output"] -and $null -ne $gate.output -and $null -ne $gate.output.PSObject.Properties["path"]) { [string]$gate.output.path } else { $null }
$legacyOutputPath = if ($null -ne $gate.PSObject.Properties["output_path"]) { [string]$gate.output_path } else { $null }
$outputPath = @($manifestOutputPath, $nestedOutputPath, $legacyOutputPath) |
    Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) -and (Test-Path -LiteralPath ([string]$_)) } |
    Select-Object -First 1
if (-not $outputPath) {
    $outputPath = @($manifestOutputPath, $nestedOutputPath, $legacyOutputPath) |
        Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } |
        Select-Object -First 1
}

$completedCycles = 0
if ($manifest) {
    foreach ($field in @("completed_cycles", "cycle_count")) {
        if ($null -ne $manifest.PSObject.Properties[$field] -and $null -ne $manifest.$field) {
            $completedCycles = ConvertTo-IntOrDefault -Value $manifest.$field -Default 0
            break
        }
    }
}
if ($completedCycles -le 0 -and $null -ne $gate.PSObject.Properties["completed_cycles"] -and $null -ne $gate.completed_cycles) {
    $completedCycles = ConvertTo-IntOrDefault -Value $gate.completed_cycles -Default 0
}
$manifestDurationSec = 0.0
$manifestIntervalSec = 0.0
$manifestElapsedSec = 0.0
$manifestActualDurationSec = 0.0
if ($manifest) {
    foreach ($field in @("duration_sec", "requested_duration_sec")) {
        if ($null -ne $manifest.PSObject.Properties[$field] -and $null -ne $manifest.$field) {
            $manifestDurationSec = ConvertTo-DoubleOrDefault -Value $manifest.$field -Default 0.0
            if ($manifestDurationSec -gt 0) { break }
        }
    }
    foreach ($field in @("interval_sec", "poll_interval_sec")) {
        if ($null -ne $manifest.PSObject.Properties[$field] -and $null -ne $manifest.$field) {
            $manifestIntervalSec = ConvertTo-DoubleOrDefault -Value $manifest.$field -Default 0.0
            if ($manifestIntervalSec -gt 0) { break }
        }
    }
    if ($null -ne $manifest.PSObject.Properties["elapsed_active_sec"] -and $null -ne $manifest.elapsed_active_sec) {
        $manifestElapsedSec = ConvertTo-DoubleOrDefault -Value $manifest.elapsed_active_sec -Default 0.0
    }
    if ($null -ne $manifest.PSObject.Properties["actual_duration_sec"] -and $null -ne $manifest.actual_duration_sec) {
        $manifestActualDurationSec = ConvertTo-DoubleOrDefault -Value $manifest.actual_duration_sec -Default 0.0
    }
}

$gateIntervalSec = 0.0
foreach ($field in @("interval_sec", "poll_interval_sec")) {
    if ($null -ne $gate.PSObject.Properties[$field] -and $null -ne $gate.$field) {
        $gateIntervalSec = ConvertTo-DoubleOrDefault -Value $gate.$field -Default 0.0
        if ($gateIntervalSec -gt 0) { break }
    }
}
$cycleIntervalSec = if ($manifestIntervalSec -gt 0) { $manifestIntervalSec } elseif ($gateIntervalSec -gt 0) { $gateIntervalSec } else { 300.0 }
$requestedDurationSec = if ($manifestDurationSec -gt 0) {
    $manifestDurationSec
} elseif ($null -ne $gate.PSObject.Properties["requested_duration_sec"] -and $null -ne $gate.requested_duration_sec) {
    ConvertTo-DoubleOrDefault -Value $gate.requested_duration_sec -Default 0.0
} else {
    0.0
}
$actualDurationSec = if ($manifestActualDurationSec -gt 0) {
    $manifestActualDurationSec
} elseif ($manifestElapsedSec -gt 0) {
    $manifestElapsedSec
} elseif ($null -ne $gate.PSObject.Properties["actual_duration_sec"] -and $null -ne $gate.actual_duration_sec) {
    ConvertTo-DoubleOrDefault -Value $gate.actual_duration_sec -Default 0.0
} else {
    0.0
}

$totalCycles = 0
if ($manifest) {
    foreach ($field in @("cycles", "total_cycles")) {
        if ($null -ne $manifest.PSObject.Properties[$field] -and $null -ne $manifest.$field) {
            $totalCycles = ConvertTo-IntOrDefault -Value $manifest.$field -Default 0
            if ($totalCycles -gt 0) { break }
        }
    }
}
if ($totalCycles -le 0 -and $requestedDurationSec -gt 0 -and $cycleIntervalSec -gt 0) {
    $totalCycles = [Math]::Max(1, [int][Math]::Ceiling($requestedDurationSec / $cycleIntervalSec))
}
if ($totalCycles -le 0 -and $gate.total_cycles) {
    $totalCycles = ConvertTo-IntOrDefault -Value $gate.total_cycles -Default 0
}
$totalCycles = [Math]::Max($totalCycles, $completedCycles)
$final = if ($manifest -and $null -ne $manifest.final) { [bool]$manifest.final } else { $false }
$errors = 0
$manifestReportsErrors = $false
if ($manifest) {
    foreach ($field in @("errors", "errors_total")) {
        if ($null -ne $manifest.PSObject.Properties[$field] -and $null -ne $manifest.$field) {
            $errors = ConvertTo-IntOrDefault -Value $manifest.$field -Default 0
            $manifestReportsErrors = $true
            break
        }
    }
}
if (-not $manifestReportsErrors -and $null -ne $gate.PSObject.Properties["errors"] -and $null -ne $gate.errors) {
    $errors = ConvertTo-IntOrDefault -Value $gate.errors -Default 0
}
$remainingCycles = [Math]::Max(0, $totalCycles - $completedCycles)
$remainingDurationSec = if ($final) {
    0.0
} elseif ($manifestDurationSec -gt 0 -and $manifestElapsedSec -ge 0) {
    [Math]::Max(0.0, $manifestDurationSec - $manifestElapsedSec)
} elseif ($totalCycles -gt 0) {
    $remainingCycles * $cycleIntervalSec
} else {
    0.0
}
$eta = if ($totalCycles -gt 0 -or $remainingDurationSec -gt 0) { $now.AddSeconds($remainingDurationSec) } else { $null }

$manifestRows = Get-ManifestRows -Manifest $manifest -Output $null
$knownLineCount = if ($manifestRows -gt 0) { [Nullable[long]]([long]$manifestRows) } else { $null }
$output = Get-OutputSummary -Path $outputPath -Now $now -KnownLineCount $knownLineCount
$rows = if ($manifestRows -gt 0) { $manifestRows } else { Get-ManifestRows -Manifest $manifest -Output $output }

$candidateProcessIds = @($gate.process_ids)
if ($null -ne $gate.PSObject.Properties["collector_pid"] -and $gate.collector_pid) {
    $candidateProcessIds += $gate.collector_pid
}
$existingProcessIds = @(Get-ExistingProcessIds -ProcessIds $candidateProcessIds | Sort-Object -Unique)
$collectorPidAlive = $false
if ($null -ne $gate.PSObject.Properties["collector_pid"] -and $gate.collector_pid) {
    $collectorPidAlive = [bool](Get-Process -Id ([int]$gate.collector_pid) -ErrorAction SilentlyContinue)
}
$monitorPidAlive = $false
if ($gate.monitor_pid) {
    $monitorPidAlive = [bool](Get-Process -Id ([int]$gate.monitor_pid) -ErrorAction SilentlyContinue)
}
$processAlive = ($existingProcessIds.Count -gt 0) -or $monitorPidAlive
$ready = $final -and ($totalCycles -eq 0 -or $completedCycles -ge $totalCycles)
$gateStatusRaw = [string]$gate.status
$expectedOutputsApplicable = (
    $null -ne $gate.PSObject.Properties["expected_outputs"]
)
$expectedOutputsComplete = if ($expectedOutputsApplicable) {
    Test-ExpectedOutputsComplete -Gate $gate
} else {
    $false
}
$primaryOutputComplete = [bool]($output -and $output.bytes -gt 0 -and ($final -or $gateStatusRaw -eq "READY_FOR_POSTPROCESS" -or $rows -gt 0))

if ($collectorPidAlive -and -not $final) {
    $status = "RUNNING"
    $warning = "Goal gate is closed: the current collector PID is active and manifest final=false. Do not start a duplicate collector."
} elseif ($gateStatusRaw -eq "STOPPED_INCOMPLETE") {
    $status = "STOPPED_INCOMPLETE"
    $warning = "Goal gate is closed: prior run was explicitly marked STOPPED_INCOMPLETE. Resume visibly or declare this dataset incomplete before proceeding."
} elseif ($gateStatusRaw -eq "READY_FOR_POSTPROCESS") {
    $status = "READY_FOR_POSTPROCESS"
    $warning = "Gate is open: run was explicitly marked READY_FOR_POSTPROCESS. Next goal step may proceed."
} elseif ($ready) {
    $status = "READY_FOR_POSTPROCESS"
    $warning = "Gate is open: run has final=true. Next goal step may proceed."
} elseif ($expectedOutputsComplete) {
    $status = "READY_FOR_POSTPROCESS"
    $warning = "Gate is open: expected output artifacts are complete. The monitor PID may still be alive because a visible -NoExit terminal was left open."
} elseif ($processAlive) {
    $status = "RUNNING"
    $warning = "Goal gate is closed: run is active. Do not run next goal/postprocess steps; only status checks are allowed."
} elseif ($gateStatusRaw -eq "RUNNING" -and -not $processAlive) {
    $status = "STOPPED_INCOMPLETE"
    $warning = "Stale lock detected: Gate was RUNNING but no active processes found. Auto-cleared to STOPPED_INCOMPLETE."
    
    $gate.status = "STOPPED_INCOMPLETE"
    if ($gate.PSObject.Properties.Match("stop_reason").Count -eq 0) {
        $gate | Add-Member -NotePropertyName "stop_reason" -NotePropertyValue "auto_stale_lock_clear"
    } else {
        $gate.stop_reason = "auto_stale_lock_clear"
    }
    # Ensure we write to the original GatePath
    $gate | ConvertTo-Json -Depth 10 | Set-Content -Path $GatePath -Encoding UTF8
} else {
    $status = "STOPPED_INCOMPLETE"
    $warning = "Goal gate is closed: run is not active and final=false. Resume visibly or declare this dataset incomplete before proceeding."
}

$postprocessBlock = $null
$rawNextStepAfterReady = $gate.next_step_after_ready
$nextStepAfterReady = $rawNextStepAfterReady
$nextGoalDecision = $gate.next_goal_decision
$nextGoalReason = $gate.next_goal_reason
$replayAllowed = if ($gate.PSObject.Properties["replay_allowed"]) { [bool]$gate.replay_allowed } else { $null }
$requiresExplicitCollectApproval = if ($gate.PSObject.Properties["requires_explicit_user_approval_for_actual_collect"]) { [bool]$gate.requires_explicit_user_approval_for_actual_collect } else { $null }
$requiresExplicitPublicProbeApproval = if ($gate.PSObject.Properties["requires_explicit_user_approval_for_public_probe"]) { [bool]$gate.requires_explicit_user_approval_for_public_probe } else { $null }
$readinessOutputPath = if ($gate.PSObject.Properties["readiness_output_path"]) { [string]$gate.readiness_output_path } else { $null }
$commandAfterExplicitApproval = if ($gate.PSObject.Properties["command_after_explicit_approval"]) { [string]$gate.command_after_explicit_approval } else { $null }
$resumeCommand = if ($gate.PSObject.Properties["resume_command"]) { [string]$gate.resume_command } else { $null }
$durableStatusCommand = if ($gate.PSObject.Properties["durable_status_command"]) { [string]$gate.durable_status_command } else { $null }
$statePath = if ($gate.PSObject.Properties["state_path"]) { [string]$gate.state_path } else { $null }
$alertPath = if ($gate.PSObject.Properties["alert_path"]) { [string]$gate.alert_path } else { $null }
$notificationRequired = if ($gate.PSObject.Properties["notification_required"]) { [bool]$gate.notification_required } else { $null }

if ($status -eq "READY_FOR_POSTPROCESS" -and $null -ne $replayAllowed -and -not $replayAllowed) {
    if ($nextGoalDecision -and ([string]$nextGoalDecision).StartsWith("START_NEW_VISIBLE_")) {
        $warning = "Gate is open for the next planning/start step, but replay/grid are blocked because replay_allowed=false. Start the new visible collect only after explicit user approval."
    } else {
        $warning = "Gate is open, but replay/grid are blocked because replay_allowed=false. Follow next_goal_decision/next_step_after_ready instead of treating this dataset as accepted."
    }
}
if ($status -eq "READY_FOR_POSTPROCESS" -and ([string]$gate.run_id) -like "funding_collect_*") {
    $postprocessBlock = Find-FundingPostprocessBlock -RepoRoot $repoRoot -ManifestPath $manifestPath -OutputPath $outputPath
    if ($postprocessBlock) {
        $reasonText = (@($postprocessBlock.readiness_reasons) + @($postprocessBlock.data_quality_reasons)) -join ", "
        if (-not $reasonText) {
            $reasonText = $postprocessBlock.status
        }
        $warning = "Gate is open because collection is final, but funding postprocess is blocked by guard review: $reasonText. Do not run funding rank/backtest/paper-forward on this dataset; use tools/trading_next_goal_step.ps1 for the next proof step."
        $nextStepAfterReady = "Funding final-review guard blocked this completed dataset ($reasonText). Next proof step: run tools/trading_next_goal_step.ps1 and follow the guarded WS collect/postprocess/replay-validation path; do not use this funding dataset for rank/backtest/paper-forward."
    }
}

$result = [pscustomobject]@{
    gate_read_attempts = $gateRead.attempts
    gate_read_sha256 = $gateRead.sha256
    current_run_pointer_read_attempts = if ($currentRunPointerRead) { $currentRunPointerRead.attempts } else { $null }
    current_run_pointer_read_sha256 = if ($currentRunPointerRead) { $currentRunPointerRead.sha256 } else { $null }
    gate_source = $gateSource
    current_run_pointer_path = if ($gateSource -eq "current_run_pointer") { $currentRunPointerPath } else { $null }
    launch_record_path = $launchRecordPath
    launch_record_error = $launchRecordError
    pointer_error = $pointerError
    status = $status
    warning = $warning
    gate_status = $gateStatusRaw
    project = $gate.project
    run_id = $gate.run_id
    run_type = if ($gate.PSObject.Properties.Name -contains "run_type") { [string]$gate.run_type } else { $null }
    now = $now.ToString("yyyy-MM-dd HH:mm:ss zzz")
    completed_cycles = $completedCycles
    total_cycles = $totalCycles
    remaining_cycles = $remainingCycles
    estimated_finish = if ($eta) { $eta.ToString("yyyy-MM-dd HH:mm:ss zzz") } else { $null }
    remaining_hours = [Math]::Round($remainingDurationSec / 3600, 2)
    final = $final
    primary_output_complete = $primaryOutputComplete
    expected_outputs_applicable = $expectedOutputsApplicable
    expected_outputs_complete = $expectedOutputsComplete
    stale_run_metadata_ignored = @($staleRunMetadataIgnored | Sort-Object -Unique)
    rows = $rows
    errors = $errors
    monitor_pid = $gate.monitor_pid
    stale_monitor_pid = $gate.stale_monitor_pid
    monitor_pid_alive = $monitorPidAlive
    live_process_ids = $existingProcessIds
    output = $output
    manifest_path = $manifestPath
    stop_reason = $gate.stop_reason
    requested_duration_sec = if ($requestedDurationSec -gt 0) { $requestedDurationSec } else { $null }
    actual_duration_sec = if ($actualDurationSec -gt 0) { $actualDurationSec } else { $null }
    postprocess_block = $postprocessBlock
    next_goal_decision = $nextGoalDecision
    next_goal_reason = $nextGoalReason
    replay_allowed = $replayAllowed
    requires_explicit_user_approval_for_actual_collect = $requiresExplicitCollectApproval
    requires_explicit_user_approval_for_public_probe = $requiresExplicitPublicProbeApproval
    readiness_output_path = $readinessOutputPath
    command_after_explicit_approval = $commandAfterExplicitApproval
    last_spot_perp_basis_public_probe_output_path = if ($gate.PSObject.Properties["last_spot_perp_basis_public_probe_output_path"]) { [string]$gate.last_spot_perp_basis_public_probe_output_path } else { $null }
    last_spot_perp_basis_public_probe_decision = if ($gate.PSObject.Properties["last_spot_perp_basis_public_probe_decision"]) { [string]$gate.last_spot_perp_basis_public_probe_decision } else { $null }
    last_spot_perp_basis_public_probe_confirmed = if ($gate.PSObject.Properties["last_spot_perp_basis_public_probe_confirmed"]) { [bool]$gate.last_spot_perp_basis_public_probe_confirmed } else { $null }
    resume_command = $resumeCommand
    durable_status_command = $durableStatusCommand
    state_path = $statePath
    alert_path = $alertPath
    notification_required = $notificationRequired
    raw_gate_next_step_after_ready = $rawNextStepAfterReady
    next_step_after_ready = $nextStepAfterReady
}

if ($OfflineWork) {
    $scopeDecision = Get-OfflineScopeDecision `
        -Status $status `
        -Gate $gate `
        -PointerError $pointerError `
        -ReadPaths @($ReadResourcePath) `
        -WritePaths @($WriteResourcePath)
    Set-ObjectProperty -Object $result -Name "scope_decision" -Value $scopeDecision
}

if ($Json) {
    $result | ConvertTo-Json -Depth 8
    exit 0
}

$result | Format-List
