[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$BundlePath)

$ErrorActionPreference = 'Stop'
$temporary = $null
try {
    if ($BundlePath.EndsWith('.zip',[StringComparison]::OrdinalIgnoreCase)) {
        $temporary = Join-Path ([IO.Path]::GetTempPath()) ("craftrelay-audit-test-" + [guid]::NewGuid())
        Expand-Archive -LiteralPath $BundlePath -DestinationPath $temporary
        $root = $temporary
    } else { $root = (Resolve-Path $BundlePath).Path }
    foreach ($required in @('summary.json','sha256-manifest.txt','command-logs','source-snapshot','evidence/head-state.txt','evidence/git-status.txt')) {
        if (-not (Test-Path -LiteralPath (Join-Path $root $required))) { throw "Missing bundle entry: $required" }
    }
    $summary = Get-Content -Raw (Join-Path $root 'summary.json') | ConvertFrom-Json
    if ($summary.head_state -notin @('UNBORN','EXISTING')) { throw 'Invalid HEAD_STATE' }
    if ($summary.validation_status -notin @('PASS','FAIL')) { throw 'Invalid validation status' }
    if ($summary.buf_breaking -ne 'NOT_APPLICABLE') { throw 'buf breaking status is inconsistent' }
    $failed = @($summary.commands | Where-Object exit_code -ne 0).Count
    if (($failed -eq 0) -ne ($summary.validation_status -eq 'PASS')) { throw 'Validation status does not match command exit codes' }
    foreach ($command in $summary.commands) { foreach ($log in @($command.stdout,$command.stderr)) { if (-not (Test-Path -LiteralPath (Join-Path $root $log))) { throw "Missing command log: $log" } } }
    $forbidden = '(^|/)(\.git|target|audit-bundles|secrets?|credentials?)(/|$)|(^|/)(\.env($|\.)|.*\.(pem|key|p12|pfx|jks)$)|(^|/)java/.*/target(/|$)'
    Get-ChildItem $root -Recurse -File | ForEach-Object { $relative=$_.FullName.Substring($root.Length+1).Replace('\','/'); if($relative -match $forbidden){throw "Forbidden bundle entry: $relative"} }
    foreach ($line in Get-Content (Join-Path $root 'sha256-manifest.txt')) {
        if ($line -notmatch '^([0-9a-f]{64})  (.+)$') { throw "Invalid manifest line: $line" }
        $expected=$Matches[1]; $relative=$Matches[2]; $file=Join-Path $root $relative
        if(-not(Test-Path -LiteralPath $file -PathType Leaf)){throw "Manifest file missing: $relative"}
        $actual=(Get-FileHash -Algorithm SHA256 -LiteralPath $file).Hash.ToLowerInvariant()
        if($actual -ne $expected){throw "Hash mismatch: $relative"}
    }
    Write-Host 'Audit bundle validation: PASS'
} finally { if ($temporary -and (Test-Path $temporary)) { Remove-Item -LiteralPath $temporary -Recurse -Force } }
