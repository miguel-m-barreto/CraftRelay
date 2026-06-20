[CmdletBinding()]
param([switch]$Live, [switch]$Observability)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$args = @('compose', '-f', (Join-Path $root 'deployment/compose/compose.yml'))
if ($Live) { $args += @('--profile', 'live') }
if ($Observability) { $args += @('--profile', 'observability') }
$args += @('up', '-d')
& docker @args
exit $LASTEXITCODE

