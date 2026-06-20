[CmdletBinding()]
param([switch]$RemoveVolumes)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$args = @('compose', '-f', (Join-Path $root 'deployment/compose/compose.yml'), '--profile', 'live', '--profile', 'observability', 'down')
if ($RemoveVolumes) { $args += '--volumes' }
& docker @args
exit $LASTEXITCODE

