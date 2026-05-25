# backup_to_onedrive.ps1 - mirror local data\ to OneDrive backup folder.
#
# One-way copy (local -> OneDrive). Files deleted locally are deleted from
# the OneDrive copy too (/MIR). OneDrive is a passive snapshot, not live
# storage; nothing in the project reads from it.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\backup_to_onedrive.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\backup_to_onedrive.ps1 -DryRun

param(
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

$src = "C:\sentiment-analyzer\data"
$dst = "C:\Users\Ashot\OneDrive\Documents\ai-market-signal-data"

if (-not (Test-Path $src)) { throw "source missing: $src" }
if (-not (Test-Path $dst)) { New-Item -ItemType Directory -Path $dst | Out-Null }

$flags = @('/MIR', '/COPY:DAT', '/R:3', '/W:5', '/NP', '/NFL', '/NDL')
if ($DryRun) { $flags += '/L' }

$started = Get-Date
Write-Host "src: $src"
Write-Host "dst: $dst"
Write-Host "started: $started"
if ($DryRun) { Write-Host "DRY RUN - no files will be written" }
Write-Host ""

& robocopy $src $dst @flags
$code = $LASTEXITCODE

$elapsed = (Get-Date) - $started
Write-Host ""
Write-Host ("elapsed: {0:mm\:ss}" -f $elapsed)
Write-Host "robocopy exit: $code"

# robocopy exit codes: 0=no change, 1=files copied, 2=extras removed, 3=both.
# >=8 indicates failures.
if ($code -ge 8) {
    Write-Host "FAILED - see robocopy output above" -ForegroundColor Red
    exit $code
}
Write-Host "OK" -ForegroundColor Green
exit 0
