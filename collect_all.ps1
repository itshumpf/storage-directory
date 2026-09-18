# collect_all.ps1 — run every local collector at once, then assemble the day.
#
#   .\collect_all.ps1                         all six local brands, then daily + audit
#   .\collect_all.ps1 -Brands cubesmart,storagemart
#   .\collect_all.ps1 -IncludePublicStorage   also run daily_scraper.py (CI does this already)
#   .\collect_all.ps1 -NoAssemble             just collect; run `daily` yourself later
#   .\collect_all.ps1 -Serve                  when done, serve the repo and open the dashboard
#
# Each brand gets its own PowerShell window (so you can watch them) and its own
# log under private\logs\. Every host is hit by exactly one process, so running
# them in parallel is as polite as running them one at a time — and the wall
# clock is the slowest collector (U-Haul, ~3h) rather than the sum.
#
# Collection goes through `storage_pipeline.py run <brand>`, which launches the
# collector with the right arguments and files its output as the dated snapshot.
# A collector that stops without publishing (exit 2) leaves its partial file and
# report behind and simply is not in today's merge; the others still are.

param(
    [string[]]$Brands = @("cubesmart", "storagesense", "uhaul", "storagemart", "smartstop", "independent"),
    [switch]$IncludePublicStorage,
    [switch]$NoAssemble,
    [switch]$Serve
)

$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"       # inherited by every collector window; prevents redirected-console Unicode failures
$env:PYTHONUNBUFFERED = "1"   # Windows fully buffers Python stdout when piped (as Tee-Object does below);
                               # without this a quiet collector (storagesense) can run 10+ minutes with
                               # zero visible output and look hung, tempting a close that kills real progress.
$repo = $PSScriptRoot
Set-Location $repo
$logDir = Join-Path $repo "private\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stamp = Get-Date -Format "yyyy-MM-dd_HHmm"

if ($IncludePublicStorage) { $Brands = @("publicstorage") + $Brands }

$procs = @{}
foreach ($b in $Brands) {
    $log = Join-Path $logDir "$b-$stamp.log"
    # Tee so the window shows progress AND the log keeps it. The window stays open
    # after the run so a failure message is not lost when the process exits.
    $cmd = "Set-Location '$repo'; python storage_pipeline.py run $b 2>&1 | Tee-Object -FilePath '$log'; " +
           "Write-Host ''; Write-Host '--- $b finished (exit ' `$LASTEXITCODE ') — this window can be closed ---' -ForegroundColor Cyan"
    $p = Start-Process powershell.exe -ArgumentList "-NoProfile", "-NoExit", "-Command", $cmd -PassThru
    $procs[$b] = $p
    Write-Host ("started {0,-15} pid {1,-6} log {2}" -f $b, $p.Id, $log)
    Start-Sleep -Seconds 2
}

if ($NoAssemble) {
    Write-Host "`nCollectors running. When they finish: python storage_pipeline.py daily" -ForegroundColor Yellow
    return
}

# Wait for the *collector* python processes, not the windows (-NoExit keeps those open).
Write-Host "`nWaiting for collectors to finish..." -ForegroundColor Yellow
$started = Get-Date
do {
    Start-Sleep -Seconds 30
    $running = @()
    foreach ($b in $Brands) {
        $shell = $procs[$b]
        $kids = Get-CimInstance Win32_Process -Filter "ParentProcessId = $($shell.Id)" -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -match "python" }
        if ($kids) { $running += $b }
    }
    $elapsed = [int]((Get-Date) - $started).TotalMinutes
    if ($running) { Write-Host ("  {0,4} min  still running: {1}" -f $elapsed, ($running -join ", ")) }
} while ($running)

Write-Host "`nAll collectors done. Assembling..." -ForegroundColor Green
python storage_pipeline.py daily
Write-Host ""
python storage_pipeline.py audit
Write-Host ""
python storage_pipeline.py status

if ($Serve) {
    Start-Process powershell.exe -ArgumentList "-NoProfile", "-Command", "Set-Location '$repo'; python -m http.server 8777"
    Start-Sleep -Seconds 2
    Start-Process "http://localhost:8777/dashboard.html"
}
