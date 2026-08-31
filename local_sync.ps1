# local_sync.ps1 — keep the local clone current, and refresh the private preview.
#
#   .\local_sync.ps1              run once now
#   .\local_sync.ps1 -Install     register it as a daily scheduled task
#   .\local_sync.ps1 -Uninstall   remove that task
#
# WHAT THIS DOES NOT DO
# ---------------------
# It does not scrape, it does not commit, and it does not push. Collection runs
# in GitHub Actions; this only brings the results down and rebuilds the local
# preview from them. Anything that writes to the remote is deliberate and manual.
#
# It also does not build Parquet. `analysis/build_parquet.py` runs as a step in
# .github/workflows/daily.yml and its output is committed to history/parquet/,
# so a pull already brings the partitions with it. Building them again locally
# would only recreate files that are already there.
#
# WHY IT EXISTS
# -------------
# The bot commits daily, so this clone runs behind origin constantly. That has
# already caused one bad session: a diagnosis run against a 12-commit-stale
# clone concluded the workflow had never run and that a real snapshot was a
# phantom, and deleted genuine rows on that basis. A clone that is current by
# default removes the whole class of error.
#
# REFUSES RATHER THAN MERGES
# --------------------------
# If the working tree is dirty it stops and does nothing. A scheduled job must
# never decide what to do with uncommitted work, and --ff-only means it can
# never create a merge commit behind your back either.

param(
    [switch]$Install,
    [switch]$Uninstall
)

$ErrorActionPreference = "Stop"
$repo     = $PSScriptRoot
$taskName = "FindStorageLocalSync"
$logFile  = Join-Path $repo "private\local_sync.log"

function Write-Log($msg) {
    $line = "{0}  {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Write-Host $line
    New-Item -ItemType Directory -Force -Path (Split-Path $logFile) | Out-Null
    Add-Content -Path $logFile -Value $line
}

if ($Uninstall) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Removed scheduled task '$taskName'." -ForegroundColor Green
    return
}

if ($Install) {
    # 7:30am — after the 6am UTC-11 Actions run has had time to finish and push.
    # The scrape itself has run as long as 2h37m, so this deliberately does not
    # sit right behind it.
    $action = New-ScheduledTaskAction -Execute "powershell.exe" `
        -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`"" `
        -WorkingDirectory $repo
    $trigger  = New-ScheduledTaskTrigger -Daily -At "09:30AM"
    # No -WakeToRun: a machine that was asleep can just sync when it wakes.
    # Missing a pull costs nothing, and waking a PC nightly for a git fetch is
    # not a trade worth making.
    $settings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit (New-TimeSpan -Minutes 20) `
        -StartWhenAvailable
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
        -Settings $settings -Force | Out-Null
    Write-Host ""
    Write-Host "Scheduled '$taskName' daily at 9:30am." -ForegroundColor Green
    Write-Host "  pull only - never scrapes, never pushes." -ForegroundColor Cyan
    Write-Host "  log: $logFile" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Test it now:  Start-ScheduledTask -TaskName '$taskName'" -ForegroundColor Yellow
    Write-Host "Remove it:    .\local_sync.ps1 -Uninstall" -ForegroundColor Yellow
    return
}

Set-Location $repo
Write-Log "sync start ($repo)"

# 1. Refuse on a dirty tree.
$dirty = git status --porcelain
if ($dirty) {
    Write-Log "ABORT: working tree has uncommitted changes. Nothing was pulled."
    $dirty -split "`n" | Select-Object -First 15 | ForEach-Object { Write-Log "    $_" }
    exit 1
}

# 2. Fast-forward only.
git fetch --quiet origin
$behind = (git rev-list --count HEAD..origin/main).Trim()
if ($behind -eq "0") {
    Write-Log "already current with origin/main"
} else {
    Write-Log "$behind commit(s) behind origin/main - fast-forwarding"
    git merge --ff-only origin/main
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ABORT: fast-forward failed. Local history has diverged; resolve by hand."
        exit 1
    }
    Write-Log "now at $((git rev-parse --short HEAD).Trim())"
}

# 3. Rebuild the private preview from whatever was just pulled.
#    Non-fatal: a preview that fails to build must not look like a failed sync.
Write-Log "rebuilding private/preview/"
python analysis/build_preview.py 2>&1 | ForEach-Object { Write-Log "    $_" }
if ($LASTEXITCODE -ne 0) {
    Write-Log "WARNING: build_preview.py exited $LASTEXITCODE - clone is still current."
}

Write-Log "sync done"
