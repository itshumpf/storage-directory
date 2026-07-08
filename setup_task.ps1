# Run this once in PowerShell as Administrator
# It sets up a daily 6am task to run the scraper automatically

$taskName = "StorageDirectoryScraper"
$scriptPath = "$PSScriptRoot\daily_update.bat"
$triggerTime = "06:00AM"

# Remove existing task if it exists
Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue

# Create the action
$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$scriptPath`"" `
    -WorkingDirectory $PSScriptRoot

# Create daily trigger at 6am
$trigger = New-ScheduledTaskTrigger -Daily -At $triggerTime

# Settings - run whether logged in or not, wake PC if sleeping
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 10) `
    -StartWhenAvailable `
    -WakeToRun

# Register the task
Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -RunLevel Highest `
    -Force

Write-Host ""
Write-Host "✅ Task '$taskName' scheduled successfully!" -ForegroundColor Green
Write-Host "   Runs daily at $triggerTime" -ForegroundColor Cyan
Write-Host "   Script: $scriptPath" -ForegroundColor Cyan
Write-Host ""
Write-Host "To test it now, run:" -ForegroundColor Yellow
Write-Host "   Start-ScheduledTask -TaskName '$taskName'" -ForegroundColor White
Write-Host ""
Write-Host "To remove it later, run:" -ForegroundColor Yellow
Write-Host "   Unregister-ScheduledTask -TaskName '$taskName' -Confirm:`$false" -ForegroundColor White
