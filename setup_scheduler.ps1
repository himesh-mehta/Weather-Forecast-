# Registers the tiered weather-ingestion schedule as Windows Scheduled Tasks (current user).
#
#   powershell -ExecutionPolicy Bypass -File setup_scheduler.ps1           # install / update all 4 tasks
#   powershell -ExecutionPolicy Bypass -File setup_scheduler.ps1 -Remove   # remove all 4 tasks
#   powershell -ExecutionPolicy Bypass -File setup_scheduler.ps1 -NoLive   # skip the 15-minute live task
#
#   Task                         When                    Runs
#   WeatherAggregatorDaily       06:00 (wakes the PC)    everything + daily archive snapshot
#   WeatherAggregatorModels      every 2 h from 07:00    tier B (generous-quota APIs)
#   WeatherAggregatorEvening     18:00                   tier C (strict-quota APIs, global models)
#   WeatherAggregatorLive        every 15 min            tier A (live station readings)
param([switch]$Remove, [switch]$NoLive)

$Tasks = "WeatherAggregatorDaily", "WeatherAggregatorModels", "WeatherAggregatorEvening", "WeatherAggregatorLive"
if ($Remove) {
    foreach ($t in $Tasks) { Unregister-ScheduledTask -TaskName $t -Confirm:$false -ErrorAction SilentlyContinue }
    Write-Output "Removed: $($Tasks -join ', ')"
    return
}

$Dir = $PSScriptRoot
$Script = Join-Path $Dir "scheduler.py"
if (-not (Test-Path $Script)) { throw "scheduler.py not found next to this script." }

# pythonw.exe = Python without a console window, so background runs don't flash a window.
$Pyw = (Get-Command pythonw.exe -ErrorAction SilentlyContinue).Source
if (-not $Pyw) { $Pyw = Join-Path $env:LOCALAPPDATA "Programs\Python\Python314\pythonw.exe" }
if (-not (Test-Path $Pyw)) { throw "pythonw.exe not found - is Python installed?" }

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited
$today = (Get-Date).Date

function Register-Tier($Name, $Tier, $Trigger, [switch]$Wake, $Limit, $Description) {
    $action = New-ScheduledTaskAction -Execute $Pyw -Argument "-X utf8 `"$Script`" --tier $Tier --trigger scheduled" -WorkingDirectory $Dir
    $s = @{ StartWhenAvailable = $true; AllowStartIfOnBatteries = $true; DontStopIfGoingOnBatteries = $true
            ExecutionTimeLimit = (New-TimeSpan -Minutes $Limit); MultipleInstances = "IgnoreNew" }
    if ($Wake) { $s.WakeToRun = $true }
    $settings = New-ScheduledTaskSettingsSet @s
    Register-ScheduledTask -TaskName $Name -Action $action -Trigger $Trigger -Settings $settings `
        -Principal $principal -Description $Description -Force | Out-Null
    $next = (Get-ScheduledTask -TaskName $Name | Get-ScheduledTaskInfo).NextRunTime
    Write-Output ("  {0,-26} tier {1,-4} next run {2}" -f $Name, $Tier, $next)
}

Write-Output "Registering weather tasks (time zone: $((Get-TimeZone).Id)):"
Register-Tier "WeatherAggregatorDaily" "full" (New-ScheduledTaskTrigger -Daily -At "06:00") -Wake 30 `
    "Weather Consensus: 06:00 full fetch of all providers + daily archive snapshot."
Register-Tier "WeatherAggregatorModels" "B" `
    (New-ScheduledTaskTrigger -Once -At $today.AddHours(7) -RepetitionInterval (New-TimeSpan -Hours 2)) 20 `
    "Weather Consensus: every 2 h - generous-quota APIs and rapid-update models."
Register-Tier "WeatherAggregatorEvening" "C" (New-ScheduledTaskTrigger -Daily -At "18:00") 20 `
    "Weather Consensus: 18:00 - strict-quota APIs and the latest global model runs."
if (-not $NoLive) {
    Register-Tier "WeatherAggregatorLive" "A" `
        (New-ScheduledTaskTrigger -Once -At $today.AddMinutes(7) -RepetitionInterval (New-TimeSpan -Minutes 15)) 5 `
        "Weather Consensus: every 15 min - live station readings (Weather Union) + current conditions."
} else {
    Unregister-ScheduledTask -TaskName "WeatherAggregatorLive" -Confirm:$false -ErrorAction SilentlyContinue
}
Write-Output "Logs: $Dir\logs\  (ingestion_*.log for B/C/full, live_*.log for the 15-minute task)"
