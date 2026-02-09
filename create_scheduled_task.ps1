$ErrorActionPreference = 'Stop'

$python = (Get-Command python -ErrorAction Stop).Source
$taskScript = 'Q:\Code\Google_nowcast\google_crawl_nowcast_scheduled_azap_devbox.py'

# Compute local times that correspond to UTC 00:00 and 12:00
$utcBase = [DateTime]::SpecifyKind((Get-Date).Date, [DateTimeKind]::Utc)
$utc00 = [TimeZoneInfo]::ConvertTimeFromUtc($utcBase, [TimeZoneInfo]::Local).ToString('HH:mm')
$utc12 = [TimeZoneInfo]::ConvertTimeFromUtc($utcBase.AddHours(12), [TimeZoneInfo]::Local).ToString('HH:mm')

$tr = '"{0}" "{1}" --mode devbox' -f $python, $taskScript
$user = "$env:USERDOMAIN\$env:USERNAME"

$action = New-ScheduledTaskAction -Execute $python -Argument "$taskScript --mode devbox" -WorkingDirectory "Q:\Code\Google_nowcast"
$trigger1 = New-ScheduledTaskTrigger -Daily -At $utc00
$trigger2 = New-ScheduledTaskTrigger -Daily -At $utc12
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName 'GoogleNowcast DevBox' -Action $action -Trigger @($trigger1, $trigger2) -Principal $principal -Settings $settings -Force

Write-Host "Created single task 'GoogleNowcast DevBox' at local times $utc00 and $utc12 for UTC 00:00/12:00"