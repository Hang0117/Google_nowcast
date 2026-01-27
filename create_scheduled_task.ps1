# Windows 任务计划程序配置脚本
# 每天 UTC 00:00 (北京时间 08:00) 自动执行爬虫任务

$TaskName = "GoogleNowcastDailyCrawler"
$ScriptPath = "Q:\Code\Google_nowcast\google_crawl_nowcast_scheduled._low_frency_devbox.py"
$PythonExe = "C:\Users\v-hangzhang\AppData\Local\Programs\Python\Python313\python.exe"
$LogPath = "Q:\Code\Google_nowcast\scheduler.log"

# 删除已存在的任务
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

# 创建触发器：每天 UTC 00:00
$Trigger = New-ScheduledTaskTrigger -Daily -At "00:00"

# 创建启动脚本，在运行 Python 前设置环境变量
$WrapperScript = "Q:\Code\Google_nowcast\run_scheduled_task.ps1"
@"
# 设置环境变量（二选一）
# 方式1：直接设置 token（不推荐，明文存储）
# `$env:wxforecasting_sas = 'YOUR_SAS_TOKEN_HERE'

# 方式2：从文件读取 token（推荐）
`$env:wxforecasting_sas_file = 'Q:\Code\Google_nowcast\.azure_sas_token'

# 启动 Python 脚本
Set-Location 'Q:\Code\Google_nowcast'
& '$PythonExe' '$ScriptPath'
"@ | Out-File -FilePath $WrapperScript -Encoding UTF8 -Force

# 创建操作：运行包装脚本
$Action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -File `"$WrapperScript`"" `
    -WorkingDirectory "Q:\Code\Google_nowcast"

# 创建任务设置
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

# 注册任务（使用当前用户权限运行）
Register-ScheduledTask `
    -TaskName $TaskName `
    -Trigger $Trigger `
    -Action $Action `
    -Settings $Settings `
    -Description "每天 UTC 00:00 自动爬取 Google Nowcast 数据并上传到 Azure"

Write-Host "✅ 定时任务创建成功！" -ForegroundColor Green
Write-Host "任务名称: $TaskName" -ForegroundColor Cyan
Write-Host "执行时间: 每天 00:00 UTC (北京时间 08:00)" -ForegroundColor Cyan
Write-Host "查看任务: taskschd.msc 或 Get-ScheduledTask -TaskName '$TaskName'" -ForegroundColor Yellow
