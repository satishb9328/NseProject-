# PowerShell script to create a Windows Task Scheduler task
# This task runs the preopen data fetcher at 9:08 AM every weekday

# Configuration
$TaskName = "NSE_Preopen_Data_Fetcher"
$Description = "Fetches NSE preopen market data at 9:08 AM on trading days"
$ScriptPath = "$PSScriptRoot\fetch_and_import_preopen.py"
$PythonPath = "C:\Users\ADMIN\Desktop\Api Project V SYNTH V\Api Project V 1\.venv\Scripts\python.exe"
$WorkingDirectory = "$PSScriptRoot"

# Check if Python executable exists
if (-not (Test-Path $PythonPath)) {
    Write-Host "ERROR: Python not found at: $PythonPath" -ForegroundColor Red
    Write-Host "Please update the `$PythonPath variable in this script with the correct path to your Python executable." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Searching for Python in common locations..." -ForegroundColor Cyan

    # Try to find Python
    $possiblePaths = @(
        "python",
        "$env:USERPROFILE\Desktop\Api Project AO\.venv\Scripts\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python*\python.exe"
    )

    foreach ($path in $possiblePaths) {
        $resolved = Get-Command $path -ErrorAction SilentlyContinue
        if ($resolved) {
            Write-Host "Found Python at: $($resolved.Source)" -ForegroundColor Green
            break
        }
    }
    exit 1
}

# Check if scheduler script exists
if (-not (Test-Path $ScriptPath)) {
    Write-Host "ERROR: Scheduler script not found at: $ScriptPath" -ForegroundColor Red
    exit 1
}

Write-Host "="*70 -ForegroundColor Cyan
Write-Host "NSE Preopen Data Fetcher - Task Scheduler Setup" -ForegroundColor Cyan
Write-Host "="*70 -ForegroundColor Cyan
Write-Host ""
Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Task Name:   $TaskName"
Write-Host "  Schedule:    Daily at 9:08 AM (Monday-Friday)"
Write-Host "  Script:      $ScriptPath"
Write-Host "  Python:      $PythonPath"
Write-Host "  Working Dir: $WorkingDirectory"
Write-Host ""

# Check if task already exists
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue

if ($existingTask) {
    Write-Host "Task '$TaskName' already exists." -ForegroundColor Yellow
    $response = Read-Host "Do you want to update it? (Y/N)"
    if ($response -ne 'Y' -and $response -ne 'y') {
        Write-Host "Setup cancelled." -ForegroundColor Red
        exit 0
    }
    Write-Host "Unregistering existing task..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create the scheduled task action
$Action = New-ScheduledTaskAction -Execute $PythonPath `
    -Argument "`"$ScriptPath`"" `
    -WorkingDirectory $WorkingDirectory

# Create the trigger (daily at 9:08 AM, Monday through Friday)
$Trigger = New-ScheduledTaskTrigger -Weekly -At "09:08AM" -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday

# Create the task settings
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

# Create the principal (run whether user is logged on or not)
$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U -RunLevel Highest

# Check if running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host ""
    Write-Host "ERROR: This script must be run as Administrator!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please run this script again with Administrator privileges:" -ForegroundColor Yellow
    Write-Host "  1. Right-click on setup_scheduler.bat" -ForegroundColor Cyan
    Write-Host "  2. Select 'Run as Administrator'" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Or run PowerShell as Administrator and execute:" -ForegroundColor Yellow
    Write-Host "  .\setup_scheduler.ps1" -ForegroundColor Cyan
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

# Register the scheduled task
try {
    Register-ScheduledTask -TaskName $TaskName `
        -Action $Action `
        -Trigger $Trigger `
        -Settings $Settings `
        -Principal $Principal `
        -Description $Description `
        -Force | Out-Null

    Write-Host ""
    Write-Host "[SUCCESS] Task '$TaskName' has been created successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Task Details:" -ForegroundColor Cyan
    Write-Host "  - Runs daily at 9:08 AM (Monday-Friday)"
    Write-Host "  - Automatically checks for weekends and NSE holidays"
    Write-Host "  - Logs saved to: $WorkingDirectory\scheduler.log"
    Write-Host ""

    # Verify the task was created
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($task) {
        $taskInfo = Get-ScheduledTaskInfo -TaskName $TaskName
        Write-Host "Task Status: " -NoNewline -ForegroundColor Cyan
        Write-Host "$($task.State)" -ForegroundColor Green
        if ($taskInfo.NextRunTime) {
            Write-Host "Next Run: " -NoNewline -ForegroundColor Cyan
            Write-Host "$($taskInfo.NextRunTime)" -ForegroundColor Green
        }
    }

    Write-Host ""
    Write-Host "To manage the task:" -ForegroundColor Yellow
    Write-Host "  - Open Task Scheduler (taskschd.msc)"
    Write-Host "  - Or run: Get-ScheduledTask -TaskName '$TaskName'"
    Write-Host ""
    Write-Host "To test the task manually:" -ForegroundColor Yellow
    Write-Host "  Start-ScheduledTask -TaskName '$TaskName'"
    Write-Host ""
    Write-Host "To disable the task:" -ForegroundColor Yellow
    Write-Host "  Disable-ScheduledTask -TaskName '$TaskName'"
    Write-Host ""
    Write-Host "To remove the task:" -ForegroundColor Yellow
    Write-Host "  Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false"
    Write-Host ""
    Read-Host "Press Enter to exit"

} catch {
    Write-Host ""
    Write-Host "[ERROR] Failed to create scheduled task" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}
