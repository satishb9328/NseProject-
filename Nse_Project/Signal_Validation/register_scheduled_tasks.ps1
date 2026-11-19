# ============================================================================
# Signal Validation System - Windows Task Scheduler registration
# ============================================================================
# Registers two daily tasks (times are LOCAL SYSTEM time; set your PC to IST):
#
#   1. "SignalValidation Logger"    daily 09:05, long-running (7h max)
#      -> starts the read-only live logger before the 09:15 market open and
#         lets Task Scheduler stop it after 7 hours (09:05 -> 16:05).
#
#   2. "SignalValidation Evaluator" daily 15:50, batch (30 min max)
#      -> runs the offline evaluator after the 15:30 market close.
#
# Usage (PowerShell):
#   powershell -NoProfile -ExecutionPolicy Bypass -File register_scheduled_tasks.ps1
#   powershell -NoProfile -ExecutionPolicy Bypass -File register_scheduled_tasks.ps1 -Unregister
#
# Tasks are registered for the CURRENT user (interactive token). If you need
# them to run while logged off, register with -User/-Password or -User SYSTEM
# (SYSTEM would also need access to the D: drive and the .venv).
# ============================================================================

param(
    [switch]$Unregister
)

$ErrorActionPreference = "Stop"

$ProjectDir = "C:\Users\ADMIN\Desktop\MyProject2"
$SV = Join-Path $ProjectDir "Nse_Project\Signal_Validation"

$LoggerTask    = "SignalValidation Logger"
$EvaluatorTask = "SignalValidation Evaluator"

function Register-SvTask {
    param(
        [string]$Name,
        [string]$Bat,
        [string]$At,
        [timespan]$ExecutionLimit,
        [int]$RestartCount
    )
    $action = New-ScheduledTaskAction -Execute $Bat -WorkingDirectory $SV
    $trigger = New-ScheduledTaskTrigger -Daily -At $At
    $settings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit $ExecutionLimit `
        -MultipleInstances IgnoreNew `
        -StartWhenAvailable `
        -RestartCount $RestartCount `
        -RestartInterval (New-TimeSpan -Seconds 60)
    Register-ScheduledTask -TaskName $Name -Action $action -Trigger $trigger `
        -Settings $settings -Description "Signal Validation System" -Force | Out-Null
    Write-Host "Registered: $Name (daily $At)"
}

if ($Unregister) {
    Unregister-ScheduledTask -TaskName $LoggerTask -Confirm:$false -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $EvaluatorTask -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Unregistered: $LoggerTask and $EvaluatorTask"
} else {
    Register-SvTask -Name $LoggerTask `
        -Bat (Join-Path $SV "run_logger.bat") `
        -At "09:05" `
        -ExecutionLimit (New-TimeSpan -Hours 7) `
        -RestartCount 3

    Register-SvTask -Name $EvaluatorTask `
        -Bat (Join-Path $SV "run_evaluator.bat") `
        -At "15:50" `
        -ExecutionLimit (New-TimeSpan -Minutes 30) `
        -RestartCount 1

    Write-Host "Done. Tasks run as the current user (interactive)."
}
