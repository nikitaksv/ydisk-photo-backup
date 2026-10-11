param([string]$OutputDirectory = 'D:\YandexPhotoBackup')
$ErrorActionPreference = 'Stop'
$taskExecutable = Join-Path $PSScriptRoot 'photoslice-go-ready.exe'
if (-not (Test-Path -LiteralPath $taskExecutable)) { throw 'photoslice-go-ready.exe is missing. Build with: go build -o photoslice-go-ready.exe .' }
$taskRoot = [IO.Path]::GetFullPath($OutputDirectory)
if ($taskRoot -eq [IO.Path]::GetPathRoot($taskRoot)) { throw 'Select a folder, not a drive root.' }
$taskURL = 'http://127.0.0.1:8789'
function Get-BackupStatus {
    try { return Invoke-RestMethod -Uri ($taskURL + '/api/status') -TimeoutSec 2 } catch { return $null }
}
$taskStatus = Get-BackupStatus
if ($null -ne $taskStatus) {
    if ($taskStatus.out -ne $taskRoot) { throw ('Another backup folder is already open: ' + $taskStatus.out) }
    Start-Process $taskURL
    return
}
New-Item -ItemType Directory -Path $taskRoot -Force | Out-Null
$taskProcess = Start-Process -FilePath $taskExecutable -ArgumentList ('serve -listen 127.0.0.1:8789 -out "' + $taskRoot + '"') -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $taskRoot 'console.log') -RedirectStandardError (Join-Path $taskRoot 'console-error.log') -PassThru
for ($taskAttempt = 0; $taskAttempt -lt 40; $taskAttempt++) {
    $taskStatus = Get-BackupStatus
    if ($null -ne $taskStatus) { Start-Process $taskURL; return }
    if ($taskProcess.HasExited) { throw 'The server stopped. See console-error.log in the backup folder.' }
    Start-Sleep -Milliseconds 250
}
throw 'Server startup timed out. See console-error.log in the backup folder.'
