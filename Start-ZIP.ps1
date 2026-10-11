param([switch]$Trial, [ValidateRange(1,500)][int]$BatchSize = 500, [string]$Service = 'http://127.0.0.1:8789')
$ErrorActionPreference = 'Stop'
$endpoint = [Uri]$Service
if (-not $endpoint.IsLoopback -or $endpoint.Scheme -ne 'http') { throw 'Нужен адрес локального сервиса.' }
$status = Invoke-RestMethod -Uri "$Service/api/status" -TimeoutSec 10
$headers = @{'X-Backup-Token' = $status.token}
if ($status.running) {
    Write-Host 'Останавливаю текущую очередь. Сохранённые версии остаются на месте.'
    Invoke-RestMethod -Uri "$Service/api/stop" -Method Post -Headers $headers -TimeoutSec 10 | Out-Null
    $deadline = [DateTime]::UtcNow.AddSeconds(60)
    do {
        Start-Sleep -Milliseconds 300
        $status = Invoke-RestMethod -Uri "$Service/api/status" -TimeoutSec 10
        if ([DateTime]::UtcNow -gt $deadline) { throw 'Очередь ещё останавливается. Повтори запуск после остановки.' }
    } while ($status.running)
}
$action = if ($Trial) { 'zip-test' } else { 'zip' }
if ($Trial -and -not $PSBoundParameters.ContainsKey('BatchSize')) { $BatchSize = 20 }
$config = @{batch_size=$BatchSize;max_batches=$(if ($Trial) { 1 } else { 0 })} | ConvertTo-Json -Compress
Invoke-RestMethod -Uri "$Service/api/$action" -Method Post -Headers $headers -ContentType 'application/json' -Body $config -TimeoutSec 10 | Out-Null
Write-Host $(if ($Trial) { "Запущена проба одного ZIP из $BatchSize фотографий." } else { "Запущена последовательная загрузка ZIP до $BatchSize фотографий." })
Write-Host "Прогресс: $Service"
