param([switch]$NoOpen)
$ErrorActionPreference = 'Stop'
$helperPath = Join-Path $PSScriptRoot 'cookie-helper'
$resolvedHelperPath = (Resolve-Path -LiteralPath $helperPath).Path
Write-Host ''
Write-Host 'Photoslice: create cookies.json' -ForegroundColor Green
Write-Host '1. Open chrome://extensions (Chrome) or edge://extensions (Edge).'
Write-Host '2. Enable Developer mode, then Load unpacked.'
Write-Host '3. Select this folder:'
Write-Host $resolvedHelperPath -ForegroundColor Yellow
Write-Host '4. Sign in at https://disk.yandex.ru/client/photo in the same browser.'
Write-Host '5. Click the Photoslice extension, then Save cookies.json.'
Write-Host '6. Select the saved JSON file at http://127.0.0.1:8789 .'
Write-Host 'The extension includes HttpOnly cookies. No cookies are sent anywhere.'
if (-not $NoOpen) {
    Start-Process explorer.exe -ArgumentList ('"' + $resolvedHelperPath + '"')
    Start-Process 'https://disk.yandex.ru/client/photo'
}
