$url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
$output = "C:\Program Files\MyProject2\NseUtility\NSE_CM.csv"

# ఫోల్డర్ లేకపోతే ఆటోమేటిక్‌గా క్రియేట్ చేస్తుంది
$dir = [System.IO.Path]::GetDirectoryName($output)
if (!(Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir }

Write-Host "Downloading EQUITY_L.csv from NSE..."
Invoke-WebRequest -Uri $url -OutFile $output
Write-Host "Download Completed Successfully! Saved to: $output" -ForegroundColor Green