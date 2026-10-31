# run-docker.ps1
# Khoi chay Hanoi Traffic Analytics bang Docker Compose.
# Cach dung: .\run-docker.ps1
# Neu PowerShell chan script: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$DashboardUrl         = "http://localhost:8501/"
$HealthUrl            = "http://localhost:8501/_stcore/health"
$HealthTimeoutSeconds = 45

function Write-Section {
    param([string]$Text)
    Write-Host ""
    Write-Host "== $Text ==" -ForegroundColor Cyan
}

# ---------------------------------------------------------------------------
# 1. Kiem tra Docker Desktop / Docker Engine
# ---------------------------------------------------------------------------
Write-Section "Kiem tra Docker"

$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
if (-not $dockerCommand) {
    Write-Host "[LOI] Khong tim thay lenh 'docker'." -ForegroundColor Red
    Write-Host "      Hay cai Docker Desktop: https://www.docker.com/products/docker-desktop/" -ForegroundColor Yellow
    exit 1
}

$dockerReady = $false
try {
    docker info *> $null
    $dockerReady = ($LASTEXITCODE -eq 0)
}
catch {
    $dockerReady = $false
}

if (-not $dockerReady) {
    Write-Host "[LOI] Docker Desktop chua chay hoac Docker Engine chua san sang." -ForegroundColor Red
    Write-Host "      Hay mo Docker Desktop, doi den khi bieu tuong o trang thai 'Engine running'," -ForegroundColor Yellow
    Write-Host "      roi chay lai: .\run-docker.ps1" -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Docker da san sang." -ForegroundColor Green

# Docker Compose v2 duoc goi qua plugin "docker compose".
try {
    docker compose version *> $null
}
catch {
    Write-Host "[LOI] Khong tim thay Docker Compose v2 ('docker compose')." -ForegroundColor Red
    Write-Host "      Hay cap nhat Docker Desktop de co Compose v2." -ForegroundColor Yellow
    exit 1
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "[LOI] Khong tim thay Docker Compose v2 ('docker compose')." -ForegroundColor Red
    Write-Host "      Hay cap nhat Docker Desktop de co Compose v2." -ForegroundColor Yellow
    exit 1
}

# ---------------------------------------------------------------------------
# 2. Chon che do chay
# ---------------------------------------------------------------------------
Write-Section "Chon che do chay"
Write-Host "  1. Full System    - dashboard + collector (thu thap du lieu that qua API)"
Write-Host "  2. Offline Demo   - chi build va chay dashboard, khong goi API"
Write-Host ""

$choice = ""
while ($choice -ne "1" -and $choice -ne "2") {
    $choice = Read-Host "Nhap lua chon (1 hoac 2)"
    $choice = "$choice".Trim()
    if ($choice -ne "1" -and $choice -ne "2") {
        Write-Host "  Lua chon khong hop le, vui long nhap 1 hoac 2." -ForegroundColor Yellow
    }
}

# ---------------------------------------------------------------------------
# 3. Don dep container cu
# ---------------------------------------------------------------------------
Write-Section "Don dep container cu"
docker compose down
if ($LASTEXITCODE -ne 0) {
    Write-Host "[CANH BAO] 'docker compose down' tra ve loi. Tiep tuc khoi chay..." -ForegroundColor Yellow
}

# ---------------------------------------------------------------------------
# 4. Build + khoi chay
# ---------------------------------------------------------------------------
if ($choice -eq "1") {
    Write-Section "Khoi chay Full System (dashboard + collector)"
    docker compose up --build -d
}
else {
    Write-Section "Khoi chay Offline Demo (chi dashboard)"
    docker compose up --build -d dashboard
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "[LOI] 'docker compose up' that bai. Xem log phia tren de biet chi tiet." -ForegroundColor Red
    exit 1
}

# ---------------------------------------------------------------------------
# 5. Cho dashboard san sang (toi da 45 giay)
# ---------------------------------------------------------------------------
Write-Section "Cho dashboard san sang (toi da $HealthTimeoutSeconds giay)"

Write-Host "Dang thu $HealthUrl ..."
$deadline = (Get-Date).AddSeconds($HealthTimeoutSeconds)
$isReady = $false

while ((Get-Date) -lt $deadline) {
    try {
        $response = Invoke-WebRequest -Uri $HealthUrl -UseBasicParsing -TimeoutSec 3
        if ($response.StatusCode -eq 200) {
            $isReady = $true
            break
        }
    }
    catch {
        # Dashboard con dang khoi dong; thu lai.
    }
    Start-Sleep -Milliseconds 1500
}

if (-not $isReady) {
    Write-Host ""
    Write-Host "[LOI] Dashboard chua san sang sau $HealthTimeoutSeconds giay." -ForegroundColor Red
    Write-Host "      Kiem tra log: docker compose logs dashboard" -ForegroundColor Yellow
    Write-Host "      Kiem tra trang thai: docker compose ps" -ForegroundColor Yellow
    exit 1
}

# ---------------------------------------------------------------------------
# 6. Thanh cong
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "[OK] Dashboard da san sang tai $DashboardUrl" -ForegroundColor Green
if ($choice -eq "1") {
    Write-Host "     Collector dang chay: python historical_collector.py --max-cycles 100" -ForegroundColor Green
    Write-Host "     Xem log collector: docker compose logs -f collector" -ForegroundColor DarkGray
}
else {
    Write-Host "     Che do Offline Demo: chi dashboard, khong goi API." -ForegroundColor Green
}
Write-Host "     Dung he thong: docker compose down" -ForegroundColor DarkGray
Write-Host ""

Start-Process $DashboardUrl
