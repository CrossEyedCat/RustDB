# Скрипт развертывания RustDB для Windows
param(
    [Parameter(Position=0)]
    [ValidateSet("deploy", "start", "stop", "restart", "status", "cleanup", "logs")]
    [string]$Action = "deploy"
)

# Функция для логирования
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $color = switch ($Level) {
        "ERROR" { "Red" }
        "WARNING" { "Yellow" }
        "SUCCESS" { "Green" }
        default { "White" }
    }
    
    Write-Host "[$timestamp] $Message" -ForegroundColor $color
}

# Проверка зависимостей
function Test-Dependencies {
    Write-Log "Проверка зависимостей..."
    
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        Write-Log "Docker не установлен" "ERROR"
        exit 1
    }
    
    if (-not (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
        Write-Log "Docker Compose не установлен" "ERROR"
        exit 1
    }
    
    Write-Log "Все зависимости установлены" "SUCCESS"
}

# Создание SSL сертификатов
function New-SSLCertificates {
    Write-Log "Создание SSL сертификатов..."
    
    $certPath = "deployment\ssl\cert.pem"
    $keyPath = "deployment\ssl\key.pem"
    
    if (-not (Test-Path $certPath) -or -not (Test-Path $keyPath)) {
        New-Item -ItemType Directory -Path "deployment\ssl" -Force | Out-Null
        
        # Создание самоподписанного сертификата
        $cert = New-SelfSignedCertificate -DnsName "localhost" -CertStoreLocation "Cert:\CurrentUser\My"
        $certPath = $cert.PSPath
        
        # Экспорт сертификата
        $certBytes = $cert.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Pkcs12)
        $certFile = "deployment\ssl\cert.pfx"
        [System.IO.File]::WriteAllBytes($certFile, $certBytes)
        
        Write-Log "SSL сертификаты созданы" "SUCCESS"
    } else {
        Write-Log "SSL сертификаты уже существуют" "SUCCESS"
    }
}

# Создание директорий для данных
function New-Directories {
    Write-Log "Создание директорий для данных..."
    
    $directories = @("data", "logs", "config")
    foreach ($dir in $directories) {
        if (-not (Test-Path $dir)) {
            New-Item -ItemType Directory -Path $dir | Out-Null
        }
    }
    
    Write-Log "Директории созданы" "SUCCESS"
}

# Сборка Docker образов
function Build-Images {
    Write-Log "Сборка Docker образов..."
    
    docker-compose build --no-cache
    
    if ($LASTEXITCODE -eq 0) {
        Write-Log "Docker образы собраны" "SUCCESS"
    } else {
        Write-Log "Ошибка сборки Docker образов" "ERROR"
        exit 1
    }
}

# Запуск сервисов
function Start-Services {
    Write-Log "Запуск сервисов..."
    
    docker-compose up -d
    
    if ($LASTEXITCODE -eq 0) {
        Write-Log "Сервисы запущены" "SUCCESS"
    } else {
        Write-Log "Ошибка запуска сервисов" "ERROR"
        exit 1
    }
}

# Проверка здоровья сервисов
function Test-Health {
    Write-Log "Проверка здоровья сервисов..."
    
    # Ожидание запуска сервисов
    Start-Sleep -Seconds 30
    
    # Проверка RustDB
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 10
        if ($response.StatusCode -eq 200) {
            Write-Log "RustDB работает" "SUCCESS"
        }
    } catch {
        Write-Log "RustDB не отвечает" "ERROR"
        return $false
    }
    
    # Проверка Prometheus
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:9090/-/healthy" -TimeoutSec 10
        if ($response.StatusCode -eq 200) {
            Write-Log "Prometheus работает" "SUCCESS"
        }
    } catch {
        Write-Log "Prometheus не отвечает" "WARNING"
    }
    
    # Проверка Grafana
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:3000/api/health" -TimeoutSec 10
        if ($response.StatusCode -eq 200) {
            Write-Log "Grafana работает" "SUCCESS"
        }
    } catch {
        Write-Log "Grafana не отвечает" "WARNING"
    }
    
    Write-Log "Проверка здоровья завершена" "SUCCESS"
    return $true
}

# Показать статус
function Show-Status {
    Write-Log "Статус сервисов:"
    docker-compose ps
    
    Write-Host ""
    Write-Log "Доступные сервисы:" "SUCCESS"
    Write-Host "  - RustDB API: http://localhost:8080"
    Write-Host "  - Prometheus: http://localhost:9090"
    Write-Host "  - Grafana: http://localhost:3000 (admin/admin123)"
    Write-Host "  - Jaeger: http://localhost:16686"
    Write-Host "  - Nginx: http://localhost (редирект на HTTPS)"
}

# Остановка сервисов
function Stop-Services {
    Write-Log "Остановка сервисов..."
    docker-compose down
    Write-Log "Сервисы остановлены" "SUCCESS"
}

# Очистка
function Clear-All {
    Write-Log "Очистка..."
    docker-compose down -v
    docker system prune -f
    Write-Log "Очистка завершена" "SUCCESS"
}

# Показать логи
function Show-Logs {
    docker-compose logs -f
}

# Основная функция
function Main {
    switch ($Action) {
        "deploy" {
            Test-Dependencies
            New-SSLCertificates
            New-Directories
            Build-Images
            Start-Services
            Test-Health
            Show-Status
        }
        "start" {
            Start-Services
            Show-Status
        }
        "stop" {
            Stop-Services
        }
        "restart" {
            Stop-Services
            Start-Services
            Show-Status
        }
        "status" {
            Show-Status
        }
        "cleanup" {
            Clear-All
        }
        "logs" {
            Show-Logs
        }
        default {
            Write-Host "Использование: .\deploy.ps1 {deploy|start|stop|restart|status|cleanup|logs}"
            Write-Host ""
            Write-Host "Команды:"
            Write-Host "  deploy   - Полное развертывание (по умолчанию)"
            Write-Host "  start    - Запуск сервисов"
            Write-Host "  stop     - Остановка сервисов"
            Write-Host "  restart  - Перезапуск сервисов"
            Write-Host "  status   - Показать статус"
            Write-Host "  cleanup  - Очистка всех данных"
            Write-Host "  logs     - Показать логи"
            exit 1
        }
    }
}

# Запуск
Main

