# QUIC network SQL smoke test (Windows PowerShell).
# Starts `rustdb server` inside Docker, copies dev TLS leaf certificate (DER),
# then sends SQL queries via host-built `rustdb_quic_client`.
#
# Usage:
#   powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\sql_quic_smoke.ps1
#   $env:RUSTDB_IMAGE='ghcr.io/crosseyedcat/rustdb:main-type-sha'; .\scripts\sql_quic_smoke.ps1
#
# Notes:
# - We connect to 127.0.0.1:<HOST_PORT> but validate TLS name "localhost" because when the server
#   is started with `--host 0.0.0.0`, it generates a dev cert for localhost.
param(
    [string] $Image = $(if ($env:RUSTDB_IMAGE) { $env:RUSTDB_IMAGE } else { "ghcr.io/crosseyedcat/rustdb:main-type-sha" }),
    [int] $HostPort = $(if ($env:HOST_PORT) { [int]$env:HOST_PORT } else { 15432 }),
    [string] $DialHost = $(if ($env:DIAL_HOST) { $env:DIAL_HOST } else { "127.0.0.1" }),
    [string] $TlsServerName = $(if ($env:TLS_SERVER_NAME) { $env:TLS_SERVER_NAME } else { "localhost" })
)

# Native commands (docker/cargo) may write to stderr even on non-fatal paths (e.g. rm missing container).
# Keep going and assert via exit codes where needed.
$ErrorActionPreference = "Continue"

$name = "rustdb-quic-smoke-$PID"
$vol = "rustdb-quic-smoke-vol-$PID"
$certDir = Join-Path $env:TEMP "rustdb-quic-smoke-$PID"
$certPath = Join-Path $certDir "server.der"

function Cleanup {
    docker rm -f $name 2>$null | Out-Null
    docker volume rm -f $vol 2>$null | Out-Null
    if (Test-Path $certDir) { Remove-Item -Recurse -Force $certDir -ErrorAction SilentlyContinue }
}

try {
    New-Item -ItemType Directory -Force -Path $certDir | Out-Null

    Write-Host "==> pull $Image"
    docker pull $Image | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "docker pull failed" }

    Write-Host "==> create volume $vol"
    docker volume rm -f $vol 2>$null | Out-Null
    docker volume create $vol | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "docker volume create failed" }

    Write-Host "==> start server container ($name) udp:$HostPort->5432"
    docker rm -f $name 2>$null | Out-Null
    docker run -d --name $name `
        -p "$HostPort`:5432/udp" `
        -v "$vol`:/app/data" `
        $Image `
        sh -c "rustdb --config /app/config/config.toml server --host 0.0.0.0 --port 5432 --cert-out /tmp/server.der" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "docker run (server) failed" }

    Write-Host "==> wait for /tmp/server.der"
    $ok = $false
    for ($i = 0; $i -lt 60; $i++) {
        docker exec $name sh -c "test -s /tmp/server.der" 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { $ok = $true; break }
        Start-Sleep -Milliseconds 200
    }
    if (-not $ok) { throw "server did not write /tmp/server.der" }

    Write-Host "==> copy server.der"
    docker cp "${name}:/tmp/server.der" $certPath | Out-Null
    if (-not (Test-Path $certPath)) { throw "cert not copied" }

    Write-Host "==> build rustdb_quic_client (host)"
    Push-Location (Split-Path -Parent $PSScriptRoot)
    try {
        cargo build -q --bin rustdb_quic_client
    } finally {
        Pop-Location
    }

    $clientExe = Join-Path (Join-Path (Split-Path -Parent $PSScriptRoot) "target\\debug") "rustdb_quic_client.exe"
    if (-not (Test-Path $clientExe)) { throw "client exe not found: $clientExe" }

    function Run-Client([string] $sql) {
        Write-Host ""
        Write-Host ">>> $sql"
        & $clientExe --addr "$DialHost`:$HostPort" --cert $certPath --server-name $TlsServerName $sql
        if ($LASTEXITCODE -ne 0) { throw "client failed: $sql" }
    }

    Write-Host "==> network queries"
    Run-Client "SELECT 1"
    Run-Client "INSERT INTO net_t (a) VALUES (10)"
    Run-Client "SELECT a FROM net_t ORDER BY a"
    Run-Client "UPDATE net_t SET a = 11 WHERE a = 10"
    Run-Client "SELECT a FROM net_t ORDER BY a"
    Run-Client "DELETE FROM net_t WHERE a = 11"
    Run-Client "SELECT a FROM net_t"

    Write-Host ""
    Write-Host "==> OK"
}
finally {
    Cleanup
}

