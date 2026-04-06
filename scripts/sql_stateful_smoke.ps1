# Stateful SQL smoke tests (Docker + volume). Windows / PowerShell.
#
# Usage:
#   .\scripts\sql_stateful_smoke.ps1
#   $env:RUSTDB_IMAGE = 'ghcr.io/crosseyedcat/rustdb:local'; .\scripts\sql_stateful_smoke.ps1
#   .\scripts\sql_stateful_smoke.ps1 -Compare 'ghcr.io/crosseyedcat/rustdb:local' 'ghcr.io/crosseyedcat/rustdb:main-type-sha'

param(
    [string[]] $Compare
)

# Native commands (docker) write to stderr; do not treat as terminating.
$ErrorActionPreference = 'Continue'
$failures = 0

function Fail([string] $msg) {
    Write-Host "ASSERT FAIL: $msg" -ForegroundColor Red
    $script:failures++
}

function Invoke-RustDbQuery {
    param(
        [string] $Volume,
        [string] $Image,
        [string] $Sql
    )
    # Bash single-quoted literal: env vars are unreliable for multi-word SQL from PowerShell.
    $sq = $Sql -replace "'", "'\''"
    docker run --rm `
        -v "${Volume}:/app/data" `
        $Image `
        sh -c "rustdb --config /app/config/config.toml query '$sq'"
}

function Start-Suite {
    param(
        [string] $Label,
        [string] $Image
    )

    Write-Host ""
    Write-Host "#####################################################################"
    Write-Host "# Image: $Image"
    Write-Host "#####################################################################"

    $vol = "rustdb-ss-$Label-$PID"
    docker volume rm $vol 2>$null | Out-Null
    docker volume create $vol | Out-Null

    try {
        Write-Host "`n==> 1) INSERT -> new container SELECT"
        $o1 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "INSERT INTO ps_a (n) VALUES (10)" 2>&1 | Out-String
        Write-Host $o1
        if ($o1 -notmatch 'rows_affected: 1') { Fail '1: INSERT rows_affected' }
        $o2 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "SELECT n FROM ps_a" 2>&1 | Out-String
        Write-Host $o2
        if ($o2 -notmatch 'Integer\(10\)') { Fail '1: persisted SELECT' }

        Write-Host "`n==> 2) ORDER BY"
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "INSERT INTO ps_a (n) VALUES (5)" | Out-Null
        $o3 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "SELECT n FROM ps_a ORDER BY n" 2>&1 | Out-String
        Write-Host $o3
        if ($o3 -notmatch 'Integer\(5\)') { Fail '2: ORDER BY 5' }
        if ($o3 -notmatch 'Integer\(10\)') { Fail '2: ORDER BY 10' }

        Write-Host "`n==> 3) CREATE TABLE (typed) + INSERT"
        $o4 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "CREATE TABLE ps_ct (x INTEGER)" 2>&1 | Out-String
        Write-Host $o4
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "INSERT INTO ps_ct (x) VALUES (100)" | Out-Null
        $o5 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "SELECT x FROM ps_ct" 2>&1 | Out-String
        Write-Host $o5
        if ($o5 -notmatch 'Integer\(100\)') { Fail '3: ct x=100' }

        Write-Host "`n==> 4) UPDATE"
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "INSERT INTO ps_u (k) VALUES (1)" | Out-Null
        $o6 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "UPDATE ps_u SET k = 2 WHERE k = 1" 2>&1 | Out-String
        Write-Host $o6
        if ($o6 -notmatch 'rows_affected: 1') { Fail '4: UPDATE rows' }
        $o7 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "SELECT k FROM ps_u" 2>&1 | Out-String
        Write-Host $o7
        if ($o7 -notmatch 'Integer\(2\)') { Fail '4: UPDATE readback' }

        Write-Host "`n==> 5) DELETE"
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "INSERT INTO ps_d (n) VALUES (1)" | Out-Null
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "INSERT INTO ps_d (n) VALUES (2)" | Out-Null
        $o8 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "DELETE FROM ps_d WHERE n = 1" 2>&1 | Out-String
        Write-Host $o8
        $o9 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "SELECT n FROM ps_d" 2>&1 | Out-String
        Write-Host $o9
        if ($o9 -notmatch 'Integer\(2\)') { Fail '5: DELETE survivor' }
        if ($o9 -match 'Integer\(1\)') { Fail '5: deleted row visible' }

        Write-Host "`n==> 6) INSERT ... SELECT"
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "INSERT INTO ps_isrc (v) VALUES (7)" | Out-Null
        $o10 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "INSERT INTO ps_idst (v) SELECT v FROM ps_isrc WHERE v = 7" 2>&1 | Out-String
        Write-Host $o10
        $o11 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "SELECT v FROM ps_idst" 2>&1 | Out-String
        Write-Host $o11
        if ($o11 -notmatch 'Integer\(7\)') { Fail '6: INSERT SELECT' }

        Write-Host "`n==> 7) JOIN (soft)"
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "CREATE TABLE ps_j1 (id INTEGER, v INTEGER)" | Out-Null 2>&1
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "CREATE TABLE ps_j2 (id INTEGER, w INTEGER)" | Out-Null 2>&1
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "INSERT INTO ps_j1 (id, v) VALUES (1, 10)" | Out-Null
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "INSERT INTO ps_j2 (id, w) VALUES (1, 20)" | Out-Null
        $o12 = Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "SELECT ps_j1.v, ps_j2.w FROM ps_j1 INNER JOIN ps_j2 ON ps_j1.id = ps_j2.id" 2>&1 | Out-String
        Write-Host $o12
        if ($o12 -match 'Integer\(10\)' -and $o12 -match 'Integer\(20\)') {
            Write-Host 'JOIN OK' -ForegroundColor Green
        }
        else {
            Write-Host 'WARN: JOIN did not return expected rows on this image' -ForegroundColor Yellow
        }

        Write-Host "`n==> 8) BEGIN / COMMIT"
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "BEGIN TRANSACTION" | Out-Null
        Invoke-RustDbQuery -Volume $vol -Image $Image -Sql "COMMIT" | Out-Null
        Write-Host 'noop txn OK'
    }
    finally {
        docker volume rm $vol 2>$null | Out-Null
    }
}

if ($Compare.Count -ge 2) {
    Start-Suite -Label 'a' -Image $Compare[0]
    Start-Suite -Label 'b' -Image $Compare[1]
}
else {
    $img = if ($env:RUSTDB_IMAGE) { $env:RUSTDB_IMAGE } else { 'ghcr.io/crosseyedcat/rustdb:main-type-sha' }
    Start-Suite -Label 'single' -Image $img
}

Write-Host ""
if ($failures -gt 0) {
    Write-Host "DONE: $failures assertion(s) failed" -ForegroundColor Red
    exit 1
}
Write-Host 'DONE: all hard assertions passed' -ForegroundColor Green
exit 0
