# Cookbook RustDB

Практические примеры для образа **GitHub Container Registry** [`ghcr.io/crosseyedcat/rustdb`](https://github.com/CrossEyedCat/RustDB/pkgs/container/rustdb). Команды ниже **проверялись** через `docker pull` и `docker run` (см. раздел «Проверка»).

## Образ и теги

CI публикует теги вида:

| Тег | Назначение |
|-----|------------|
| `latest` | Ветка по умолчанию (`main`), если включён в metadata |
| `main` | Последняя сборка ветки `main` |
| `main-<git-sha>` | Фиксация на коммит (`type=sha,prefix={{branch}}-` в workflow) |

Подставляйте свой тег или **digest** для воспроизводимости:

```bash
export RUSTDB_IMAGE="ghcr.io/crosseyedcat/rustdb:main"
# или, например: ghcr.io/crosseyedcat/rustdb:main-7a3b2c1d
docker pull "$RUSTDB_IMAGE"
```

**Важно:** бинарь в образе может **отставать** от `main` в git (сборка по расписанию CI). Флаги `rustdb server --help` и порты смотрите в контейнере; для QUIC/фрейминга актуальное описание — в [docs/network/README.md](network/README.md).

---

## 1. Версия

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb --version
```

Ожидаемо: строка вида `rustdb 0.1.0`.

---

## 2. Системная информация

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb info
```

Печатает версию, язык по умолчанию, ОС и архитектуру внутри контейнера (обычно `linux` / `x86_64` или `aarch64`).

---

## 3. Язык интерфейса (i18n)

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb language list
docker run --rm "$RUSTDB_IMAGE" rustdb language show
docker run --rm "$RUSTDB_IMAGE" rustdb language set en
```

---

## 4. Запрос SQL (CLI, упрощённый путь)

В текущем виде подкоманда `query` **не выполняет** полноценный SQL через движок (см. [README](../README.md) — публичный API и e2e в разработке); команда полезна для проверки CLI и локализации.

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb query "SELECT 1"
docker run --rm "$RUSTDB_IMAGE" rustdb query "SELECT 1" -d mydb
```

---

## 5. Создание «базы» (заглушка)

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb create demo --data-dir /app/data
```

Каталог `/app/data` в одноразовом контейнере не сохраняется; для данных смонтируйте volume (см. ниже).

---

## 6. Сервер в фоне (UDP)

Протокол доступа к сетевому слою в актуальном коде — **QUIC поверх UDP** (ALPN `rustdb-v1`). Для публикации порта хоста используйте **`/udp`**.

В **проверенном** образе `rustdb server` по умолчанию слушает порт **8080** (см. `rustdb server --help` внутри контейнера). Пример:

```bash
docker rm -f rustdb-server 2>/dev/null || true
docker run -d --name rustdb-server \
  -p 8080:8080/udp \
  "$RUSTDB_IMAGE" \
  rustdb server --host 0.0.0.0 --port 8080

docker logs rustdb-server
```

Остановка:

```bash
docker stop rustdb-server && docker rm rustdb-server
```

В **свежем коде** репозитория порт по умолчанию в `config.toml` — **5432**; после обновления образа проверьте `--help` и `config.toml` в образе.

---

## 7. Данные и конфиг на диске

Пример с томом и конфигом только для чтения (пути как в [Dockerfile](../Dockerfile)):

```bash
docker run --rm \
  -v rustdb-data:/app/data \
  -v "$(pwd)/config.toml:/app/config/config.toml:ro" \
  "$RUSTDB_IMAGE" \
  rustdb info
```

---

## 8. QUIC-клиент (из исходников репозитория)

Образ на GHCR содержит только бинарь **`rustdb`**. Отдельный пример **`rustdb_quic_client`** собирается из этого же репозитория:

```bash
git clone https://github.com/CrossEyedCat/RustDB.git && cd RustDB
cargo build --release --bin rustdb_quic_client
```

Дальше — по [docs/network/README.md](network/README.md): запуск `rustdb server`, экспорт leaf-сертификата (в новых версиях — `--cert-out`), подключение клиента с `--addr`, `--cert`, `--server-name`.

---

## Проверка (как гоняли при написании cookbook)

Команды проверялись на хосте с Docker после:

```bash
docker pull ghcr.io/crosseyedcat/rustdb:main
```

**Пример digest** на момент проверки (меняется при каждой новой сборке):

```text
ghcr.io/crosseyedcat/rustdb@sha256:1f10f604c6355b6ef93c243139b89c1ad143cfd6a422720adc913e3e7861c3c7
```

Закрепить образ по digest:

```bash
export RUSTDB_IMAGE="ghcr.io/crosseyedcat/rustdb@sha256:1f10f604c6355b6ef93c243139b89c1ad143cfd6a422720adc913e3e7861c3c7"
```

Автоматический прогон тех же шагов на Linux/macOS:

```bash
./scripts/verify-cookbook-docker.sh
```

### Бенчмарк через образ GHCR (QUIC + SQLite)

Скрипт [`scripts/bench_via_ghcr_image.sh`](../scripts/bench_via_ghcr_image.sh): `docker pull`, том с данными, `CREATE`/`INSERT` для `bench_t`, сервер `rustdb server` с `--cert-out`, копирование leaf DER на хост и запуск [`scripts/bench_sqlite_vs_rustdb.py`](../scripts/bench_sqlite_vs_rustdb.py).

```bash
export RUSTDB_IMAGE="ghcr.io/crosseyedcat/rustdb:main"   # или тег вида main-<sha> с страницы пакета
./scripts/bench_via_ghcr_image.sh
```

На **Windows** с Docker Desktop удобнее PowerShell-скрипт (если команда `bash` указывает на WSL без дистрибутива):

```powershell
$env:RUSTDB_IMAGE = "ghcr.io/crosseyedcat/rustdb:main-type-sha"   # при необходимости
.\scripts\bench_via_ghcr_image.ps1
```

Либо Git Bash: `"C:\Program Files\Git\bin\bash.exe" -lc './scripts/bench_via_ghcr_image.sh'`.

Опционально: `POSTGRES_DSN=...` для строки Postgres в отчёте. Результаты: `target/bench_docker_ghcr/bench.md` (или `OUT_DIR`).

---

## См. также

- [README](../README.md) — статус проекта и ограничения тестов
- [Сеть (QUIC)](network/README.md)
- [Dockerfile](../Dockerfile), [docker-compose.yml](../docker-compose.yml) — ориентир для своих compose-стеков (в compose могут быть дополнительные сервисы, не обязательные для минимального cookbook)
