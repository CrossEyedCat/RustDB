# Покрытие кода

## Цель

- **CI:** job **Coverage** в `.github/workflows/ci-cd.yml` — `cargo llvm-cov` → **`lcov.info`** → [Codecov](https://codecov.io).
- **Порог ~85%** по строкам: локально через `cargo llvm-cov report`, на сайте — см. **`codecov.yml`** (`coverage.status.project`).

## Файлы

| Файл | Назначение |
|------|------------|
| **`codecov.yml`** | Официальный конфиг Codecov ([документация](https://docs.codecov.com/docs/codecov-yaml)): пороги, комментарии к PR. Проверка: `curl --data-binary @codecov.yml https://codecov.io/validate` |
| **`lcov.info`** | Генерируется в CI и не коммитится; загружается action’ом `codecov/codecov-action@v5` |

## Локальные команды

```bash
cargo llvm-cov --workspace --summary-only

cargo llvm-cov --workspace --lcov --output-path lcov.info \
  --ignore-filename-regex '<как COV_REGEX в workflow>'
```

Исключения путей для порога задаются через **`--ignore-filename-regex`** в CI (`COV_REGEX` в job **Coverage**).

## CI (стандартный поток)

1. **`actions/checkout@v4`** с **`fetch-depth: 0`** — полная история git помогает Codecov сопоставлять коммиты и уменьшает «Missing Head Report».
2. **`cargo llvm-cov --workspace --lcov --output-path lcov.info`** + `COV_REGEX` — **без** `--fail-under-lines`, чтобы всегда получить файл и успеть загрузить его.
3. **Upload:** только **`lcov.info`**, **`disable_search: true`** (иначе CLI подмешивает файлы вроде `*_coverage_tests.rs` по маске `*coverage*`).
4. **`cargo llvm-cov report --fail-under-lines 85`** — проверка порога после загрузки.

Секрет репозитория: **`CODECOV_TOKEN`**.

## Если порог не достигается

| Область | Комментарий |
|---------|-------------|
| `src/executor/operators.rs` | Много веток операторов |
| `src/storage/schema_manager.rs` | ALTER / валидации |
| `src/planner/optimizer.rs` | Ветки оптимизаций |
| `src/core/recovery.rs` | WAL / async |
