# Покрытие кода (line coverage)



## Цель



CI и локальная проверка: **`cargo llvm-cov --workspace --fail-under-lines 85`** (порог по строкам).



## Команды



```bash

cargo llvm-cov --workspace --summary-only

cargo llvm-cov --workspace --lcov --output-path lcov.info --fail-under-lines 85

```



Исключения путей для расчёта порога задаются в CI через **`--ignore-filename-regex`** (в т.ч. `src/cli.rs`, `src/main.rs` и ряд крупных модулей).



## CI

В `.github/workflows/ci-cd.yml` отдельный job **`Coverage (lines ≥85%)`** (Ubuntu):

1. **`cargo llvm-cov --workspace --lcov --output-path lcov.info`** с **`--ignore-filename-regex`** — тесты и **lcov без** `--fail-under-lines`, чтобы отчёт всегда генерировался.
2. Загрузка **`lcov.info`** в [Codecov](https://codecov.io) (`codecov/codecov-action@v5`). Нужен секрет **`CODECOV_TOKEN`** в настройках репозитория.
3. **`cargo llvm-cov report --workspace --fail-under-lines 85`** — отдельная проверка порога по уже собранным данным.

Если **`--fail-under-lines`** стоит в одной команде с **`--lcov`**, при провале порога процесс завершается с ошибкой **до** шага загрузки — на Codecov для коммита не будет отчёта («Missing report»).



## Если порог не достигается



Имеет смысл дополнительно покрыть в первую очередь:



| Область | Комментарий |

|--------|-------------|

| `src/executor/operators.rs` | Большой файл, много веток операторов |

| `src/storage/schema_manager.rs` | ALTER / валидации |

| `src/planner/optimizer.rs` | Ветки оптимизаций |

| `src/main.rs` | Обычно исключён из отчёта; при необходимости — интеграционные тесты / `cargo run` |

| `src/core/recovery.rs` | Полный цикл с WAL (async, тяжёлые зависимости) |

