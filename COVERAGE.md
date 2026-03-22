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
2. Загрузка **`lcov.info`** в [Codecov](https://codecov.io) (`codecov/codecov-action@v5` с **`disable_search: true`** и **`plugins: noop`**, чтобы не подхватывались файлы вроде `*_coverage_tests.rs` по маске `*coverage*` — иначе отчёт на сайте может не собраться («Missing Head Report»)). Нужен секрет **`CODECOV_TOKEN`**.
3. **`cargo llvm-cov report --fail-under-lines 85`** (без `--workspace`: у подкоманды `report` этого флага нет) — проверка порога по уже собранным данным.

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

