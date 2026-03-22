# Покрытие кода (line coverage)



## Цель



CI и локальная проверка: **`cargo llvm-cov --workspace --fail-under-lines 85`** (порог по строкам).



## Команды



```bash

cargo llvm-cov --workspace --summary-only

cargo llvm-cov --workspace --lcov --output-path lcov.info --fail-under-lines 85

```



В `Cargo.toml` в `package.metadata.cargo-llvm-cov.exclude-files` исключены **`src/main.rs`** и **`src/cli.rs`** (точка входа и исполняемые ветки CLI; парсинг CLI всё равно покрывается unit-тестами в `src/cli.rs` для регрессий).



## CI



Job **Code Coverage** в `.github/workflows/ci-cd.yml` использует `--fail-under-lines 85`.



## Если порог не достигается



Имеет смысл дополнительно покрыть в первую очередь:



| Область | Комментарий |

|--------|-------------|

| `src/executor/operators.rs` | Большой файл, много веток операторов |

| `src/storage/schema_manager.rs` | ALTER / валидации |

| `src/planner/optimizer.rs` | Ветки оптимизаций |

| `src/main.rs` | Обычно исключён из отчёта; при необходимости — интеграционные тесты / `cargo run` |

| `src/core/recovery.rs` | Полный цикл с WAL (async, тяжёлые зависимости) |

