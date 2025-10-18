# Многоэтапная сборка для RustDB
FROM rust:1.90-slim as builder

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Создание рабочей директории
WORKDIR /app

# Копирование файлов зависимостей
COPY Cargo.toml Cargo.lock ./

# Создание фиктивного main.rs для кэширования зависимостей
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Сборка зависимостей (кэширование)
RUN cargo build --release && rm -rf src

# Копирование исходного кода
COPY src ./src
COPY examples ./examples

# Сборка приложения
RUN cargo build --release

# Финальный образ
FROM debian:bookworm-slim

# Установка runtime зависимостей
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Создание пользователя для безопасности
RUN groupadd -r rustdb && useradd -r -g rustdb rustdb

# Создание директорий
RUN mkdir -p /app/data /app/logs /app/config && \
    chown -R rustdb:rustdb /app

# Копирование бинарного файла
COPY --from=builder /app/target/release/rustdb /usr/local/bin/rustdb

# Копирование конфигурационных файлов
COPY config.toml /app/config/
COPY --chown=rustdb:rustdb . /app/

# Переключение на пользователя rustdb
USER rustdb

# Рабочая директория
WORKDIR /app

# Открытие портов
EXPOSE 8080 8081

# Переменные окружения
ENV RUST_LOG=info
ENV RUSTDB_DATA_DIR=/app/data
ENV RUSTDB_LOG_DIR=/app/logs
ENV RUSTDB_CONFIG_DIR=/app/config

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD rustdb --version || exit 1

# Команда по умолчанию
CMD ["rustdb", "--config", "/app/config/config.toml"]