# Многоэтапная сборка для оптимизации размера образа

# Этап сборки
FROM rust:1.89-slim as builder

# Установка зависимостей для сборки
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Создание рабочей директории
WORKDIR /app

# Копирование файлов конфигурации для кэширования зависимостей
COPY Cargo.toml Cargo.lock ./

# Создание пустого проекта для кэширования зависимостей
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs

# Сборка зависимостей
RUN cargo build --release && \
    rm -rf src

# Копирование исходного кода
COPY src/ src/
COPY benches/ benches/

# Пересборка только нашего кода
RUN touch src/main.rs && \
    cargo build --release

# Этап выполнения
FROM debian:bookworm-slim as runtime

# Установка runtime зависимостей
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Создание пользователя для безопасности
RUN useradd -r -s /bin/false rustbd

# Создание рабочей директории
WORKDIR /app

# Копирование бинарного файла
COPY --from=builder /app/target/release/rustbd /usr/local/bin/rustbd

# Создание директории для данных
RUN mkdir -p /data && \
    chown rustbd:rustbd /data

# Переключение на непривилегированного пользователя
USER rustbd

# Настройка переменных окружения
ENV RUST_LOG=info
ENV RUSTBD_DATA_DIR=/data

# Открытие порта (если будет сетевой интерфейс)
EXPOSE 5432

# Настройка volume для данных
VOLUME ["/data"]

# Проверка здоровья контейнера
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD rustbd info || exit 1

# Запуск приложения
ENTRYPOINT ["rustbd"]
CMD ["--help"]

# Метаданные
LABEL maintainer="CrossEyedCat"
LABEL description="RustBD - Реляционная база данных на Rust"
LABEL version="0.1.0"
LABEL org.opencontainers.image.source="https://github.com/CrossEyedCat/RustDB"
