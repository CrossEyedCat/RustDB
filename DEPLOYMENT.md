# Руководство по развертыванию RustBD

## 🚀 Обзор

Это руководство описывает процесс развертывания RustBD в различных средах, от локальной разработки до продакшн-среды.

## 📋 Требования

### Системные требования

#### Минимальные требования
- **ОС**: Linux (Ubuntu 20.04+, CentOS 8+), macOS 10.15+, Windows 10+
- **CPU**: 2 ядра, 2.0 GHz
- **RAM**: 4 GB
- **Диск**: 20 GB свободного места
- **Сеть**: 100 Mbps

#### Рекомендуемые требования
- **ОС**: Linux (Ubuntu 22.04+, CentOS 9+), macOS 12+, Windows 11+
- **CPU**: 4+ ядра, 3.0+ GHz
- **RAM**: 8+ GB
- **Диск**: 100+ GB SSD
- **Сеть**: 1 Gbps

### Зависимости

#### Rust
```bash
# Установка Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Проверка версии
rustc --version  # Должна быть 1.70+
cargo --version  # Должна быть 1.70+
```

#### Системные библиотеки (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    libclang-dev \
    clang \
    cmake \
    git
```

#### Системные библиотеки (CentOS/RHEL)
```bash
sudo yum groupinstall -y "Development Tools"
sudo yum install -y \
    openssl-devel \
    clang-devel \
    cmake \
    git
```

#### Системные библиотеки (macOS)
```bash
# Установка Homebrew (если не установлен)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Установка зависимостей
brew install \
    openssl \
    cmake \
    llvm \
    git
```

## 🏗️ Сборка из исходного кода

### Клонирование репозитория
```bash
git clone https://github.com/your-org/rustbd.git
cd rustbd
```

### Сборка проекта
```bash
# Сборка в режиме отладки
cargo build

# Сборка в режиме релиза
cargo build --release

# Сборка с оптимизациями
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### Запуск тестов
```bash
# Запуск всех тестов
cargo test

# Запуск тестов с выводом
cargo test -- --nocapture

# Запуск конкретного теста
cargo test test_function_name

# Запуск бенчмарков
cargo bench
```

## 🐳 Развертывание с Docker

### Сборка Docker образа
```bash
# Сборка образа
docker build -t rustbd:latest .

# Сборка с тегами версий
docker build -t rustbd:v1.0.0 .
docker build -t rustbd:latest .
```

### Запуск контейнера
```bash
# Простой запуск
docker run -d \
    --name rustbd \
    -p 5432:5432 \
    -v rustbd_data:/data \
    rustbd:latest

# Запуск с переменными окружения
docker run -d \
    --name rustbd \
    -p 5432:5432 \
    -e RUSTBD_HOST=0.0.0.0 \
    -e RUSTBD_PORT=5432 \
    -e RUSTBD_DATABASE=mydb \
    -e RUSTBD_USERNAME=admin \
    -e RUSTBD_PASSWORD=secret \
    -v rustbd_data:/data \
    rustbd:latest
```

### Docker Compose
```yaml
# docker-compose.yml
version: '3.8'

services:
  rustbd:
    build: .
    container_name: rustbd
    ports:
      - "5432:5432"
    environment:
      - RUSTBD_HOST=0.0.0.0
      - RUSTBD_PORT=5432
      - RUSTBD_DATABASE=mydb
      - RUSTBD_USERNAME=admin
      - RUSTBD_PASSWORD=secret
    volumes:
      - rustbd_data:/data
      - ./config:/etc/rustbd
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "rustbd", "health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  rustbd_data:
```

## 🚀 Развертывание в продакшн

### Системная служба (systemd)

#### Создание файла службы
```ini
# /etc/systemd/system/rustbd.service
[Unit]
Description=RustBD Database Server
After=network.target
Wants=network.target

[Service]
Type=simple
User=rustbd
Group=rustbd
WorkingDirectory=/opt/rustbd
ExecStart=/opt/rustbd/rustbd --config /etc/rustbd/config.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=rustbd

# Безопасность
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/rustbd /var/log/rustbd

[Install]
WantedBy=multi-user.target
```

#### Управление службой
```bash
# Создание пользователя
sudo useradd -r -s /bin/false rustbd

# Создание директорий
sudo mkdir -p /opt/rustbd /var/lib/rustbd /var/log/rustbd /etc/rustbd
sudo chown rustbd:rustbd /opt/rustbd /var/lib/rustbd /var/log/rustbd /etc/rustbd

# Копирование файлов
sudo cp target/release/rustbd /opt/rustbd/
sudo cp config/config.toml /etc/rustbd/

# Включение и запуск службы
sudo systemctl daemon-reload
sudo systemctl enable rustbd
sudo systemctl start rustbd

# Проверка статуса
sudo systemctl status rustbd
```

### Конфигурация продакшн

#### Основной конфигурационный файл
```toml
# /etc/rustbd/config.toml
[server]
host = "0.0.0.0"
port = 5432
max_connections = 100
connection_timeout = 30
idle_timeout = 300

[database]
data_directory = "/var/lib/rustbd"
log_directory = "/var/log/rustbd"
max_file_size = "1GB"
checkpoint_interval = 300

[security]
ssl_enabled = true
ssl_cert_file = "/etc/rustbd/ssl/server.crt"
ssl_key_file = "/etc/rustbd/ssl/server.key"
ssl_ca_file = "/etc/rustbd/ssl/ca.crt"

[logging]
level = "info"
file = "/var/log/rustbd/rustbd.log"
max_size = "100MB"
max_files = 10
format = "json"

[performance]
buffer_size = "2GB"
max_workers = 8
query_cache_size = "256MB"
```

#### SSL сертификаты
```bash
# Создание директории для SSL
sudo mkdir -p /etc/rustbd/ssl

# Генерация приватного ключа
sudo openssl genrsa -out /etc/rustbd/ssl/server.key 2048

# Генерация CSR
sudo openssl req -new -key /etc/rustbd/ssl/server.key \
    -out /etc/rustbd/ssl/server.csr \
    -subj "/C=RU/ST=Moscow/L=Moscow/O=YourOrg/CN=rustbd.example.com"

# Генерация самоподписанного сертификата (для тестирования)
sudo openssl x509 -req -days 365 \
    -in /etc/rustbd/ssl/server.csr \
    -signkey /etc/rustbd/ssl/server.key \
    -out /etc/rustbd/ssl/server.crt

# Установка прав доступа
sudo chown -R rustbd:rustbd /etc/rustbd/ssl
sudo chmod 600 /etc/rustbd/ssl/server.key
sudo chmod 644 /etc/rustbd/ssl/server.crt
```

## 🔒 Безопасность

### Сетевая безопасность

#### Firewall (UFW)
```bash
# Установка UFW
sudo apt install ufw

# Настройка правил
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow ssh
sudo ufw allow 5432/tcp

# Включение firewall
sudo ufw enable
```

#### Firewall (firewalld)
```bash
# Настройка firewalld
sudo firewall-cmd --permanent --add-service=postgresql
sudo firewall-cmd --permanent --add-port=5432/tcp
sudo firewall-cmd --reload
```

### Аутентификация и авторизация

#### Создание администратора
```bash
# Подключение к базе данных
rustbd-cli --host localhost --port 5432 --username admin --password secret

# Создание администратора
CREATE USER admin WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE * TO admin;
```

#### Настройка ролей
```sql
-- Создание роли для приложения
CREATE ROLE app_user;
GRANT CONNECT ON DATABASE mydb TO app_user;
GRANT USAGE ON SCHEMA public TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user;

-- Создание пользователя приложения
CREATE USER myapp WITH PASSWORD 'app_password';
GRANT app_user TO myapp;
```

## 📊 Мониторинг и логирование

### Логирование

#### Настройка logrotate
```bash
# /etc/logrotate.d/rustbd
/var/log/rustbd/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 rustbd rustbd
    postrotate
        systemctl reload rustbd
    endscript
}
```

#### Централизованное логирование
```bash
# Установка rsyslog
sudo apt install rsyslog

# Настройка rsyslog для RustBD
echo "local0.* /var/log/rustbd/rustbd.log" | sudo tee -a /etc/rsyslog.conf

# Перезапуск rsyslog
sudo systemctl restart rsyslog
```

### Мониторинг

#### Prometheus экспортер
```toml
# /etc/rustbd/config.toml
[monitoring]
prometheus_enabled = true
prometheus_port = 9090
metrics_interval = 15
```

#### Grafana дашборд
```json
{
  "dashboard": {
    "title": "RustBD Metrics",
    "panels": [
      {
        "title": "Active Connections",
        "type": "stat",
        "targets": [
          {
            "expr": "rustbd_active_connections",
            "legendFormat": "Connections"
          }
        ]
      },
      {
        "title": "Query Performance",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(rustbd_queries_total[5m])",
            "legendFormat": "Queries/sec"
          }
        ]
      }
    ]
  }
}
```

## 🔄 Обновления и миграции

### Стратегия обновления

#### Blue-Green развертывание
```bash
# Остановка старой версии
sudo systemctl stop rustbd

# Резервное копирование данных
sudo -u rustbd pg_dump -h localhost -U admin mydb > backup_$(date +%Y%m%d_%H%M%S).sql

# Обновление бинарного файла
sudo cp rustbd_new /opt/rustbd/rustbd
sudo chown rustbd:rustbd /opt/rustbd/rustbd

# Запуск новой версии
sudo systemctl start rustbd

# Проверка работоспособности
sudo systemctl status rustbd
rustbd-cli --host localhost --port 5432 --username admin --password secret -c "SELECT version();"
```

#### Rolling обновление
```bash
# Обновление одной ноды за раз
for node in node1 node2 node3; do
    echo "Обновление $node..."
    ssh $node "sudo systemctl stop rustbd"
    scp rustbd_new $node:/opt/rustbd/rustbd
    ssh $node "sudo systemctl start rustbd"
    sleep 30
done
```

### Миграции схемы

#### Создание миграции
```sql
-- migrations/001_create_users_table.sql
BEGIN;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_email ON users(email);

COMMIT;
```

#### Применение миграции
```bash
# Применение миграции
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -f migrations/001_create_users_table.sql

# Проверка статуса
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "SELECT * FROM information_schema.tables WHERE table_name = 'users';"
```

## 🧪 Тестирование развертывания

### Проверка работоспособности

#### Базовые тесты
```bash
# Проверка подключения
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "SELECT 1 as test;"

# Проверка создания таблицы
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(50));"

# Проверка вставки данных
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "INSERT INTO test VALUES (1, 'test');"

# Проверка выборки данных
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "SELECT * FROM test;"

# Очистка
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "DROP TABLE test;"
```

#### Нагрузочное тестирование
```bash
# Установка pgbench
sudo apt install postgresql-client

# Создание тестовых данных
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "CREATE TABLE pgbench_accounts (aid INTEGER PRIMARY KEY, bid INTEGER, abalance INTEGER, filler CHAR(84));"

# Запуск теста
pgbench -h localhost -p 5432 -U admin -d mydb -c 10 -t 1000 -f pgbench_test.sql
```

### Мониторинг производительности

#### Проверка метрик
```bash
# Проверка метрик Prometheus
curl http://localhost:9090/metrics | grep rustbd

# Проверка логов
tail -f /var/log/rustbd/rustbd.log | grep -E "(ERROR|WARN|INFO)"
```

## 🚨 Устранение неполадок

### Общие проблемы

#### Проблемы с подключением
```bash
# Проверка статуса службы
sudo systemctl status rustbd

# Проверка портов
sudo netstat -tlnp | grep 5432

# Проверка логов
sudo journalctl -u rustbd -f

# Проверка конфигурации
rustbd --config /etc/rustbd/config.toml --check-config
```

#### Проблемы с производительностью
```bash
# Проверка использования ресурсов
htop
iostat -x 1
iotop

# Проверка метрик базы данных
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "SELECT * FROM pg_stat_database;"
```

#### Проблемы с диском
```bash
# Проверка свободного места
df -h

# Проверка inode
df -i

# Проверка производительности диска
dd if=/dev/zero of=/tmp/test bs=1M count=1000
```

### Восстановление после сбоя

#### Восстановление из резервной копии
```bash
# Остановка службы
sudo systemctl stop rustbd

# Восстановление данных
sudo -u rustbd rustbd-restore -h localhost -U admin -d mydb backup.sql

# Запуск службы
sudo systemctl start rustbd

# Проверка восстановления
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "SELECT COUNT(*) FROM users;"
```

#### Восстановление после повреждения
```bash
# Проверка целостности
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "CHECK TABLE users;"

# Восстановление таблицы
rustbd-cli --host localhost --port 5432 --username admin --password secret \
    -c "REPAIR TABLE users;"
```

## 📚 Дополнительные ресурсы

- [Архитектура системы](ARCHITECTURE.md)
- [Руководство по разработке](DEVELOPMENT.md)
- [Стандарты кодирования](CODING_STANDARDS.md)
- [API справочник](API_REFERENCE.md)
- [Примеры использования](EXAMPLES.md)

## 🤝 Поддержка

При возникновении проблем:

1. Проверьте логи: `sudo journalctl -u rustbd -f`
2. Обратитесь к разделу "Устранение неполадок"
3. Создайте issue в репозитории проекта
4. Обратитесь к сообществу разработчиков

Для получения дополнительной помощи обратитесь к документации проекта или создайте issue в репозитории.
