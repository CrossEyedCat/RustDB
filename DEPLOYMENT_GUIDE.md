# Руководство по развертыванию RustDB

## Обзор

RustDB теперь полностью готов к развертыванию в различных средах. Система включает в себя:

- 🐳 **Контейнеризация** - Docker образы с многоэтапной сборкой
- 🚀 **CI/CD** - Автоматизированные пайплайны GitHub Actions
- ☸️ **Kubernetes** - Полная поддержка оркестрации
- 📊 **Мониторинг** - Prometheus, Grafana, Jaeger
- 🔒 **Безопасность** - SSL/TLS, секреты, rate limiting
- 📈 **Масштабирование** - HPA, load balancing

## Быстрый старт

### 1. Локальная разработка (Docker Compose)

```bash
# Клонирование репозитория
git clone https://github.com/your-org/rustdb.git
cd rustdb

# Развертывание (Linux/macOS)
chmod +x deployment/deploy.sh
./deployment/deploy.sh deploy

# Развертывание (Windows)
.\deployment\deploy.ps1 deploy
```

### 2. Продакшн (Kubernetes)

```bash
# Развертывание в Kubernetes
cd deployment/k8s
chmod +x deploy.sh
./deploy.sh deploy
```

## Архитектура развертывания

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Nginx       │    │     RustDB      │    │     Redis       │
│  Load Balancer  │───▶│   Application   │───▶│     Cache       │
│   SSL/TLS       │    │   (3 replicas)  │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    Prometheus   │    │     Grafana     │    │     Jaeger      │
│    Metrics      │    │   Dashboards    │    │   Tracing       │
│   Collection    │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Компоненты системы

### Основные сервисы

| Сервис | Порт | Описание |
|--------|------|----------|
| RustDB API | 8080 | Основной API сервер |
| RustDB gRPC | 8081 | gRPC интерфейс |
| Nginx | 80/443 | Load balancer и SSL терминация |
| Redis | 6379 | Кэширование |
| Prometheus | 9090 | Сбор метрик |
| Grafana | 3000 | Визуализация |
| Jaeger | 16686 | Трассировка |

### Мониторинг

- **Prometheus** - Сбор метрик производительности
- **Grafana** - Дашборды для визуализации
- **Jaeger** - Распределенная трассировка запросов
- **Health checks** - Проверки здоровья всех сервисов

## Конфигурация

### Переменные окружения

```bash
# Основные настройки
RUST_LOG=info
RUSTDB_DATA_DIR=/app/data
RUSTDB_LOG_DIR=/app/logs
RUSTDB_CONFIG_DIR=/app/config

# Безопасность
RUSTDB_API_KEY=your-secret-key
RUSTDB_ADMIN_PASSWORD=secure-password

# Производительность
RUSTDB_MAX_CONNECTIONS=100
RUSTDB_CONNECTION_TIMEOUT=30
RUSTDB_QUERY_TIMEOUT=60
```

### Конфигурационный файл

```toml
# config.toml
name = "rustdb"
data_directory = "/app/data"
max_connections = 100
connection_timeout = 30
query_timeout = 60
language = "en"  # "en" или "ru"
```

## Безопасность

### SSL/TLS

- Автоматическая генерация самоподписанных сертификатов для разработки
- Поддержка Let's Encrypt для продакшена
- Принудительное перенаправление HTTP → HTTPS

### Аутентификация

- API ключи для доступа к API
- Административные пароли для Grafana
- Kubernetes Secrets для хранения секретов

### Сетевая безопасность

- Rate limiting (10 req/s для API, 1 req/s для логина)
- Security headers (HSTS, X-Frame-Options, etc.)
- Network policies в Kubernetes

## Масштабирование

### Горизонтальное масштабирование

```yaml
# HPA конфигурация
minReplicas: 2
maxReplicas: 10
targetCPUUtilizationPercentage: 70
targetMemoryUtilizationPercentage: 80
```

### Вертикальное масштабирование

```yaml
# Ресурсы контейнера
resources:
  requests:
    memory: "256Mi"
    cpu: "250m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

## Мониторинг и алертинг

### Ключевые метрики

- **Производительность**: Request rate, response time, throughput
- **Ресурсы**: CPU, memory, disk usage
- **Соединения**: Active connections, connection pool
- **Ошибки**: Error rate, failed requests

### Дашборды Grafana

1. **RustDB Dashboard** - Основные метрики производительности
2. **System Overview** - Обзор системы и ресурсов
3. **Error Analysis** - Анализ ошибок и исключений

### Алерты

- Высокое использование CPU (>80%)
- Высокое использование памяти (>90%)
- Высокая частота ошибок (>5%)
- Медленные запросы (>1s)

## Резервное копирование

### Docker Compose

```bash
# Создание бэкапа
docker run --rm -v rustdb_data:/data -v $(pwd):/backup alpine \
  tar czf /backup/rustdb-backup-$(date +%Y%m%d).tar.gz -C /data .

# Восстановление
docker run --rm -v rustdb_data:/data -v $(pwd):/backup alpine \
  tar xzf /backup/rustdb-backup-20231201.tar.gz -C /data
```

### Kubernetes

```bash
# Создание снапшота
kubectl create -f - <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: rustdb-snapshot
  namespace: rustdb
spec:
  source:
    persistentVolumeClaimName: rustdb-data-pvc
EOF
```

## CI/CD Pipeline

### GitHub Actions Workflows

1. **CI/CD Pipeline** (`.github/workflows/ci-cd.yml`)
   - Тестирование на multiple Rust версиях
   - Сборка и публикация Docker образов
   - Security scanning
   - Автоматическое развертывание

2. **Release Pipeline** (`.github/workflows/release.yml`)
   - Создание релизов по тегам
   - Публикация в Docker Hub
   - Генерация release notes

### Этапы пайплайна

1. **Test** - Запуск всех тестов
2. **Build** - Сборка приложения
3. **Docker** - Сборка и публикация образов
4. **Security** - Проверка уязвимостей
5. **Deploy** - Развертывание в staging/production

## Устранение неполадок

### Частые проблемы

1. **Порты заняты**
   ```bash
   # Проверка занятых портов
   netstat -tulpn | grep :8080
   
   # Изменение портов в docker-compose.yml
   ports:
     - "8081:8080"  # Внешний:Внутренний
   ```

2. **Недостаточно памяти**
   ```bash
   # Увеличение лимитов в Kubernetes
   kubectl patch deployment rustdb -n rustdb -p '{"spec":{"template":{"spec":{"containers":[{"name":"rustdb","resources":{"limits":{"memory":"1Gi"}}}]}}}}'
   ```

3. **SSL ошибки**
   ```bash
   # Проверка сертификатов
   openssl x509 -in deployment/ssl/cert.pem -text -noout
   ```

### Логи и диагностика

```bash
# Docker Compose
docker-compose logs -f rustdb

# Kubernetes
kubectl logs -l app=rustdb -n rustdb -f

# Проверка здоровья
curl http://localhost:8080/health
```

## Производительность

### Рекомендуемые настройки

- **CPU**: 2+ cores для продакшена
- **Memory**: 4GB+ RAM
- **Storage**: SSD для данных
- **Network**: 1Gbps+ для высоконагруженных систем

### Оптимизация

- Включение gzip сжатия в Nginx
- Настройка connection pooling
- Использование Redis для кэширования
- Оптимизация запросов к базе данных

## Поддержка

### Документация

- [API Reference](API_REFERENCE.md)
- [Technical Documentation](TECHNICAL_DOCUMENTATION.md)
- [User Guide](USER_GUIDE.md)

### Контакты

- GitHub Issues: [Создать issue](https://github.com/your-org/rustdb/issues)
- Email: support@rustdb.com
- Discord: [RustDB Community](https://discord.gg/rustdb)

---

**RustDB готов к продакшену!** 🚀

Система развертывания обеспечивает высокую доступность, безопасность и масштабируемость для любых нагрузок.

