# Развертывание RustDB

Этот каталог содержит все необходимые файлы и скрипты для развертывания RustDB в различных средах.

## Структура каталога

```
deployment/
├── README.md                    # Этот файл
├── deploy.sh                    # Скрипт развертывания для Linux/macOS
├── deploy.ps1                   # Скрипт развертывания для Windows
├── docker-compose.yml           # Docker Compose конфигурация
├── prometheus.yml               # Конфигурация Prometheus
├── nginx.conf                   # Конфигурация Nginx
├── ssl/                         # SSL сертификаты
├── grafana/                     # Конфигурация Grafana
│   ├── dashboards/              # Дашборды
│   └── datasources/             # Источники данных
└── k8s/                         # Конфигурации Kubernetes
    ├── namespace.yaml
    ├── configmap.yaml
    ├── secret.yaml
    ├── rustdb-deployment.yaml
    ├── rustdb-service.yaml
    ├── pvc.yaml
    ├── ingress.yaml
    ├── hpa.yaml
    └── deploy.sh
```

## Быстрый старт

### Docker Compose (рекомендуется для разработки)

1. **Linux/macOS:**
   ```bash
   chmod +x deploy.sh
   ./deploy.sh deploy
   ```

2. **Windows:**
   ```powershell
   .\deploy.ps1 deploy
   ```

### Kubernetes (для продакшена)

```bash
cd k8s
chmod +x deploy.sh
./deploy.sh deploy
```

## Доступные сервисы

После развертывания будут доступны следующие сервисы:

| Сервис | URL | Описание |
|--------|-----|----------|
| RustDB API | http://localhost:8080 | Основной API сервер |
| Prometheus | http://localhost:9090 | Мониторинг метрик |
| Grafana | http://localhost:3000 | Дашборды (admin/admin123) |
| Jaeger | http://localhost:16686 | Трассировка запросов |
| Nginx | http://localhost | Load balancer (редирект на HTTPS) |

## Команды управления

### Docker Compose

```bash
# Полное развертывание
./deploy.sh deploy

# Запуск сервисов
./deploy.sh start

# Остановка сервисов
./deploy.sh stop

# Перезапуск
./deploy.sh restart

# Показать статус
./deploy.sh status

# Показать логи
./deploy.sh logs

# Очистка
./deploy.sh cleanup
```

### Kubernetes

```bash
cd k8s

# Развертывание
./deploy.sh deploy

# Показать статус
./deploy.sh status

# Показать логи
./deploy.sh logs

# Масштабирование
./deploy.sh scale 5

# Удаление
./deploy.sh delete
```

## Конфигурация

### Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `RUST_LOG` | Уровень логирования | `info` |
| `RUSTDB_DATA_DIR` | Директория данных | `/app/data` |
| `RUSTDB_LOG_DIR` | Директория логов | `/app/logs` |
| `RUSTDB_CONFIG_DIR` | Директория конфигурации | `/app/config` |

### Конфигурационный файл

Основные настройки находятся в `config.toml`:

```toml
name = "rustdb"
data_directory = "./data"
max_connections = 100
connection_timeout = 30
query_timeout = 60
language = "en"  # Поддерживаемые языки: "en", "ru"
```

## Мониторинг

### Prometheus метрики

RustDB экспортирует следующие метрики:

- `rustdb_requests_total` - Общее количество запросов
- `rustdb_request_duration_seconds` - Время выполнения запросов
- `rustdb_active_connections` - Активные соединения
- `rustdb_memory_usage_bytes` - Использование памяти
- `rustdb_disk_usage_bytes` - Использование диска

### Grafana дашборды

Предустановленные дашборды:
- **RustDB Dashboard** - Основные метрики производительности
- **System Overview** - Обзор системы
- **Error Analysis** - Анализ ошибок

## Безопасность

### SSL/TLS

Для продакшена рекомендуется использовать валидные SSL сертификаты:

```bash
# Создание Let's Encrypt сертификата
certbot certonly --standalone -d your-domain.com
```

### Секреты

В Kubernetes секреты хранятся в `k8s/secret.yaml`. Обязательно измените значения по умолчанию:

```bash
# Генерация нового API ключа
openssl rand -base64 32
```

## Масштабирование

### Горизонтальное масштабирование

В Kubernetes используется HPA (Horizontal Pod Autoscaler):

```yaml
minReplicas: 2
maxReplicas: 10
targetCPUUtilizationPercentage: 70
targetMemoryUtilizationPercentage: 80
```

### Вертикальное масштабирование

Настройки ресурсов в `rustdb-deployment.yaml`:

```yaml
resources:
  requests:
    memory: "256Mi"
    cpu: "250m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

## Резервное копирование

### Docker Compose

```bash
# Создание бэкапа данных
docker run --rm -v rustdb_data:/data -v $(pwd):/backup alpine tar czf /backup/rustdb-backup-$(date +%Y%m%d).tar.gz -C /data .

# Восстановление из бэкапа
docker run --rm -v rustdb_data:/data -v $(pwd):/backup alpine tar xzf /backup/rustdb-backup-20231201.tar.gz -C /data
```

### Kubernetes

```bash
# Создание снапшота PVC
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

## Устранение неполадок

### Проверка логов

```bash
# Docker Compose
docker-compose logs -f rustdb

# Kubernetes
kubectl logs -l app=rustdb -n rustdb -f
```

### Проверка здоровья

```bash
# HTTP health check
curl http://localhost:8080/health

# Kubernetes health check
kubectl get pods -n rustdb
kubectl describe pod <pod-name> -n rustdb
```

### Частые проблемы

1. **Порты заняты**: Измените порты в `docker-compose.yml`
2. **Недостаточно памяти**: Увеличьте лимиты в Kubernetes
3. **SSL ошибки**: Проверьте сертификаты в `deployment/ssl/`

## Поддержка

Для получения помощи:
- Создайте issue в GitHub репозитории
- Проверьте логи приложения
- Обратитесь к документации API

