#!/bin/bash

# Скрипт развертывания RustDB
set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функция для логирования
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}" >&2
}

warning() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

# Проверка зависимостей
check_dependencies() {
    log "Проверка зависимостей..."
    
    if ! command -v docker &> /dev/null; then
        error "Docker не установлен"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose не установлен"
        exit 1
    fi
    
    log "Все зависимости установлены"
}

# Создание SSL сертификатов
create_ssl_certificates() {
    log "Создание SSL сертификатов..."
    
    if [ ! -f "deployment/ssl/cert.pem" ] || [ ! -f "deployment/ssl/key.pem" ]; then
        mkdir -p deployment/ssl
        openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
            -keyout deployment/ssl/key.pem \
            -out deployment/ssl/cert.pem \
            -subj "/C=RU/ST=Moscow/L=Moscow/O=RustDB/CN=localhost"
        log "SSL сертификаты созданы"
    else
        log "SSL сертификаты уже существуют"
    fi
}

# Создание директорий для данных
create_directories() {
    log "Создание директорий для данных..."
    
    mkdir -p data logs config
    chmod 755 data logs config
    
    log "Директории созданы"
}

# Сборка Docker образов
build_images() {
    log "Сборка Docker образов..."
    
    docker-compose build --no-cache
    
    log "Docker образы собраны"
}

# Запуск сервисов
start_services() {
    log "Запуск сервисов..."
    
    docker-compose up -d
    
    log "Сервисы запущены"
}

# Проверка здоровья сервисов
health_check() {
    log "Проверка здоровья сервисов..."
    
    # Ожидание запуска сервисов
    sleep 30
    
    # Проверка RustDB
    if curl -f http://localhost:8080/health > /dev/null 2>&1; then
        log "RustDB работает"
    else
        error "RustDB не отвечает"
        return 1
    fi
    
    # Проверка Prometheus
    if curl -f http://localhost:9090/-/healthy > /dev/null 2>&1; then
        log "Prometheus работает"
    else
        warning "Prometheus не отвечает"
    fi
    
    # Проверка Grafana
    if curl -f http://localhost:3000/api/health > /dev/null 2>&1; then
        log "Grafana работает"
    else
        warning "Grafana не отвечает"
    fi
    
    log "Проверка здоровья завершена"
}

# Показать статус
show_status() {
    log "Статус сервисов:"
    docker-compose ps
    
    echo ""
    log "Доступные сервисы:"
    echo "  - RustDB API: http://localhost:8080"
    echo "  - Prometheus: http://localhost:9090"
    echo "  - Grafana: http://localhost:3000 (admin/admin123)"
    echo "  - Jaeger: http://localhost:16686"
    echo "  - Nginx: http://localhost (редирект на HTTPS)"
}

# Остановка сервисов
stop_services() {
    log "Остановка сервисов..."
    docker-compose down
    log "Сервисы остановлены"
}

# Очистка
cleanup() {
    log "Очистка..."
    docker-compose down -v
    docker system prune -f
    log "Очистка завершена"
}

# Основная функция
main() {
    case "${1:-deploy}" in
        "deploy")
            check_dependencies
            create_ssl_certificates
            create_directories
            build_images
            start_services
            health_check
            show_status
            ;;
        "start")
            start_services
            show_status
            ;;
        "stop")
            stop_services
            ;;
        "restart")
            stop_services
            start_services
            show_status
            ;;
        "status")
            show_status
            ;;
        "cleanup")
            cleanup
            ;;
        "logs")
            docker-compose logs -f
            ;;
        *)
            echo "Использование: $0 {deploy|start|stop|restart|status|cleanup|logs}"
            echo ""
            echo "Команды:"
            echo "  deploy   - Полное развертывание (по умолчанию)"
            echo "  start    - Запуск сервисов"
            echo "  stop     - Остановка сервисов"
            echo "  restart  - Перезапуск сервисов"
            echo "  status   - Показать статус"
            echo "  cleanup  - Очистка всех данных"
            echo "  logs     - Показать логи"
            exit 1
            ;;
    esac
}

# Обработка сигналов
trap 'error "Прерывание выполнения"; exit 1' INT TERM

# Запуск
main "$@"

