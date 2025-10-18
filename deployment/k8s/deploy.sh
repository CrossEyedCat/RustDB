#!/bin/bash

# Скрипт развертывания RustDB в Kubernetes
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

# Проверка kubectl
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        error "kubectl не установлен"
        exit 1
    fi
    
    if ! kubectl cluster-info &> /dev/null; then
        error "Нет подключения к кластеру Kubernetes"
        exit 1
    fi
    
    log "kubectl настроен корректно"
}

# Применение манифестов
apply_manifests() {
    log "Применение манифестов Kubernetes..."
    
    # Создание namespace
    kubectl apply -f namespace.yaml
    
    # Применение ConfigMap и Secret
    kubectl apply -f configmap.yaml
    kubectl apply -f secret.yaml
    
    # Создание PVC
    kubectl apply -f pvc.yaml
    
    # Развертывание приложения
    kubectl apply -f rustdb-deployment.yaml
    kubectl apply -f rustdb-service.yaml
    
    # Применение Ingress и HPA
    kubectl apply -f ingress.yaml
    kubectl apply -f hpa.yaml
    
    log "Манифесты применены"
}

# Ожидание готовности
wait_for_ready() {
    log "Ожидание готовности подов..."
    
    kubectl wait --for=condition=ready pod -l app=rustdb -n rustdb --timeout=300s
    
    log "Поды готовы"
}

# Проверка статуса
check_status() {
    log "Проверка статуса развертывания..."
    
    echo ""
    echo "=== Namespace ==="
    kubectl get namespace rustdb
    
    echo ""
    echo "=== Pods ==="
    kubectl get pods -n rustdb
    
    echo ""
    echo "=== Services ==="
    kubectl get services -n rustdb
    
    echo ""
    echo "=== Ingress ==="
    kubectl get ingress -n rustdb
    
    echo ""
    echo "=== HPA ==="
    kubectl get hpa -n rustdb
}

# Показать логи
show_logs() {
    log "Показ логов RustDB..."
    kubectl logs -l app=rustdb -n rustdb --tail=100 -f
}

# Удаление развертывания
delete_deployment() {
    log "Удаление развертывания..."
    
    kubectl delete -f hpa.yaml --ignore-not-found=true
    kubectl delete -f ingress.yaml --ignore-not-found=true
    kubectl delete -f rustdb-service.yaml --ignore-not-found=true
    kubectl delete -f rustdb-deployment.yaml --ignore-not-found=true
    kubectl delete -f pvc.yaml --ignore-not-found=true
    kubectl delete -f secret.yaml --ignore-not-found=true
    kubectl delete -f configmap.yaml --ignore-not-found=true
    kubectl delete -f namespace.yaml --ignore-not-found=true
    
    log "Развертывание удалено"
}

# Масштабирование
scale_deployment() {
    local replicas=$1
    if [ -z "$replicas" ]; then
        error "Не указано количество реплик"
        exit 1
    fi
    
    log "Масштабирование до $replicas реплик..."
    kubectl scale deployment rustdb -n rustdb --replicas=$replicas
    log "Масштабирование завершено"
}

# Основная функция
main() {
    case "${1:-deploy}" in
        "deploy")
            check_kubectl
            apply_manifests
            wait_for_ready
            check_status
            ;;
        "status")
            check_status
            ;;
        "logs")
            show_logs
            ;;
        "delete")
            delete_deployment
            ;;
        "scale")
            scale_deployment $2
            ;;
        *)
            echo "Использование: $0 {deploy|status|logs|delete|scale <replicas>}"
            echo ""
            echo "Команды:"
            echo "  deploy           - Развертывание в Kubernetes (по умолчанию)"
            echo "  status           - Показать статус развертывания"
            echo "  logs             - Показать логи"
            echo "  delete           - Удалить развертывание"
            echo "  scale <replicas> - Масштабировать развертывание"
            exit 1
            ;;
    esac
}

# Обработка сигналов
trap 'error "Прерывание выполнения"; exit 1' INT TERM

# Запуск
main "$@"

