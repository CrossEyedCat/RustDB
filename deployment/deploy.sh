#!/bin/bash

# RustDB deployment helper (Docker Compose)
set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}" >&2
}

warning() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

check_dependencies() {
    log "Checking dependencies..."
    
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed"
        exit 1
    fi
    
    log "All dependencies are present"
}

create_ssl_certificates() {
    log "Creating SSL certificates..."
    
    if [ ! -f "deployment/ssl/cert.pem" ] || [ ! -f "deployment/ssl/key.pem" ]; then
        mkdir -p deployment/ssl
        openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
            -keyout deployment/ssl/key.pem \
            -out deployment/ssl/cert.pem \
            -subj "/C=RU/ST=Moscow/L=Moscow/O=RustDB/CN=localhost"
        log "SSL certificates created"
    else
        log "SSL certificates already exist"
    fi
}

create_directories() {
    log "Creating data directories..."
    
    mkdir -p data logs config
    chmod 755 data logs config
    
    log "Directories created"
}

build_images() {
    log "Building Docker images..."
    
    docker-compose build --no-cache
    
    log "Docker images built"
}

start_services() {
    log "Starting services..."
    
    docker-compose up -d
    
    log "Services started"
}

health_check() {
    log "Running health checks..."
    
    sleep 30
    
    if curl -f http://localhost:8080/health > /dev/null 2>&1; then
        log "RustDB is up"
    else
        error "RustDB is not responding"
        return 1
    fi
    
    if curl -f http://localhost:9090/-/healthy > /dev/null 2>&1; then
        log "Prometheus is up"
    else
        warning "Prometheus is not responding"
    fi
    
    if curl -f http://localhost:3000/api/health > /dev/null 2>&1; then
        log "Grafana is up"
    else
        warning "Grafana is not responding"
    fi
    
    log "Health checks finished"
}

show_status() {
    log "Service status:"
    docker-compose ps
    
    echo ""
    log "Endpoints:"
    echo "  - RustDB API: http://localhost:8080"
    echo "  - Prometheus: http://localhost:9090"
    echo "  - Grafana: http://localhost:3000 (admin/admin123)"
    echo "  - Jaeger: http://localhost:16686"
    echo "  - Nginx: http://localhost (redirects to HTTPS)"
}

stop_services() {
    log "Stopping services..."
    docker-compose down
    log "Services stopped"
}

cleanup() {
    log "Cleaning up..."
    docker-compose down -v
    docker system prune -f
    log "Cleanup finished"
}

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
            echo "Usage: $0 {deploy|start|stop|restart|status|cleanup|logs}"
            echo ""
            echo "Commands:"
            echo "  deploy   - Full deployment (default)"
            echo "  start    - Start services"
            echo "  stop     - Stop services"
            echo "  restart  - Restart services"
            echo "  status   - Show status"
            echo "  cleanup  - Remove volumes and prune"
            echo "  logs     - Follow logs"
            exit 1
            ;;
    esac
}

trap 'error "Interrupted"; exit 1' INT TERM

main "$@"
