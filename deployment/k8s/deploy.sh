#!/bin/bash

# RustDB Kubernetes deployment helper
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}" >&2
}

warning() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        error "kubectl is not installed"
        exit 1
    fi
    
    if ! kubectl cluster-info &> /dev/null; then
        error "Cannot reach Kubernetes cluster"
        exit 1
    fi
    
    log "kubectl is configured"
}

apply_manifests() {
    log "Applying Kubernetes manifests..."
    
    kubectl apply -f namespace.yaml
    kubectl apply -f configmap.yaml
    kubectl apply -f secret.yaml
    kubectl apply -f pvc.yaml
    kubectl apply -f rustdb-deployment.yaml
    kubectl apply -f rustdb-service.yaml
    kubectl apply -f ingress.yaml
    kubectl apply -f hpa.yaml
    
    log "Manifests applied"
}

wait_for_ready() {
    log "Waiting for pods..."
    
    kubectl wait --for=condition=ready pod -l app=rustdb -n rustdb --timeout=300s
    
    log "Pods are ready"
}

check_status() {
    log "Deployment status..."
    
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

show_logs() {
    log "RustDB logs..."
    kubectl logs -l app=rustdb -n rustdb --tail=100 -f
}

delete_deployment() {
    log "Deleting deployment..."
    
    kubectl delete -f hpa.yaml --ignore-not-found=true
    kubectl delete -f ingress.yaml --ignore-not-found=true
    kubectl delete -f rustdb-service.yaml --ignore-not-found=true
    kubectl delete -f rustdb-deployment.yaml --ignore-not-found=true
    kubectl delete -f pvc.yaml --ignore-not-found=true
    kubectl delete -f secret.yaml --ignore-not-found=true
    kubectl delete -f configmap.yaml --ignore-not-found=true
    kubectl delete -f namespace.yaml --ignore-not-found=true
    
    log "Deployment removed"
}

scale_deployment() {
    local replicas=$1
    if [ -z "$replicas" ]; then
        error "Replica count not specified"
        exit 1
    fi
    
    log "Scaling to $replicas replicas..."
    kubectl scale deployment rustdb -n rustdb --replicas=$replicas
    log "Scale complete"
}

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
            echo "Usage: $0 {deploy|status|logs|delete|scale <replicas>}"
            echo ""
            echo "Commands:"
            echo "  deploy           - Apply manifests and wait (default)"
            echo "  status           - Show resources"
            echo "  logs             - Tail RustDB logs"
            echo "  delete           - Delete resources"
            echo "  scale <replicas> - Scale deployment"
            exit 1
            ;;
    esac
}

trap 'error "Interrupted"; exit 1' INT TERM

main "$@"
