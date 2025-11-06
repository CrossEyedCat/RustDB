# RustDB Deployment Guide

## Overview

RustDB is now fully ready for deployment in various environments. The system includes:

- 🐳 **Containerization** - Docker images with multi-stage builds
- 🚀 **CI/CD** - Automated GitHub Actions pipelines
- ☸️ **Kubernetes** - Full orchestration support
- 📊 **Monitoring** - Prometheus, Grafana, Jaeger
- 🔒 **Security** - SSL/TLS, secrets, rate limiting
- 📈 **Scaling** - HPA, load balancing

## Quick Start

### 1. Local Development (Docker Compose)

```bash
# Clone repository
git clone https://github.com/your-org/rustdb.git
cd rustdb

# Deployment (Linux/macOS)
chmod +x deployment/deploy.sh
./deployment/deploy.sh deploy

# Deployment (Windows)
.\deployment\deploy.ps1 deploy
```

### 2. Production (Kubernetes)

```bash
# Deploy to Kubernetes
cd deployment/k8s
chmod +x deploy.sh
./deploy.sh deploy
```

## Deployment Architecture

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

## System Components

### Main Services

| Service | Port | Description |
|---------|------|-------------|
| RustDB API | 8080 | Main API server |
| RustDB gRPC | 8081 | gRPC interface |
| Nginx | 80/443 | Load balancer and SSL termination |
| Redis | 6379 | Caching |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Visualization |
| Jaeger | 16686 | Tracing |

### Monitoring

- **Prometheus** - Performance metrics collection
- **Grafana** - Dashboards for visualization
- **Jaeger** - Distributed request tracing
- **Health checks** - Health checks for all services

## Configuration

### Environment Variables

```bash
# Main settings
RUST_LOG=info
RUSTDB_DATA_DIR=/app/data
RUSTDB_LOG_DIR=/app/logs
RUSTDB_CONFIG_DIR=/app/config

# Security
RUSTDB_API_KEY=your-secret-key
RUSTDB_ADMIN_PASSWORD=secure-password

# Performance
RUSTDB_MAX_CONNECTIONS=100
RUSTDB_CONNECTION_TIMEOUT=30
RUSTDB_QUERY_TIMEOUT=60
```

### Configuration File

```toml
# config.toml
name = "rustdb"
data_directory = "/app/data"
max_connections = 100
connection_timeout = 30
query_timeout = 60
language = "en"  # "en" or "ru"
```

## Security

### SSL/TLS

- Automatic generation of self-signed certificates for development
- Let's Encrypt support for production
- Forced HTTP → HTTPS redirect

### Authentication

- API keys for API access
- Administrative passwords for Grafana
- Kubernetes Secrets for storing secrets

### Network Security

- Rate limiting (10 req/s for API, 1 req/s for login)
- Security headers (HSTS, X-Frame-Options, etc.)
- Network policies in Kubernetes

## Scaling

### Horizontal Scaling

```yaml
# HPA configuration
minReplicas: 2
maxReplicas: 10
targetCPUUtilizationPercentage: 70
targetMemoryUtilizationPercentage: 80
```

### Vertical Scaling

```yaml
# Container resources
resources:
  requests:
    memory: "256Mi"
    cpu: "250m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

## Monitoring and Alerting

### Key Metrics

- **Performance**: Request rate, response time, throughput
- **Resources**: CPU, memory, disk usage
- **Connections**: Active connections, connection pool
- **Errors**: Error rate, failed requests

### Grafana Dashboards

1. **RustDB Dashboard** - Main performance metrics
2. **System Overview** - System and resource overview
3. **Error Analysis** - Error and exception analysis

### Alerts

- High CPU usage (>80%)
- High memory usage (>90%)
- High error rate (>5%)
- Slow queries (>1s)

## Backup

### Docker Compose

```bash
# Create backup
docker run --rm -v rustdb_data:/data -v $(pwd):/backup alpine \
  tar czf /backup/rustdb-backup-$(date +%Y%m%d).tar.gz -C /data .

# Restore
docker run --rm -v rustdb_data:/data -v $(pwd):/backup alpine \
  tar xzf /backup/rustdb-backup-20231201.tar.gz -C /data
```

### Kubernetes

```bash
# Create snapshot
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
   - Testing on multiple Rust versions
   - Building and publishing Docker images
   - Security scanning
   - Automatic deployment

2. **Release Pipeline** (`.github/workflows/release.yml`)
   - Creating releases from tags
   - Publishing to Docker Hub
   - Generating release notes

### Pipeline Stages

1. **Test** - Run all tests
2. **Build** - Build application
3. **Docker** - Build and publish images
4. **Security** - Vulnerability scanning
5. **Deploy** - Deploy to staging/production

## Troubleshooting

### Common Issues

1. **Ports in use**
   ```bash
   # Check used ports
   netstat -tulpn | grep :8080
   
   # Change ports in docker-compose.yml
   ports:
     - "8081:8080"  # External:Internal
   ```

2. **Insufficient memory**
   ```bash
   # Increase limits in Kubernetes
   kubectl patch deployment rustdb -n rustdb -p '{"spec":{"template":{"spec":{"containers":[{"name":"rustdb","resources":{"limits":{"memory":"1Gi"}}}]}}}}'
   ```

3. **SSL errors**
   ```bash
   # Check certificates
   openssl x509 -in deployment/ssl/cert.pem -text -noout
   ```

### Logs and Diagnostics

```bash
# Docker Compose
docker-compose logs -f rustdb

# Kubernetes
kubectl logs -l app=rustdb -n rustdb -f

# Health check
curl http://localhost:8080/health
```

## Performance

### Recommended Settings

- **CPU**: 2+ cores for production
- **Memory**: 4GB+ RAM
- **Storage**: SSD for data
- **Network**: 1Gbps+ for high-load systems

### Optimization

- Enable gzip compression in Nginx
- Configure connection pooling
- Use Redis for caching
- Optimize database queries

## Support

### Documentation

- [API Reference](API_REFERENCE.md)
- [Technical Documentation](TECHNICAL_DOCUMENTATION.md)
- [User Guide](USER_GUIDE.md)

### Contacts

- GitHub Issues: [Create issue](https://github.com/your-org/rustdb/issues)
- Email: support@rustdb.com
- Discord: [RustDB Community](https://discord.gg/rustdb)

---

**RustDB is ready for production!** 🚀

The deployment system provides high availability, security, and scalability for any workload.
