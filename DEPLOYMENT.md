# 🚀 Production Deployment Guide

## Prerequisites

### System Requirements

| Component | Minimum | Recommended | Enterprise |
|-----------|---------|-------------|------------|
| CPU | 4 cores | 16 cores | 32+ cores |
| RAM | 8GB | 32GB | 64GB+ |
| Storage | 50GB SSD | 200GB SSD | 1TB+ NVMe |
| GPU | None | RTX 3080 | RTX 4090/A100 |
| Network | 100 Mbps | 1 Gbps | 10 Gbps+ |

### Software Dependencies

- Docker 20.10+ with Docker Compose
- Python 3.8+ (for development)
- PostgreSQL 14+
- Redis 7+
- NGINX 1.20+
- Optional: NVIDIA Docker runtime for GPU support

## 🐳 Docker Production Deployment

### 1. Clone and Configure

```bash
# Clone the repository
git clone https://github.com/weedhounds/cannabis-platform.git
cd cannabis-platform

# Copy production environment
cp .env.production.example .env.production
```

### 2. Configure Environment Variables

Edit `.env.production`:

```bash
# Core Configuration
ENV=production
DEBUG=false
SECRET_KEY=your-ultra-secure-secret-key-here

# Database
DATABASE_URL=postgresql://cannabis_user:secure_password@postgres:5432/cannabis
DATABASE_POOL_SIZE=30

# Redis
REDIS_URL=redis://redis:6379/0

# Cannabis Platform
SUPPORTED_STATES=CA,CO,WA,OR,NY,MA,IL,MI,AZ,NV
COMPLIANCE_MODE=strict

# Performance
WORKER_PROCESSES=8
CACHE_TTL=3600
GPU_ENABLED=true

# Security
ALLOWED_HOSTS=yourdomain.com,api.yourdomain.com
CORS_ORIGINS=https://yourdomain.com

# Monitoring
PROMETHEUS_ENABLED=true
LOG_LEVEL=INFO
```

### 3. Start Production Services

```bash
# Start all services
docker-compose -f docker-compose.prod.yml up -d

# Verify deployment
docker-compose ps
curl http://localhost:8000/health

# View logs
docker-compose logs -f cannabis-api
```

### 4. SSL/TLS Configuration

```bash
# Generate SSL certificates (Let's Encrypt)
docker run --rm -v "${PWD}/ssl:/etc/letsencrypt" \
  certbot/certbot certonly --standalone \
  -d yourdomain.com -d api.yourdomain.com

# Or use existing certificates
cp your-certificate.crt ssl/
cp your-private-key.key ssl/
```

## ☸️ Kubernetes Deployment

### 1. Create Namespace

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: cannabis-platform
---
apiVersion: v1
kind: Secret
metadata:
  name: cannabis-secrets
  namespace: cannabis-platform
type: Opaque
stringData:
  database-url: "postgresql://user:pass@postgres:5432/cannabis"
  redis-url: "redis://redis:6379/0"
  secret-key: "your-secret-key"
```

### 2. Deploy Database

```yaml
# postgres.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  namespace: cannabis-platform
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:14
        env:
        - name: POSTGRES_DB
          value: cannabis
        - name: POSTGRES_USER
          value: cannabis_user
        - name: POSTGRES_PASSWORD
          value: secure_password
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: postgres-storage
          mountPath: /var/lib/postgresql/data
  volumeClaimTemplates:
  - metadata:
      name: postgres-storage
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi
```

### 3. Deploy Application

```yaml
# cannabis-platform.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cannabis-platform
  namespace: cannabis-platform
spec:
  replicas: 5
  selector:
    matchLabels:
      app: cannabis-platform
  template:
    metadata:
      labels:
        app: cannabis-platform
    spec:
      containers:
      - name: cannabis-api
        image: weedhounds/cannabis-platform:latest
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: cannabis-secrets
              key: database-url
        - name: REDIS_URL
          valueFrom:
            secretKeyRef:
              name: cannabis-secrets
              key: redis-url
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
```

### 4. Deploy with GPU Support

```yaml
# cannabis-platform-gpu.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cannabis-platform-gpu
  namespace: cannabis-platform
spec:
  replicas: 2
  selector:
    matchLabels:
      app: cannabis-platform-gpu
  template:
    metadata:
      labels:
        app: cannabis-platform-gpu
    spec:
      containers:
      - name: cannabis-api-gpu
        image: weedhounds/cannabis-platform:gpu
        ports:
        - containerPort: 8000
        env:
        - name: GPU_ENABLED
          value: "true"
        - name: CUDA_VISIBLE_DEVICES
          value: "0"
        resources:
          requests:
            memory: "4Gi"
            cpu: "2000m"
            nvidia.com/gpu: 1
          limits:
            memory: "8Gi"
            cpu: "4000m"
            nvidia.com/gpu: 1
      nodeSelector:
        accelerator: nvidia-tesla-k80
```

## ☁️ Cloud Deployments

### AWS ECS

```bash
# Create ECS cluster
aws ecs create-cluster --cluster-name cannabis-platform

# Register task definition
aws ecs register-task-definition --cli-input-json file://task-definition.json

# Create service
aws ecs create-service \
  --cluster cannabis-platform \
  --service-name cannabis-api \
  --task-definition cannabis-platform:1 \
  --desired-count 3
```

### Google Cloud Run

```bash
# Build and push image
gcloud builds submit --tag gcr.io/PROJECT-ID/cannabis-platform

# Deploy to Cloud Run
gcloud run deploy cannabis-platform \
  --image gcr.io/PROJECT-ID/cannabis-platform \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --concurrency 100 \
  --min-instances 2 \
  --max-instances 100
```

### Azure Container Instances

```bash
# Create resource group
az group create --name cannabis-rg --location eastus

# Create container instance
az container create \
  --resource-group cannabis-rg \
  --name cannabis-platform \
  --image weedhounds/cannabis-platform:latest \
  --ports 8000 \
  --memory 4 \
  --cpu 2 \
  --environment-variables ENV=production
```

## 🔧 Production Configuration

### Load Balancer (NGINX)

```nginx
# /etc/nginx/sites-available/cannabis-platform
upstream cannabis_backend {
    least_conn;
    server 127.0.0.1:8001 weight=3;
    server 127.0.0.1:8002 weight=3;
    server 127.0.0.1:8003 weight=3;
    server 127.0.0.1:8004 weight=1 backup;
}

server {
    listen 80;
    listen 443 ssl http2;
    server_name yourdomain.com api.yourdomain.com;

    # SSL Configuration
    ssl_certificate /etc/ssl/certs/yourdomain.crt;
    ssl_certificate_key /etc/ssl/private/yourdomain.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    # Security Headers
    add_header X-Frame-Options DENY always;
    add_header X-Content-Type-Options nosniff always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;

    # Rate Limiting
    limit_req_zone $binary_remote_addr zone=api:10m rate=10r/s;
    limit_req zone=api burst=20 nodelay;

    # Cannabis-specific rate limiting
    location /api/strains {
        limit_req zone=api burst=50 nodelay;
        proxy_pass http://cannabis_backend;
    }

    location /api/terpenes {
        limit_req zone=api burst=30 nodelay;
        proxy_pass http://cannabis_backend;
    }

    # General API proxy
    location /api/ {
        proxy_pass http://cannabis_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Timeouts
        proxy_connect_timeout 5s;
        proxy_send_timeout 10s;
        proxy_read_timeout 30s;
        
        # Cannabis compliance headers
        proxy_set_header X-Cannabis-State $arg_state;
        proxy_set_header X-Cannabis-Age-Verified $arg_age_verified;
    }

    # Health check endpoint
    location /health {
        access_log off;
        proxy_pass http://cannabis_backend;
    }

    # Static assets
    location /static/ {
        expires 1y;
        add_header Cache-Control "public, immutable";
        gzip_static on;
        alias /var/www/static/;
    }
}
```

### Database Optimization

```sql
-- PostgreSQL production configuration
-- /etc/postgresql/14/main/postgresql.conf

# Memory Configuration
shared_buffers = 8GB                 # 25% of RAM
effective_cache_size = 24GB          # 75% of RAM
work_mem = 256MB
maintenance_work_mem = 2GB

# Connection Configuration
max_connections = 200
max_prepared_transactions = 200

# Performance Configuration
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 300

# Cannabis-specific optimizations
CREATE INDEX CONCURRENTLY idx_strains_state_type ON strains(state, strain_type);
CREATE INDEX CONCURRENTLY idx_strains_thc_cbd ON strains(thc_percentage, cbd_percentage);
CREATE INDEX CONCURRENTLY idx_strains_terpene_gin ON strains USING gin(terpene_profile);
CREATE INDEX CONCURRENTLY idx_dispensaries_location ON dispensaries USING gist(location);
CREATE INDEX CONCURRENTLY idx_products_dispensary_category ON products(dispensary_id, category);

# Partitioning for large tables
CREATE TABLE user_interactions_2025 PARTITION OF user_interactions
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
```

### Redis Configuration

```redis
# /etc/redis/redis.conf

# Memory Configuration
maxmemory 4gb
maxmemory-policy allkeys-lru

# Persistence
save 900 1
save 300 10
save 60 10000

# Performance
tcp-keepalive 300
timeout 300
databases 16

# Cannabis-specific configurations
# Database 0: General cache
# Database 1: Session storage
# Database 2: Rate limiting
# Database 3: Background jobs
```

## 📊 Monitoring Setup

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "cannabis_rules.yml"

scrape_configs:
  - job_name: 'cannabis-platform'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
    scrape_interval: 5s
    
  - job_name: 'postgres'
    static_configs:
      - targets: ['localhost:9187']
      
  - job_name: 'redis'
    static_configs:
      - targets: ['localhost:9121']
      
  - job_name: 'nginx'
    static_configs:
      - targets: ['localhost:9113']

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093
```

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "Cannabis Platform Monitoring",
    "panels": [
      {
        "title": "Request Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(http_requests_total[5m])",
            "legendFormat": "{{method}} {{endpoint}}"
          }
        ]
      },
      {
        "title": "Response Time",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
            "legendFormat": "95th percentile"
          }
        ]
      },
      {
        "title": "Cannabis Metrics",
        "type": "stat",
        "targets": [
          {
            "expr": "cannabis_strain_searches_total",
            "legendFormat": "Strain Searches"
          },
          {
            "expr": "cannabis_terpene_analyses_total",
            "legendFormat": "Terpene Analyses"
          }
        ]
      }
    ]
  }
}
```

## 🔒 Security Configuration

### SSL/TLS Setup

```bash
# Let's Encrypt SSL certificates
certbot certonly --nginx -d yourdomain.com -d api.yourdomain.com

# Auto-renewal
echo "0 12 * * * /usr/bin/certbot renew --quiet" | crontab -
```

### Firewall Configuration

```bash
# UFW Configuration
ufw default deny incoming
ufw default allow outgoing
ufw allow ssh
ufw allow 80/tcp
ufw allow 443/tcp
ufw allow from 10.0.0.0/8 to any port 5432  # PostgreSQL
ufw allow from 10.0.0.0/8 to any port 6379  # Redis
ufw enable
```

### Application Security

```python
# Security middleware configuration
SECURITY_HEADERS = {
    'X-Frame-Options': 'DENY',
    'X-Content-Type-Options': 'nosniff',
    'X-XSS-Protection': '1; mode=block',
    'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
    'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline'",
}

# Rate limiting
RATE_LIMITS = {
    'default': '1000/hour',
    'auth': '10/minute',
    'search': '100/minute',
    'terpene_analysis': '50/minute',
}

# Cannabis compliance
COMPLIANCE_SETTINGS = {
    'age_verification_required': True,
    'state_verification': True,
    'audit_logging': True,
    'data_retention_days': 2555,  # 7 years
}
```

## 🚀 Scaling Strategies

### Horizontal Scaling

```yaml
# Auto-scaling configuration
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: cannabis-platform-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: cannabis-platform
  minReplicas: 3
  maxReplicas: 50
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

### Database Scaling

```bash
# Read replicas setup
docker run -d --name postgres-replica-1 \
  -e PGUSER=replication_user \
  -e POSTGRES_MASTER_SERVICE=postgres-master \
  postgres:14

# Connection pooling with PgBouncer
docker run -d --name pgbouncer \
  -e DATABASES_HOST=postgres-master \
  -e DATABASES_PORT=5432 \
  -e DATABASES_USER=cannabis_user \
  -e DATABASES_PASSWORD=secure_password \
  -e DATABASES_DBNAME=cannabis \
  -e POOL_MODE=transaction \
  -e MAX_CLIENT_CONN=1000 \
  -e DEFAULT_POOL_SIZE=25 \
  pgbouncer/pgbouncer:latest
```

## 📋 Production Checklist

### Pre-Deployment

- [ ] Environment variables configured
- [ ] SSL certificates installed
- [ ] Database migrations applied
- [ ] Static assets built and deployed
- [ ] Load balancer configured
- [ ] Monitoring setup complete
- [ ] Backup strategy implemented
- [ ] Security hardening applied

### Post-Deployment

- [ ] Health checks passing
- [ ] Performance metrics within targets
- [ ] Logs flowing correctly
- [ ] Alerts configured and tested
- [ ] Documentation updated
- [ ] Team trained on operations
- [ ] Incident response plan ready
- [ ] Cannabis compliance verified

### Performance Targets

- [ ] Response time < 100ms (95th percentile)
- [ ] Throughput > 1,000 RPS
- [ ] Uptime > 99.9%
- [ ] Error rate < 0.1%
- [ ] Cache hit ratio > 95%
- [ ] GPU utilization optimized
- [ ] Memory usage < 80%

---

🌿 **Production deployment complete!** Your Cannabis Data Platform is ready to serve millions of users with enterprise-grade performance and reliability.