# Production Requirements

## System Requirements

### Minimum Hardware Requirements

| Component | Specification |
|-----------|---------------|
| CPU | 8 cores, 2.4GHz+ |
| RAM | 16GB |
| Storage | 100GB SSD |
| Network | 1 Gbps |
| GPU | Optional (NVIDIA GTX 1060+) |

### Recommended Hardware Requirements

| Component | Specification |
|-----------|---------------|
| CPU | 16+ cores, 3.0GHz+ (Intel Xeon or AMD EPYC) |
| RAM | 64GB+ DDR4 |
| Storage | 500GB+ NVMe SSD |
| Network | 10 Gbps |
| GPU | NVIDIA RTX 4090 or Tesla A100 |

### Enterprise Hardware Requirements

| Component | Specification |
|-----------|---------------|
| CPU | 32+ cores, 3.2GHz+ (Multi-socket) |
| RAM | 128GB+ DDR4/DDR5 |
| Storage | 2TB+ NVMe SSD (RAID configuration) |
| Network | 25+ Gbps |
| GPU | Multiple NVIDIA A100 or H100 |

## Software Dependencies

### Core Dependencies

```
python>=3.8,<4.0
fastapi>=0.104.0
uvicorn[standard]>=0.24.0
streamlit>=1.28.0
pandas>=2.1.0
polars>=0.19.0
numpy>=1.24.0
scipy>=1.11.0
scikit-learn>=1.3.0
```

### Database Dependencies

```
psycopg2-binary>=2.9.0
sqlalchemy>=2.0.0
alembic>=1.12.0
redis>=5.0.0
```

### GPU Dependencies (Optional)

```
cupy-cuda11x>=12.0.0  # For NVIDIA CUDA 11.x
cupy-cuda12x>=12.0.0  # For NVIDIA CUDA 12.x
pyopencl>=2023.1.0    # For OpenCL support
torch>=2.1.0+cu118    # PyTorch with CUDA
tensorflow-gpu>=2.13.0 # TensorFlow with GPU
```

### Machine Learning Dependencies

```
torch>=2.1.0
torchvision>=0.16.0
tensorflow>=2.13.0
transformers>=4.35.0
huggingface-hub>=0.17.0
```

### Monitoring Dependencies

```
prometheus-client>=0.18.0
grafana-api>=1.0.3
psutil>=5.9.0
sentry-sdk>=1.38.0
```

### Web Dependencies

```
jinja2>=3.1.0
aiofiles>=23.2.0
python-multipart>=0.0.6
```

### Security Dependencies

```
cryptography>=41.0.0
pyjwt>=2.8.0
passlib[bcrypt]>=1.7.4
python-jose[cryptography]>=3.3.0
```

### Development Dependencies

```
pytest>=7.4.0
pytest-asyncio>=0.21.0
pytest-cov>=4.1.0
black>=23.9.0
isort>=5.12.0
flake8>=6.1.0
mypy>=1.6.0
```

## Environment Setup

### Python Environment

```bash
# Create virtual environment
python -m venv cannabis-platform-env

# Activate environment
source cannabis-platform-env/bin/activate  # Linux/Mac
cannabis-platform-env\Scripts\activate     # Windows

# Upgrade pip
pip install --upgrade pip setuptools wheel

# Install production dependencies
pip install -r requirements.txt

# Install GPU dependencies (if available)
pip install -r requirements-gpu.txt
```

### Docker Environment

```dockerfile
# Dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install NVIDIA Docker support (for GPU)
RUN distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
    && curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | apt-key add - \
    && curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | tee /etc/apt/sources.list.d/nvidia-docker.list

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt requirements-gpu.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install GPU dependencies if CUDA is available
RUN if nvidia-smi; then pip install --no-cache-dir -r requirements-gpu.txt; fi

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 cannabis && chown -R cannabis:cannabis /app
USER cannabis

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Start command
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Docker Compose Production

```yaml
version: '3.8'

services:
  cannabis-api:
    build: .
    image: weedhounds/cannabis-platform:latest
    restart: unless-stopped
    environment:
      - ENV=production
      - DATABASE_URL=postgresql://cannabis_user:${DB_PASSWORD}@postgres:5432/cannabis
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - postgres
      - redis
    deploy:
      replicas: 3
      resources:
        limits:
          memory: 4G
          cpus: '2.0'
        reservations:
          memory: 2G
          cpus: '1.0'
    networks:
      - cannabis-network

  cannabis-gpu:
    build: .
    image: weedhounds/cannabis-platform:gpu
    restart: unless-stopped
    environment:
      - ENV=production
      - GPU_ENABLED=true
    runtime: nvidia
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
    networks:
      - cannabis-network

  postgres:
    image: postgres:15
    restart: unless-stopped
    environment:
      - POSTGRES_DB=cannabis
      - POSTGRES_USER=cannabis_user
      - POSTGRES_PASSWORD=${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '2.0'
    networks:
      - cannabis-network

  redis:
    image: redis:7-alpine
    restart: unless-stopped
    command: redis-server --appendonly yes --maxmemory 2gb --maxmemory-policy allkeys-lru
    volumes:
      - redis_data:/data
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
    networks:
      - cannabis-network

  nginx:
    image: nginx:alpine
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./ssl:/etc/ssl
    depends_on:
      - cannabis-api
    networks:
      - cannabis-network

  prometheus:
    image: prom/prometheus:latest
    restart: unless-stopped
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    networks:
      - cannabis-network

  grafana:
    image: grafana/grafana:latest
    restart: unless-stopped
    ports:
      - "3001:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD}
    volumes:
      - grafana_data:/var/lib/grafana
    networks:
      - cannabis-network

volumes:
  postgres_data:
  redis_data:
  prometheus_data:
  grafana_data:

networks:
  cannabis-network:
    driver: bridge
```

## Performance Targets

### Response Time Targets

| Endpoint Category | Target | Acceptable |
|------------------|--------|------------|
| Health Checks | <10ms | <50ms |
| Strain Search | <50ms | <100ms |
| Strain Details | <25ms | <75ms |
| Terpene Analysis | <100ms | <200ms |
| Price Comparison | <75ms | <150ms |
| User Recommendations | <150ms | <300ms |

### Throughput Targets

| Metric | Minimum | Target | Enterprise |
|--------|---------|--------|------------|
| Requests per Second | 500 | 2,000 | 10,000+ |
| Concurrent Users | 1,000 | 5,000 | 25,000+ |
| Data Processing | 10k/hour | 100k/hour | 1M+/hour |
| GPU Acceleration | 5x speedup | 10x speedup | 50x+ speedup |

### Availability Targets

| Environment | Uptime Target | Max Downtime/Month |
|-------------|---------------|-------------------|
| Development | 95% | 36 hours |
| Staging | 99% | 7.2 hours |
| Production | 99.9% | 43 minutes |
| Enterprise | 99.99% | 4.3 minutes |

### Resource Utilization Targets

| Resource | Normal | Warning | Critical |
|----------|--------|---------|----------|
| CPU Usage | <70% | 70-85% | >85% |
| Memory Usage | <80% | 80-90% | >90% |
| Disk Usage | <70% | 70-85% | >85% |
| Network Usage | <60% | 60-80% | >80% |
| GPU Usage | <80% | 80-95% | >95% |

## Security Requirements

### Authentication Requirements

- JWT token-based authentication
- Multi-factor authentication (MFA) support
- OAuth 2.0 integration
- Cannabis age verification
- Session management

### Authorization Requirements

- Role-based access control (RBAC)
- State-based access restrictions
- API rate limiting
- Cannabis compliance validation

### Data Security Requirements

- End-to-end encryption
- Data at rest encryption
- TLS 1.2+ for data in transit
- Cannabis compliance audit trails
- GDPR/CCPA compliance

### Network Security Requirements

- Web Application Firewall (WAF)
- DDoS protection
- IP whitelisting/blacklisting
- VPN/private network access
- Network segmentation

## Compliance Requirements

### Cannabis Compliance

- State-specific regulations compliance
- Age verification mechanisms
- Audit trail maintenance
- Data retention policies (7 years)
- Regulatory reporting capabilities

### Data Privacy Compliance

- GDPR compliance (EU users)
- CCPA compliance (California users)
- HIPAA considerations (medical cannabis)
- Cannabis-specific state privacy laws

### Security Compliance

- SOC 2 Type II certification
- PCI DSS compliance (payment processing)
- ISO 27001 certification
- Cannabis industry security standards

## Monitoring Requirements

### Application Monitoring

- Real-time performance metrics
- Error tracking and alerting
- User behavior analytics
- Cannabis-specific business metrics

### Infrastructure Monitoring

- System resource monitoring
- Database performance monitoring
- Network monitoring
- GPU utilization monitoring

### Security Monitoring

- Intrusion detection system (IDS)
- Security information and event management (SIEM)
- Vulnerability scanning
- Compliance monitoring

### Cannabis-Specific Monitoring

- Strain search analytics
- Terpene analysis performance
- Dispensary data freshness
- Price comparison accuracy
- User recommendation quality

## Backup and Recovery Requirements

### Backup Requirements

- Automated daily backups
- Geographic backup distribution
- Encryption of backup data
- Cannabis compliance data retention

### Recovery Requirements

| Recovery Type | RTO Target | RPO Target |
|---------------|------------|------------|
| Hot Standby | <5 minutes | <1 minute |
| Warm Standby | <30 minutes | <15 minutes |
| Cold Backup | <4 hours | <1 hour |
| Disaster Recovery | <24 hours | <4 hours |

### Testing Requirements

- Monthly backup restoration tests
- Quarterly disaster recovery drills
- Annual full system recovery test
- Cannabis compliance validation tests

## Scaling Requirements

### Horizontal Scaling

- Auto-scaling based on demand
- Load balancer configuration
- Database read replicas
- Cannabis-aware request routing

### Vertical Scaling

- Dynamic resource allocation
- GPU scaling for terpene analysis
- Memory optimization
- Storage scaling

### Geographic Scaling

- Multi-region deployment
- State-specific cannabis compliance
- Content delivery network (CDN)
- Edge computing for low latency

---

🌿 **Production-ready requirements defined!** Your Cannabis Data Platform meets enterprise-grade standards for performance, security, compliance, and scalability.