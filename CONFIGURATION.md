# 🔧 Configuration Guide

## Environment Configuration

### Development Environment

```bash
# .env.development
ENV=development
DEBUG=true
LOG_LEVEL=DEBUG

# Database
DATABASE_URL=postgresql://cannabis_dev:dev_password@localhost:5432/cannabis_dev
DATABASE_POOL_SIZE=5
DATABASE_ECHO=true

# Redis
REDIS_URL=redis://localhost:6379/1
REDIS_CACHE_TTL=300

# Cannabis Configuration
SUPPORTED_STATES=CA,CO,WA
COMPLIANCE_MODE=relaxed
AGE_VERIFICATION_REQUIRED=false

# Performance
WORKER_PROCESSES=2
GPU_ENABLED=false
CACHE_ENABLED=true

# Security
SECRET_KEY=development-secret-key-change-in-production
CORS_ORIGINS=http://localhost:3000,http://localhost:8080
ALLOWED_HOSTS=localhost,127.0.0.1

# External APIs
DUTCHIE_API_KEY=The-development-api-key
JANE_API_KEY=The-development-api-key

# Monitoring
PROMETHEUS_ENABLED=false
SENTRY_DSN=
```

### Staging Environment

```bash
# .env.staging
ENV=staging
DEBUG=false
LOG_LEVEL=INFO

# Database
DATABASE_URL=postgresql://cannabis_staging:staging_password@staging-db:5432/cannabis_staging
DATABASE_POOL_SIZE=15
DATABASE_ECHO=false

# Redis
REDIS_URL=redis://staging-redis:6379/0
REDIS_CACHE_TTL=1800

# Cannabis Configuration
SUPPORTED_STATES=CA,CO,WA,OR,NY,MA,IL,MI,AZ,NV
COMPLIANCE_MODE=strict
AGE_VERIFICATION_REQUIRED=true

# Performance
WORKER_PROCESSES=4
GPU_ENABLED=true
CUDA_DEVICE_COUNT=1
CACHE_ENABLED=true

# Security
SECRET_KEY=staging-ultra-secure-secret-key-here
JWT_SECRET=staging-jwt-secret-key
CORS_ORIGINS=https://staging.weedhounds.com
ALLOWED_HOSTS=staging.weedhounds.com,staging-api.weedhounds.com

# External APIs
DUTCHIE_API_KEY=The-staging-api-key
JANE_API_KEY=The-staging-api-key
WEEDMAPS_API_KEY=The-staging-api-key

# Monitoring
PROMETHEUS_ENABLED=true
GRAFANA_ENABLED=true
SENTRY_DSN=https://The-staging-sentry-dsn@sentry.io/project
```

### Production Environment

```bash
# .env.production
ENV=production
DEBUG=false
LOG_LEVEL=INFO

# Database Configuration
DATABASE_URL=postgresql://cannabis_prod:ultra_secure_password@prod-db-cluster:5432/cannabis_production
DATABASE_POOL_SIZE=30
DATABASE_MAX_OVERFLOW=50
DATABASE_POOL_TIMEOUT=30
DATABASE_POOL_RECYCLE=3600

# Read Replicas
DATABASE_READ_REPLICA_1=postgresql://readonly:password@prod-db-replica-1:5432/cannabis_production
DATABASE_READ_REPLICA_2=postgresql://readonly:password@prod-db-replica-2:5432/cannabis_production

# Redis Configuration
REDIS_URL=redis://prod-redis-cluster:6379/0
REDIS_CLUSTER_NODES=redis-1:6379,redis-2:6379,redis-3:6379
REDIS_CACHE_TTL=3600
REDIS_MAX_CONNECTIONS=100

# Cannabis Platform Configuration
SUPPORTED_STATES=CA,CO,WA,OR,NY,MA,IL,MI,AZ,NV,CT,NJ,VA,MT,VT,NM,DE,RI,AK,HI
COMPLIANCE_MODE=strict
AGE_VERIFICATION_REQUIRED=true
STATE_COMPLIANCE_VALIDATION=true
AUDIT_LOGGING=true
DATA_RETENTION_DAYS=2555  # 7 years

# Performance Configuration
WORKER_PROCESSES=16
WORKER_CONNECTIONS=2000
MAX_REQUESTS_PER_WORKER=50000
WORKER_TIMEOUT=300

# GPU Configuration
GPU_ENABLED=true
CUDA_DEVICE_COUNT=4
GPU_MEMORY_FRACTION=0.8
CUDA_COMPUTE_CAPABILITY=7.5
GPU_BATCH_SIZE=1000

# Cache Configuration
CACHE_ENABLED=true
CACHE_DEFAULT_TTL=3600
CACHE_MAX_MEMORY=8GB
CACHE_COMPRESSION=true
CACHE_LEVELS=3  # Browser, Application, Database

# Security Configuration
SECRET_KEY=production-ultra-secure-secret-key-256-bits-here
JWT_SECRET=production-jwt-secret-key-256-bits-here
JWT_EXPIRY=3600
REFRESH_TOKEN_EXPIRY=604800
ENCRYPTION_KEY=production-encryption-key-here

# CORS and Hosts
CORS_ORIGINS=https://weedhounds.com,https://app.weedhounds.com
ALLOWED_HOSTS=weedhounds.com,api.weedhounds.com,app.weedhounds.com
TRUSTED_PROXIES=10.0.0.0/8,172.16.0.0/12,192.168.0.0/16

# External API Keys
DUTCHIE_API_KEY=The-production-dutchie-api-key
JANE_API_KEY=The-production-jane-api-key
WEEDMAPS_API_KEY=The-production-weedmaps-api-key
LEAFLY_API_KEY=The-production-leafly-api-key
TERPENE_LAB_API_KEY=The-production-terpene-lab-key

# Rate Limiting
RATE_LIMIT_REQUESTS=10000
RATE_LIMIT_WINDOW=3600
RATE_LIMIT_BURST=5000
DDOS_PROTECTION=true
BOT_DETECTION=true

# Load Balancer Configuration
LOAD_BALANCER_ALGORITHM=cannabis_aware
HEALTH_CHECK_INTERVAL=15
HEALTH_CHECK_TIMEOUT=5
SESSION_AFFINITY=true
STICKY_SESSIONS=true

# Background Jobs
CELERY_BROKER_URL=redis://prod-redis-cluster:6379/1
CELERY_RESULT_BACKEND=redis://prod-redis-cluster:6379/2
CELERY_WORKER_CONCURRENCY=8
CELERY_TASK_TIME_LIMIT=3600
CELERY_BEAT_SCHEDULE=true

# Monitoring and Logging
PROMETHEUS_ENABLED=true
GRAFANA_ENABLED=true
ELASTICSEARCH_URL=https://prod-elasticsearch:9200
KIBANA_ENABLED=true
SENTRY_DSN=https://The-production-sentry-dsn@sentry.io/project
SENTRY_TRACES_SAMPLE_RATE=0.1

# File Storage
FILE_STORAGE_BACKEND=s3
AWS_S3_BUCKET=weedhounds-prod-assets
AWS_S3_REGION=us-west-2
CDN_URL=https://cdn.weedhounds.com

# Email Configuration
EMAIL_BACKEND=ses
AWS_SES_REGION=us-west-2
DEFAULT_FROM_EMAIL=noreply@weedhounds.com

# Backup Configuration
BACKUP_ENABLED=true
BACKUP_FREQUENCY=daily
BACKUP_RETENTION_DAYS=90
BACKUP_S3_BUCKET=weedhounds-prod-backups

# SSL/TLS Configuration
SSL_REDIRECT=true
SECURE_HSTS_SECONDS=31536000
SECURE_CONTENT_TYPE_NOSNIFF=true
SECURE_BROWSER_XSS_FILTER=true
```

## Application Configuration Files

### Main Application Config (`config/app.yaml`)

```yaml
# Application Configuration
application:
  name: "Cannabis Data Platform"
  version: "2.0.0"
  description: "Enterprise Cannabis Data Processing Platform"
  timezone: "UTC"
  
# Server Configuration
server:
  host: "0.0.0.0"
  port: 8000
  workers: 8
  worker_class: "uvicorn.workers.UvicornWorker"
  worker_connections: 1000
  max_requests: 10000
  max_requests_jitter: 1000
  timeout: 30
  keepalive: 2
  graceful_timeout: 30

# Database Configuration
database:
  primary:
    url: "${DATABASE_URL}"
    pool_size: 30
    max_overflow: 50
    pool_timeout: 30
    pool_recycle: 3600
    echo: false
    
  read_replicas:
    - url: "${DATABASE_READ_REPLICA_1}"
      weight: 1
    - url: "${DATABASE_READ_REPLICA_2}"
      weight: 1
      
  migrations:
    auto_migrate: false
    migration_path: "migrations/"
    
# Redis Configuration
redis:
  primary:
    url: "${REDIS_URL}"
    max_connections: 100
    retry_on_timeout: true
    health_check_interval: 30
    
  cluster:
    enabled: true
    nodes: "${REDIS_CLUSTER_NODES}"
    
# Cannabis Configuration
cannabis:
  supported_states: 
    - "CA"
    - "CO" 
    - "WA"
    - "OR"
    - "NY"
    - "MA"
    - "IL"
    - "MI"
    - "AZ"
    - "NV"
    
  compliance:
    mode: "strict"
    age_verification: true
    state_validation: true
    audit_logging: true
    data_retention_days: 2555
    
  terpene_analysis:
    precision: 0.01
    confidence_threshold: 0.85
    batch_size: 1000
    
  strain_recommendations:
    max_results: 20
    similarity_threshold: 0.7
    user_preference_weight: 0.6
    
# Performance Configuration
performance:
  cache:
    enabled: true
    default_ttl: 3600
    max_memory: "8GB"
    compression: true
    levels:
      browser: 300
      application: 1800
      database: 7200
      
  gpu:
    enabled: true
    device_count: 4
    memory_fraction: 0.8
    batch_size: 1000
    
  optimization:
    connection_pooling: true
    lazy_loading: true
    compression: true
    minification: true
    
# Security Configuration
security:
  cors:
    origins: 
      - "https://weedhounds.com"
      - "https://app.weedhounds.com"
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    headers: ["*"]
    credentials: true
    
  rate_limiting:
    enabled: true
    default_limit: "1000/hour"
    endpoints:
      "/api/auth/login": "10/minute"
      "/api/strains/search": "100/minute"
      "/api/terpenes/analyze": "50/minute"
      
  jwt:
    algorithm: "HS256"
    expiry_seconds: 3600
    refresh_expiry_seconds: 604800
    
# Monitoring Configuration
monitoring:
  prometheus:
    enabled: true
    port: 9090
    metrics_path: "/metrics"
    
  health_checks:
    enabled: true
    endpoints:
      - "/health"
      - "/ready"
      - "/live"
      
  logging:
    level: "INFO"
    format: "json"
    handlers:
      - "console"
      - "file" 
      - "elasticsearch"
      
# External API Configuration
external_apis:
  dutchie:
    base_url: "https://dutchie.com/graphql"
    timeout: 30
    retry_attempts: 3
    
  jane:
    base_url: "https://api.iheartjane.com"
    timeout: 30
    retry_attempts: 3
    
  weedmaps:
    base_url: "https://api.weedmaps.com"
    timeout: 30
    retry_attempts: 3
```

### Load Balancer Config (`config/load_balancer.yaml`)

```yaml
# Load Balancer Configuration
load_balancer:
  algorithm: "cannabis_aware"
  
  # Health Check Configuration
  health_check:
    enabled: true
    interval: 15
    timeout: 5
    path: "/health"
    healthy_threshold: 2
    unhealthy_threshold: 3
    
  # Cannabis-Aware Routing
  cannabis_routing:
    state_compliance: true
    age_verification: true
    geographic_distribution: true
    strain_specialization: true
    
  # Session Management
  session:
    affinity: true
    timeout: 1800
    cookie_name: "cannabis_session"
    
  # Rate Limiting
  rate_limiting:
    enabled: true
    requests_per_second: 1000
    burst_size: 5000
    window: 3600
    
  # DDoS Protection
  ddos_protection:
    enabled: true
    threshold: 10000
    block_duration: 3600
    whitelist_ips: []
    
# Upstream Servers
upstreams:
  cannabis_api:
    servers:
      - server: "cannabis-api-1:8000"
        weight: 3
        max_fails: 3
        fail_timeout: 30
      - server: "cannabis-api-2:8000"
        weight: 3
        max_fails: 3
        fail_timeout: 30
      - server: "cannabis-api-3:8000"
        weight: 2
        max_fails: 3
        fail_timeout: 30
      - server: "cannabis-api-4:8000"
        weight: 1
        backup: true
        
  gpu_processing:
    servers:
      - server: "gpu-node-1:8001"
        weight: 2
      - server: "gpu-node-2:8001"
        weight: 2
```

### Monitoring Config (`config/monitoring.yaml`)

```yaml
# Monitoring Configuration
monitoring:
  prometheus:
    enabled: true
    port: 9090
    scrape_interval: 15
    evaluation_interval: 15
    
    # Cannabis-specific metrics
    cannabis_metrics:
      strain_searches: true
      terpene_analyses: true
      dispensary_lookups: true
      price_comparisons: true
      user_recommendations: true
      
  grafana:
    enabled: true
    port: 3001
    admin_user: "admin"
    admin_password: "${GRAFANA_ADMIN_PASSWORD}"
    
    # Dashboards
    dashboards:
      - name: "Cannabis Platform Overview"
        file: "dashboards/cannabis_overview.json"
      - name: "GPU Performance"
        file: "dashboards/gpu_performance.json"
      - name: "API Performance"
        file: "dashboards/api_performance.json"
        
  alerting:
    enabled: true
    
    # Alert Rules
    rules:
      - name: "High Response Time"
        condition: "avg_response_time > 200"
        duration: "5m"
        severity: "warning"
        
      - name: "Low Cache Hit Ratio"  
        condition: "cache_hit_ratio < 0.90"
        duration: "5m"
        severity: "warning"
        
      - name: "GPU Utilization High"
        condition: "gpu_utilization > 90"
        duration: "2m"
        severity: "critical"
        
      - name: "Cannabis API Down"
        condition: "up{job='cannabis-api'} == 0"
        duration: "1m"
        severity: "critical"
        
    # Notification Channels
    notifications:
      slack:
        webhook_url: "${SLACK_WEBHOOK_URL}"
        channel: "#cannabis-alerts"
        
      email:
        to: ["ops@weedhounds.com"]
        from: "alerts@weedhounds.com"
        
      pagerduty:
        integration_key: "${PAGERDUTY_INTEGRATION_KEY}"
        
# Logging Configuration
logging:
  level: "INFO"
  format: "json"
  
  # Log Handlers
  handlers:
    console:
      enabled: true
      level: "INFO"
      
    file:
      enabled: true
      path: "/var/log/cannabis-platform/"
      rotation: "daily"
      retention: 30
      
    elasticsearch:
      enabled: true
      host: "${ELASTICSEARCH_URL}"
      index: "cannabis-platform-logs"
      
  # Cannabis-specific logging
  cannabis_logging:
    audit_trail: true
    compliance_events: true
    user_interactions: true
    api_requests: true
    
# Performance Monitoring
performance:
  apm:
    enabled: true
    service_name: "cannabis-platform"
    
  tracing:
    enabled: true
    sample_rate: 0.1
    
  profiling:
    enabled: true
    cpu_profiling: true
    memory_profiling: true
```

## Security Configuration

### SSL/TLS Configuration

```yaml
# SSL/TLS Configuration
ssl:
  enabled: true
  certificate_path: "/etc/ssl/certs/weedhounds.crt"
  private_key_path: "/etc/ssl/private/weedhounds.key"
  
  # SSL Settings
  protocols: ["TLSv1.2", "TLSv1.3"]
  ciphers: "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS"
  prefer_server_ciphers: true
  
  # HSTS Configuration
  hsts:
    enabled: true
    max_age: 31536000
    include_subdomains: true
    preload: true
    
  # Certificate Management
  auto_renewal: true
  renewal_days_before: 30
```

### Authentication Configuration

```yaml
# Authentication Configuration
authentication:
  jwt:
    algorithm: "HS256"
    secret_key: "${JWT_SECRET}"
    access_token_expiry: 3600
    refresh_token_expiry: 604800
    
  oauth:
    providers:
      google:
        client_id: "${GOOGLE_CLIENT_ID}"
        client_secret: "${GOOGLE_CLIENT_SECRET}"
        
  # Cannabis Age Verification
  age_verification:
    required: true
    minimum_age: 21
    verification_methods:
      - "id_scan"
      - "credit_card"
      - "phone_verification"
      
  # Multi-Factor Authentication
  mfa:
    enabled: true
    methods: ["totp", "sms"]
    backup_codes: true
```

## Backup Configuration

```yaml
# Backup Configuration
backup:
  enabled: true
  frequency: "daily"
  retention:
    daily: 30
    weekly: 12
    monthly: 12
    yearly: 7
    
  # Database Backup
  database:
    method: "pg_dump"
    compression: true
    encryption: true
    
  # File Backup
  files:
    include:
      - "/app/uploads/"
      - "/app/static/"
      - "/etc/ssl/"
    exclude:
      - "*.log"
      - "*.tmp"
      
  # Storage
  storage:
    backend: "s3"
    bucket: "${BACKUP_S3_BUCKET}"
    region: "us-west-2"
    encryption: "AES256"
```

## Environment-Specific Overrides

### Development Overrides

```yaml
# config/environments/development.yaml
database:
  primary:
    echo: true
    pool_size: 5
    
performance:
  cache:
    enabled: false
  gpu:
    enabled: false
    
monitoring:
  prometheus:
    enabled: false
  alerting:
    enabled: false
    
security:
  cors:
    origins: ["*"]
```

### Production Overrides

```yaml
# config/environments/production.yaml
database:
  primary:
    echo: false
    pool_size: 50
    
performance:
  cache:
    enabled: true
    max_memory: "16GB"
  gpu:
    enabled: true
    device_count: 8
    
monitoring:
  prometheus:
    enabled: true
  alerting:
    enabled: true
    
security:
  cors:
    origins:
      - "https://weedhounds.com"
      - "https://app.weedhounds.com"
```

---

🌿 **Configuration complete!** The Cannabis Data Platform is now fully configured for development, staging, and production environments with enterprise-grade security and monitoring.
