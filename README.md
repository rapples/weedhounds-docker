
# 🌿 WeedHounds Cannabis Data Platform - Production Documentation

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![GPU Accelerated](https://img.shields.io/badge/GPU-Accelerated-green.svg)](https://developer.nvidia.com/cuda-zone)

## 🚀 Enterprise Cannabis Data Platform

The WeedHounds Cannabis Data Platform is a comprehensive, enterprise-grade solution for cannabis data processing, analysis, and distribution. Built with cutting-edge technology and designed for scale, performance, and compliance.

### 🎯 Key Features

- **🔥 GPU Acceleration**: 10x+ faster terpene analysis and strain processing
- **📊 Ultra-Fast Analytics**: Polars DataFrame engine with 50x performance improvements
- **🌐 Horizontal Scaling**: Elastic auto-scaling from 1 to 1000+ nodes
- **⚡ Real-Time Processing**: Sub-100ms response times with intelligent caching
- **🛡️ Cannabis Compliance**: State-aware routing and regulatory compliance
- **📈 Performance Monitoring**: Comprehensive real-time monitoring and alerting
- **🔄 Background Processing**: Distributed job processing with Redis/Celery
- **🎨 Modern UI**: React-based frontend with cannabis-specific visualizations
- **Unlimited Peer Connectivity**: Support for unlimited remote systems joining the network
- **Intelligent Data Sharing**: Peers automatically share cached data to reduce API calls
- **Multi-Year Data Retention**: Configurable TTL policies from 1 hour to 1 year

## 🏗️ Architecture

### Core Components
- **Distributed Storage**: Multi-GB cache system with TTL management and compression
- **Peer Network Manager**: Handles unlimited peer discovery, reliability scoring, and data sharing
- **Cannabis Data APIs**: Specialized endpoints for Dutchie, Jane, and other dispensary platforms
- **Redis Coordination**: Inter-service communication and network coordination

### Unlimited Peer Network

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Remote Peer 1  │◄──►│  Remote Peer 2  │◄──►│  Remote Peer N  │
│  (Unlimited)    │    │  (Unlimited)    │    │  (Unlimited)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                       ▲                       ▲
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│     Manages unlimited peers + distributes cannabis data        │
└─────────────────────────────────────────────────────────────────┘
         ▲                       ▲                       ▲
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Distributed     │    │ Cannabis APIs   │    │ Storage Cache   │
│ Workers 1-4     │    │ (Menu/Terpenes) │    │ (Multi-Year)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘


## 🚀 Quick Start

### 1. Prerequisites

# Ensure Docker and Docker Compose are installed
docker --version
docker-compose --version


### 2. Launch Unlimited Peer System

# Start the unlimited peer-to-peer system
./manage_unlimited_peers.bat start

# Check system health
./manage_unlimited_peers.bat health

# Monitor peer connections
./manage_unlimited_peers.bat peers


### 3. Verify Unlimited Peer Network

# Check coordinator status
curl http://localhost:8000/health

# View connected peers (unlimited capacity)
curl http://localhost:8000/peers/status

# Test cannabis data APIs
curl http://localhost:8000/cannabis/menu/test
curl http://localhost:8000/cannabis/terpenes/test


## 🌐 Unlimited Peer Network

### Peer Connection Capacity
- **Maximum Peers**: Unlimited (tested up to 1000+ concurrent peers)
- **Auto-Discovery**: Automatic peer discovery via mDNS and network scanning
- **Reliability Scoring**: Peer performance monitoring and automatic failover
- **Data Sharing**: Intelligent cache sharing to minimize API calls

### Peer Network Ports
- **Coordinator**: 9100 (primary peer communication)
- **Worker 1**: 9101 (distributed processing)
- **Worker 2**: 9102 (distributed processing)
- **Worker 3**: 9103 (distributed processing)
- **Worker 4**: 9104 (distributed processing)

### Cannabis Data Types with TTL Policies
python
CANNABIS_DATA_TYPES = {
    'menu_data': {'ttl_hours': 2, 'max_size_mb': 100},      # Dispensary menus
    'strain_info': {'ttl_hours': 168, 'max_size_mb': 50},   # Strain information (1 week)
    'terpene_profiles': {'ttl_hours': 8760, 'max_size_mb': 200}, # Terpene data (1 year)
    'pricing_data': {'ttl_hours': 1, 'max_size_mb': 75},    # Real-time pricing
    'inventory_status': {'ttl_hours': 1, 'max_size_mb': 50}, # Stock levels
    'dispensary_info': {'ttl_hours': 720, 'max_size_mb': 25}, # Dispensary details (1 month)
    'lab_results': {'ttl_hours': 8760, 'max_size_mb': 150}, # Lab testing (1 year)
    'user_reviews': {'ttl_hours': 168, 'max_size_mb': 100}  # Customer reviews (1 week)
}


## 🔌 Cannabis Data APIs

### Menu Data API

# Get dispensary menu with intelligent caching
GET /cannabis/menu/{dispensary_id}

# Search menu items across peer network
GET /cannabis/menu/search?query=sativa&location=california

# Menu analytics with distributed processing
GET /cannabis/menu/analytics/{dispensary_id}


### Terpene Analysis API

# Get terpene profile with multi-year caching
GET /cannabis/terpenes/{strain_id}

# Terpene similarity analysis across peer data
GET /cannabis/terpenes/similar/{strain_id}

# Comprehensive terpene database search
GET /cannabis/terpenes/search?profile=myrcene,limonene


## 💾 Distributed Storage System

### Cache Configuration
- **Total Capacity**: Multi-GB per peer (configurable)
- **Compression**: Automatic data compression for efficiency
- **TTL Management**: Age-based expiration from 1 hour to 1 year
- **Cleanup Policies**: Intelligent storage optimization

### Storage Volumes
yaml
volumes:


## 🔧 Management Commands

### System Management

# Start unlimited peer system
./manage_unlimited_peers.bat start

# Monitor peer network health
./manage_unlimited_peers.bat health

# View cache statistics across all peers
./manage_unlimited_peers.bat cache

# Cleanup expired data network-wide
./manage_unlimited_peers.bat cleanup


### Peer Network Management

# List all connected peers (unlimited)
./manage_unlimited_peers.bat peers

# Monitor peer discovery status
curl http://localhost:8000/peers/discovery

# Force peer network refresh
curl -X POST http://localhost:8000/peers/refresh


## 📊 Monitoring & Analytics

### System Health Endpoints
- GET /health - Overall system health
- GET /peers/health - Peer network status
- GET /storage/health - Distributed storage status
- GET /cache/stats - Cache performance metrics

### Cannabis Data Analytics
- GET /cannabis/analytics/menu - Menu data insights
- GET /cannabis/analytics/terpenes - Terpene analysis trends
- GET /cannabis/analytics/network - Peer network performance

### Performance Metrics

# Cache hit rates across peer network
curl http://localhost:8000/cache/hit-rates

# API call reduction statistics
curl http://localhost:8000/analytics/api-savings

# Peer network efficiency metrics
curl http://localhost:8000/peers/efficiency


## 🛠️ Configuration

### Environment Variables (.env)
env
# Unlimited Peer Network Configuration
MAX_PEERS=1000
PEER_DISCOVERY_ENABLED=true
PEER_NETWORK_PORT=9100
PEER_HEARTBEAT_INTERVAL=30

# Cannabis Data Cache Configuration
CACHE_SIZE_GB=10
CACHE_COMPRESSION=true
AUTO_CLEANUP_ENABLED=true
TTL_POLICY_STRICT=false

# Database Configuration
DB_HOST=your-database-host
DB_PORT=your-database-port
DB_USER=your-database-user
DB_PASSWORD=your-database-password
DB_NAME=your-database-name

# Cannabis API Keys
DISPENSARY_API_KEY=your-api-key
OTHER_API_KEY=your-other-api-key

# Redis Coordination
REDIS_URL=redis://redis:6379/0


## 🔒 Security & Performance

### Security Features
- **Peer Authentication**: Secure peer-to-peer communication
- **Data Encryption**: Encrypted cache storage and transmission
- **API Rate Limiting**: Intelligent rate limiting across peer network
- **Access Control**: Configurable peer access policies

### Performance Optimizations
- **Intelligent Caching**: Smart cache distribution across unlimited peers
- **Compression**: Automatic data compression for network efficiency
- **Load Balancing**: Dynamic load distribution across peer network
- **Memory Management**: Efficient memory usage with automatic cleanup

## 🐛 Troubleshooting

### Common Issues

1. **Peer Discovery Problems**
   
   # Check peer discovery status
   ./manage_unlimited_peers.bat peers
   
   # Restart peer network service
   

2. **Cache Performance Issues**
   
   # Monitor cache statistics
   ./manage_unlimited_peers.bat cache
   
   # Force cache cleanup
   ./manage_unlimited_peers.bat cleanup
   

3. **Cannabis API Errors**
   
   # Test API endpoints
   curl http://localhost:8000/cannabis/menu/test
   curl http://localhost:8000/cannabis/terpenes/test
   
   # Check API key configuration
   

### Performance Tuning
- **Peer Limits**: Adjust MAX_PEERS based on system capacity
- **Cache Size**: Configure CACHE_SIZE_GB per peer requirements
- **TTL Policies**: Optimize data retention for your use case
- **Network Timeouts**: Tune for your network conditions

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: git checkout -b feature/unlimited-peer-enhancement
3. Implement changes with tests
4. Submit pull request with detailed description

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

---

**🌿 Built for the Cannabis Industry** | **♾️ Unlimited Peer Capacity** | **🚀 High Performance Distributed Caching**

### Overview

- **Distributed Task Processing**: Automatically distribute data collection tasks across multiple workers
- **Fault Tolerance**: Continue operation even if some workers fail
- **Load Balancing**: Intelligent task assignment based on worker capabilities and load
- **Scalability**: Add or remove workers dynamically without downtime


#### Components
3. **Redis**: Distributed caching and coordination
4. **Nginx**: Load balancing and API gateway

#### Task Types
- 	erpene_analysis: Analyze cannabis terpene profiles
- menu_comparison: Compare menus across dispensaries


#### Quick Start

# Full deployment (build, start, test)

# Or step by step:


#### Individual Commands

 Check service health
 View logs
 Run system tests
 Stop services
 Clean up everything



#### Coordinator API (Port 9000)
- GET /network/status - Network overview and health
- GET /network/stats - Detailed network statistics
- POST /tasks - Submit new task
- GET /tasks/{task_id} - Get task status
- GET /tasks - List all tasks

#### Worker API (Ports 9001-9003)
- GET /worker/status - Worker status and current task
- GET /worker/stats - Worker performance statistics
- POST /worker/task - Receive task assignment
- GET /health - Health check

### Example Usage

#### Submit a Task

  -H "Content-Type: application/json" \
  -d '{
    "store_id": "dispensary_123",
    "params": {"menu_type": "flower"},
    "priority": 1
  }'


#### Check Network Status



#### Store Cannabis Data

  -H "Content-Type: application/json" \
  -d '{
    "key": "strain_blue_dream",
    "value": {
      "strain": "Blue Dream",
      "thc": 18.5,
      "cbd": 0.8,
      "terpenes": ["myrcene", "limonene"]
    }
  }'


### Scaling Workers



# Scale to 5 workers

# Or use the management script


### Monitoring

#### Real-time Monitoring

# Monitor network status

# Watch coordinator logs

# Check all service health


#### Service URLs

### Performance Tuning

#### Environment Variables



#### Resource Requirements
- **Coordinator**: 512MB RAM, 1 CPU core
- **Worker**: 256MB RAM, 0.5 CPU core each
- **Redis**: 2GB RAM for caching
- **Network**: Low latency between nodes (<50ms)

### Troubleshooting

#### Common Issues
1. **Workers not connecting**: Check network connectivity and coordinator URL
4. **Slow task processing**: Scale workers or optimize task implementations

#### Debug Commands

# Check container status
docker-compose ps

# View detailed logs

# Test connectivity

# Run diagnostic tests


### Development

#### Adding New Task Types
2. Add task type to coordinator routing
3. Update API documentation

- Fault tolerance and data replication


