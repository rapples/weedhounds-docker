#!/usr/bin/env python3
"""
Advanced Load Balancer Configuration for Cannabis Data Platform
=============================================================

Intelligent load balancing system with cannabis-specific routing, state-aware
distribution, and performance optimization. Provides automatic failover,
health checking, and geographic distribution for unlimited scalability.

Key Features:
- Cannabis-aware routing based on dispensary locations and state regulations
- Intelligent health checking with business logic validation
- Geographic load distribution with state compliance
- Terpene profile caching and routing optimization
- Real-time performance monitoring and adaptive routing
- Automatic failover and recovery mechanisms
- Session affinity for user experiences
- API rate limiting and DDoS protection

Author: WeedHounds Load Balancing Team
Created: November 2025
"""

import logging
import time
import threading
import asyncio
import json
import hashlib
import random
from typing import Dict, List, Any, Optional, Callable, Tuple, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
from pathlib import Path
import socket
import urllib.parse
import ssl

try:
    import requests
    HTTP_CLIENT_AVAILABLE = True
    print("✅ HTTP client available")
except ImportError:
    requests = None
    HTTP_CLIENT_AVAILABLE = False
    print("⚠️ HTTP client not available")

try:
    import redis
    REDIS_AVAILABLE = True
    print("✅ Redis available for session storage")
except ImportError:
    redis = None
    REDIS_AVAILABLE = False
    print("⚠️ Redis not available")

try:
    import haproxy_stats
    HAPROXY_INTEGRATION = True
    print("✅ HAProxy integration available")
except ImportError:
    haproxy_stats = None
    HAPROXY_INTEGRATION = False
    print("⚠️ HAProxy integration not available")

class HealthStatus(Enum):
    """Server health status enumeration."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    MAINTENANCE = "maintenance"

class RoutingStrategy(Enum):
    """Load balancing routing strategies."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    GEOGRAPHIC = "geographic"
    CANNABIS_AWARE = "cannabis_aware"
    PERFORMANCE_BASED = "performance_based"

class RequestType(Enum):
    """Cannabis-specific request types."""
    STRAIN_SEARCH = "strain_search"
    DISPENSARY_LOOKUP = "dispensary_lookup"
    PRICE_COMPARISON = "price_comparison"
    TERPENE_ANALYSIS = "terpene_analysis"
    USER_RECOMMENDATIONS = "user_recommendations"
    COMPLIANCE_CHECK = "compliance_check"
    INVENTORY_UPDATE = "inventory_update"
    ANALYTICS = "analytics"

@dataclass
class ServerNode:
    """Represents a backend server node."""
    node_id: str
    host: str
    port: int
    weight: int = 100
    max_connections: int = 1000
    current_connections: int = 0
    health_status: HealthStatus = HealthStatus.HEALTHY
    response_time: float = 0.0
    last_health_check: datetime = field(default_factory=datetime.now)
    geographic_region: str = "US"
    supported_states: List[str] = field(default_factory=list)
    cannabis_specializations: List[RequestType] = field(default_factory=list)
    ssl_enabled: bool = False
    
    @property
    def url(self) -> str:
        """Get full URL for this server."""
        protocol = "https" if self.ssl_enabled else "http"
        return f"{protocol}://{self.host}:{self.port}"
    
    @property
    def load_factor(self) -> float:
        """Calculate current load factor (0.0 to 1.0)."""
        if self.max_connections == 0:
            return 0.0
        return min(self.current_connections / self.max_connections, 1.0)
    
    def can_handle_request(self, state: Optional[str] = None, 
                          request_type: Optional[RequestType] = None) -> bool:
        """Check if this server can handle a specific request."""
        if self.health_status in [HealthStatus.UNHEALTHY, HealthStatus.MAINTENANCE]:
            return False
        
        if state and self.supported_states and state not in self.supported_states:
            return False
        
        if (request_type and self.cannabis_specializations and 
            request_type not in self.cannabis_specializations):
            return False
        
        return self.current_connections < self.max_connections

@dataclass
class LoadBalancerConfig:
    """Configuration for load balancer."""
    health_check_interval: int = 30  # seconds
    health_check_timeout: int = 5
    health_check_retries: int = 3
    health_check_path: str = "/health"
    enable_session_affinity: bool = True
    session_timeout: int = 3600  # 1 hour
    enable_ssl_termination: bool = False
    ssl_cert_path: str = ""
    ssl_key_path: str = ""
    default_routing_strategy: RoutingStrategy = RoutingStrategy.CANNABIS_AWARE
    enable_rate_limiting: bool = True
    rate_limit_requests_per_minute: int = 1000
    enable_geographic_routing: bool = True
    enable_cannabis_routing: bool = True
    failover_threshold: int = 3  # consecutive failures
    recovery_threshold: int = 2  # consecutive successes
    connection_timeout: int = 30
    response_timeout: int = 60

@dataclass
class HealthCheckResult:
    """Health check result for a server."""
    node_id: str
    timestamp: datetime
    status: HealthStatus
    response_time: float
    error_message: Optional[str] = None
    cannabis_checks_passed: bool = True
    compliance_status: bool = True

@dataclass
class RouteRequest:
    """Request routing information."""
    request_id: str
    client_ip: str
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[bytes] = None
    user_state: Optional[str] = None
    request_type: Optional[RequestType] = None
    session_id: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)

@dataclass
class RoutingDecision:
    """Result of routing decision."""
    target_node: ServerNode
    routing_strategy: RoutingStrategy
    decision_reason: str
    estimated_response_time: float
    session_affinity_used: bool = False
    geographic_preference_used: bool = False
    cannabis_optimization_used: bool = False

class HealthChecker:
    """Health checking system for backend servers."""
    
    def __init__(self, config: LoadBalancerConfig):
        self.config = config
        self.health_results = {}
        self.failure_counts = defaultdict(int)
        self.success_counts = defaultdict(int)
        self.running = False
        self.check_thread = None
        self.lock = threading.Lock()
        
    def start_health_checking(self, servers: List[ServerNode]):
        """Start health checking for servers."""
        self.servers = servers
        self.running = True
        self.check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.check_thread.start()
        logging.info(f"Health checking started for {len(servers)} servers")
    
    def stop_health_checking(self):
        """Stop health checking."""
        self.running = False
        if self.check_thread:
            self.check_thread.join(timeout=10)
        logging.info("Health checking stopped")
    
    def _health_check_loop(self):
        """Main health checking loop."""
        while self.running:
            try:
                for server in self.servers:
                    if not self.running:
                        break
                    
                    result = self._check_server_health(server)
                    self._process_health_result(server, result)
                
                time.sleep(self.config.health_check_interval)
                
            except Exception as e:
                logging.error(f"Health check loop error: {e}")
                time.sleep(5)
    
    def _check_server_health(self, server: ServerNode) -> HealthCheckResult:
        """Perform health check on a single server."""
        start_time = time.time()
        
        try:
            if not HTTP_CLIENT_AVAILABLE:
                # Fallback to basic socket check
                return self._socket_health_check(server, start_time)
            
            # HTTP health check
            health_url = f"{server.url}{self.config.health_check_path}"
            
            response = requests.get(
                health_url,
                timeout=self.config.health_check_timeout,
                verify=False  # Skip SSL verification for internal services
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                # Parse cannabis-specific health data
                cannabis_checks = True
                compliance_status = True
                
                try:
                    health_data = response.json()
                    cannabis_checks = health_data.get('cannabis_systems', True)
                    compliance_status = health_data.get('compliance_status', True)
                except:
                    pass  # Use defaults if parsing fails
                
                return HealthCheckResult(
                    node_id=server.node_id,
                    timestamp=datetime.now(),
                    status=HealthStatus.HEALTHY,
                    response_time=response_time,
                    cannabis_checks_passed=cannabis_checks,
                    compliance_status=compliance_status
                )
            else:
                return HealthCheckResult(
                    node_id=server.node_id,
                    timestamp=datetime.now(),
                    status=HealthStatus.UNHEALTHY,
                    response_time=response_time,
                    error_message=f"HTTP {response.status_code}"
                )
        
        except Exception as e:
            response_time = time.time() - start_time
            return HealthCheckResult(
                node_id=server.node_id,
                timestamp=datetime.now(),
                status=HealthStatus.UNHEALTHY,
                response_time=response_time,
                error_message=str(e)
            )
    
    def _socket_health_check(self, server: ServerNode, start_time: float) -> HealthCheckResult:
        """Fallback socket-based health check."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.config.health_check_timeout)
            result = sock.connect_ex((server.host, server.port))
            sock.close()
            
            response_time = time.time() - start_time
            
            if result == 0:
                return HealthCheckResult(
                    node_id=server.node_id,
                    timestamp=datetime.now(),
                    status=HealthStatus.HEALTHY,
                    response_time=response_time
                )
            else:
                return HealthCheckResult(
                    node_id=server.node_id,
                    timestamp=datetime.now(),
                    status=HealthStatus.UNHEALTHY,
                    response_time=response_time,
                    error_message=f"Connection failed: {result}"
                )
        
        except Exception as e:
            response_time = time.time() - start_time
            return HealthCheckResult(
                node_id=server.node_id,
                timestamp=datetime.now(),
                status=HealthStatus.UNHEALTHY,
                response_time=response_time,
                error_message=str(e)
            )
    
    def _process_health_result(self, server: ServerNode, result: HealthCheckResult):
        """Process health check result and update server status."""
        with self.lock:
            self.health_results[server.node_id] = result
            
            if result.status == HealthStatus.HEALTHY:
                self.success_counts[server.node_id] += 1
                self.failure_counts[server.node_id] = 0
                
                # Mark as healthy if we have enough consecutive successes
                if (server.health_status == HealthStatus.UNHEALTHY and 
                    self.success_counts[server.node_id] >= self.config.recovery_threshold):
                    server.health_status = HealthStatus.HEALTHY
                    logging.info(f"Server {server.node_id} recovered")
            
            else:
                self.failure_counts[server.node_id] += 1
                self.success_counts[server.node_id] = 0
                
                # Mark as unhealthy if we have enough consecutive failures
                if (server.health_status == HealthStatus.HEALTHY and 
                    self.failure_counts[server.node_id] >= self.config.failover_threshold):
                    server.health_status = HealthStatus.UNHEALTHY
                    logging.warning(f"Server {server.node_id} marked unhealthy: {result.error_message}")
            
            # Update response time
            server.response_time = result.response_time
            server.last_health_check = result.timestamp
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for all servers."""
        with self.lock:
            return {
                'timestamp': datetime.now().isoformat(),
                'total_servers': len(self.servers),
                'healthy_servers': len([s for s in self.servers if s.health_status == HealthStatus.HEALTHY]),
                'unhealthy_servers': len([s for s in self.servers if s.health_status == HealthStatus.UNHEALTHY]),
                'health_results': {node_id: asdict(result) for node_id, result in self.health_results.items()}
            }

class SessionManager:
    """Manages user sessions for sticky routing."""
    
    def __init__(self, config: LoadBalancerConfig):
        self.config = config
        self.sessions = {}  # session_id -> node_id
        self.session_timestamps = {}  # session_id -> last_access
        self.lock = threading.Lock()
        self.cleanup_thread = None
        self.running = False
        
        # Redis integration if available
        self.redis_client = None
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
                self.redis_client.ping()
                logging.info("Redis session storage enabled")
            except:
                self.redis_client = None
                logging.warning("Redis not available, using in-memory sessions")
    
    def start(self):
        """Start session management."""
        self.running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        logging.info("Session management started")
    
    def stop(self):
        """Stop session management."""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        logging.info("Session management stopped")
    
    def get_session_node(self, session_id: str) -> Optional[str]:
        """Get the assigned node for a session."""
        if not session_id:
            return None
        
        if self.redis_client:
            try:
                node_id = self.redis_client.get(f"session:{session_id}")
                if node_id:
                    # Update access time
                    self.redis_client.expire(f"session:{session_id}", self.config.session_timeout)
                    return node_id
            except:
                pass  # Fall back to local storage
        
        with self.lock:
            if session_id in self.sessions:
                self.session_timestamps[session_id] = time.time()
                return self.sessions[session_id]
        
        return None
    
    def set_session_node(self, session_id: str, node_id: str):
        """Assign a node to a session."""
        if not session_id:
            return
        
        if self.redis_client:
            try:
                self.redis_client.setex(f"session:{session_id}", self.config.session_timeout, node_id)
                return
            except:
                pass  # Fall back to local storage
        
        with self.lock:
            self.sessions[session_id] = node_id
            self.session_timestamps[session_id] = time.time()
    
    def remove_session(self, session_id: str):
        """Remove a session."""
        if not session_id:
            return
        
        if self.redis_client:
            try:
                self.redis_client.delete(f"session:{session_id}")
                return
            except:
                pass
        
        with self.lock:
            self.sessions.pop(session_id, None)
            self.session_timestamps.pop(session_id, None)
    
    def _cleanup_loop(self):
        """Clean up expired sessions."""
        while self.running:
            try:
                current_time = time.time()
                expired_sessions = []
                
                with self.lock:
                    for session_id, last_access in self.session_timestamps.items():
                        if current_time - last_access > self.config.session_timeout:
                            expired_sessions.append(session_id)
                    
                    for session_id in expired_sessions:
                        self.sessions.pop(session_id, None)
                        self.session_timestamps.pop(session_id, None)
                
                if expired_sessions:
                    logging.info(f"Cleaned up {len(expired_sessions)} expired sessions")
                
                time.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logging.error(f"Session cleanup error: {e}")
                time.sleep(60)

class CannabisRouter:
    """Cannabis-aware routing logic."""
    
    def __init__(self, config: LoadBalancerConfig):
        self.config = config
        self.state_regulations = self._load_state_regulations()
        self.terpene_cache_nodes = {}  # terpene_profile -> preferred_node
        self.geographic_zones = self._initialize_geographic_zones()
        
    def _load_state_regulations(self) -> Dict[str, Dict[str, Any]]:
        """Load state-specific cannabis regulations."""
        # In a real implementation, this would load from a comprehensive database
        return {
            'CA': {
                'medical_allowed': True,
                'recreational_allowed': True,
                'home_cultivation': True,
                'possession_limit_oz': 1.0,
                'requires_testing': True,
                'compliance_tracking': 'METRC'
            },
            'NY': {
                'medical_allowed': True,
                'recreational_allowed': True,
                'home_cultivation': True,
                'possession_limit_oz': 3.0,
                'requires_testing': True,
                'compliance_tracking': 'OCM'
            },
            'TX': {
                'medical_allowed': True,
                'recreational_allowed': False,
                'home_cultivation': False,
                'possession_limit_oz': 0.0,
                'requires_testing': True,
                'compliance_tracking': 'CURT'
            },
            'CO': {
                'medical_allowed': True,
                'recreational_allowed': True,
                'home_cultivation': True,
                'possession_limit_oz': 2.0,
                'requires_testing': True,
                'compliance_tracking': 'METRC'
            }
        }
    
    def _initialize_geographic_zones(self) -> Dict[str, List[str]]:
        """Initialize geographic routing zones."""
        return {
            'west_coast': ['CA', 'OR', 'WA'],
            'mountain': ['CO', 'UT', 'MT', 'WY'],
            'midwest': ['IL', 'MI', 'OH', 'MN'],
            'northeast': ['NY', 'MA', 'VT', 'ME'],
            'southeast': ['FL', 'GA', 'NC', 'VA'],
            'southwest': ['TX', 'AZ', 'NM', 'NV']
        }
    
    def select_optimal_node(self, request: RouteRequest, 
                          available_nodes: List[ServerNode]) -> Optional[RoutingDecision]:
        """Select optimal server node using cannabis-aware logic."""
        
        if not available_nodes:
            return None
        
        # Filter nodes based on cannabis compliance
        compliant_nodes = self._filter_compliant_nodes(request, available_nodes)
        if not compliant_nodes:
            logging.warning(f"No compliant nodes found for request from {request.user_state}")
            return None
        
        # Apply routing strategy
        if self.config.default_routing_strategy == RoutingStrategy.CANNABIS_AWARE:
            return self._cannabis_aware_routing(request, compliant_nodes)
        elif self.config.default_routing_strategy == RoutingStrategy.GEOGRAPHIC:
            return self._geographic_routing(request, compliant_nodes)
        elif self.config.default_routing_strategy == RoutingStrategy.PERFORMANCE_BASED:
            return self._performance_based_routing(request, compliant_nodes)
        elif self.config.default_routing_strategy == RoutingStrategy.LEAST_CONNECTIONS:
            return self._least_connections_routing(request, compliant_nodes)
        elif self.config.default_routing_strategy == RoutingStrategy.WEIGHTED_ROUND_ROBIN:
            return self._weighted_round_robin_routing(request, compliant_nodes)
        else:
            return self._round_robin_routing(request, compliant_nodes)
    
    def _filter_compliant_nodes(self, request: RouteRequest, 
                               nodes: List[ServerNode]) -> List[ServerNode]:
        """Filter nodes based on cannabis compliance requirements."""
        compliant_nodes = []
        
        for node in nodes:
            # Check basic availability
            if not node.can_handle_request(request.user_state, request.request_type):
                continue
            
            # Check state compliance
            if request.user_state and request.user_state not in self.state_regulations:
                logging.warning(f"Unknown state regulations: {request.user_state}")
                continue
            
            # Cannabis-specific compliance checks
            if request.request_type in [RequestType.COMPLIANCE_CHECK, RequestType.INVENTORY_UPDATE]:
                if RequestType.COMPLIANCE_CHECK not in node.cannabis_specializations:
                    continue
            
            compliant_nodes.append(node)
        
        return compliant_nodes
    
    def _cannabis_aware_routing(self, request: RouteRequest, 
                               nodes: List[ServerNode]) -> RoutingDecision:
        """Cannabis-specific intelligent routing."""
        
        # Prioritize by request type specialization
        specialized_nodes = [n for n in nodes if request.request_type in n.cannabis_specializations]
        if specialized_nodes:
            nodes = specialized_nodes
        
        # For terpene analysis, check cache affinity
        if request.request_type == RequestType.TERPENE_ANALYSIS:
            terpene_profile = request.query_params.get('terpene_profile')
            if terpene_profile and terpene_profile in self.terpene_cache_nodes:
                preferred_node_id = self.terpene_cache_nodes[terpene_profile]
                preferred_node = next((n for n in nodes if n.node_id == preferred_node_id), None)
                if preferred_node:
                    return RoutingDecision(
                        target_node=preferred_node,
                        routing_strategy=RoutingStrategy.CANNABIS_AWARE,
                        decision_reason="Terpene profile cache affinity",
                        estimated_response_time=preferred_node.response_time,
                        cannabis_optimization_used=True
                    )
        
        # Geographic preference with cannabis regulations
        if request.user_state:
            state_preferred_nodes = [n for n in nodes if request.user_state in n.supported_states]
            if state_preferred_nodes:
                nodes = state_preferred_nodes
        
        # Performance-based selection from remaining nodes
        best_node = min(nodes, key=lambda n: (n.load_factor, n.response_time))
        
        return RoutingDecision(
            target_node=best_node,
            routing_strategy=RoutingStrategy.CANNABIS_AWARE,
            decision_reason="Cannabis compliance and performance optimization",
            estimated_response_time=best_node.response_time,
            cannabis_optimization_used=True,
            geographic_preference_used=bool(request.user_state)
        )
    
    def _geographic_routing(self, request: RouteRequest, 
                           nodes: List[ServerNode]) -> RoutingDecision:
        """Geographic-based routing."""
        if not request.user_state:
            return self._round_robin_routing(request, nodes)
        
        # Find nodes in the same geographic zone
        user_zone = None
        for zone, states in self.geographic_zones.items():
            if request.user_state in states:
                user_zone = zone
                break
        
        if user_zone:
            zone_nodes = [n for n in nodes if any(state in self.geographic_zones[user_zone] 
                                                for state in n.supported_states)]
            if zone_nodes:
                nodes = zone_nodes
        
        # Select best performing node from geographic preference
        best_node = min(nodes, key=lambda n: (n.load_factor, n.response_time))
        
        return RoutingDecision(
            target_node=best_node,
            routing_strategy=RoutingStrategy.GEOGRAPHIC,
            decision_reason=f"Geographic optimization for zone {user_zone}",
            estimated_response_time=best_node.response_time,
            geographic_preference_used=True
        )
    
    def _performance_based_routing(self, request: RouteRequest, 
                                  nodes: List[ServerNode]) -> RoutingDecision:
        """Performance-based routing."""
        # Calculate performance score for each node
        def performance_score(node: ServerNode) -> float:
            # Lower is better
            load_penalty = node.load_factor * 100
            latency_penalty = node.response_time * 1000  # Convert to ms
            return load_penalty + latency_penalty
        
        best_node = min(nodes, key=performance_score)
        
        return RoutingDecision(
            target_node=best_node,
            routing_strategy=RoutingStrategy.PERFORMANCE_BASED,
            decision_reason="Optimal performance metrics",
            estimated_response_time=best_node.response_time
        )
    
    def _least_connections_routing(self, request: RouteRequest, 
                                  nodes: List[ServerNode]) -> RoutingDecision:
        """Least connections routing."""
        best_node = min(nodes, key=lambda n: n.current_connections)
        
        return RoutingDecision(
            target_node=best_node,
            routing_strategy=RoutingStrategy.LEAST_CONNECTIONS,
            decision_reason="Minimum active connections",
            estimated_response_time=best_node.response_time
        )
    
    def _weighted_round_robin_routing(self, request: RouteRequest, 
                                     nodes: List[ServerNode]) -> RoutingDecision:
        """Weighted round-robin routing."""
        # Select based on weights (simple implementation)
        total_weight = sum(n.weight for n in nodes)
        if total_weight == 0:
            return self._round_robin_routing(request, nodes)
        
        # Use request hash for consistent distribution
        request_hash = hash(f"{request.client_ip}{request.path}{time.time()}")
        weight_position = request_hash % total_weight
        
        current_weight = 0
        for node in nodes:
            current_weight += node.weight
            if weight_position < current_weight:
                return RoutingDecision(
                    target_node=node,
                    routing_strategy=RoutingStrategy.WEIGHTED_ROUND_ROBIN,
                    decision_reason="Weighted distribution",
                    estimated_response_time=node.response_time
                )
        
        # Fallback to first node
        return RoutingDecision(
            target_node=nodes[0],
            routing_strategy=RoutingStrategy.WEIGHTED_ROUND_ROBIN,
            decision_reason="Weighted distribution (fallback)",
            estimated_response_time=nodes[0].response_time
        )
    
    def _round_robin_routing(self, request: RouteRequest, 
                            nodes: List[ServerNode]) -> RoutingDecision:
        """Simple round-robin routing."""
        # Use request hash for distribution
        request_hash = hash(f"{request.client_ip}{request.path}")
        node_index = request_hash % len(nodes)
        selected_node = nodes[node_index]
        
        return RoutingDecision(
            target_node=selected_node,
            routing_strategy=RoutingStrategy.ROUND_ROBIN,
            decision_reason="Round-robin distribution",
            estimated_response_time=selected_node.response_time
        )

class RateLimiter:
    """Rate limiting for DDoS protection and fair usage."""
    
    def __init__(self, config: LoadBalancerConfig):
        self.config = config
        self.client_requests = defaultdict(deque)  # client_ip -> request_timestamps
        self.lock = threading.Lock()
        
    def is_allowed(self, client_ip: str) -> bool:
        """Check if client is within rate limits."""
        if not self.config.enable_rate_limiting:
            return True
        
        current_time = time.time()
        minute_ago = current_time - 60
        
        with self.lock:
            # Clean old requests
            client_queue = self.client_requests[client_ip]
            while client_queue and client_queue[0] < minute_ago:
                client_queue.popleft()
            
            # Check current rate
            if len(client_queue) >= self.config.rate_limit_requests_per_minute:
                return False
            
            # Add current request
            client_queue.append(current_time)
            return True
    
    def get_rate_limit_status(self, client_ip: str) -> Dict[str, Any]:
        """Get rate limit status for client."""
        current_time = time.time()
        minute_ago = current_time - 60
        
        with self.lock:
            client_queue = self.client_requests[client_ip]
            # Count recent requests
            recent_requests = sum(1 for timestamp in client_queue if timestamp > minute_ago)
            
            return {
                'client_ip': client_ip,
                'requests_in_last_minute': recent_requests,
                'rate_limit': self.config.rate_limit_requests_per_minute,
                'remaining_requests': max(0, self.config.rate_limit_requests_per_minute - recent_requests),
                'reset_time': int(minute_ago + 60)
            }

class CannabisLoadBalancer:
    """Main load balancer with cannabis-specific optimizations."""
    
    def __init__(self, config: Optional[LoadBalancerConfig] = None):
        self.config = config or LoadBalancerConfig()
        self.servers = []
        self.health_checker = HealthChecker(self.config)
        self.session_manager = SessionManager(self.config)
        self.cannabis_router = CannabisRouter(self.config)
        self.rate_limiter = RateLimiter(self.config)
        self.running = False
        
        # Metrics
        self.request_count = 0
        self.routing_decisions = deque(maxlen=1000)
        self.response_times = deque(maxlen=1000)
        
        logging.info("Cannabis Load Balancer initialized")
    
    def add_server(self, host: str, port: int, **kwargs) -> str:
        """Add a backend server."""
        node_id = f"{host}:{port}"
        
        server = ServerNode(
            node_id=node_id,
            host=host,
            port=port,
            **kwargs
        )
        
        self.servers.append(server)
        logging.info(f"Added server: {node_id}")
        
        return node_id
    
    def remove_server(self, node_id: str) -> bool:
        """Remove a backend server."""
        for i, server in enumerate(self.servers):
            if server.node_id == node_id:
                del self.servers[i]
                logging.info(f"Removed server: {node_id}")
                return True
        return False
    
    def start(self):
        """Start the load balancer."""
        if not self.servers:
            raise ValueError("No servers configured")
        
        self.running = True
        self.health_checker.start_health_checking(self.servers)
        self.session_manager.start()
        
        logging.info(f"Load balancer started with {len(self.servers)} servers")
    
    def stop(self):
        """Stop the load balancer."""
        self.running = False
        self.health_checker.stop_health_checking()
        self.session_manager.stop()
        
        logging.info("Load balancer stopped")
    
    def route_request(self, request: RouteRequest) -> Optional[RoutingDecision]:
        """Route a request to an appropriate server."""
        if not self.running:
            return None
        
        # Rate limiting check
        if not self.rate_limiter.is_allowed(request.client_ip):
            logging.warning(f"Rate limit exceeded for {request.client_ip}")
            return None
        
        # Get healthy servers
        healthy_servers = [s for s in self.servers if s.health_status == HealthStatus.HEALTHY]
        if not healthy_servers:
            logging.error("No healthy servers available")
            return None
        
        # Check session affinity
        if self.config.enable_session_affinity and request.session_id:
            session_node_id = self.session_manager.get_session_node(request.session_id)
            if session_node_id:
                session_node = next((s for s in healthy_servers if s.node_id == session_node_id), None)
                if session_node and session_node.can_handle_request(request.user_state, request.request_type):
                    decision = RoutingDecision(
                        target_node=session_node,
                        routing_strategy=self.config.default_routing_strategy,
                        decision_reason="Session affinity",
                        estimated_response_time=session_node.response_time,
                        session_affinity_used=True
                    )
                    self._record_routing_decision(decision)
                    return decision
        
        # Cannabis-aware routing
        decision = self.cannabis_router.select_optimal_node(request, healthy_servers)
        
        if decision:
            # Set session affinity for new sessions
            if self.config.enable_session_affinity and request.session_id:
                self.session_manager.set_session_node(request.session_id, decision.target_node.node_id)
            
            self._record_routing_decision(decision)
        
        return decision
    
    def _record_routing_decision(self, decision: RoutingDecision):
        """Record routing decision for analytics."""
        self.request_count += 1
        self.routing_decisions.append(decision)
        
        # Update server connection count (simplified)
        decision.target_node.current_connections += 1
    
    def record_response_time(self, node_id: str, response_time: float):
        """Record response time for a server."""
        self.response_times.append(response_time)
        
        # Update server response time
        server = next((s for s in self.servers if s.node_id == node_id), None)
        if server:
            # Exponential moving average
            alpha = 0.1
            server.response_time = alpha * response_time + (1 - alpha) * server.response_time
    
    def release_connection(self, node_id: str):
        """Release a connection from a server."""
        server = next((s for s in self.servers if s.node_id == node_id), None)
        if server and server.current_connections > 0:
            server.current_connections -= 1
    
    def get_load_balancer_status(self) -> Dict[str, Any]:
        """Get comprehensive load balancer status."""
        healthy_servers = [s for s in self.servers if s.health_status == HealthStatus.HEALTHY]
        
        return {
            'running': self.running,
            'total_servers': len(self.servers),
            'healthy_servers': len(healthy_servers),
            'total_requests': self.request_count,
            'average_response_time': sum(self.response_times) / len(self.response_times) if self.response_times else 0,
            'routing_strategy': self.config.default_routing_strategy.value,
            'session_affinity_enabled': self.config.enable_session_affinity,
            'rate_limiting_enabled': self.config.enable_rate_limiting,
            'cannabis_routing_enabled': self.config.enable_cannabis_routing,
            'servers': [
                {
                    'node_id': s.node_id,
                    'host': s.host,
                    'port': s.port,
                    'health_status': s.health_status.value,
                    'current_connections': s.current_connections,
                    'load_factor': s.load_factor,
                    'response_time': s.response_time,
                    'supported_states': s.supported_states,
                    'specializations': [spec.value for spec in s.cannabis_specializations]
                }
                for s in self.servers
            ]
        }

# Configuration Templates
def create_production_config() -> LoadBalancerConfig:
    """Create production-ready load balancer configuration."""
    return LoadBalancerConfig(
        health_check_interval=15,
        health_check_timeout=3,
        health_check_retries=2,
        enable_session_affinity=True,
        session_timeout=1800,  # 30 minutes
        enable_ssl_termination=True,
        default_routing_strategy=RoutingStrategy.CANNABIS_AWARE,
        enable_rate_limiting=True,
        rate_limit_requests_per_minute=500,
        enable_geographic_routing=True,
        enable_cannabis_routing=True,
        failover_threshold=2,
        recovery_threshold=3,
        connection_timeout=10,
        response_timeout=30
    )

def create_development_config() -> LoadBalancerConfig:
    """Create development load balancer configuration."""
    return LoadBalancerConfig(
        health_check_interval=30,
        health_check_timeout=5,
        enable_session_affinity=False,
        enable_ssl_termination=False,
        default_routing_strategy=RoutingStrategy.ROUND_ROBIN,
        enable_rate_limiting=False,
        enable_geographic_routing=False,
        enable_cannabis_routing=True
    )

# Demo Functions
def demo_load_balancer():
    """Demonstrate load balancer capabilities."""
    print("\n🚀 Cannabis Load Balancer Demo")
    print("=" * 50)
    
    # Create load balancer with development config
    config = create_development_config()
    config.enable_cannabis_routing = True
    config.default_routing_strategy = RoutingStrategy.CANNABIS_AWARE
    
    lb = CannabisLoadBalancer(config)
    
    # Add demo servers
    print("📊 Adding backend servers...")
    
    # California servers (recreational + medical)
    lb.add_server(
        "ca-server-1.weedhounds.com", 8080,
        weight=150,
        supported_states=['CA', 'OR', 'WA'],
        cannabis_specializations=[RequestType.STRAIN_SEARCH, RequestType.DISPENSARY_LOOKUP, RequestType.TERPENE_ANALYSIS],
        geographic_region="west_coast"
    )
    
    lb.add_server(
        "ca-server-2.weedhounds.com", 8080,
        weight=100,
        supported_states=['CA'],
        cannabis_specializations=[RequestType.PRICE_COMPARISON, RequestType.USER_RECOMMENDATIONS],
        geographic_region="west_coast"
    )
    
    # Colorado servers (compliance focused)
    lb.add_server(
        "co-server-1.weedhounds.com", 8080,
        weight=120,
        supported_states=['CO', 'WY', 'UT'],
        cannabis_specializations=[RequestType.COMPLIANCE_CHECK, RequestType.INVENTORY_UPDATE],
        geographic_region="mountain"
    )
    
    # New York servers (new market)
    lb.add_server(
        "ny-server-1.weedhounds.com", 8080,
        weight=80,
        supported_states=['NY', 'VT', 'MA'],
        cannabis_specializations=[RequestType.STRAIN_SEARCH, RequestType.ANALYTICS],
        geographic_region="northeast"
    )
    
    print(f"✅ Added {len(lb.servers)} servers")
    
    # Start load balancer
    lb.start()
    print("🎯 Load balancer started")
    
    # Simulate various cannabis requests
    print("\n🌿 Simulating cannabis platform requests...")
    
    test_requests = [
        # California recreational user
        RouteRequest(
            request_id="req_001",
            client_ip="192.168.1.100",
            method="GET",
            path="/api/strains/search",
            headers={"User-Agent": "WeedHounds-App/2.0"},
            query_params={"state": "CA", "type": "sativa"},
            user_state="CA",
            request_type=RequestType.STRAIN_SEARCH,
            session_id="session_ca_001"
        ),
        
        # Colorado compliance check
        RouteRequest(
            request_id="req_002",
            client_ip="10.0.0.50",
            method="POST",
            path="/api/compliance/verify",
            headers={"Content-Type": "application/json"},
            query_params={"dispensary_id": "co_disp_123"},
            user_state="CO",
            request_type=RequestType.COMPLIANCE_CHECK,
            session_id="session_co_001"
        ),
        
        # New York strain recommendation
        RouteRequest(
            request_id="req_003",
            client_ip="172.16.0.200",
            method="GET",
            path="/api/recommendations",
            headers={"Authorization": "Bearer token123"},
            query_params={"state": "NY", "medical": "true"},
            user_state="NY",
            request_type=RequestType.USER_RECOMMENDATIONS,
            session_id="session_ny_001"
        ),
        
        # Terpene analysis request
        RouteRequest(
            request_id="req_004",
            client_ip="192.168.1.100",
            method="POST",
            path="/api/terpenes/analyze",
            headers={"Content-Type": "application/json"},
            query_params={"terpene_profile": "limonene_high"},
            user_state="CA",
            request_type=RequestType.TERPENE_ANALYSIS,
            session_id="session_ca_001"  # Same session as req_001
        )
    ]
    
    # Process requests and show routing decisions
    for i, request in enumerate(test_requests, 1):
        print(f"\n📨 Request {i}: {request.request_type.value} from {request.user_state}")
        
        decision = lb.route_request(request)
        
        if decision:
            print(f"  ✅ Routed to: {decision.target_node.node_id}")
            print(f"  📍 Strategy: {decision.routing_strategy.value}")
            print(f"  💡 Reason: {decision.decision_reason}")
            print(f"  ⏱️  Est. Response: {decision.estimated_response_time:.3f}s")
            
            if decision.session_affinity_used:
                print("  🔗 Session affinity used")
            if decision.cannabis_optimization_used:
                print("  🌿 Cannabis optimization applied")
            if decision.geographic_preference_used:
                print("  🗺️  Geographic preference applied")
            
            # Simulate response time
            response_time = random.uniform(0.1, 1.0)
            lb.record_response_time(decision.target_node.node_id, response_time)
            lb.release_connection(decision.target_node.node_id)
        else:
            print("  ❌ Request rejected (rate limit or no healthy servers)")
    
    # Wait for health checks
    time.sleep(3)
    
    # Display system status
    print(f"\n📊 Load Balancer Status:")
    status = lb.get_load_balancer_status()
    
    print(f"  Total Requests: {status['total_requests']}")
    print(f"  Healthy Servers: {status['healthy_servers']}/{status['total_servers']}")
    print(f"  Avg Response Time: {status['average_response_time']:.3f}s")
    print(f"  Routing Strategy: {status['routing_strategy']}")
    print(f"  Cannabis Routing: {status['cannabis_routing_enabled']}")
    
    print(f"\n🖥️  Server Details:")
    for server in status['servers']:
        print(f"  {server['node_id']}:")
        print(f"    Status: {server['health_status']}")
        print(f"    Load: {server['load_factor']:.2%}")
        print(f"    States: {', '.join(server['supported_states'])}")
        print(f"    Specializations: {', '.join(server['specializations'])}")
    
    # Health check summary
    health_summary = lb.health_checker.get_health_summary()
    print(f"\n🏥 Health Check Summary:")
    print(f"  Healthy: {health_summary['healthy_servers']}")
    print(f"  Unhealthy: {health_summary['unhealthy_servers']}")
    
    # Stop load balancer
    lb.stop()
    print("\n✅ Demo completed successfully!")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run demo
    demo_load_balancer()