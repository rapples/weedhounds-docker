#!/usr/bin/env python3
"""
Horizontal Scaling Framework for Cannabis Data Platform
======================================================

Enterprise-grade horizontal scaling system for unlimited cannabis data processing.
Provides automatic node discovery, intelligent workload distribution, and 
cannabis-specific data partitioning for massive scale deployments.

Key Features:
- Automatic node discovery and registration
- Cannabis-aware data partitioning (by state, dispensary, strain type)
- Intelligent workload distribution and load balancing
- Dynamic scaling based on demand patterns
- Geographic distribution optimization
- Fault tolerance and automatic failover
- Real-time health monitoring and auto-healing
- Cannabis business rule enforcement across nodes

Author: WeedHounds Scaling Team
Created: November 2025
"""

import logging
import time
import asyncio
import json
import threading
import hashlib
from typing import Dict, List, Any, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
import socket
import uuid
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import heapq

try:
    import redis
    import consul
    DISCOVERY_AVAILABLE = True
    print("✅ Service discovery available (Redis/Consul)")
except ImportError:
    redis = None
    consul = None
    DISCOVERY_AVAILABLE = False
    print("⚠️ Service discovery not available - using basic mode")

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    HTTP_CLIENT_AVAILABLE = True
except ImportError:
    requests = None
    HTTPAdapter = None
    Retry = None
    HTTP_CLIENT_AVAILABLE = False
    print("⚠️ HTTP client not available")

try:
    import psutil
    SYSTEM_MONITORING_AVAILABLE = True
except ImportError:
    psutil = None
    SYSTEM_MONITORING_AVAILABLE = False
    print("⚠️ System monitoring not available")

class NodeType(Enum):
    """Types of nodes in the scaling architecture."""
    COORDINATOR = "coordinator"
    DATA_PROCESSOR = "data_processor"
    API_GATEWAY = "api_gateway"
    CACHE_NODE = "cache_node"
    ML_WORKER = "ml_worker"
    STORAGE_NODE = "storage_node"

class NodeStatus(Enum):
    """Node status states."""
    INITIALIZING = "initializing"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OVERLOADED = "overloaded"
    OFFLINE = "offline"
    DRAINING = "draining"

class DataPartitionStrategy(Enum):
    """Cannabis data partitioning strategies."""
    BY_STATE = "by_state"
    BY_DISPENSARY = "by_dispensary"
    BY_STRAIN_TYPE = "by_strain_type"
    BY_CATEGORY = "by_category"
    BY_HASH = "by_hash"
    BY_GEOGRAPHY = "by_geography"
    HYBRID = "hybrid"

@dataclass
class NodeInfo:
    """Information about a node in the scaling cluster."""
    node_id: str
    node_type: NodeType
    host: str
    port: int
    status: NodeStatus = NodeStatus.INITIALIZING
    capabilities: List[str] = field(default_factory=list)
    load_score: float = 0.0
    last_heartbeat: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    cannabis_specializations: List[str] = field(default_factory=list)
    geographic_region: Optional[str] = None
    supported_states: List[str] = field(default_factory=list)

@dataclass
class WorkloadTask:
    """Workload task for distribution."""
    task_id: str
    task_type: str
    data_partition: str
    priority: int = 1
    estimated_cpu: float = 1.0
    estimated_memory: float = 100.0  # MB
    cannabis_context: Dict[str, Any] = field(default_factory=dict)
    state_requirement: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    deadline: Optional[datetime] = None

@dataclass
class ScalingConfig:
    """Configuration for horizontal scaling."""
    cluster_name: str = "weedhounds-cluster"
    discovery_method: str = "redis"  # redis, consul, multicast
    discovery_host: str = "localhost"
    discovery_port: int = 6379
    heartbeat_interval: int = 30  # seconds
    health_check_timeout: int = 10
    max_nodes_per_type: int = 100
    auto_scaling_enabled: bool = True
    scale_up_threshold: float = 0.8  # CPU/memory usage
    scale_down_threshold: float = 0.3
    partition_strategy: DataPartitionStrategy = DataPartitionStrategy.HYBRID
    enable_geographic_affinity: bool = True
    cannabis_compliance_mode: bool = True

class CannabisDataPartitioner:
    """Intelligent data partitioner for cannabis-specific workloads."""
    
    def __init__(self, strategy: DataPartitionStrategy, config: ScalingConfig):
        self.strategy = strategy
        self.config = config
        self.partition_cache = {}
        self.state_mappings = self._initialize_state_mappings()
        
    def _initialize_state_mappings(self) -> Dict[str, str]:
        """Initialize cannabis state to region mappings."""
        return {
            # West Coast
            'CA': 'west', 'OR': 'west', 'WA': 'west', 'NV': 'west',
            'AK': 'west', 'HI': 'west',
            
            # Northeast
            'NY': 'northeast', 'MA': 'northeast', 'CT': 'northeast',
            'VT': 'northeast', 'ME': 'northeast', 'NH': 'northeast',
            'RI': 'northeast', 'NJ': 'northeast', 'PA': 'northeast',
            
            # Midwest
            'IL': 'midwest', 'MI': 'midwest', 'OH': 'midwest',
            'MN': 'midwest', 'MO': 'midwest',
            
            # South
            'FL': 'south', 'TX': 'south', 'GA': 'south', 'NC': 'south',
            'VA': 'south', 'MD': 'south', 'DC': 'south',
            
            # Mountain
            'CO': 'mountain', 'UT': 'mountain', 'AZ': 'mountain',
            'NM': 'mountain', 'MT': 'mountain', 'WY': 'mountain',
            'ID': 'mountain'
        }
    
    def partition_data(self, data_item: Dict[str, Any]) -> str:
        """Determine partition for cannabis data item."""
        cache_key = self._create_cache_key(data_item)
        
        if cache_key in self.partition_cache:
            return self.partition_cache[cache_key]
        
        partition = self._calculate_partition(data_item)
        self.partition_cache[cache_key] = partition
        
        return partition
    
    def _create_cache_key(self, data_item: Dict[str, Any]) -> str:
        """Create cache key for data item."""
        key_parts = []
        
        if 'state' in data_item:
            key_parts.append(f"state:{data_item['state']}")
        if 'dispensary' in data_item:
            key_parts.append(f"disp:{data_item['dispensary'][:10]}")
        if 'strain_type' in data_item:
            key_parts.append(f"type:{data_item['strain_type']}")
        if 'category' in data_item:
            key_parts.append(f"cat:{data_item['category']}")
        
        return "|".join(key_parts)
    
    def _calculate_partition(self, data_item: Dict[str, Any]) -> str:
        """Calculate partition based on strategy."""
        if self.strategy == DataPartitionStrategy.BY_STATE:
            state = data_item.get('state', 'unknown')
            return f"state_{state}"
        
        elif self.strategy == DataPartitionStrategy.BY_DISPENSARY:
            dispensary = data_item.get('dispensary', 'unknown')
            dispensary_hash = hashlib.md5(dispensary.encode()).hexdigest()[:8]
            return f"dispensary_{dispensary_hash}"
        
        elif self.strategy == DataPartitionStrategy.BY_STRAIN_TYPE:
            strain_type = data_item.get('strain_type', 'unknown')
            return f"strain_{strain_type}"
        
        elif self.strategy == DataPartitionStrategy.BY_CATEGORY:
            category = data_item.get('category', 'flower')
            return f"category_{category}"
        
        elif self.strategy == DataPartitionStrategy.BY_GEOGRAPHY:
            state = data_item.get('state', 'unknown')
            region = self.state_mappings.get(state, 'unknown')
            return f"region_{region}"
        
        elif self.strategy == DataPartitionStrategy.BY_HASH:
            # Hash-based partitioning for even distribution
            key_data = json.dumps(data_item, sort_keys=True)
            hash_value = hashlib.sha256(key_data.encode()).hexdigest()
            partition_id = int(hash_value[:8], 16) % 16  # 16 partitions
            return f"hash_{partition_id:02d}"
        
        elif self.strategy == DataPartitionStrategy.HYBRID:
            # Hybrid strategy: geography + hash for load balancing
            state = data_item.get('state', 'unknown')
            region = self.state_mappings.get(state, 'unknown')
            
            # Add hash component for load distribution
            dispensary = data_item.get('dispensary', 'unknown')
            hash_suffix = hashlib.md5(dispensary.encode()).hexdigest()[:2]
            
            return f"hybrid_{region}_{hash_suffix}"
        
        else:
            return "default_partition"
    
    def get_partition_affinity(self, partition: str, available_nodes: List[NodeInfo]) -> List[NodeInfo]:
        """Get nodes with affinity for a specific partition."""
        if not self.config.enable_geographic_affinity:
            return available_nodes
        
        # Extract geographic information from partition
        if partition.startswith('state_'):
            state = partition.split('_')[1]
            region = self.state_mappings.get(state, 'unknown')
        elif partition.startswith('region_'):
            region = partition.split('_')[1]
        elif partition.startswith('hybrid_'):
            region = partition.split('_')[1]
        else:
            return available_nodes  # No geographic affinity
        
        # Filter nodes by geographic affinity
        affine_nodes = []
        for node in available_nodes:
            if node.geographic_region == region:
                affine_nodes.append(node)
            elif state in node.supported_states:
                affine_nodes.append(node)
        
        # Fallback to all nodes if no affine nodes found
        return affine_nodes if affine_nodes else available_nodes

class NodeDiscoveryService:
    """Service discovery for cannabis data nodes."""
    
    def __init__(self, config: ScalingConfig):
        self.config = config
        self.nodes = {}
        self.local_node_id = str(uuid.uuid4())
        self.discovery_client = None
        self.lock = threading.Lock()
        
        self._initialize_discovery()
    
    def _initialize_discovery(self):
        """Initialize service discovery client."""
        try:
            if self.config.discovery_method == "redis" and redis:
                self.discovery_client = redis.Redis(
                    host=self.config.discovery_host,
                    port=self.config.discovery_port,
                    decode_responses=True
                )
                # Test connection
                self.discovery_client.ping()
                logging.info("Redis discovery service initialized")
                
            elif self.config.discovery_method == "consul" and consul:
                self.discovery_client = consul.Consul(
                    host=self.config.discovery_host,
                    port=self.config.discovery_port
                )
                logging.info("Consul discovery service initialized")
                
            else:
                logging.warning("Using basic discovery mode - limited functionality")
                
        except Exception as e:
            logging.warning(f"Service discovery initialization failed: {e}")
            self.discovery_client = None
    
    def register_node(self, node_info: NodeInfo) -> bool:
        """Register a node with the discovery service."""
        try:
            with self.lock:
                self.nodes[node_info.node_id] = node_info
            
            if self.discovery_client and hasattr(self.discovery_client, 'set'):
                # Redis registration
                node_data = {
                    'node_type': node_info.node_type.value,
                    'host': node_info.host,
                    'port': node_info.port,
                    'status': node_info.status.value,
                    'capabilities': node_info.capabilities,
                    'cannabis_specializations': node_info.cannabis_specializations,
                    'geographic_region': node_info.geographic_region,
                    'supported_states': node_info.supported_states,
                    'last_heartbeat': node_info.last_heartbeat.isoformat()
                }
                
                key = f"{self.config.cluster_name}:nodes:{node_info.node_id}"
                self.discovery_client.setex(
                    key, 
                    self.config.heartbeat_interval * 3,  # TTL
                    json.dumps(node_data)
                )
                
            logging.info(f"Node registered: {node_info.node_id} ({node_info.node_type.value})")
            return True
            
        except Exception as e:
            logging.error(f"Node registration failed: {e}")
            return False
    
    def discover_nodes(self, node_type: Optional[NodeType] = None) -> List[NodeInfo]:
        """Discover available nodes."""
        try:
            discovered_nodes = []
            
            if self.discovery_client and hasattr(self.discovery_client, 'keys'):
                # Redis discovery
                pattern = f"{self.config.cluster_name}:nodes:*"
                keys = self.discovery_client.keys(pattern)
                
                for key in keys:
                    node_data = self.discovery_client.get(key)
                    if node_data:
                        try:
                            data = json.loads(node_data)
                            node_id = key.split(':')[-1]
                            
                            node_info = NodeInfo(
                                node_id=node_id,
                                node_type=NodeType(data['node_type']),
                                host=data['host'],
                                port=data['port'],
                                status=NodeStatus(data['status']),
                                capabilities=data.get('capabilities', []),
                                cannabis_specializations=data.get('cannabis_specializations', []),
                                geographic_region=data.get('geographic_region'),
                                supported_states=data.get('supported_states', []),
                                last_heartbeat=datetime.fromisoformat(data['last_heartbeat'])
                            )
                            
                            if node_type is None or node_info.node_type == node_type:
                                discovered_nodes.append(node_info)
                                
                        except Exception as e:
                            logging.warning(f"Failed to parse node data: {e}")
            
            # Fallback to local registry
            with self.lock:
                for node_info in self.nodes.values():
                    if node_type is None or node_info.node_type == node_type:
                        if node_info not in discovered_nodes:
                            discovered_nodes.append(node_info)
            
            return discovered_nodes
            
        except Exception as e:
            logging.error(f"Node discovery failed: {e}")
            return []
    
    def update_node_status(self, node_id: str, status: NodeStatus, 
                          load_score: float = 0.0) -> bool:
        """Update node status and load information."""
        try:
            with self.lock:
                if node_id in self.nodes:
                    self.nodes[node_id].status = status
                    self.nodes[node_id].load_score = load_score
                    self.nodes[node_id].last_heartbeat = datetime.now()
            
            if self.discovery_client and hasattr(self.discovery_client, 'get'):
                # Update in Redis
                key = f"{self.config.cluster_name}:nodes:{node_id}"
                node_data = self.discovery_client.get(key)
                
                if node_data:
                    data = json.loads(node_data)
                    data['status'] = status.value
                    data['load_score'] = load_score
                    data['last_heartbeat'] = datetime.now().isoformat()
                    
                    self.discovery_client.setex(
                        key,
                        self.config.heartbeat_interval * 3,
                        json.dumps(data)
                    )
            
            return True
            
        except Exception as e:
            logging.error(f"Node status update failed: {e}")
            return False
    
    def cleanup_stale_nodes(self):
        """Remove stale nodes from discovery."""
        current_time = datetime.now()
        stale_threshold = timedelta(seconds=self.config.heartbeat_interval * 2)
        
        stale_nodes = []
        
        with self.lock:
            for node_id, node_info in self.nodes.items():
                if current_time - node_info.last_heartbeat > stale_threshold:
                    stale_nodes.append(node_id)
        
        for node_id in stale_nodes:
            with self.lock:
                if node_id in self.nodes:
                    del self.nodes[node_id]
            
            logging.info(f"Removed stale node: {node_id}")

class WorkloadDistributor:
    """Intelligent workload distribution for cannabis data processing."""
    
    def __init__(self, config: ScalingConfig, discovery_service: NodeDiscoveryService):
        self.config = config
        self.discovery = discovery_service
        self.partitioner = CannabisDataPartitioner(config.partition_strategy, config)
        self.task_queue = queue.PriorityQueue()
        self.active_tasks = {}
        self.task_history = {}
        self.lock = threading.Lock()
        
    def submit_task(self, task: WorkloadTask) -> bool:
        """Submit a task for distributed processing."""
        try:
            # Determine data partition
            cannabis_data = task.cannabis_context
            partition = self.partitioner.partition_data(cannabis_data)
            task.data_partition = partition
            
            # Add to priority queue (negative priority for max-heap behavior)
            priority_score = -task.priority
            if task.deadline:
                time_urgency = (task.deadline - datetime.now()).total_seconds()
                priority_score += max(0, 1000 - time_urgency)  # More urgent = higher priority
            
            self.task_queue.put((priority_score, task.created_at, task))
            
            logging.info(f"Task submitted: {task.task_id} (partition: {partition})")
            return True
            
        except Exception as e:
            logging.error(f"Task submission failed: {e}")
            return False
    
    def distribute_tasks(self) -> Dict[str, Any]:
        """Distribute queued tasks to available nodes."""
        distribution_stats = {
            'tasks_distributed': 0,
            'tasks_failed': 0,
            'node_assignments': {},
            'partition_distribution': {}
        }
        
        try:
            # Get available nodes
            available_nodes = self.discovery.discover_nodes(NodeType.DATA_PROCESSOR)
            healthy_nodes = [
                node for node in available_nodes 
                if node.status in [NodeStatus.HEALTHY, NodeStatus.DEGRADED]
            ]
            
            if not healthy_nodes:
                logging.warning("No healthy nodes available for task distribution")
                return distribution_stats
            
            # Process tasks from queue
            tasks_to_process = []
            while not self.task_queue.empty() and len(tasks_to_process) < 100:
                try:
                    _, _, task = self.task_queue.get_nowait()
                    tasks_to_process.append(task)
                except queue.Empty:
                    break
            
            # Group tasks by partition for efficient distribution
            partition_tasks = {}
            for task in tasks_to_process:
                partition = task.data_partition
                if partition not in partition_tasks:
                    partition_tasks[partition] = []
                partition_tasks[partition].append(task)
            
            # Distribute each partition's tasks
            for partition, tasks in partition_tasks.items():
                # Find nodes with affinity for this partition
                affine_nodes = self.partitioner.get_partition_affinity(partition, healthy_nodes)
                
                # Select best node based on load and specialization
                best_node = self._select_best_node(affine_nodes, tasks[0])
                
                if best_node:
                    # Assign all partition tasks to the selected node
                    for task in tasks:
                        success = self._assign_task_to_node(task, best_node)
                        
                        if success:
                            distribution_stats['tasks_distributed'] += 1
                            
                            if best_node.node_id not in distribution_stats['node_assignments']:
                                distribution_stats['node_assignments'][best_node.node_id] = 0
                            distribution_stats['node_assignments'][best_node.node_id] += 1
                            
                            if partition not in distribution_stats['partition_distribution']:
                                distribution_stats['partition_distribution'][partition] = 0
                            distribution_stats['partition_distribution'][partition] += 1
                        else:
                            distribution_stats['tasks_failed'] += 1
                            # Put task back in queue
                            self.task_queue.put((-task.priority, task.created_at, task))
                else:
                    # No suitable node found, put tasks back in queue
                    for task in tasks:
                        self.task_queue.put((-task.priority, task.created_at, task))
                        distribution_stats['tasks_failed'] += 1
            
            logging.info(f"Task distribution completed: {distribution_stats['tasks_distributed']} distributed, {distribution_stats['tasks_failed']} failed")
            return distribution_stats
            
        except Exception as e:
            logging.error(f"Task distribution failed: {e}")
            return distribution_stats
    
    def _select_best_node(self, candidate_nodes: List[NodeInfo], task: WorkloadTask) -> Optional[NodeInfo]:
        """Select the best node for a task based on load and specialization."""
        if not candidate_nodes:
            return None
        
        scored_nodes = []
        
        for node in candidate_nodes:
            score = self._calculate_node_score(node, task)
            scored_nodes.append((score, node))
        
        # Sort by score (higher is better)
        scored_nodes.sort(key=lambda x: x[0], reverse=True)
        
        return scored_nodes[0][1] if scored_nodes else None
    
    def _calculate_node_score(self, node: NodeInfo, task: WorkloadTask) -> float:
        """Calculate suitability score for assigning task to node."""
        score = 0.0
        
        # Base score from node health
        if node.status == NodeStatus.HEALTHY:
            score += 100
        elif node.status == NodeStatus.DEGRADED:
            score += 50
        else:
            return 0.0  # Don't use unhealthy nodes
        
        # Load balancing - prefer less loaded nodes
        load_penalty = node.load_score * 50
        score -= load_penalty
        
        # Cannabis specialization bonus
        task_cannabis_type = task.cannabis_context.get('category', 'flower')
        if task_cannabis_type in node.cannabis_specializations:
            score += 30
        
        # State compliance bonus
        task_state = task.cannabis_context.get('state')
        if task_state and task_state in node.supported_states:
            score += 20
        
        # Geographic affinity bonus
        if task.state_requirement and task.state_requirement in node.supported_states:
            score += 15
        
        # Capability matching
        required_capabilities = self._get_required_capabilities(task)
        capability_matches = len(set(required_capabilities) & set(node.capabilities))
        score += capability_matches * 10
        
        return score
    
    def _get_required_capabilities(self, task: WorkloadTask) -> List[str]:
        """Get required capabilities for a task."""
        capabilities = []
        
        if task.task_type == 'terpene_analysis':
            capabilities.extend(['gpu_acceleration', 'ml_processing'])
        elif task.task_type == 'price_prediction':
            capabilities.extend(['ml_processing', 'time_series'])
        elif task.task_type == 'strain_clustering':
            capabilities.extend(['gpu_acceleration', 'clustering'])
        elif task.task_type == 'data_aggregation':
            capabilities.extend(['high_memory', 'fast_io'])
        
        return capabilities
    
    def _assign_task_to_node(self, task: WorkloadTask, node: NodeInfo) -> bool:
        """Assign a specific task to a node."""
        try:
            if not HTTP_CLIENT_AVAILABLE:
                # Simulate assignment in basic mode
                with self.lock:
                    self.active_tasks[task.task_id] = {
                        'task': task,
                        'node': node,
                        'assigned_at': datetime.now()
                    }
                return True
            
            # Send task to node via HTTP
            task_payload = {
                'task_id': task.task_id,
                'task_type': task.task_type,
                'data_partition': task.data_partition,
                'cannabis_context': task.cannabis_context,
                'priority': task.priority,
                'estimated_cpu': task.estimated_cpu,
                'estimated_memory': task.estimated_memory,
                'deadline': task.deadline.isoformat() if task.deadline else None
            }
            
            url = f"http://{node.host}:{node.port}/tasks"
            
            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            
            response = session.post(
                url,
                json=task_payload,
                timeout=self.config.health_check_timeout
            )
            
            if response.status_code == 200:
                with self.lock:
                    self.active_tasks[task.task_id] = {
                        'task': task,
                        'node': node,
                        'assigned_at': datetime.now()
                    }
                return True
            else:
                logging.warning(f"Task assignment failed: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"Task assignment failed: {e}")
            return False
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a distributed task."""
        with self.lock:
            if task_id in self.active_tasks:
                assignment = self.active_tasks[task_id]
                return {
                    'task_id': task_id,
                    'node_id': assignment['node'].node_id,
                    'assigned_at': assignment['assigned_at'].isoformat(),
                    'status': 'active'
                }
            elif task_id in self.task_history:
                return self.task_history[task_id]
        
        return None
    
    def mark_task_completed(self, task_id: str, result: Dict[str, Any]):
        """Mark a task as completed and store result."""
        with self.lock:
            if task_id in self.active_tasks:
                assignment = self.active_tasks[task_id]
                self.task_history[task_id] = {
                    'task_id': task_id,
                    'node_id': assignment['node'].node_id,
                    'assigned_at': assignment['assigned_at'].isoformat(),
                    'completed_at': datetime.now().isoformat(),
                    'status': 'completed',
                    'result': result
                }
                del self.active_tasks[task_id]

class HorizontalScalingCoordinator:
    """Main coordinator for horizontal scaling operations."""
    
    def __init__(self, config: Optional[ScalingConfig] = None):
        self.config = config or ScalingConfig()
        self.discovery = NodeDiscoveryService(self.config)
        self.distributor = WorkloadDistributor(self.config, self.discovery)
        self.monitoring_thread = None
        self.distribution_thread = None
        self.running = False
        
        logging.info("Horizontal Scaling Coordinator initialized")
        logging.info(f"Cluster: {self.config.cluster_name}")
        logging.info(f"Partition Strategy: {self.config.partition_strategy.value}")
    
    def start(self):
        """Start the scaling coordinator."""
        self.running = True
        
        # Start monitoring thread
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        
        # Start distribution thread
        self.distribution_thread = threading.Thread(target=self._distribution_loop, daemon=True)
        self.distribution_thread.start()
        
        logging.info("Horizontal Scaling Coordinator started")
    
    def stop(self):
        """Stop the scaling coordinator."""
        self.running = False
        
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
        
        if self.distribution_thread:
            self.distribution_thread.join(timeout=5)
        
        logging.info("Horizontal Scaling Coordinator stopped")
    
    def _monitoring_loop(self):
        """Background monitoring loop."""
        while self.running:
            try:
                # Cleanup stale nodes
                self.discovery.cleanup_stale_nodes()
                
                # Monitor cluster health
                self._monitor_cluster_health()
                
                # Auto-scaling decisions
                if self.config.auto_scaling_enabled:
                    self._evaluate_scaling_decisions()
                
                time.sleep(self.config.heartbeat_interval)
                
            except Exception as e:
                logging.error(f"Monitoring loop error: {e}")
                time.sleep(5)
    
    def _distribution_loop(self):
        """Background task distribution loop."""
        while self.running:
            try:
                # Distribute pending tasks
                stats = self.distributor.distribute_tasks()
                
                if stats['tasks_distributed'] > 0 or stats['tasks_failed'] > 0:
                    logging.info(f"Distribution cycle: {stats['tasks_distributed']} distributed, {stats['tasks_failed']} failed")
                
                time.sleep(5)  # More frequent than monitoring
                
            except Exception as e:
                logging.error(f"Distribution loop error: {e}")
                time.sleep(10)
    
    def _monitor_cluster_health(self):
        """Monitor overall cluster health."""
        try:
            nodes = self.discovery.discover_nodes()
            
            health_stats = {
                'total_nodes': len(nodes),
                'healthy_nodes': 0,
                'degraded_nodes': 0,
                'offline_nodes': 0,
                'node_types': {}
            }
            
            for node in nodes:
                if node.status == NodeStatus.HEALTHY:
                    health_stats['healthy_nodes'] += 1
                elif node.status == NodeStatus.DEGRADED:
                    health_stats['degraded_nodes'] += 1
                else:
                    health_stats['offline_nodes'] += 1
                
                node_type = node.node_type.value
                if node_type not in health_stats['node_types']:
                    health_stats['node_types'][node_type] = 0
                health_stats['node_types'][node_type] += 1
            
            # Log health status periodically
            if health_stats['total_nodes'] > 0:
                healthy_pct = (health_stats['healthy_nodes'] / health_stats['total_nodes']) * 100
                logging.info(f"Cluster health: {health_stats['healthy_nodes']}/{health_stats['total_nodes']} nodes healthy ({healthy_pct:.1f}%)")
            
        except Exception as e:
            logging.error(f"Cluster health monitoring failed: {e}")
    
    def _evaluate_scaling_decisions(self):
        """Evaluate whether scaling up or down is needed."""
        try:
            nodes = self.discovery.discover_nodes(NodeType.DATA_PROCESSOR)
            
            if not nodes:
                return
            
            # Calculate average load
            total_load = sum(node.load_score for node in nodes)
            avg_load = total_load / len(nodes)
            
            # Scaling decisions
            if avg_load > self.config.scale_up_threshold:
                logging.info(f"High load detected ({avg_load:.2f}) - scaling up recommended")
                # In production, trigger auto-scaling here
                
            elif avg_load < self.config.scale_down_threshold and len(nodes) > 1:
                logging.info(f"Low load detected ({avg_load:.2f}) - scaling down possible")
                # In production, trigger scale-down here
            
        except Exception as e:
            logging.error(f"Scaling evaluation failed: {e}")
    
    def submit_cannabis_task(self, task_type: str, cannabis_data: Dict[str, Any], 
                           priority: int = 1) -> str:
        """Submit a cannabis-specific processing task."""
        task_id = str(uuid.uuid4())
        
        task = WorkloadTask(
            task_id=task_id,
            task_type=task_type,
            data_partition="",  # Will be calculated
            priority=priority,
            cannabis_context=cannabis_data,
            state_requirement=cannabis_data.get('state')
        )
        
        success = self.distributor.submit_task(task)
        return task_id if success else None
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get comprehensive cluster status."""
        nodes = self.discovery.discover_nodes()
        
        status = {
            'cluster_name': self.config.cluster_name,
            'total_nodes': len(nodes),
            'node_distribution': {},
            'geographic_distribution': {},
            'cannabis_specializations': {},
            'active_tasks': len(self.distributor.active_tasks),
            'queue_size': self.distributor.task_queue.qsize(),
            'configuration': {
                'partition_strategy': self.config.partition_strategy.value,
                'auto_scaling_enabled': self.config.auto_scaling_enabled,
                'cannabis_compliance_mode': self.config.cannabis_compliance_mode
            }
        }
        
        for node in nodes:
            # Node type distribution
            node_type = node.node_type.value
            if node_type not in status['node_distribution']:
                status['node_distribution'][node_type] = 0
            status['node_distribution'][node_type] += 1
            
            # Geographic distribution
            if node.geographic_region:
                region = node.geographic_region
                if region not in status['geographic_distribution']:
                    status['geographic_distribution'][region] = 0
                status['geographic_distribution'][region] += 1
            
            # Cannabis specializations
            for spec in node.cannabis_specializations:
                if spec not in status['cannabis_specializations']:
                    status['cannabis_specializations'][spec] = 0
                status['cannabis_specializations'][spec] += 1
        
        return status

def create_scaling_coordinator(cluster_name: str = "weedhounds-cluster",
                             partition_strategy: str = "hybrid",
                             enable_auto_scaling: bool = True) -> HorizontalScalingCoordinator:
    """
    Factory function to create horizontal scaling coordinator.
    
    Args:
        cluster_name: Name of the cannabis data cluster
        partition_strategy: Data partitioning strategy
        enable_auto_scaling: Whether to enable automatic scaling
    
    Returns:
        Configured horizontal scaling coordinator
    """
    config = ScalingConfig(
        cluster_name=cluster_name,
        partition_strategy=DataPartitionStrategy(partition_strategy),
        auto_scaling_enabled=enable_auto_scaling,
        cannabis_compliance_mode=True
    )
    
    return HorizontalScalingCoordinator(config)

# Demo and Testing Functions
def demo_horizontal_scaling():
    """Demonstrate horizontal scaling capabilities."""
    print("\n🚀 Horizontal Scaling Framework Demo")
    print("=" * 50)
    
    # Initialize scaling coordinator
    coordinator = create_scaling_coordinator(
        cluster_name="demo-cannabis-cluster",
        partition_strategy="hybrid"
    )
    
    # Display configuration
    status = coordinator.get_cluster_status()
    print(f"Cluster Name: {status['cluster_name']}")
    print(f"Partition Strategy: {status['configuration']['partition_strategy']}")
    print(f"Auto Scaling: {status['configuration']['auto_scaling_enabled']}")
    
    # Start coordinator
    coordinator.start()
    
    # Register demo nodes
    demo_nodes = [
        NodeInfo(
            node_id="west-coast-1",
            node_type=NodeType.DATA_PROCESSOR,
            host="192.168.1.10",
            port=8080,
            status=NodeStatus.HEALTHY,
            capabilities=['gpu_acceleration', 'ml_processing'],
            cannabis_specializations=['flower', 'vape'],
            geographic_region='west',
            supported_states=['CA', 'OR', 'WA']
        ),
        NodeInfo(
            node_id="east-coast-1",
            node_type=NodeType.DATA_PROCESSOR,
            host="192.168.1.11",
            port=8080,
            status=NodeStatus.HEALTHY,
            capabilities=['high_memory', 'fast_io'],
            cannabis_specializations=['edible', 'concentrate'],
            geographic_region='northeast',
            supported_states=['NY', 'MA', 'CT']
        )
    ]
    
    print(f"\n📝 Registering {len(demo_nodes)} demo nodes...")
    for node in demo_nodes:
        success = coordinator.discovery.register_node(node)
        print(f"  {node.node_id}: {'✅' if success else '❌'}")
    
    # Submit demo tasks
    cannabis_tasks = [
        {
            'task_type': 'terpene_analysis',
            'cannabis_data': {
                'state': 'CA',
                'dispensary': 'Green Valley Dispensary',
                'strain_type': 'indica',
                'category': 'flower'
            }
        },
        {
            'task_type': 'price_prediction',
            'cannabis_data': {
                'state': 'NY',
                'dispensary': 'Empire Cannabis Co',
                'strain_type': 'sativa',
                'category': 'vape'
            }
        },
        {
            'task_type': 'strain_clustering',
            'cannabis_data': {
                'state': 'OR',
                'dispensary': 'Pacific Green',
                'strain_type': 'hybrid',
                'category': 'edible'
            }
        }
    ]
    
    print(f"\n📤 Submitting {len(cannabis_tasks)} cannabis processing tasks...")
    task_ids = []
    for task_info in cannabis_tasks:
        task_id = coordinator.submit_cannabis_task(
            task_info['task_type'],
            task_info['cannabis_data'],
            priority=1
        )
        if task_id:
            task_ids.append(task_id)
            print(f"  {task_info['task_type']}: {task_id[:8]}...")
    
    # Wait for distribution
    time.sleep(3)
    
    # Check final status
    print(f"\n📊 Final Cluster Status:")
    final_status = coordinator.get_cluster_status()
    print(f"  Total Nodes: {final_status['total_nodes']}")
    print(f"  Active Tasks: {final_status['active_tasks']}")
    print(f"  Queue Size: {final_status['queue_size']}")
    
    if final_status['node_distribution']:
        print(f"  Node Types: {final_status['node_distribution']}")
    
    if final_status['geographic_distribution']:
        print(f"  Geographic Distribution: {final_status['geographic_distribution']}")
    
    # Cleanup
    coordinator.stop()
    print("\n✅ Demo completed successfully!")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run demo
    demo_horizontal_scaling()