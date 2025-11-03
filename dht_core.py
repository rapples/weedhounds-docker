"""
DHT Core Implementation for WeedHounds Distributed Workers
Based on Kademlia DHT protocol for optimal performance and scalability
"""

import asyncio
import hashlib
import json
import logging
import time
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import aiohttp
import socket
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DHTNode:
    """Represents a node in the DHT network"""
    node_id: str
    ip_address: str
    port: int
    last_seen: datetime
    node_type: str  # coordinator, worker, storage
    capabilities: List[str]
    load: float = 0.0
    
    def __post_init__(self):
        if not self.node_id:
            self.node_id = self.generate_node_id()
    
    def generate_node_id(self) -> str:
        """Generate unique 160-bit node ID using SHA-1"""
        data = f"{self.ip_address}:{self.port}:{time.time()}:{random.random()}"
        return hashlib.sha1(data.encode()).hexdigest()
    
    def distance_to(self, other_id: str) -> int:
        """Calculate XOR distance to another node"""
        return int(self.node_id, 16) ^ int(other_id, 16)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['last_seen'] = self.last_seen.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'DHTNode':
        """Create DHTNode from dictionary"""
        data['last_seen'] = datetime.fromisoformat(data['last_seen'])
        return cls(**data)

@dataclass
class DHTTask:
    """Represents a task in the DHT system"""
    task_id: str
    task_type: str
    store_id: str
    params: Dict[str, Any]
    priority: int = 1
    assigned_node: Optional[str] = None
    status: str = 'pending'  # pending, assigned, processing, completed, failed
    created_at: datetime = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict] = None
    retry_count: int = 0
    max_retries: int = 3
    
    def __post_init__(self):
        if not self.task_id:
            self.task_id = self.generate_task_id()
        if not self.created_at:
            self.created_at = datetime.now()
    
    def generate_task_id(self) -> str:
        """Generate unique task ID"""
        data = f"{self.task_type}:{self.store_id}:{time.time()}:{random.random()}"
        return hashlib.md5(data.encode()).hexdigest()
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['created_at'] = self.created_at.isoformat()
        if self.completed_at:
            data['completed_at'] = self.completed_at.isoformat()
        return data

class KBucket:
    """K-bucket for storing node contacts in Kademlia DHT"""
    
    def __init__(self, k: int = 20):
        self.k = k
        self.nodes: List[DHTNode] = []
        self.last_updated = datetime.now()
    
    def add_node(self, node: DHTNode) -> bool:
        """Add node to k-bucket"""
        # Remove if already exists
        self.nodes = [n for n in self.nodes if n.node_id != node.node_id]
        
        if len(self.nodes) < self.k:
            self.nodes.append(node)
            self.last_updated = datetime.now()
            return True
        
        # Bucket full, ping least recently seen
        oldest_node = min(self.nodes, key=lambda n: n.last_seen)
        if self._ping_node(oldest_node):
            oldest_node.last_seen = datetime.now()
            return False
        else:
            # Replace dead node
            self.nodes.remove(oldest_node)
            self.nodes.append(node)
            self.last_updated = datetime.now()
            return True
    
    def _ping_node(self, node: DHTNode) -> bool:
        """Ping node to check if alive"""
        # Simplified ping - in real implementation would use async HTTP/UDP
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((node.ip_address, node.port))
            sock.close()
            return result == 0
        except:
            return False
    
    def find_closest_nodes(self, target_id: str, count: int = 20) -> List[DHTNode]:
        """Find closest nodes to target ID"""
        distances = [(node.distance_to(target_id), node) for node in self.nodes]
        distances.sort(key=lambda x: x[0])
        return [node for _, node in distances[:count]]

class DHTCore:
    """Core DHT implementation with Kademlia protocol"""
    
    def __init__(self, local_node: DHTNode, k: int = 20, alpha: int = 3):
        self.local_node = local_node
        self.k = k  # Bucket size
        self.alpha = alpha  # Concurrency parameter
        self.routing_table: Dict[int, KBucket] = {}
        self.data_store: Dict[str, Any] = {}
        self.task_queue: Dict[str, DHTTask] = {}
        self.running = False
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Initialize routing table
        for i in range(160):  # 160-bit key space
            self.routing_table[i] = KBucket(k)
    
    async def start(self):
        """Start DHT node"""
        self.running = True
        self.session = aiohttp.ClientSession()
        logger.info(f"DHT node {self.local_node.node_id} started")
        
        # Start background tasks
        asyncio.create_task(self._maintenance_loop())
    
    async def stop(self):
        """Stop DHT node"""
        self.running = False
        if self.session:
            await self.session.close()
        logger.info(f"DHT node {self.local_node.node_id} stopped")
    
    def _get_bucket_index(self, node_id: str) -> int:
        """Get bucket index for node ID"""
        distance = self.local_node.distance_to(node_id)
        if distance == 0:
            return 0
        return 159 - distance.bit_length() + 1
    
    def add_node(self, node: DHTNode) -> bool:
        """Add node to routing table"""
        if node.node_id == self.local_node.node_id:
            return False
        
        bucket_index = self._get_bucket_index(node.node_id)
        bucket = self.routing_table[bucket_index]
        return bucket.add_node(node)
    
    def find_closest_nodes(self, target_id: str, count: int = 20) -> List[DHTNode]:
        """Find closest nodes to target ID"""
        all_nodes = []
        for bucket in self.routing_table.values():
            all_nodes.extend(bucket.nodes)
        
        distances = [(node.distance_to(target_id), node) for node in all_nodes]
        distances.sort(key=lambda x: x[0])
        return [node for _, node in distances[:count]]
    
    async def store_data(self, key: str, value: Any, replicas: int = 3) -> bool:
        """Store data in DHT with replication"""
        try:
            # Store locally
            self.data_store[key] = {
                'value': value,
                'timestamp': datetime.now().isoformat(),
                'replicas': replicas
            }
            
            # Find nodes responsible for this key
            target_nodes = self.find_closest_nodes(key, replicas)
            
            # Replicate to other nodes
            success_count = 1  # Local storage counts as one
            for node in target_nodes:
                if node.node_id != self.local_node.node_id:
                    if await self._store_on_node(node, key, value):
                        success_count += 1
            
            return success_count >= (replicas + 1) // 2  # Majority
            
        except Exception as e:
            logger.error(f"Failed to store data {key}: {e}")
            return False
    
    async def get_data(self, key: str) -> Optional[Any]:
        """Retrieve data from DHT"""
        try:
            # Check local storage first
            if key in self.data_store:
                return self.data_store[key]['value']
            
            # Query closest nodes
            target_nodes = self.find_closest_nodes(key, self.k)
            
            for node in target_nodes:
                if node.node_id != self.local_node.node_id:
                    value = await self._get_from_node(node, key)
                    if value is not None:
                        # Cache locally
                        self.data_store[key] = {
                            'value': value,
                            'timestamp': datetime.now().isoformat(),
                            'cached': True
                        }
                        return value
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get data {key}: {e}")
            return None
    
    async def submit_task(self, task: DHTTask) -> bool:
        """Submit task to DHT task queue"""
        try:
            self.task_queue[task.task_id] = task
            
            # Find best worker node for this task
            worker_nodes = [
                node for node in self.find_closest_nodes(task.task_id, self.k)
                if 'worker' in node.capabilities and node.load < 0.8
            ]
            
            if not worker_nodes:
                logger.warning(f"No available workers for task {task.task_id}")
                return False
            
            # Assign to least loaded worker
            best_worker = min(worker_nodes, key=lambda n: n.load)
            task.assigned_node = best_worker.node_id
            task.status = 'assigned'
            
            # Notify worker
            success = await self._assign_task_to_node(best_worker, task)
            if success:
                logger.info(f"Task {task.task_id} assigned to worker {best_worker.node_id}")
            else:
                task.status = 'pending'
                task.assigned_node = None
                logger.warning(f"Failed to assign task {task.task_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to submit task {task.task_id}: {e}")
            return False
    
    async def _store_on_node(self, node: DHTNode, key: str, value: Any) -> bool:
        """Store data on remote node"""
        try:
            url = f"http://{node.ip_address}:{node.port}/dht/store"
            data = {'key': key, 'value': value}
            
            async with self.session.post(url, json=data, timeout=10) as response:
                return response.status == 200
                
        except Exception as e:
            logger.error(f"Failed to store on node {node.node_id}: {e}")
            return False
    
    async def _get_from_node(self, node: DHTNode, key: str) -> Optional[Any]:
        """Get data from remote node"""
        try:
            url = f"http://{node.ip_address}:{node.port}/dht/get/{key}"
            
            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('value')
                return None
                
        except Exception as e:
            logger.error(f"Failed to get from node {node.node_id}: {e}")
            return None
    
    async def _assign_task_to_node(self, node: DHTNode, task: DHTTask) -> bool:
        """Assign task to worker node"""
        try:
            url = f"http://{node.ip_address}:{node.port}/tasks/assign"
            data = task.to_dict()
            
            async with self.session.post(url, json=data, timeout=10) as response:
                return response.status == 200
                
        except Exception as e:
            logger.error(f"Failed to assign task to node {node.node_id}: {e}")
            return False
    
    async def _maintenance_loop(self):
        """Background maintenance tasks"""
        while self.running:
            try:
                await self._refresh_routing_table()
                await self._cleanup_stale_data()
                await self._rebalance_tasks()
                await asyncio.sleep(60)  # Run every minute
                
            except Exception as e:
                logger.error(f"Maintenance loop error: {e}")
                await asyncio.sleep(10)
    
    async def _refresh_routing_table(self):
        """Refresh routing table by pinging nodes"""
        stale_cutoff = datetime.now() - timedelta(minutes=5)
        
        for bucket in self.routing_table.values():
            stale_nodes = [n for n in bucket.nodes if n.last_seen < stale_cutoff]
            
            for node in stale_nodes:
                if await self._ping_node_async(node):
                    node.last_seen = datetime.now()
                else:
                    bucket.nodes.remove(node)
                    logger.info(f"Removed stale node {node.node_id}")
    
    async def _ping_node_async(self, node: DHTNode) -> bool:
        """Async ping node to check if alive"""
        try:
            url = f"http://{node.ip_address}:{node.port}/ping"
            async with self.session.get(url, timeout=5) as response:
                return response.status == 200
        except:
            return False
    
    async def _cleanup_stale_data(self):
        """Clean up old cached data"""
        cutoff = datetime.now() - timedelta(hours=24)
        
        stale_keys = []
        for key, data in self.data_store.items():
            if data.get('cached') and datetime.fromisoformat(data['timestamp']) < cutoff:
                stale_keys.append(key)
        
        for key in stale_keys:
            del self.data_store[key]
            logger.debug(f"Cleaned up stale data: {key}")
    
    async def _rebalance_tasks(self):
        """Rebalance tasks across workers"""
        pending_tasks = [t for t in self.task_queue.values() if t.status == 'pending']
        
        if pending_tasks:
            logger.info(f"Rebalancing {len(pending_tasks)} pending tasks")
            
            for task in pending_tasks[:10]:  # Process in batches
                await self.submit_task(task)
    
    def get_network_stats(self) -> Dict:
        """Get network statistics"""
        total_nodes = sum(len(bucket.nodes) for bucket in self.routing_table.values())
        
        node_types = {}
        total_load = 0
        
        for bucket in self.routing_table.values():
            for node in bucket.nodes:
                node_types[node.node_type] = node_types.get(node.node_type, 0) + 1
                total_load += node.load
        
        avg_load = total_load / max(total_nodes, 1)
        
        task_stats = {
            'pending': len([t for t in self.task_queue.values() if t.status == 'pending']),
            'assigned': len([t for t in self.task_queue.values() if t.status == 'assigned']),
            'processing': len([t for t in self.task_queue.values() if t.status == 'processing']),
            'completed': len([t for t in self.task_queue.values() if t.status == 'completed']),
            'failed': len([t for t in self.task_queue.values() if t.status == 'failed'])
        }
        
        return {
            'node_id': self.local_node.node_id,
            'total_nodes': total_nodes,
            'node_types': node_types,
            'average_load': avg_load,
            'data_items': len(self.data_store),
            'tasks': task_stats,
            'uptime': datetime.now().isoformat()
        }

# Utility functions
def generate_key(data: str) -> str:
    """Generate DHT key from data"""
    return hashlib.sha1(data.encode()).hexdigest()

def xor_distance(id1: str, id2: str) -> int:
    """Calculate XOR distance between two IDs"""
    return int(id1, 16) ^ int(id2, 16)

def closest_nodes(nodes: List[DHTNode], target_id: str, count: int) -> List[DHTNode]:
    """Find closest nodes to target ID"""
    distances = [(node.distance_to(target_id), node) for node in nodes]
    distances.sort(key=lambda x: x[0])
    return [node for _, node in distances[:count]]