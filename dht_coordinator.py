"""
DHT Coordinator Service for WeedHounds Distributed Workers
Manages the DHT network, task distribution, and worker coordination
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from aiohttp import web, ClientSession
import signal
import sys

from dht_core import DHTCore, DHTNode, DHTTask
from dht_storage import DHTStorage, DataType, generate_cache_key
from dht_peer_network import PeerNetworkManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DHTCoordinator:
    """DHT Coordinator manages the distributed network of workers"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 9000):
        self.host = host
        self.port = port
        self.app = web.Application()
        self.setup_routes()
        
        # Create local DHT node
        self.local_node = DHTNode(
            node_id='',
            ip_address=host,
            port=port,
            last_seen=datetime.now(),
            node_type='coordinator',
            capabilities=['coordination', 'task_management', 'network_management']
        )
        
        # Initialize DHT core
        self.dht = DHTCore(self.local_node)
        
        # Initialize distributed storage system
        max_storage_gb = float(os.getenv('DHT_MAX_STORAGE_GB', '100'))
        self.storage = DHTStorage(
            node_id=self.local_node.node_id,
            storage_path=os.getenv('DHT_STORAGE_PATH', './dht_cache'),
            max_storage_gb=max_storage_gb
        )
        
        # Initialize peer network manager
        peer_port = int(os.getenv('DHT_PEER_PORT', '9100'))
        max_peers = int(os.getenv('DHT_MAX_PEERS', '1000'))
        self.peer_network = PeerNetworkManager(
            node_id=self.local_node.node_id,
            listen_port=peer_port,
            max_peers=max_peers,
            storage=self.storage
        )
        
        # Network state
        self.workers: Dict[str, DHTNode] = {}
        self.task_history: List[DHTTask] = []
        self.network_config = {
            'max_workers': int(os.getenv('DHT_MAX_WORKERS', '50')),
            'replication_factor': int(os.getenv('DHT_REPLICATION_FACTOR', '3')),
            'task_timeout': int(os.getenv('DHT_TASK_TIMEOUT', '300')),
            'heartbeat_interval': int(os.getenv('DHT_HEARTBEAT_INTERVAL', '30')),
            'cache_ttl_hours': int(os.getenv('DHT_CACHE_TTL_HOURS', '24')),
            'max_storage_gb': max_storage_gb,
            'peer_sharing_enabled': os.getenv('DHT_PEER_SHARING', 'true').lower() == 'true'
        }
        
        # Statistics
        self.stats = {
            'tasks_submitted': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'workers_joined': 0,
            'workers_left': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'peer_data_requests': 0,
            'api_calls_saved': 0,
            'start_time': datetime.now()
        }
    
    def setup_routes(self):
        """Setup HTTP API routes"""
        # Network management
        self.app.router.add_get('/network/status', self.get_network_status)
        self.app.router.add_get('/network/stats', self.get_network_stats)
        self.app.router.add_post('/network/rebalance', self.rebalance_network)
        
        # Node management
        self.app.router.add_post('/nodes/register', self.register_node)
        self.app.router.add_delete('/nodes/{node_id}', self.remove_node)
        self.app.router.add_get('/nodes/{node_id}/status', self.get_node_status)
        self.app.router.add_get('/nodes', self.list_nodes)
        
        # Task management
        self.app.router.add_post('/tasks', self.submit_task)
        self.app.router.add_get('/tasks/{task_id}', self.get_task_status)
        self.app.router.add_get('/tasks', self.list_tasks)
        self.app.router.add_post('/tasks/{task_id}/cancel', self.cancel_task)
        
        # Distributed caching
        self.app.router.add_get('/cache/get/{key}', self.cache_get)
        self.app.router.add_post('/cache/store', self.cache_store)
        self.app.router.add_get('/cache/stats', self.cache_stats)
        self.app.router.add_get('/cache/search', self.cache_search)
        self.app.router.add_delete('/cache/cleanup', self.cache_cleanup)
        
        # Peer network management
        self.app.router.add_get('/peers/status', self.get_peer_status)
        self.app.router.add_get('/peers/list', self.get_peer_list)
        self.app.router.add_post('/peers/connect', self.connect_to_peer)
        self.app.router.add_get('/peers/stats', self.get_peer_stats)
        
        # Cannabis data APIs
        self.app.router.add_post('/cannabis/menu', self.get_cannabis_menu)
        self.app.router.add_post('/cannabis/terpenes', self.get_terpene_data)
        self.app.router.add_post('/cannabis/strain', self.get_strain_info)
        self.app.router.add_post('/cannabis/dispensary', self.get_dispensary_info)
        
        # Health and monitoring
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/ping', self.ping)
        
        # DHT operations (legacy)
        self.app.router.add_get('/dht/get/{key}', self.dht_get)
        self.app.router.add_post('/dht/store', self.dht_store)
        self.app.router.add_delete('/dht/delete/{key}', self.dht_delete)
    
    async def start(self):
        """Start the DHT coordinator"""
        try:
            # Start DHT core
            await self.dht.start()
            
            # Initialize distributed storage
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
            await self.storage.initialize(redis_url)
            
            # Start peer network
            bootstrap_nodes = []
            bootstrap_env = os.getenv('DHT_BOOTSTRAP_NODES', '')
            if bootstrap_env:
                for node in bootstrap_env.split(','):
                    if ':' in node:
                        host, port = node.strip().split(':')
                        bootstrap_nodes.append((host, int(port)))
            
            await self.peer_network.start(bootstrap_nodes)
            
            # Start background tasks
            asyncio.create_task(self._heartbeat_loop())
            asyncio.create_task(self._task_monitor_loop())
            asyncio.create_task(self._cleanup_loop())
            asyncio.create_task(self._cache_optimization_loop())
            
            # Start HTTP server
            runner = web.AppRunner(self.app)
            await runner.setup()
            site = web.TCPSite(runner, self.host, self.port)
            await site.start()
            
            logger.info(f"DHT Coordinator started on {self.host}:{self.port}")
            logger.info(f"Node ID: {self.local_node.node_id}")
            logger.info(f"Peer network on port: {self.peer_network.listen_port}")
            logger.info(f"Max storage: {self.network_config['max_storage_gb']} GB")
            logger.info(f"Max peers: {self.peer_network.max_peers}")
            
        except Exception as e:
            logger.error(f"Failed to start coordinator: {e}")
            raise
    
    async def stop(self):
        """Stop the DHT coordinator"""
        logger.info("Stopping DHT Coordinator...")
        await self.peer_network.stop()
        await self.dht.stop()
    
    # Cannabis Data API Handlers
    
    async def get_cannabis_menu(self, request):
        """Get cannabis menu data with intelligent caching"""
        try:
            data = await request.json()
            
            dispensary_id = data.get('dispensary_id')
            api_source = data.get('api_source', 'dutchie')  # dutchie, jane, etc.
            menu_type = data.get('menu_type', 'all')
            location = data.get('location', '')
            
            if not dispensary_id:
                return web.json_response({'error': 'dispensary_id required'}, status=400)
            
            # Generate cache key
            data_type = DataType.DUTCHIE_MENU if api_source == 'dutchie' else DataType.JANE_MENU
            cache_key = generate_cache_key(
                data_type, 
                dispensary_id,
                menu_type=menu_type,
                location=location
            )
            
            # Try to get from distributed cache first
            cached_entry = await self.storage.get(cache_key, check_peers=self.network_config['peer_sharing_enabled'])
            
            if cached_entry:
                self.stats['cache_hits'] += 1
                return web.json_response({
                    'source': 'cache',
                    'cached_at': cached_entry.timestamp.isoformat(),
                    'data_age_hours': cached_entry.age_hours,
                    'menu_data': cached_entry.value,
                    'from_peer': cached_entry.source_node != self.local_node.node_id
                })
            
            # Cache miss - need to fetch from API
            self.stats['cache_misses'] += 1
            
            # Submit task to workers to fetch fresh data
            task_data = {
                'task_type': f'{api_source}_menu',
                'store_id': dispensary_id,
                'params': {
                    'menu_type': menu_type,
                    'location': location,
                    'cache_key': cache_key
                },
                'priority': 1
            }
            
            # Create task
            task = DHTTask(
                task_id='',
                task_type=task_data['task_type'],
                store_id=task_data['store_id'],
                params=task_data['params'],
                priority=task_data['priority']
            )
            
            # Submit to DHT for processing
            success = await self.dht.submit_task(task)
            
            if success:
                self.stats['tasks_submitted'] += 1
                return web.json_response({
                    'source': 'api',
                    'task_id': task.task_id,
                    'status': 'processing',
                    'message': 'Fetching fresh data from API'
                })
            else:
                return web.json_response({
                    'error': 'No available workers to process request'
                }, status=503)
                
        except Exception as e:
            logger.error(f"Error getting cannabis menu: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_terpene_data(self, request):
        """Get terpene profile data with caching"""
        try:
            data = await request.json()
            
            product_id = data.get('product_id')
            strain_name = data.get('strain_name')
            lab = data.get('lab', '')
            
            if not (product_id or strain_name):
                return web.json_response({'error': 'product_id or strain_name required'}, status=400)
            
            # Generate cache key
            identifier = product_id or strain_name
            cache_key = generate_cache_key(
                DataType.TERPENE_PROFILE,
                identifier,
                strain_name=strain_name,
                lab=lab
            )
            
            # Try cache first
            cached_entry = await self.storage.get(cache_key, check_peers=True)
            
            if cached_entry:
                self.stats['cache_hits'] += 1
                return web.json_response({
                    'source': 'cache',
                    'cached_at': cached_entry.timestamp.isoformat(),
                    'terpene_data': cached_entry.value,
                    'lab': cached_entry.value.get('lab', 'unknown'),
                    'confidence': cached_entry.value.get('confidence', 0.0)
                })
            
            # Submit terpene analysis task
            task = DHTTask(
                task_id='',
                task_type='terpene_analysis',
                store_id=identifier,
                params={
                    'product_id': product_id,
                    'strain_name': strain_name,
                    'lab': lab,
                    'cache_key': cache_key
                },
                priority=2
            )
            
            success = await self.dht.submit_task(task)
            
            if success:
                return web.json_response({
                    'source': 'analysis',
                    'task_id': task.task_id,
                    'status': 'processing'
                })
            else:
                return web.json_response({'error': 'No workers available'}, status=503)
                
        except Exception as e:
            logger.error(f"Error getting terpene data: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    # Distributed Cache API Handlers
    
    async def cache_get(self, request):
        """Get data from distributed cache"""
        try:
            key = request.match_info['key']
            
            entry = await self.storage.get(key, check_peers=True)
            
            if entry:
                return web.json_response({
                    'found': True,
                    'key': key,
                    'data': entry.value,
                    'metadata': {
                        'data_type': entry.data_type.value,
                        'timestamp': entry.timestamp.isoformat(),
                        'age_hours': entry.age_hours,
                        'source_node': entry.source_node,
                        'api_source': entry.api_source,
                        'access_count': entry.access_count,
                        'data_size': entry.data_size
                    }
                })
            else:
                return web.json_response({'found': False, 'key': key})
                
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def cache_store(self, request):
        """Store data in distributed cache"""
        try:
            data = await request.json()
            
            key = data.get('key')
            value = data.get('value')
            data_type_str = data.get('data_type', 'strain_info')
            api_source = data.get('api_source', 'manual')
            ttl_hours = data.get('ttl_hours')
            
            if not key or value is None:
                return web.json_response({'error': 'key and value required'}, status=400)
            
            try:
                data_type = DataType(data_type_str)
            except ValueError:
                return web.json_response({'error': f'Invalid data_type: {data_type_str}'}, status=400)
            
            success = await self.storage.store(key, value, data_type, api_source, ttl_hours)
            
            if success:
                return web.json_response({
                    'status': 'stored',
                    'key': key,
                    'data_type': data_type.value,
                    'ttl_hours': ttl_hours or self.storage.default_ttl_hours.get(data_type, 24)
                })
            else:
                return web.json_response({'error': 'Storage failed'}, status=500)
                
        except Exception as e:
            logger.error(f"Cache store error: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def cache_stats(self, request):
        """Get cache statistics"""
        try:
            storage_stats = self.storage.get_storage_stats()
            network_stats = self.peer_network.get_network_status()
            
            combined_stats = {
                'coordinator_stats': self.stats,
                'storage_stats': storage_stats,
                'peer_network_stats': network_stats,
                'cache_efficiency': {
                    'total_requests': self.stats['cache_hits'] + self.stats['cache_misses'],
                    'hit_rate_percent': (self.stats['cache_hits'] / 
                                       max(self.stats['cache_hits'] + self.stats['cache_misses'], 1)) * 100,
                    'api_calls_saved': self.stats['api_calls_saved'],
                    'peer_data_requests': self.stats['peer_data_requests']
                }
            }
            
            return web.json_response(combined_stats)
            
        except Exception as e:
            logger.error(f"Cache stats error: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    # Peer Network API Handlers
    
    async def get_peer_status(self, request):
        """Get peer network status"""
        try:
            status = self.peer_network.get_network_status()
            return web.json_response(status)
            
        except Exception as e:
            logger.error(f"Peer status error: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def connect_to_peer(self, request):
        """Manually connect to a peer"""
        try:
            data = await request.json()
            
            address = data.get('address')
            port = data.get('port')
            
            if not address or not port:
                return web.json_response({'error': 'address and port required'}, status=400)
            
            success = await self.peer_network.connect_to_peer(address, port)
            
            if success:
                return web.json_response({
                    'status': 'connected',
                    'peer_address': f"{address}:{port}"
                })
            else:
                return web.json_response({
                    'status': 'failed',
                    'peer_address': f"{address}:{port}"
                }, status=400)
                
        except Exception as e:
            logger.error(f"Connect to peer error: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    # HTTP Route Handlers
    
    async def get_network_status(self, request):
        """Get overall network status"""
        try:
            active_workers = [w for w in self.workers.values() 
                            if w.last_seen > datetime.now() - timedelta(minutes=5)]
            
            status = {
                'coordinator_id': self.local_node.node_id,
                'total_workers': len(self.workers),
                'active_workers': len(active_workers),
                'network_health': self._calculate_network_health(),
                'configuration': self.network_config,
                'uptime': str(datetime.now() - self.stats['start_time']),
                **self.dht.get_network_stats()
            }
            
            return web.json_response(status)
            
        except Exception as e:
            logger.error(f"Error getting network status: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_network_stats(self, request):
        """Get detailed network statistics"""
        try:
            worker_stats = {}
            for worker_id, worker in self.workers.items():
                worker_stats[worker_id] = {
                    'node_type': worker.node_type,
                    'capabilities': worker.capabilities,
                    'load': worker.load,
                    'last_seen': worker.last_seen.isoformat(),
                    'uptime': str(datetime.now() - worker.last_seen)
                }
            
            task_performance = self._calculate_task_performance()
            
            stats = {
                'coordinator': self.stats,
                'workers': worker_stats,
                'tasks': task_performance,
                'dht': self.dht.get_network_stats()
            }
            
            return web.json_response(stats)
            
        except Exception as e:
            logger.error(f"Error getting network stats: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def register_node(self, request):
        """Register a new worker node"""
        try:
            data = await request.json()
            
            node = DHTNode(
                node_id=data.get('node_id', ''),
                ip_address=data['ip_address'],
                port=data['port'],
                last_seen=datetime.now(),
                node_type=data.get('node_type', 'worker'),
                capabilities=data.get('capabilities', []),
                load=data.get('load', 0.0)
            )
            
            # Add to DHT and local registry
            self.dht.add_node(node)
            self.workers[node.node_id] = node
            self.stats['workers_joined'] += 1
            
            logger.info(f"Registered new {node.node_type} node: {node.node_id}")
            
            return web.json_response({
                'status': 'registered',
                'node_id': node.node_id,
                'coordinator_id': self.local_node.node_id
            })
            
        except Exception as e:
            logger.error(f"Error registering node: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    async def submit_task(self, request):
        """Submit a new task to the DHT network"""
        try:
            data = await request.json()
            
            task = DHTTask(
                task_id='',
                task_type=data['task_type'],
                store_id=data['store_id'],
                params=data.get('params', {}),
                priority=data.get('priority', 1)
            )
            
            # Submit to DHT for processing
            success = await self.dht.submit_task(task)
            
            if success:
                self.stats['tasks_submitted'] += 1
                self.task_history.append(task)
                
                return web.json_response({
                    'status': 'submitted',
                    'task_id': task.task_id,
                    'assigned_node': task.assigned_node
                })
            else:
                return web.json_response({
                    'status': 'failed',
                    'error': 'No available workers'
                }, status=503)
                
        except Exception as e:
            logger.error(f"Error submitting task: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    async def get_task_status(self, request):
        """Get status of a specific task"""
        try:
            task_id = request.match_info['task_id']
            
            # Check DHT task queue
            if task_id in self.dht.task_queue:
                task = self.dht.task_queue[task_id]
                return web.json_response(task.to_dict())
            
            # Check history
            for task in self.task_history:
                if task.task_id == task_id:
                    return web.json_response(task.to_dict())
            
            return web.json_response({'error': 'Task not found'}, status=404)
            
        except Exception as e:
            logger.error(f"Error getting task status: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def list_tasks(self, request):
        """List tasks with optional filtering"""
        try:
            status_filter = request.query.get('status')
            limit = int(request.query.get('limit', '100'))
            
            tasks = list(self.dht.task_queue.values())
            
            if status_filter:
                tasks = [t for t in tasks if t.status == status_filter]
            
            tasks = tasks[:limit]
            
            return web.json_response({
                'tasks': [task.to_dict() for task in tasks],
                'total': len(tasks)
            })
            
        except Exception as e:
            logger.error(f"Error listing tasks: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def health_check(self, request):
        """Health check endpoint"""
        return web.json_response({
            'status': 'healthy',
            'coordinator_id': self.local_node.node_id,
            'timestamp': datetime.now().isoformat(),
            'active_workers': len([w for w in self.workers.values() 
                                 if w.last_seen > datetime.now() - timedelta(minutes=5)])
        })
    
    async def ping(self, request):
        """Simple ping endpoint"""
        return web.json_response({'pong': datetime.now().isoformat()})
    
    # DHT Operation Handlers
    
    async def dht_get(self, request):
        """Get data from DHT"""
        try:
            key = request.match_info['key']
            value = await self.dht.get_data(key)
            
            if value is not None:
                return web.json_response({'key': key, 'value': value})
            else:
                return web.json_response({'error': 'Key not found'}, status=404)
                
        except Exception as e:
            logger.error(f"DHT get error: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def dht_store(self, request):
        """Store data in DHT"""
        try:
            data = await request.json()
            key = data['key']
            value = data['value']
            replicas = data.get('replicas', 3)
            
            success = await self.dht.store_data(key, value, replicas)
            
            if success:
                return web.json_response({'status': 'stored', 'key': key})
            else:
                return web.json_response({'error': 'Storage failed'}, status=500)
                
        except Exception as e:
            logger.error(f"DHT store error: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    # Background Tasks
    
    async def _heartbeat_loop(self):
        """Monitor worker heartbeats"""
        while True:
            try:
                cutoff = datetime.now() - timedelta(minutes=5)
                dead_workers = []
                
                for worker_id, worker in self.workers.items():
                    if worker.last_seen < cutoff:
                        dead_workers.append(worker_id)
                
                for worker_id in dead_workers:
                    logger.info(f"Removing dead worker: {worker_id}")
                    del self.workers[worker_id]
                    self.stats['workers_left'] += 1
                
                await asyncio.sleep(self.network_config['heartbeat_interval'])
                
            except Exception as e:
                logger.error(f"Heartbeat loop error: {e}")
                await asyncio.sleep(10)
    
    async def _task_monitor_loop(self):
        """Monitor task completion and handle timeouts"""
        while True:
            try:
                timeout_cutoff = datetime.now() - timedelta(
                    seconds=self.network_config['task_timeout']
                )
                
                timed_out_tasks = []
                for task_id, task in self.dht.task_queue.items():
                    if (task.status == 'processing' and 
                        task.created_at < timeout_cutoff):
                        timed_out_tasks.append(task)
                
                for task in timed_out_tasks:
                    logger.warning(f"Task {task.task_id} timed out")
                    task.status = 'failed'
                    task.retry_count += 1
                    
                    if task.retry_count < task.max_retries:
                        task.status = 'pending'
                        task.assigned_node = None
                        logger.info(f"Retrying task {task.task_id} ({task.retry_count}/{task.max_retries})")
                    else:
                        self.stats['tasks_failed'] += 1
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Task monitor error: {e}")
                await asyncio.sleep(10)
    
    async def _cache_optimization_loop(self):
        """Background cache optimization and data sharing"""
        while True:
            try:
                # Optimize cache based on access patterns
                await self._optimize_cache_placement()
                
                # Share popular data with peers
                await self._share_popular_data()
                
                # Request missing data from peers
                await self._request_missing_data()
                
                await asyncio.sleep(600)  # Run every 10 minutes
                
            except Exception as e:
                logger.error(f"Cache optimization error: {e}")
                await asyncio.sleep(60)
    
    async def _optimize_cache_placement(self):
        """Optimize cache data placement based on access patterns"""
        try:
            # Find frequently accessed data
            popular_entries = []
            for entry in self.storage.memory_cache.values():
                if entry.access_count > 5:  # Accessed more than 5 times
                    popular_entries.append(entry)
            
            # Ensure popular data is replicated to multiple peers
            for entry in popular_entries[:10]:  # Top 10 popular entries
                await self._ensure_data_replication(entry)
                
        except Exception as e:
            logger.error(f"Cache optimization failed: {e}")
    
    async def _ensure_data_replication(self, entry):
        """Ensure important data is replicated across multiple peers"""
        try:
            # This would implement replication logic
            # For now, just log the intent
            logger.debug(f"Ensuring replication for popular entry: {entry.key}")
            
        except Exception as e:
            logger.error(f"Data replication failed: {e}")
    
    async def _share_popular_data(self):
        """Share our popular data with the peer network"""
        try:
            if not self.network_config['peer_sharing_enabled']:
                return
            
            # Find data that should be shared
            shareable_data = []
            for entry in self.storage.memory_cache.values():
                if (entry.access_count > 3 and 
                    entry.age_hours < 24 and  # Recent data
                    entry.data_type in [DataType.DUTCHIE_MENU, DataType.JANE_MENU, DataType.TERPENE_PROFILE]):
                    shareable_data.append(entry)
            
            logger.debug(f"Sharing {len(shareable_data)} popular entries with peers")
            
        except Exception as e:
            logger.error(f"Data sharing failed: {e}")
    
    async def _request_missing_data(self):
        """Proactively request missing data from peers"""
        try:
            # This could implement predictive caching
            # Based on common request patterns
            pass
            
        except Exception as e:
            logger.error(f"Missing data request failed: {e}")
    
    async def _cleanup_loop(self):
        """Clean up old completed tasks"""
        while True:
            try:
                cutoff = datetime.now() - timedelta(hours=24)
                
                # Clean up completed tasks older than 24 hours
                old_tasks = []
                for task_id, task in self.dht.task_queue.items():
                    if (task.status in ['completed', 'failed'] and 
                        task.completed_at and task.completed_at < cutoff):
                        old_tasks.append(task_id)
                
                for task_id in old_tasks:
                    del self.dht.task_queue[task_id]
                
                logger.debug(f"Cleaned up {len(old_tasks)} old tasks")
                await asyncio.sleep(3600)  # Clean every hour
                
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")
                await asyncio.sleep(600)
    
    # Utility Methods
    
    def _calculate_network_health(self) -> str:
        """Calculate overall network health"""
        active_workers = len([w for w in self.workers.values() 
                            if w.last_seen > datetime.now() - timedelta(minutes=5)])
        
        if active_workers == 0:
            return 'critical'
        elif active_workers < 3:
            return 'degraded'
        elif active_workers < 10:
            return 'good'
        else:
            return 'excellent'
    
    def _calculate_task_performance(self) -> Dict:
        """Calculate task performance metrics"""
        completed_tasks = [t for t in self.task_history if t.status == 'completed']
        failed_tasks = [t for t in self.task_history if t.status == 'failed']
        
        if completed_tasks:
            avg_completion_time = sum(
                (t.completed_at - t.created_at).total_seconds() 
                for t in completed_tasks if t.completed_at
            ) / len(completed_tasks)
        else:
            avg_completion_time = 0
        
        success_rate = (
            len(completed_tasks) / max(len(completed_tasks) + len(failed_tasks), 1)
        ) * 100
        
        return {
            'total_submitted': self.stats['tasks_submitted'],
            'completed': len(completed_tasks),
            'failed': len(failed_tasks),
            'success_rate': success_rate,
            'avg_completion_time': avg_completion_time
        }

async def main():
    """Main entry point"""
    # Configuration
    host = os.getenv('DHT_COORDINATOR_HOST', '0.0.0.0')
    port = int(os.getenv('DHT_COORDINATOR_PORT', '9000'))
    
    # Create and start coordinator
    coordinator = DHTCoordinator(host, port)
    
    # Handle shutdown gracefully
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        asyncio.create_task(coordinator.stop())
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await coordinator.start()
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await coordinator.stop()

if __name__ == '__main__':
    asyncio.run(main())