"""
DHT Worker Service for WeedHounds Distributed Cannabis Data Collection
Connects to DHT network and processes dispensary data collection tasks
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from aiohttp import web, ClientSession
import signal
import uuid

from dht_core import DHTCore, DHTNode, DHTTask

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DHTWorker:
    """DHT Worker processes cannabis dispensary data collection tasks"""
    
    def __init__(self, coordinator_host: str, coordinator_port: int, 
                 worker_port: int = 9001):
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.worker_port = worker_port
        
        # Generate unique worker ID
        self.worker_id = f"worker_{uuid.uuid4().hex[:8]}"
        
        # Create local worker node
        self.local_node = DHTNode(
            node_id=self.worker_id,
            ip_address='0.0.0.0',
            port=worker_port,
            last_seen=datetime.now(),
            node_type='worker',
            capabilities=['dutchie_api', 'jane_api', 'menu_scraping', 'terpene_analysis'],
            load=0.0
        )
        
        # Initialize DHT core
        self.dht = DHTCore(self.local_node)
        
        # Worker state
        self.registered = False
        self.processing_task = None
        self.task_count = 0
        self.last_heartbeat = datetime.now()
        
        # HTTP app for worker API
        self.app = web.Application()
        self.setup_routes()
        
        # Task processing configuration
        self.config = {
            'heartbeat_interval': int(os.getenv('DHT_HEARTBEAT_INTERVAL', '30')),
            'max_concurrent_tasks': int(os.getenv('DHT_MAX_CONCURRENT_TASKS', '1')),
            'task_poll_interval': int(os.getenv('DHT_TASK_POLL_INTERVAL', '5')),
            'coordinator_timeout': int(os.getenv('DHT_COORDINATOR_TIMEOUT', '10'))
        }
        
        # Statistics
        self.stats = {
            'tasks_completed': 0,
            'tasks_failed': 0,
            'total_processing_time': 0,
            'start_time': datetime.now(),
            'last_task_time': None
        }
    
    def setup_routes(self):
        """Setup HTTP API routes for worker"""
        self.app.router.add_get('/worker/status', self.get_status)
        self.app.router.add_get('/worker/stats', self.get_stats)
        self.app.router.add_post('/worker/task', self.receive_task)
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/ping', self.ping)
    
    async def start(self):
        """Start the DHT worker"""
        try:
            # Start DHT core
            await self.dht.start()
            
            # Start HTTP server
            runner = web.AppRunner(self.app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', self.worker_port)
            await site.start()
            
            # Register with coordinator
            await self.register_with_coordinator()
            
            # Start background tasks
            asyncio.create_task(self._heartbeat_loop())
            asyncio.create_task(self._task_polling_loop())
            
            logger.info(f"DHT Worker {self.worker_id} started on port {self.worker_port}")
            logger.info(f"Registered with coordinator at {self.coordinator_host}:{self.coordinator_port}")
            
        except Exception as e:
            logger.error(f"Failed to start worker: {e}")
            raise
    
    async def stop(self):
        """Stop the DHT worker"""
        logger.info(f"Stopping DHT Worker {self.worker_id}...")
        
        try:
            # Unregister from coordinator
            if self.registered:
                await self.unregister_from_coordinator()
            
            # Stop DHT core
            await self.dht.stop()
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    async def register_with_coordinator(self):
        """Register this worker with the DHT coordinator"""
        try:
            async with ClientSession() as session:
                url = f"http://{self.coordinator_host}:{self.coordinator_port}/nodes/register"
                
                registration_data = {
                    'node_id': self.worker_id,
                    'ip_address': self.local_node.ip_address,
                    'port': self.worker_port,
                    'node_type': 'worker',
                    'capabilities': self.local_node.capabilities,
                    'load': 0.0
                }
                
                async with session.post(url, json=registration_data,
                                      timeout=self.config['coordinator_timeout']) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        self.registered = True
                        logger.info(f"Successfully registered with coordinator: {result}")
                    else:
                        raise Exception(f"Registration failed: {resp.status}")
                        
        except Exception as e:
            logger.error(f"Failed to register with coordinator: {e}")
            raise
    
    async def unregister_from_coordinator(self):
        """Unregister this worker from the DHT coordinator"""
        try:
            async with ClientSession() as session:
                url = f"http://{self.coordinator_host}:{self.coordinator_port}/nodes/{self.worker_id}"
                
                async with session.delete(url, timeout=self.config['coordinator_timeout']) as resp:
                    if resp.status == 200:
                        logger.info("Successfully unregistered from coordinator")
                    else:
                        logger.warning(f"Unregistration failed: {resp.status}")
                        
        except Exception as e:
            logger.error(f"Failed to unregister from coordinator: {e}")
    
    async def send_heartbeat(self):
        """Send heartbeat to coordinator"""
        try:
            async with ClientSession() as session:
                url = f"http://{self.coordinator_host}:{self.coordinator_port}/nodes/{self.worker_id}/heartbeat"
                
                heartbeat_data = {
                    'load': self.get_current_load(),
                    'status': 'processing' if self.processing_task else 'idle',
                    'task_count': self.task_count,
                    'last_task_time': self.stats['last_task_time'].isoformat() if self.stats['last_task_time'] else None
                }
                
                async with session.post(url, json=heartbeat_data,
                                      timeout=self.config['coordinator_timeout']) as resp:
                    if resp.status == 200:
                        self.last_heartbeat = datetime.now()
                    else:
                        logger.warning(f"Heartbeat failed: {resp.status}")
                        
        except Exception as e:
            logger.error(f"Failed to send heartbeat: {e}")
    
    # HTTP Route Handlers
    
    async def get_status(self, request):
        """Get worker status"""
        return web.json_response({
            'worker_id': self.worker_id,
            'status': 'processing' if self.processing_task else 'idle',
            'registered': self.registered,
            'current_task': self.processing_task.task_id if self.processing_task else None,
            'load': self.get_current_load(),
            'capabilities': self.local_node.capabilities,
            'uptime': str(datetime.now() - self.stats['start_time']),
            'last_heartbeat': self.last_heartbeat.isoformat()
        })
    
    async def get_stats(self, request):
        """Get worker statistics"""
        return web.json_response({
            'worker_id': self.worker_id,
            'stats': {
                **self.stats,
                'start_time': self.stats['start_time'].isoformat(),
                'last_task_time': self.stats['last_task_time'].isoformat() if self.stats['last_task_time'] else None,
                'avg_task_time': (self.stats['total_processing_time'] / 
                                max(self.stats['tasks_completed'], 1)),
                'success_rate': (self.stats['tasks_completed'] / 
                               max(self.task_count, 1)) * 100 if self.task_count > 0 else 0
            },
            'current_load': self.get_current_load(),
            'configuration': self.config
        })
    
    async def receive_task(self, request):
        """Receive a task from coordinator"""
        try:
            if self.processing_task:
                return web.json_response({
                    'status': 'busy',
                    'error': 'Worker is already processing a task'
                }, status=503)
            
            task_data = await request.json()
            task = DHTTask.from_dict(task_data)
            
            # Start processing task
            asyncio.create_task(self.process_task(task))
            
            return web.json_response({
                'status': 'accepted',
                'task_id': task.task_id,
                'worker_id': self.worker_id
            })
            
        except Exception as e:
            logger.error(f"Error receiving task: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    async def health_check(self, request):
        """Health check endpoint"""
        return web.json_response({
            'status': 'healthy',
            'worker_id': self.worker_id,
            'timestamp': datetime.now().isoformat(),
            'load': self.get_current_load()
        })
    
    async def ping(self, request):
        """Simple ping endpoint"""
        return web.json_response({'pong': datetime.now().isoformat()})
    
    # Task Processing
    
    async def process_task(self, task: DHTTask):
        """Process a dispensary data collection task"""
        start_time = datetime.now()
        self.processing_task = task
        task.status = 'processing'
        task.assigned_node = self.worker_id
        
        logger.info(f"Processing task {task.task_id} of type {task.task_type}")
        
        try:
            # Route task to appropriate processor
            if task.task_type == 'dutchie_menu':
                result = await self.process_dutchie_menu_task(task)
            elif task.task_type == 'jane_menu':
                result = await self.process_jane_menu_task(task)
            elif task.task_type == 'terpene_analysis':
                result = await self.process_terpene_analysis_task(task)
            elif task.task_type == 'menu_comparison':
                result = await self.process_menu_comparison_task(task)
            else:
                raise ValueError(f"Unknown task type: {task.task_type}")
            
            # Task completed successfully
            task.status = 'completed'
            task.result = result
            task.completed_at = datetime.now()
            
            self.stats['tasks_completed'] += 1
            processing_time = (datetime.now() - start_time).total_seconds()
            self.stats['total_processing_time'] += processing_time
            self.stats['last_task_time'] = datetime.now()
            
            logger.info(f"Task {task.task_id} completed in {processing_time:.2f} seconds")
            
        except Exception as e:
            # Task failed
            task.status = 'failed'
            task.error = str(e)
            task.completed_at = datetime.now()
            
            self.stats['tasks_failed'] += 1
            logger.error(f"Task {task.task_id} failed: {e}")
            
        finally:
            self.processing_task = None
            self.task_count += 1
            
            # Report completion to coordinator
            await self.report_task_completion(task)
    
    async def process_dutchie_menu_task(self, task: DHTTask) -> Dict[str, Any]:
        """Process Dutchie menu collection task"""
        logger.info(f"Processing Dutchie menu for store {task.store_id}")
        
        # Simulate Dutchie API call (replace with actual implementation)
        await asyncio.sleep(2)  # Simulate processing time
        
        return {
            'store_id': task.store_id,
            'menu_items': [
                {
                    'id': 'item_1',
                    'name': 'Blue Dream',
                    'category': 'flower',
                    'price': 25.99,
                    'thc': 18.5,
                    'cbd': 0.8
                }
            ],
            'collection_time': datetime.now().isoformat(),
            'source': 'dutchie'
        }
    
    async def process_jane_menu_task(self, task: DHTTask) -> Dict[str, Any]:
        """Process Jane menu collection task"""
        logger.info(f"Processing Jane menu for store {task.store_id}")
        
        # Simulate Jane API call (replace with actual implementation)
        await asyncio.sleep(1.5)  # Simulate processing time
        
        return {
            'store_id': task.store_id,
            'menu_items': [
                {
                    'id': 'jane_item_1',
                    'name': 'OG Kush',
                    'category': 'flower',
                    'price': 28.99,
                    'thc': 22.3,
                    'cbd': 0.5
                }
            ],
            'collection_time': datetime.now().isoformat(),
            'source': 'jane'
        }
    
    async def process_terpene_analysis_task(self, task: DHTTask) -> Dict[str, Any]:
        """Process terpene analysis task"""
        logger.info(f"Processing terpene analysis for product {task.params.get('product_id')}")
        
        # Simulate terpene analysis (replace with actual implementation)
        await asyncio.sleep(3)  # Simulate processing time
        
        return {
            'product_id': task.params.get('product_id'),
            'terpenes': {
                'myrcene': 0.45,
                'limonene': 0.32,
                'pinene': 0.18,
                'linalool': 0.12
            },
            'analysis_time': datetime.now().isoformat(),
            'confidence': 0.95
        }
    
    async def process_menu_comparison_task(self, task: DHTTask) -> Dict[str, Any]:
        """Process menu comparison task"""
        logger.info(f"Processing menu comparison for stores {task.params.get('store_ids')}")
        
        # Simulate menu comparison (replace with actual implementation)
        await asyncio.sleep(2.5)  # Simulate processing time
        
        return {
            'store_ids': task.params.get('store_ids', []),
            'common_products': 15,
            'price_differences': {
                'avg_difference': 3.25,
                'max_difference': 12.50,
                'min_difference': 0.50
            },
            'comparison_time': datetime.now().isoformat()
        }
    
    async def report_task_completion(self, task: DHTTask):
        """Report task completion to coordinator"""
        try:
            async with ClientSession() as session:
                url = f"http://{self.coordinator_host}:{self.coordinator_port}/tasks/{task.task_id}/complete"
                
                completion_data = {
                    'worker_id': self.worker_id,
                    'status': task.status,
                    'result': task.result,
                    'error': task.error,
                    'completed_at': task.completed_at.isoformat() if task.completed_at else None
                }
                
                async with session.post(url, json=completion_data,
                                      timeout=self.config['coordinator_timeout']) as resp:
                    if resp.status == 200:
                        logger.debug(f"Reported completion of task {task.task_id}")
                    else:
                        logger.warning(f"Failed to report task completion: {resp.status}")
                        
        except Exception as e:
            logger.error(f"Failed to report task completion: {e}")
    
    # Background Tasks
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats to coordinator"""
        while True:
            try:
                if self.registered:
                    await self.send_heartbeat()
                
                await asyncio.sleep(self.config['heartbeat_interval'])
                
            except Exception as e:
                logger.error(f"Heartbeat loop error: {e}")
                await asyncio.sleep(10)
    
    async def _task_polling_loop(self):
        """Poll coordinator for available tasks"""
        while True:
            try:
                if self.registered and not self.processing_task:
                    await self.poll_for_tasks()
                
                await asyncio.sleep(self.config['task_poll_interval'])
                
            except Exception as e:
                logger.error(f"Task polling error: {e}")
                await asyncio.sleep(10)
    
    async def poll_for_tasks(self):
        """Poll coordinator for available tasks"""
        try:
            async with ClientSession() as session:
                url = f"http://{self.coordinator_host}:{self.coordinator_port}/tasks/available"
                params = {
                    'worker_id': self.worker_id,
                    'capabilities': ','.join(self.local_node.capabilities)
                }
                
                async with session.get(url, params=params,
                                     timeout=self.config['coordinator_timeout']) as resp:
                    if resp.status == 200:
                        task_data = await resp.json()
                        if task_data.get('task'):
                            task = DHTTask.from_dict(task_data['task'])
                            asyncio.create_task(self.process_task(task))
                    elif resp.status != 204:  # 204 = No Content (no tasks available)
                        logger.warning(f"Task polling failed: {resp.status}")
                        
        except Exception as e:
            logger.error(f"Failed to poll for tasks: {e}")
    
    # Utility Methods
    
    def get_current_load(self) -> float:
        """Calculate current worker load (0.0 to 1.0)"""
        if self.processing_task:
            return 1.0
        else:
            return 0.0

async def main():
    """Main entry point"""
    # Configuration
    coordinator_host = os.getenv('DHT_COORDINATOR_HOST', 'localhost')
    coordinator_port = int(os.getenv('DHT_COORDINATOR_PORT', '9000'))
    worker_port = int(os.getenv('DHT_WORKER_PORT', '9001'))
    
    # Create and start worker
    worker = DHTWorker(coordinator_host, coordinator_port, worker_port)
    
    # Handle shutdown gracefully
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        asyncio.create_task(worker.stop())
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await worker.start()
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await worker.stop()

if __name__ == '__main__':
    asyncio.run(main())