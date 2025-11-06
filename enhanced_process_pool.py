"""
Enhanced Process Pool Management for WeedHounds Cannabis Data System
Robust process lifecycle management with health monitoring, automatic restarts,
resource limits, and distributed workload balancing across unlimited peer network
"""

import asyncio
import multiprocessing as mp
import threading
import time
import logging
import psutil
import signal
import os
import sys
import json
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Union, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager
import queue
import socket
import subprocess

try:
    import setproctitle
    SETPROCTITLE_AVAILABLE = True
except ImportError:
    setproctitle = None
    SETPROCTITLE_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProcessStatus(Enum):
    """Process status enumeration"""
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    RESTARTING = "restarting"

class WorkloadType(Enum):
    """Cannabis data workload types"""
    API_FETCH = "api_fetch"
    DATA_PROCESSING = "data_processing"
    CACHE_OPERATIONS = "cache_operations"
    DHT_OPERATIONS = "dht_operations"
    SERIALIZATION = "serialization"
    ANALYTICS = "analytics"
    BACKUP = "backup"
    MAINTENANCE = "maintenance"

class ResourceLimitType(Enum):
    """Resource limit types"""
    CPU_PERCENT = "cpu_percent"
    MEMORY_MB = "memory_mb"
    DISK_IO_MB = "disk_io_mb"
    NETWORK_IO_MB = "network_io_mb"
    EXECUTION_TIME = "execution_time"

@dataclass
class ResourceLimits:
    """Resource limits for processes"""
    max_cpu_percent: float = 80.0
    max_memory_mb: int = 1024
    max_disk_io_mb: int = 100
    max_network_io_mb: int = 50
    max_execution_time: int = 3600  # seconds
    max_file_descriptors: int = 1024

@dataclass
class ProcessMetrics:
    """Process performance metrics"""
    pid: int
    cpu_percent: float
    memory_mb: float
    disk_io_mb: float
    network_io_mb: float
    execution_time: float
    status: ProcessStatus
    start_time: datetime
    last_heartbeat: datetime
    restart_count: int
    error_count: int
    success_count: int

@dataclass
class WorkItem:
    """Cannabis data work item"""
    id: str
    workload_type: WorkloadType
    priority: int  # 1 (highest) to 10 (lowest)
    data: Any
    timeout: Optional[int] = None
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

class ProcessWorker:
    """Individual process worker for cannabis data processing"""
    
    def __init__(self, 
                 worker_id: str,
                 workload_types: List[WorkloadType],
                 resource_limits: ResourceLimits):
        self.worker_id = worker_id
        self.workload_types = workload_types
        self.resource_limits = resource_limits
        self.status = ProcessStatus.STOPPED
        self.process = None
        self.metrics = None
        self.work_queue = mp.Queue()
        self.result_queue = mp.Queue()
        self.control_queue = mp.Queue()
        self.shutdown_event = mp.Event()
        
        # Performance tracking
        self.start_time = None
        self.restart_count = 0
        self.last_heartbeat = None
    
    def start(self):
        """Start the worker process"""
        try:
            self.status = ProcessStatus.STARTING
            self.start_time = datetime.now()
            
            # Create and start process
            self.process = mp.Process(
                target=self._worker_main,
                args=(self.worker_id, self.workload_types, self.resource_limits,
                      self.work_queue, self.result_queue, self.control_queue,
                      self.shutdown_event),
                name=f"cannabis_worker_{self.worker_id}"
            )
            
            self.process.start()
            self.status = ProcessStatus.RUNNING
            
            logger.info(f"Started worker {self.worker_id} (PID: {self.process.pid})")
            
        except Exception as e:
            logger.error(f"Failed to start worker {self.worker_id}: {e}")
            self.status = ProcessStatus.FAILED
            raise
    
    def stop(self, timeout: int = 30):
        """Stop the worker process gracefully"""
        try:
            if self.process and self.process.is_alive():
                self.status = ProcessStatus.STOPPING
                
                # Signal shutdown
                self.shutdown_event.set()
                
                # Wait for graceful shutdown
                self.process.join(timeout)
                
                # Force terminate if still alive
                if self.process.is_alive():
                    logger.warning(f"Force terminating worker {self.worker_id}")
                    self.process.terminate()
                    self.process.join(5)
                    
                    if self.process.is_alive():
                        self.process.kill()
                        self.process.join(2)
            
            self.status = ProcessStatus.STOPPED
            logger.info(f"Stopped worker {self.worker_id}")
            
        except Exception as e:
            logger.error(f"Error stopping worker {self.worker_id}: {e}")
    
    def restart(self):
        """Restart the worker process"""
        logger.info(f"Restarting worker {self.worker_id}")
        self.status = ProcessStatus.RESTARTING
        self.restart_count += 1
        
        self.stop()
        time.sleep(1)  # Brief pause
        self.start()
    
    def submit_work(self, work_item: WorkItem) -> bool:
        """Submit work to the worker"""
        try:
            if self.status != ProcessStatus.RUNNING:
                return False
            
            self.work_queue.put(work_item, timeout=1)
            return True
            
        except queue.Full:
            logger.warning(f"Work queue full for worker {self.worker_id}")
            return False
        except Exception as e:
            logger.error(f"Error submitting work to {self.worker_id}: {e}")
            return False
    
    def get_result(self, timeout: float = 0.1) -> Optional[Tuple[str, Any, bool]]:
        """Get completed work result"""
        try:
            return self.result_queue.get(timeout=timeout)
        except queue.Empty:
            return None
        except Exception as e:
            logger.error(f"Error getting result from {self.worker_id}: {e}")
            return None
    
    def get_metrics(self) -> Optional[ProcessMetrics]:
        """Get current process metrics"""
        try:
            if not self.process or not self.process.is_alive():
                return None
            
            # Get process info
            proc = psutil.Process(self.process.pid)
            
            # CPU and memory
            cpu_percent = proc.cpu_percent()
            memory_info = proc.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            # I/O stats
            try:
                io_counters = proc.io_counters()
                disk_io_mb = (io_counters.read_bytes + io_counters.write_bytes) / (1024 * 1024)
            except (psutil.AccessDenied, AttributeError):
                disk_io_mb = 0.0
            
            # Network I/O (system-wide approximation)
            try:
                net_io = psutil.net_io_counters()
                network_io_mb = (net_io.bytes_sent + net_io.bytes_recv) / (1024 * 1024)
            except AttributeError:
                network_io_mb = 0.0
            
            # Execution time
            execution_time = (datetime.now() - self.start_time).total_seconds()
            
            self.metrics = ProcessMetrics(
                pid=self.process.pid,
                cpu_percent=cpu_percent,
                memory_mb=memory_mb,
                disk_io_mb=disk_io_mb,
                network_io_mb=network_io_mb,
                execution_time=execution_time,
                status=self.status,
                start_time=self.start_time,
                last_heartbeat=datetime.now(),
                restart_count=self.restart_count,
                error_count=0,  # Would track from worker
                success_count=0  # Would track from worker
            )
            
            return self.metrics
            
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            logger.warning(f"Cannot get metrics for worker {self.worker_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting metrics for worker {self.worker_id}: {e}")
            return None
    
    def check_resource_limits(self) -> List[str]:
        """Check if process exceeds resource limits"""
        violations = []
        metrics = self.get_metrics()
        
        if not metrics:
            return violations
        
        if metrics.cpu_percent > self.resource_limits.max_cpu_percent:
            violations.append(f"CPU usage {metrics.cpu_percent:.1f}% > {self.resource_limits.max_cpu_percent}%")
        
        if metrics.memory_mb > self.resource_limits.max_memory_mb:
            violations.append(f"Memory usage {metrics.memory_mb:.1f}MB > {self.resource_limits.max_memory_mb}MB")
        
        if metrics.execution_time > self.resource_limits.max_execution_time:
            violations.append(f"Execution time {metrics.execution_time:.1f}s > {self.resource_limits.max_execution_time}s")
        
        return violations
    
    @staticmethod
    def _worker_main(worker_id: str,
                    workload_types: List[WorkloadType],
                    resource_limits: ResourceLimits,
                    work_queue: mp.Queue,
                    result_queue: mp.Queue,
                    control_queue: mp.Queue,
                    shutdown_event: mp.Event):
        """Main worker process function"""
        
        # Set process title if available
        if SETPROCTITLE_AVAILABLE:
            setproctitle.setproctitle(f"weedhounds-worker-{worker_id}")
        
        # Setup signal handlers
        def signal_handler(signum, frame):
            logger.info(f"Worker {worker_id} received signal {signum}")
            shutdown_event.set()
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        logger.info(f"Worker {worker_id} started (PID: {os.getpid()})")
        
        # Performance counters
        success_count = 0
        error_count = 0
        last_heartbeat = time.time()
        
        try:
            while not shutdown_event.is_set():
                try:
                    # Send heartbeat every 30 seconds
                    current_time = time.time()
                    if current_time - last_heartbeat > 30:
                        try:
                            control_queue.put(('heartbeat', worker_id, current_time), timeout=0.1)
                            last_heartbeat = current_time
                        except queue.Full:
                            pass
                    
                    # Get work item
                    try:
                        work_item = work_queue.get(timeout=1.0)
                    except queue.Empty:
                        continue
                    
                    # Process work item
                    success = ProcessWorker._process_work_item(work_item, worker_id)
                    
                    if success:
                        success_count += 1
                        result_queue.put((work_item.id, "completed", True))
                    else:
                        error_count += 1
                        result_queue.put((work_item.id, "failed", False))
                    
                except Exception as e:
                    logger.error(f"Worker {worker_id} error: {e}")
                    error_count += 1
                    try:
                        result_queue.put(("unknown", f"error: {e}", False))
                    except queue.Full:
                        pass
                    
                    # Brief pause after error
                    time.sleep(0.1)
        
        except KeyboardInterrupt:
            logger.info(f"Worker {worker_id} interrupted")
        except Exception as e:
            logger.error(f"Worker {worker_id} fatal error: {e}")
        finally:
            logger.info(f"Worker {worker_id} shutting down (success: {success_count}, errors: {error_count})")
    
    @staticmethod
    def _process_work_item(work_item: WorkItem, worker_id: str) -> bool:
        """Process a cannabis data work item"""
        try:
            work_item.started_at = datetime.now()
            
            logger.debug(f"Worker {worker_id} processing {work_item.workload_type.value} work item {work_item.id}")
            
            # Simulate different types of cannabis data processing
            if work_item.workload_type == WorkloadType.API_FETCH:
                # Simulate API fetch
                time.sleep(0.1 + (work_item.priority * 0.01))
                
            elif work_item.workload_type == WorkloadType.DATA_PROCESSING:
                # Simulate data processing
                time.sleep(0.2 + (work_item.priority * 0.02))
                
            elif work_item.workload_type == WorkloadType.CACHE_OPERATIONS:
                # Simulate cache operations
                time.sleep(0.05 + (work_item.priority * 0.005))
                
            elif work_item.workload_type == WorkloadType.DHT_OPERATIONS:
                # Simulate DHT operations
                time.sleep(0.15 + (work_item.priority * 0.015))
                
            elif work_item.workload_type == WorkloadType.SERIALIZATION:
                # Simulate serialization
                time.sleep(0.03 + (work_item.priority * 0.003))
                
            elif work_item.workload_type == WorkloadType.ANALYTICS:
                # Simulate analytics processing
                time.sleep(0.5 + (work_item.priority * 0.05))
                
            elif work_item.workload_type == WorkloadType.BACKUP:
                # Simulate backup operations
                time.sleep(1.0 + (work_item.priority * 0.1))
                
            elif work_item.workload_type == WorkloadType.MAINTENANCE:
                # Simulate maintenance tasks
                time.sleep(0.3 + (work_item.priority * 0.03))
            
            work_item.completed_at = datetime.now()
            return True
            
        except Exception as e:
            logger.error(f"Error processing work item {work_item.id}: {e}")
            return False

class EnhancedProcessPool:
    """
    Enhanced process pool manager for cannabis data processing
    with health monitoring, automatic restarts, and resource limits
    """
    
    def __init__(self,
                 max_workers: int = None,
                 resource_limits: ResourceLimits = None,
                 health_check_interval: int = 30,
                 auto_restart: bool = True):
        """
        Initialize enhanced process pool
        
        Args:
            max_workers: Maximum number of worker processes
            resource_limits: Resource limits for workers
            health_check_interval: Health check interval in seconds
            auto_restart: Enable automatic restart of failed workers
        """
        self.max_workers = max_workers or mp.cpu_count()
        self.resource_limits = resource_limits or ResourceLimits()
        self.health_check_interval = health_check_interval
        self.auto_restart = auto_restart
        
        # Worker management
        self.workers: Dict[str, ProcessWorker] = {}
        self.work_queue = asyncio.Queue()
        self.result_queue = asyncio.Queue()
        
        # Workload balancing
        self.workload_assignments = {
            WorkloadType.API_FETCH: [],
            WorkloadType.DATA_PROCESSING: [],
            WorkloadType.CACHE_OPERATIONS: [],
            WorkloadType.DHT_OPERATIONS: [],
            WorkloadType.SERIALIZATION: [],
            WorkloadType.ANALYTICS: [],
            WorkloadType.BACKUP: [],
            WorkloadType.MAINTENANCE: []
        }
        
        # Statistics
        self.stats = {
            'total_workers': 0,
            'active_workers': 0,
            'failed_workers': 0,
            'restart_count': 0,
            'work_items_processed': 0,
            'work_items_failed': 0,
            'avg_processing_time': 0.0,
            'resource_violations': 0
        }
        
        # Control
        self.shutdown_event = asyncio.Event()
        self.health_check_task = None
        self.result_processing_task = None
    
    async def start(self):
        """Start the process pool"""
        try:
            logger.info(f"Starting enhanced process pool with {self.max_workers} workers")
            
            # Create and start workers
            await self._create_workers()
            
            # Start background tasks
            self.health_check_task = asyncio.create_task(self._health_check_loop())
            self.result_processing_task = asyncio.create_task(self._result_processing_loop())
            
            logger.info("Enhanced process pool started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start process pool: {e}")
            raise
    
    async def stop(self, timeout: int = 60):
        """Stop the process pool gracefully"""
        try:
            logger.info("Stopping enhanced process pool")
            
            # Signal shutdown
            self.shutdown_event.set()
            
            # Cancel background tasks
            if self.health_check_task:
                self.health_check_task.cancel()
            if self.result_processing_task:
                self.result_processing_task.cancel()
            
            # Stop all workers
            stop_tasks = []
            for worker in self.workers.values():
                stop_tasks.append(asyncio.create_task(self._stop_worker_async(worker)))
            
            if stop_tasks:
                await asyncio.gather(*stop_tasks, return_exceptions=True)
            
            logger.info("Enhanced process pool stopped")
            
        except Exception as e:
            logger.error(f"Error stopping process pool: {e}")
    
    async def submit_work(self, work_item: WorkItem) -> bool:
        """Submit work item to the pool"""
        try:
            # Find best worker for this workload type
            worker = self._select_worker(work_item.workload_type)
            
            if worker and worker.submit_work(work_item):
                await self.work_queue.put(work_item)
                return True
            else:
                logger.warning(f"No available worker for {work_item.workload_type.value}")
                return False
                
        except Exception as e:
            logger.error(f"Error submitting work: {e}")
            return False
    
    async def get_result(self, timeout: float = 1.0) -> Optional[Tuple[str, Any, bool]]:
        """Get completed work result"""
        try:
            return await asyncio.wait_for(self.result_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"Error getting result: {e}")
            return None
    
    def get_pool_statistics(self) -> Dict:
        """Get comprehensive pool statistics"""
        active_workers = sum(1 for w in self.workers.values() 
                           if w.status == ProcessStatus.RUNNING)
        failed_workers = sum(1 for w in self.workers.values() 
                           if w.status == ProcessStatus.FAILED)
        
        self.stats.update({
            'total_workers': len(self.workers),
            'active_workers': active_workers,
            'failed_workers': failed_workers
        })
        
        # Worker metrics
        worker_metrics = {}
        for worker_id, worker in self.workers.items():
            metrics = worker.get_metrics()
            if metrics:
                worker_metrics[worker_id] = asdict(metrics)
        
        return {
            'pool_stats': self.stats,
            'worker_metrics': worker_metrics,
            'workload_assignments': {wt.value: len(workers) 
                                   for wt, workers in self.workload_assignments.items()},
            'resource_limits': asdict(self.resource_limits)
        }
    
    async def _create_workers(self):
        """Create and start worker processes"""
        # Distribute workload types among workers
        workload_distribution = self._distribute_workloads()
        
        for i in range(self.max_workers):
            worker_id = f"worker_{i:03d}"
            workload_types = workload_distribution[i % len(workload_distribution)]
            
            worker = ProcessWorker(worker_id, workload_types, self.resource_limits)
            
            try:
                worker.start()
                self.workers[worker_id] = worker
                
                # Update workload assignments
                for workload_type in workload_types:
                    self.workload_assignments[workload_type].append(worker_id)
                
                logger.debug(f"Created worker {worker_id} for {[wt.value for wt in workload_types]}")
                
            except Exception as e:
                logger.error(f"Failed to create worker {worker_id}: {e}")
                self.stats['failed_workers'] += 1
    
    def _distribute_workloads(self) -> List[List[WorkloadType]]:
        """Distribute workload types among workers"""
        workload_types = list(WorkloadType)
        distributions = []
        
        # Create specialized workers for heavy workloads
        heavy_workloads = [
            [WorkloadType.DATA_PROCESSING],
            [WorkloadType.ANALYTICS],
            [WorkloadType.BACKUP]
        ]
        
        # Create general workers for lighter workloads
        light_workloads = [
            [WorkloadType.API_FETCH, WorkloadType.CACHE_OPERATIONS],
            [WorkloadType.DHT_OPERATIONS, WorkloadType.SERIALIZATION],
            [WorkloadType.MAINTENANCE]
        ]
        
        distributions.extend(heavy_workloads)
        distributions.extend(light_workloads)
        
        # Fill remaining with mixed workers
        while len(distributions) < self.max_workers:
            mixed_workloads = [
                WorkloadType.API_FETCH,
                WorkloadType.CACHE_OPERATIONS,
                WorkloadType.SERIALIZATION
            ]
            distributions.append(mixed_workloads)
        
        return distributions[:self.max_workers]
    
    def _select_worker(self, workload_type: WorkloadType) -> Optional[ProcessWorker]:
        """Select best worker for workload type"""
        candidate_workers = self.workload_assignments.get(workload_type, [])
        
        if not candidate_workers:
            # Fallback to any available worker
            candidate_workers = list(self.workers.keys())
        
        # Find worker with lowest load
        best_worker = None
        lowest_load = float('inf')
        
        for worker_id in candidate_workers:
            worker = self.workers.get(worker_id)
            if not worker or worker.status != ProcessStatus.RUNNING:
                continue
            
            # Calculate load based on queue size and metrics
            try:
                queue_size = worker.work_queue.qsize()
                metrics = worker.get_metrics()
                
                if metrics:
                    load_score = (
                        queue_size * 10 +
                        metrics.cpu_percent +
                        (metrics.memory_mb / 100)
                    )
                else:
                    load_score = queue_size * 10
                
                if load_score < lowest_load:
                    lowest_load = load_score
                    best_worker = worker
                    
            except Exception as e:
                logger.warning(f"Error calculating load for worker {worker_id}: {e}")
                continue
        
        return best_worker
    
    async def _health_check_loop(self):
        """Background health check loop"""
        while not self.shutdown_event.is_set():
            try:
                await self._perform_health_checks()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(5)
    
    async def _perform_health_checks(self):
        """Perform health checks on all workers"""
        for worker_id, worker in list(self.workers.items()):
            try:
                # Check if process is alive
                if worker.process and not worker.process.is_alive():
                    logger.warning(f"Worker {worker_id} process died")
                    if self.auto_restart:
                        await self._restart_worker(worker_id)
                    continue
                
                # Check resource limits
                violations = worker.check_resource_limits()
                if violations:
                    logger.warning(f"Worker {worker_id} resource violations: {violations}")
                    self.stats['resource_violations'] += len(violations)
                    
                    # Restart worker if severe violations
                    if any('CPU usage' in v or 'Memory usage' in v for v in violations):
                        if self.auto_restart:
                            logger.info(f"Restarting worker {worker_id} due to resource violations")
                            await self._restart_worker(worker_id)
                
                # Update metrics
                metrics = worker.get_metrics()
                if metrics:
                    worker.last_heartbeat = metrics.last_heartbeat
                
            except Exception as e:
                logger.error(f"Health check error for worker {worker_id}: {e}")
    
    async def _restart_worker(self, worker_id: str):
        """Restart a specific worker"""
        try:
            worker = self.workers.get(worker_id)
            if worker:
                logger.info(f"Restarting worker {worker_id}")
                
                # Stop old worker
                await self._stop_worker_async(worker)
                
                # Create new worker with same configuration
                workload_types = worker.workload_types
                new_worker = ProcessWorker(worker_id, workload_types, self.resource_limits)
                new_worker.start()
                
                self.workers[worker_id] = new_worker
                self.stats['restart_count'] += 1
                
        except Exception as e:
            logger.error(f"Error restarting worker {worker_id}: {e}")
            self.stats['failed_workers'] += 1
    
    async def _stop_worker_async(self, worker: ProcessWorker):
        """Stop worker asynchronously"""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, worker.stop, 30
            )
        except Exception as e:
            logger.error(f"Error stopping worker {worker.worker_id}: {e}")
    
    async def _result_processing_loop(self):
        """Background result processing loop"""
        while not self.shutdown_event.is_set():
            try:
                # Collect results from all workers
                for worker in self.workers.values():
                    result = worker.get_result(timeout=0.01)
                    if result:
                        await self.result_queue.put(result)
                        self.stats['work_items_processed'] += 1
                
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Result processing error: {e}")
                await asyncio.sleep(1)

# Process pool decorator for cannabis data functions
def process_pool_task(workload_type: WorkloadType, priority: int = 5):
    """Decorator to submit function to process pool"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # This would integrate with a global process pool instance
            work_item = WorkItem(
                id=f"{func.__name__}_{int(time.time() * 1000)}",
                workload_type=workload_type,
                priority=priority,
                data={'args': args, 'kwargs': kwargs}
            )
            
            # Submit to process pool (implementation would depend on global instance)
            logger.info(f"Submitting {func.__name__} to process pool as {workload_type.value}")
            
            # For now, execute locally
            return func(*args, **kwargs)
        
        return wrapper
    return decorator

# Example cannabis data processing functions
@process_pool_task(WorkloadType.API_FETCH, priority=3)
def fetch_dispensary_menu(dispensary_id: str) -> Dict:
    """Example: Fetch dispensary menu data"""
    # Simulate API fetch work
    time.sleep(0.1)
    return {"dispensary_id": dispensary_id, "menu": "fetched"}

@process_pool_task(WorkloadType.DATA_PROCESSING, priority=6)
def process_terpene_data(terpene_profiles: List[Dict]) -> Dict:
    """Example: Process terpene profile data"""
    # Simulate data processing work
    time.sleep(0.5)
    return {"processed_profiles": len(terpene_profiles)}

@process_pool_task(WorkloadType.ANALYTICS, priority=8)
def analyze_cannabis_trends(data_points: List[Dict]) -> Dict:
    """Example: Analyze cannabis market trends"""
    # Simulate analytics work
    time.sleep(1.0)
    return {"trends": "analyzed", "data_points": len(data_points)}

if __name__ == "__main__":
    # Example usage
    async def test_process_pool():
        # Create resource limits
        limits = ResourceLimits(
            max_cpu_percent=70.0,
            max_memory_mb=512,
            max_execution_time=300
        )
        
        # Create and start process pool
        pool = EnhancedProcessPool(
            max_workers=4,
            resource_limits=limits,
            health_check_interval=10,
            auto_restart=True
        )
        
        await pool.start()
        
        try:
            # Submit various work items
            work_items = [
                WorkItem("api_1", WorkloadType.API_FETCH, 3, {"dispensary": "123"}),
                WorkItem("process_1", WorkloadType.DATA_PROCESSING, 5, {"data": "terpenes"}),
                WorkItem("cache_1", WorkloadType.CACHE_OPERATIONS, 2, {"key": "menu_123"}),
                WorkItem("dht_1", WorkloadType.DHT_OPERATIONS, 4, {"peer": "node_456"}),
                WorkItem("analytics_1", WorkloadType.ANALYTICS, 7, {"dataset": "trends"})
            ]
            
            # Submit work
            for work_item in work_items:
                success = await pool.submit_work(work_item)
                print(f"Submitted {work_item.id}: {success}")
            
            # Process results
            for _ in range(len(work_items)):
                result = await pool.get_result(timeout=5.0)
                if result:
                    work_id, data, success = result
                    print(f"Result {work_id}: {data} (success: {success})")
            
            # Get statistics
            stats = pool.get_pool_statistics()
            print(f"Pool statistics: {stats['pool_stats']}")
            print(f"Active workers: {stats['pool_stats']['active_workers']}")
            
            # Wait a bit for monitoring
            await asyncio.sleep(15)
            
        finally:
            await pool.stop()
    
    # Run test
    asyncio.run(test_process_pool())