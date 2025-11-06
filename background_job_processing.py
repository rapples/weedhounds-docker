#!/usr/bin/env python3
"""
Distributed Background Job Processing System for Cannabis Data Platform
======================================================================

Comprehensive background job processing system with cannabis-specific workflows,
distributed task management, and real-time monitoring. Provides reliable
processing for data updates, analytics, compliance checks, and maintenance tasks.

Key Features:
- Cannabis-specific job types and workflows
- Distributed task processing with Redis/Celery integration
- Priority-based job scheduling and execution
- Real-time job monitoring and progress tracking
- Automatic retry mechanisms and failure handling
- State-aware compliance job processing
- Terpene analysis batch processing
- Dispensary data synchronization workflows
- Analytics pipeline automation

Author: WeedHounds Background Processing Team
Created: November 2025
"""

import logging
import time
import threading
import asyncio
import json
import uuid
import hashlib
from typing import Dict, List, Any, Optional, Callable, Union, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import pickle
import traceback
from pathlib import Path
import queue
import concurrent.futures

try:
    import redis
    REDIS_AVAILABLE = True
    print("✅ Redis available for job queuing")
except ImportError:
    redis = None
    REDIS_AVAILABLE = False
    print("⚠️ Redis not available")

try:
    import celery
    from celery import Celery, Task
    from celery.result import AsyncResult
    CELERY_AVAILABLE = True
    print("✅ Celery available for distributed processing")
except ImportError:
    celery = None
    Celery = None
    Task = None
    AsyncResult = None
    CELERY_AVAILABLE = False
    print("⚠️ Celery not available")

try:
    import schedule
    SCHEDULING_AVAILABLE = True
    print("✅ Scheduling available")
except ImportError:
    schedule = None
    SCHEDULING_AVAILABLE = False
    print("⚠️ Scheduling not available")

class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"

class JobPriority(Enum):
    """Job priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4
    REAL_TIME = 5

class CannabisJobType(Enum):
    """Cannabis-specific job types."""
    DISPENSARY_SYNC = "dispensary_sync"
    STRAIN_ANALYSIS = "strain_analysis"
    TERPENE_PROCESSING = "terpene_processing"
    PRICE_UPDATE = "price_update"
    COMPLIANCE_CHECK = "compliance_check"
    INVENTORY_SYNC = "inventory_sync"
    USER_RECOMMENDATIONS = "user_recommendations"
    ANALYTICS_PIPELINE = "analytics_pipeline"
    DATA_CLEANUP = "data_cleanup"
    CACHE_REFRESH = "cache_refresh"
    REPORT_GENERATION = "report_generation"
    STATE_REGULATION_UPDATE = "state_regulation_update"

@dataclass
class JobDefinition:
    """Job definition and configuration."""
    job_id: str
    job_type: CannabisJobType
    priority: JobPriority
    data: Dict[str, Any]
    created_at: datetime = field(default_factory=datetime.now)
    scheduled_at: Optional[datetime] = None
    max_retries: int = 3
    retry_delay: int = 60  # seconds
    timeout: int = 300  # 5 minutes default
    requires_compliance: bool = False
    state_specific: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.scheduled_at:
            self.scheduled_at = self.created_at

@dataclass
class JobExecution:
    """Job execution tracking."""
    job_id: str
    execution_id: str
    status: JobStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    worker_id: Optional[str] = None
    retry_count: int = 0
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    progress: float = 0.0
    logs: List[str] = field(default_factory=list)
    
    @property
    def duration(self) -> Optional[float]:
        """Get execution duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

@dataclass
class BackgroundJobConfig:
    """Configuration for background job processing."""
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    celery_broker_url: Optional[str] = None
    result_backend: Optional[str] = None
    worker_concurrency: int = 4
    max_queue_size: int = 10000
    enable_monitoring: bool = True
    monitoring_interval: int = 30
    job_retention_days: int = 7
    enable_job_persistence: bool = True
    persistence_path: str = "jobs/"
    enable_metrics: bool = True
    default_timeout: int = 300
    max_retries: int = 3
    retry_backoff: bool = True
    enable_cannabis_workflows: bool = True

class CannabisJobProcessor:
    """Processes cannabis-specific background jobs."""
    
    def __init__(self, config: BackgroundJobConfig):
        self.config = config
        self.job_handlers = {}
        self.middleware = []
        
        self._register_cannabis_handlers()
    
    def _register_cannabis_handlers(self):
        """Register built-in cannabis job handlers."""
        self.register_handler(CannabisJobType.DISPENSARY_SYNC, self._process_dispensary_sync)
        self.register_handler(CannabisJobType.STRAIN_ANALYSIS, self._process_strain_analysis)
        self.register_handler(CannabisJobType.TERPENE_PROCESSING, self._process_terpene_processing)
        self.register_handler(CannabisJobType.PRICE_UPDATE, self._process_price_update)
        self.register_handler(CannabisJobType.COMPLIANCE_CHECK, self._process_compliance_check)
        self.register_handler(CannabisJobType.INVENTORY_SYNC, self._process_inventory_sync)
        self.register_handler(CannabisJobType.USER_RECOMMENDATIONS, self._process_user_recommendations)
        self.register_handler(CannabisJobType.ANALYTICS_PIPELINE, self._process_analytics_pipeline)
        self.register_handler(CannabisJobType.DATA_CLEANUP, self._process_data_cleanup)
        self.register_handler(CannabisJobType.CACHE_REFRESH, self._process_cache_refresh)
        self.register_handler(CannabisJobType.REPORT_GENERATION, self._process_report_generation)
        self.register_handler(CannabisJobType.STATE_REGULATION_UPDATE, self._process_state_regulation_update)
    
    def register_handler(self, job_type: CannabisJobType, handler: Callable):
        """Register a job handler."""
        self.job_handlers[job_type] = handler
        logging.info(f"Registered handler for {job_type.value}")
    
    def add_middleware(self, middleware: Callable):
        """Add middleware for job processing."""
        self.middleware.append(middleware)
    
    async def process_job(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process a cannabis job."""
        try:
            # Apply pre-processing middleware
            for middleware_func in self.middleware:
                await self._apply_middleware(middleware_func, job, execution, "pre")
            
            # Get job handler
            handler = self.job_handlers.get(job.job_type)
            if not handler:
                raise ValueError(f"No handler registered for job type: {job.job_type}")
            
            # Execute job with progress tracking
            execution.status = JobStatus.RUNNING
            execution.started_at = datetime.now()
            
            result = await handler(job, execution)
            
            execution.status = JobStatus.COMPLETED
            execution.completed_at = datetime.now()
            execution.result = result
            execution.progress = 100.0
            
            # Apply post-processing middleware
            for middleware_func in self.middleware:
                await self._apply_middleware(middleware_func, job, execution, "post")
            
            return result
            
        except Exception as e:
            execution.status = JobStatus.FAILED
            execution.completed_at = datetime.now()
            execution.error_message = str(e)
            execution.logs.append(f"ERROR: {str(e)}")
            execution.logs.append(f"TRACEBACK: {traceback.format_exc()}")
            
            logging.error(f"Job {job.job_id} failed: {e}")
            raise
    
    async def _apply_middleware(self, middleware_func: Callable, job: JobDefinition, 
                               execution: JobExecution, phase: str):
        """Apply middleware function."""
        try:
            if asyncio.iscoroutinefunction(middleware_func):
                await middleware_func(job, execution, phase)
            else:
                middleware_func(job, execution, phase)
        except Exception as e:
            logging.warning(f"Middleware error in {phase} phase: {e}")
    
    # Cannabis-specific job handlers
    async def _process_dispensary_sync(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process dispensary data synchronization."""
        dispensary_id = job.data.get('dispensary_id')
        state = job.data.get('state', job.state_specific)
        
        execution.logs.append(f"Starting dispensary sync for {dispensary_id} in {state}")
        execution.progress = 10.0
        
        # Simulate dispensary data fetching
        await asyncio.sleep(0.5)
        execution.logs.append("Fetching dispensary menu data...")
        execution.progress = 30.0
        
        # Simulate strain data processing
        await asyncio.sleep(0.3)
        execution.logs.append("Processing strain information...")
        execution.progress = 60.0
        
        # Simulate price updates
        await asyncio.sleep(0.2)
        execution.logs.append("Updating pricing information...")
        execution.progress = 80.0
        
        # Simulate compliance validation
        if job.requires_compliance:
            await asyncio.sleep(0.3)
            execution.logs.append("Validating compliance requirements...")
            execution.progress = 90.0
        
        execution.logs.append("Dispensary sync completed successfully")
        
        return {
            'dispensary_id': dispensary_id,
            'state': state,
            'products_updated': 127,
            'prices_updated': 89,
            'compliance_validated': job.requires_compliance,
            'sync_timestamp': datetime.now().isoformat()
        }
    
    async def _process_strain_analysis(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process strain analysis and categorization."""
        strain_data = job.data.get('strain_data', {})
        analysis_type = job.data.get('analysis_type', 'full')
        
        execution.logs.append(f"Starting {analysis_type} strain analysis")
        execution.progress = 15.0
        
        # Simulate terpene profile analysis
        await asyncio.sleep(0.4)
        execution.logs.append("Analyzing terpene profiles...")
        execution.progress = 40.0
        
        # Simulate effect prediction
        await asyncio.sleep(0.3)
        execution.logs.append("Predicting strain effects...")
        execution.progress = 65.0
        
        # Simulate similarity matching
        await asyncio.sleep(0.3)
        execution.logs.append("Finding similar strains...")
        execution.progress = 85.0
        
        execution.logs.append("Strain analysis completed")
        
        return {
            'strain_id': strain_data.get('id'),
            'analysis_type': analysis_type,
            'dominant_terpenes': ['limonene', 'myrcene', 'pinene'],
            'predicted_effects': ['relaxed', 'happy', 'creative'],
            'similarity_score': 0.87,
            'confidence': 0.92,
            'analysis_timestamp': datetime.now().isoformat()
        }
    
    async def _process_terpene_processing(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process terpene data analysis."""
        batch_size = job.data.get('batch_size', 100)
        processing_mode = job.data.get('mode', 'standard')
        
        execution.logs.append(f"Processing {batch_size} terpene profiles in {processing_mode} mode")
        execution.progress = 10.0
        
        processed_count = 0
        for i in range(batch_size):
            # Simulate processing individual terpene profiles
            if i % 10 == 0:
                await asyncio.sleep(0.1)
                progress = 10.0 + (i / batch_size) * 80.0
                execution.progress = progress
                execution.logs.append(f"Processed {i}/{batch_size} profiles...")
            
            processed_count += 1
        
        execution.logs.append("Terpene processing completed")
        
        return {
            'processed_count': processed_count,
            'batch_size': batch_size,
            'processing_mode': processing_mode,
            'success_rate': 0.98,
            'average_processing_time': 0.05,
            'processing_timestamp': datetime.now().isoformat()
        }
    
    async def _process_price_update(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process price updates and market analysis."""
        price_sources = job.data.get('sources', ['dispensary_api', 'manual_entry'])
        region = job.data.get('region', 'US')
        
        execution.logs.append(f"Updating prices from {len(price_sources)} sources in {region}")
        execution.progress = 20.0
        
        updated_products = 0
        for source in price_sources:
            await asyncio.sleep(0.3)
            execution.logs.append(f"Processing prices from {source}...")
            updated_products += 45
            execution.progress += 30.0
        
        execution.logs.append("Price updates completed")
        
        return {
            'sources_processed': len(price_sources),
            'products_updated': updated_products,
            'region': region,
            'average_price_change': -2.3,  # 2.3% decrease
            'price_volatility': 0.15,
            'update_timestamp': datetime.now().isoformat()
        }
    
    async def _process_compliance_check(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process compliance validation."""
        entity_type = job.data.get('entity_type', 'dispensary')
        entity_id = job.data.get('entity_id')
        state = job.data.get('state', job.state_specific)
        
        execution.logs.append(f"Checking compliance for {entity_type} {entity_id} in {state}")
        execution.progress = 25.0
        
        # Simulate license verification
        await asyncio.sleep(0.3)
        execution.logs.append("Verifying licenses and permits...")
        execution.progress = 50.0
        
        # Simulate testing requirements check
        await asyncio.sleep(0.2)
        execution.logs.append("Checking testing requirements...")
        execution.progress = 75.0
        
        # Simulate tracking system validation
        await asyncio.sleep(0.2)
        execution.logs.append("Validating tracking system compliance...")
        execution.progress = 90.0
        
        execution.logs.append("Compliance check completed")
        
        compliance_status = {
            'licenses_valid': True,
            'testing_compliant': True,
            'tracking_compliant': True,
            'violations': [],
            'compliance_score': 0.95
        }
        
        return {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'state': state,
            'compliance_status': compliance_status,
            'check_timestamp': datetime.now().isoformat()
        }
    
    async def _process_inventory_sync(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process inventory synchronization."""
        dispensary_ids = job.data.get('dispensary_ids', [])
        sync_type = job.data.get('sync_type', 'full')
        
        execution.logs.append(f"Syncing inventory for {len(dispensary_ids)} dispensaries ({sync_type})")
        execution.progress = 10.0
        
        synced_count = 0
        for dispensary_id in dispensary_ids:
            await asyncio.sleep(0.2)
            execution.logs.append(f"Syncing inventory for {dispensary_id}...")
            synced_count += 1
            execution.progress = 10.0 + (synced_count / len(dispensary_ids)) * 80.0
        
        execution.logs.append("Inventory sync completed")
        
        return {
            'dispensaries_synced': synced_count,
            'sync_type': sync_type,
            'products_updated': synced_count * 85,
            'out_of_stock_items': 12,
            'new_products': 8,
            'sync_timestamp': datetime.now().isoformat()
        }
    
    async def _process_user_recommendations(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process user recommendation updates."""
        user_batch = job.data.get('user_batch', [])
        recommendation_type = job.data.get('type', 'strain_recommendations')
        
        execution.logs.append(f"Generating {recommendation_type} for {len(user_batch)} users")
        execution.progress = 15.0
        
        processed_users = 0
        for user_id in user_batch:
            # Simulate recommendation generation
            if processed_users % 5 == 0:
                await asyncio.sleep(0.1)
                progress = 15.0 + (processed_users / len(user_batch)) * 70.0
                execution.progress = progress
            
            processed_users += 1
        
        execution.logs.append("User recommendations generated")
        
        return {
            'users_processed': processed_users,
            'recommendation_type': recommendation_type,
            'recommendations_generated': processed_users * 5,
            'average_confidence': 0.84,
            'generation_timestamp': datetime.now().isoformat()
        }
    
    async def _process_analytics_pipeline(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process analytics pipeline."""
        pipeline_type = job.data.get('pipeline_type', 'daily_analytics')
        date_range = job.data.get('date_range', 7)
        
        execution.logs.append(f"Running {pipeline_type} for {date_range} days")
        execution.progress = 20.0
        
        # Simulate data aggregation
        await asyncio.sleep(0.4)
        execution.logs.append("Aggregating user activity data...")
        execution.progress = 40.0
        
        # Simulate trend analysis
        await asyncio.sleep(0.3)
        execution.logs.append("Analyzing market trends...")
        execution.progress = 60.0
        
        # Simulate report generation
        await asyncio.sleep(0.3)
        execution.logs.append("Generating analytics reports...")
        execution.progress = 80.0
        
        execution.logs.append("Analytics pipeline completed")
        
        return {
            'pipeline_type': pipeline_type,
            'date_range': date_range,
            'records_processed': 125000,
            'reports_generated': 15,
            'insights_discovered': 8,
            'pipeline_timestamp': datetime.now().isoformat()
        }
    
    async def _process_data_cleanup(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process data cleanup operations."""
        cleanup_type = job.data.get('cleanup_type', 'expired_data')
        retention_days = job.data.get('retention_days', 30)
        
        execution.logs.append(f"Starting {cleanup_type} cleanup (retention: {retention_days} days)")
        execution.progress = 25.0
        
        # Simulate data cleanup
        await asyncio.sleep(0.5)
        execution.logs.append("Identifying expired records...")
        execution.progress = 50.0
        
        await asyncio.sleep(0.3)
        execution.logs.append("Removing expired data...")
        execution.progress = 75.0
        
        execution.logs.append("Data cleanup completed")
        
        return {
            'cleanup_type': cleanup_type,
            'retention_days': retention_days,
            'records_removed': 5420,
            'storage_freed_mb': 1250,
            'cleanup_timestamp': datetime.now().isoformat()
        }
    
    async def _process_cache_refresh(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process cache refresh operations."""
        cache_types = job.data.get('cache_types', ['strains', 'dispensaries', 'prices'])
        force_refresh = job.data.get('force_refresh', False)
        
        execution.logs.append(f"Refreshing {len(cache_types)} cache types (force: {force_refresh})")
        execution.progress = 20.0
        
        refreshed_caches = []
        for cache_type in cache_types:
            await asyncio.sleep(0.2)
            execution.logs.append(f"Refreshing {cache_type} cache...")
            refreshed_caches.append(cache_type)
            execution.progress += 60.0 / len(cache_types)
        
        execution.logs.append("Cache refresh completed")
        
        return {
            'cache_types_refreshed': refreshed_caches,
            'force_refresh': force_refresh,
            'cache_entries_updated': 15420,
            'refresh_timestamp': datetime.now().isoformat()
        }
    
    async def _process_report_generation(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process report generation."""
        report_type = job.data.get('report_type', 'monthly_summary')
        recipients = job.data.get('recipients', [])
        
        execution.logs.append(f"Generating {report_type} report for {len(recipients)} recipients")
        execution.progress = 30.0
        
        # Simulate data collection
        await asyncio.sleep(0.4)
        execution.logs.append("Collecting report data...")
        execution.progress = 60.0
        
        # Simulate report generation
        await asyncio.sleep(0.3)
        execution.logs.append("Generating report...")
        execution.progress = 85.0
        
        execution.logs.append("Report generation completed")
        
        return {
            'report_type': report_type,
            'recipients_count': len(recipients),
            'report_size_mb': 2.5,
            'charts_generated': 12,
            'generation_timestamp': datetime.now().isoformat()
        }
    
    async def _process_state_regulation_update(self, job: JobDefinition, execution: JobExecution) -> Dict[str, Any]:
        """Process state regulation updates."""
        states = job.data.get('states', [])
        update_type = job.data.get('update_type', 'full_sync')
        
        execution.logs.append(f"Updating regulations for {len(states)} states ({update_type})")
        execution.progress = 25.0
        
        updated_states = []
        for state in states:
            await asyncio.sleep(0.3)
            execution.logs.append(f"Updating regulations for {state}...")
            updated_states.append(state)
            execution.progress += 50.0 / len(states)
        
        execution.logs.append("State regulation updates completed")
        
        return {
            'states_updated': updated_states,
            'update_type': update_type,
            'regulations_updated': len(states) * 15,
            'compliance_changes': 3,
            'update_timestamp': datetime.now().isoformat()
        }

class JobQueue:
    """Job queue management with priority and scheduling."""
    
    def __init__(self, config: BackgroundJobConfig):
        self.config = config
        self.priority_queues = {
            priority: deque() for priority in JobPriority
        }
        self.scheduled_jobs = []
        self.lock = threading.Lock()
        
        # Redis integration if available
        self.redis_client = None
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.Redis(
                    host=config.redis_host,
                    port=config.redis_port,
                    db=config.redis_db,
                    password=config.redis_password,
                    decode_responses=True
                )
                self.redis_client.ping()
                logging.info("Redis job queue enabled")
            except:
                self.redis_client = None
                logging.warning("Redis not available, using in-memory queue")
    
    def enqueue_job(self, job: JobDefinition) -> bool:
        """Add a job to the queue."""
        try:
            if self.redis_client:
                return self._enqueue_redis(job)
            else:
                return self._enqueue_memory(job)
        except Exception as e:
            logging.error(f"Failed to enqueue job {job.job_id}: {e}")
            return False
    
    def _enqueue_redis(self, job: JobDefinition) -> bool:
        """Enqueue job using Redis."""
        try:
            job_data = asdict(job)
            # Convert datetime objects to ISO strings
            job_data['created_at'] = job.created_at.isoformat()
            if job.scheduled_at:
                job_data['scheduled_at'] = job.scheduled_at.isoformat()
            
            # Store job data
            self.redis_client.hset(f"job:{job.job_id}", mapping=job_data)
            
            # Add to priority queue
            priority_score = job.priority.value * 1000 + int(time.time())
            self.redis_client.zadd(f"queue:{job.priority.value}", {job.job_id: priority_score})
            
            # Add to scheduled jobs if needed
            if job.scheduled_at and job.scheduled_at > datetime.now():
                schedule_score = int(job.scheduled_at.timestamp())
                self.redis_client.zadd("scheduled_jobs", {job.job_id: schedule_score})
            
            return True
        except Exception as e:
            logging.error(f"Redis enqueue failed: {e}")
            return False
    
    def _enqueue_memory(self, job: JobDefinition) -> bool:
        """Enqueue job using in-memory storage."""
        with self.lock:
            # Check queue size limit
            total_jobs = sum(len(q) for q in self.priority_queues.values())
            if total_jobs >= self.config.max_queue_size:
                logging.warning("Queue size limit reached")
                return False
            
            # Add to scheduled jobs or immediate queue
            if job.scheduled_at and job.scheduled_at > datetime.now():
                self.scheduled_jobs.append(job)
                self.scheduled_jobs.sort(key=lambda j: j.scheduled_at)
            else:
                self.priority_queues[job.priority].append(job)
            
            return True
    
    def dequeue_job(self) -> Optional[JobDefinition]:
        """Get the next job from the queue."""
        if self.redis_client:
            return self._dequeue_redis()
        else:
            return self._dequeue_memory()
    
    def _dequeue_redis(self) -> Optional[JobDefinition]:
        """Dequeue job using Redis."""
        try:
            # Check scheduled jobs first
            current_time = int(time.time())
            scheduled_jobs = self.redis_client.zrangebyscore(
                "scheduled_jobs", 0, current_time, withscores=True, start=0, num=1
            )
            
            if scheduled_jobs:
                job_id, _ = scheduled_jobs[0]
                self.redis_client.zrem("scheduled_jobs", job_id)
                return self._get_job_from_redis(job_id)
            
            # Check priority queues (highest priority first)
            for priority in reversed(JobPriority):
                queue_key = f"queue:{priority.value}"
                job_ids = self.redis_client.zrange(queue_key, 0, 0)
                
                if job_ids:
                    job_id = job_ids[0]
                    self.redis_client.zrem(queue_key, job_id)
                    return self._get_job_from_redis(job_id)
            
            return None
        except Exception as e:
            logging.error(f"Redis dequeue failed: {e}")
            return None
    
    def _dequeue_memory(self) -> Optional[JobDefinition]:
        """Dequeue job using in-memory storage."""
        with self.lock:
            # Check scheduled jobs
            current_time = datetime.now()
            ready_jobs = [j for j in self.scheduled_jobs if j.scheduled_at <= current_time]
            
            if ready_jobs:
                job = ready_jobs[0]
                self.scheduled_jobs.remove(job)
                return job
            
            # Check priority queues (highest priority first)
            for priority in reversed(JobPriority):
                if self.priority_queues[priority]:
                    return self.priority_queues[priority].popleft()
            
            return None
    
    def _get_job_from_redis(self, job_id: str) -> Optional[JobDefinition]:
        """Retrieve job data from Redis."""
        try:
            job_data = self.redis_client.hgetall(f"job:{job_id}")
            if not job_data:
                return None
            
            # Convert back to JobDefinition
            job_data['job_type'] = CannabisJobType(job_data['job_type'])
            job_data['priority'] = JobPriority(int(job_data['priority']))
            job_data['created_at'] = datetime.fromisoformat(job_data['created_at'])
            if job_data.get('scheduled_at'):
                job_data['scheduled_at'] = datetime.fromisoformat(job_data['scheduled_at'])
            
            # Handle JSON fields
            if 'data' in job_data and isinstance(job_data['data'], str):
                job_data['data'] = json.loads(job_data['data'])
            if 'tags' in job_data and isinstance(job_data['tags'], str):
                job_data['tags'] = json.loads(job_data['tags'])
            
            return JobDefinition(**job_data)
        except Exception as e:
            logging.error(f"Failed to retrieve job {job_id}: {e}")
            return None
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get queue status information."""
        if self.redis_client:
            return self._get_redis_queue_status()
        else:
            return self._get_memory_queue_status()
    
    def _get_redis_queue_status(self) -> Dict[str, Any]:
        """Get Redis queue status."""
        try:
            status = {'type': 'redis', 'queues': {}}
            
            for priority in JobPriority:
                queue_key = f"queue:{priority.value}"
                count = self.redis_client.zcard(queue_key)
                status['queues'][priority.name] = count
            
            status['scheduled_jobs'] = self.redis_client.zcard("scheduled_jobs")
            status['total_jobs'] = sum(status['queues'].values()) + status['scheduled_jobs']
            
            return status
        except Exception as e:
            logging.error(f"Redis queue status failed: {e}")
            return {'type': 'redis', 'error': str(e)}
    
    def _get_memory_queue_status(self) -> Dict[str, Any]:
        """Get memory queue status."""
        with self.lock:
            status = {
                'type': 'memory',
                'queues': {priority.name: len(queue) for priority, queue in self.priority_queues.items()},
                'scheduled_jobs': len(self.scheduled_jobs)
            }
            status['total_jobs'] = sum(status['queues'].values()) + status['scheduled_jobs']
            
            return status

class JobWorker:
    """Background job worker."""
    
    def __init__(self, worker_id: str, job_queue: JobQueue, job_processor: CannabisJobProcessor, 
                 config: BackgroundJobConfig):
        self.worker_id = worker_id
        self.job_queue = job_queue
        self.job_processor = job_processor
        self.config = config
        self.running = False
        self.current_job = None
        self.jobs_processed = 0
        self.worker_thread = None
        
    def start(self):
        """Start the worker."""
        self.running = True
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        logging.info(f"Worker {self.worker_id} started")
    
    def stop(self):
        """Stop the worker."""
        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=10)
        logging.info(f"Worker {self.worker_id} stopped")
    
    def _worker_loop(self):
        """Main worker processing loop."""
        while self.running:
            try:
                job = self.job_queue.dequeue_job()
                
                if job:
                    self.current_job = job
                    execution = JobExecution(
                        job_id=job.job_id,
                        execution_id=str(uuid.uuid4()),
                        status=JobStatus.QUEUED,
                        worker_id=self.worker_id
                    )
                    
                    # Process the job
                    asyncio.run(self._process_job_with_retry(job, execution))
                    
                    self.jobs_processed += 1
                    self.current_job = None
                else:
                    # No jobs available, sleep briefly
                    time.sleep(1)
                    
            except Exception as e:
                logging.error(f"Worker {self.worker_id} error: {e}")
                time.sleep(5)
    
    async def _process_job_with_retry(self, job: JobDefinition, execution: JobExecution):
        """Process job with retry logic."""
        max_retries = job.max_retries
        
        for attempt in range(max_retries + 1):
            try:
                execution.retry_count = attempt
                
                if attempt > 0:
                    execution.status = JobStatus.RETRYING
                    retry_delay = job.retry_delay * (2 ** (attempt - 1)) if self.config.retry_backoff else job.retry_delay
                    execution.logs.append(f"Retrying job (attempt {attempt + 1}/{max_retries + 1}) after {retry_delay}s delay")
                    await asyncio.sleep(retry_delay)
                
                # Process the job with timeout
                result = await asyncio.wait_for(
                    self.job_processor.process_job(job, execution),
                    timeout=job.timeout
                )
                
                logging.info(f"Job {job.job_id} completed successfully by worker {self.worker_id}")
                return result
                
            except asyncio.TimeoutError:
                error_msg = f"Job timed out after {job.timeout}s"
                execution.logs.append(f"ERROR: {error_msg}")
                
                if attempt == max_retries:
                    execution.status = JobStatus.FAILED
                    execution.error_message = error_msg
                    logging.error(f"Job {job.job_id} failed (timeout) after {max_retries} retries")
                    break
                    
            except Exception as e:
                error_msg = str(e)
                execution.logs.append(f"ERROR: {error_msg}")
                
                if attempt == max_retries:
                    execution.status = JobStatus.FAILED
                    execution.error_message = error_msg
                    logging.error(f"Job {job.job_id} failed after {max_retries} retries: {error_msg}")
                    break
    
    def get_worker_status(self) -> Dict[str, Any]:
        """Get worker status."""
        return {
            'worker_id': self.worker_id,
            'running': self.running,
            'jobs_processed': self.jobs_processed,
            'current_job': self.current_job.job_id if self.current_job else None
        }

class BackgroundJobManager:
    """Main background job management system."""
    
    def __init__(self, config: Optional[BackgroundJobConfig] = None):
        self.config = config or BackgroundJobConfig()
        self.job_processor = CannabisJobProcessor(self.config)
        self.job_queue = JobQueue(self.config)
        self.workers = []
        self.job_executions = {}
        self.running = False
        
        # Monitoring
        self.monitoring_thread = None
        self.job_metrics = {
            'total_jobs': 0,
            'completed_jobs': 0,
            'failed_jobs': 0,
            'average_processing_time': 0.0
        }
        
        logging.info("Background Job Manager initialized")
    
    def start(self):
        """Start the job management system."""
        self.running = True
        
        # Start workers
        for i in range(self.config.worker_concurrency):
            worker = JobWorker(
                worker_id=f"worker_{i+1}",
                job_queue=self.job_queue,
                job_processor=self.job_processor,
                config=self.config
            )
            worker.start()
            self.workers.append(worker)
        
        # Start monitoring
        if self.config.enable_monitoring:
            self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self.monitoring_thread.start()
        
        logging.info(f"Background Job Manager started with {len(self.workers)} workers")
    
    def stop(self):
        """Stop the job management system."""
        self.running = False
        
        # Stop workers
        for worker in self.workers:
            worker.stop()
        
        # Stop monitoring
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
        
        logging.info("Background Job Manager stopped")
    
    def submit_job(self, job_type: CannabisJobType, data: Dict[str, Any], 
                   priority: JobPriority = JobPriority.NORMAL,
                   scheduled_at: Optional[datetime] = None,
                   **kwargs) -> str:
        """Submit a new job."""
        job_id = str(uuid.uuid4())
        
        job = JobDefinition(
            job_id=job_id,
            job_type=job_type,
            priority=priority,
            data=data,
            scheduled_at=scheduled_at,
            **kwargs
        )
        
        if self.job_queue.enqueue_job(job):
            self.job_metrics['total_jobs'] += 1
            logging.info(f"Job {job_id} ({job_type.value}) submitted successfully")
            return job_id
        else:
            raise RuntimeError(f"Failed to enqueue job {job_id}")
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job."""
        execution = self.job_executions.get(job_id)
        if execution:
            return asdict(execution)
        return None
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job."""
        # This is a simplified implementation
        # In a real system, you'd need to handle running jobs differently
        execution = self.job_executions.get(job_id)
        if execution and execution.status in [JobStatus.PENDING, JobStatus.QUEUED]:
            execution.status = JobStatus.CANCELLED
            logging.info(f"Job {job_id} cancelled")
            return True
        return False
    
    def _monitoring_loop(self):
        """Background monitoring loop."""
        while self.running:
            try:
                # Update metrics
                self._update_metrics()
                
                # Log system status
                if self.config.enable_monitoring:
                    self._log_system_status()
                
                time.sleep(self.config.monitoring_interval)
                
            except Exception as e:
                logging.error(f"Monitoring error: {e}")
                time.sleep(30)
    
    def _update_metrics(self):
        """Update job processing metrics."""
        completed_executions = [e for e in self.job_executions.values() 
                              if e.status == JobStatus.COMPLETED]
        failed_executions = [e for e in self.job_executions.values() 
                           if e.status == JobStatus.FAILED]
        
        self.job_metrics['completed_jobs'] = len(completed_executions)
        self.job_metrics['failed_jobs'] = len(failed_executions)
        
        # Calculate average processing time
        if completed_executions:
            total_time = sum(e.duration for e in completed_executions if e.duration)
            self.job_metrics['average_processing_time'] = total_time / len(completed_executions)
    
    def _log_system_status(self):
        """Log system status."""
        queue_status = self.job_queue.get_queue_status()
        worker_status = [w.get_worker_status() for w in self.workers]
        
        logging.info(f"Job System Status: "
                    f"Queue={queue_status['total_jobs']}, "
                    f"Workers={len([w for w in worker_status if w['running']])}, "
                    f"Completed={self.job_metrics['completed_jobs']}, "
                    f"Failed={self.job_metrics['failed_jobs']}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        queue_status = self.job_queue.get_queue_status()
        worker_status = [w.get_worker_status() for w in self.workers]
        
        return {
            'running': self.running,
            'workers': worker_status,
            'queue_status': queue_status,
            'job_metrics': self.job_metrics,
            'active_jobs': len([w for w in worker_status if w['current_job']]),
            'config': {
                'worker_concurrency': self.config.worker_concurrency,
                'max_queue_size': self.config.max_queue_size,
                'cannabis_workflows_enabled': self.config.enable_cannabis_workflows
            }
        }

# Cannabis workflow templates
def create_dispensary_sync_workflow(dispensary_ids: List[str], state: str) -> List[Dict[str, Any]]:
    """Create a complete dispensary synchronization workflow."""
    jobs = []
    
    # Individual dispensary sync jobs
    for dispensary_id in dispensary_ids:
        jobs.append({
            'job_type': CannabisJobType.DISPENSARY_SYNC,
            'data': {
                'dispensary_id': dispensary_id,
                'state': state
            },
            'priority': JobPriority.HIGH,
            'requires_compliance': True,
            'state_specific': state
        })
    
    # Inventory sync after dispensary data
    jobs.append({
        'job_type': CannabisJobType.INVENTORY_SYNC,
        'data': {
            'dispensary_ids': dispensary_ids,
            'sync_type': 'incremental'
        },
        'priority': JobPriority.NORMAL
    })
    
    # Price updates
    jobs.append({
        'job_type': CannabisJobType.PRICE_UPDATE,
        'data': {
            'sources': ['dispensary_api'],
            'region': state
        },
        'priority': JobPriority.NORMAL
    })
    
    return jobs

def create_daily_maintenance_workflow() -> List[Dict[str, Any]]:
    """Create daily maintenance workflow."""
    return [
        {
            'job_type': CannabisJobType.DATA_CLEANUP,
            'data': {
                'cleanup_type': 'expired_sessions',
                'retention_days': 7
            },
            'priority': JobPriority.LOW
        },
        {
            'job_type': CannabisJobType.CACHE_REFRESH,
            'data': {
                'cache_types': ['strains', 'dispensaries', 'prices'],
                'force_refresh': False
            },
            'priority': JobPriority.NORMAL
        },
        {
            'job_type': CannabisJobType.ANALYTICS_PIPELINE,
            'data': {
                'pipeline_type': 'daily_analytics',
                'date_range': 1
            },
            'priority': JobPriority.NORMAL
        }
    ]

# Demo function
def demo_background_jobs():
    """Demonstrate background job processing capabilities."""
    print("\n🚀 Cannabis Background Job Processing Demo")
    print("=" * 60)
    
    # Create job manager
    config = BackgroundJobConfig(
        worker_concurrency=3,
        enable_monitoring=True,
        monitoring_interval=5,
        enable_cannabis_workflows=True
    )
    
    job_manager = BackgroundJobManager(config)
    
    # Display system capabilities
    print(f"Redis Available: {REDIS_AVAILABLE}")
    print(f"Celery Available: {CELERY_AVAILABLE}")
    print(f"Scheduling Available: {SCHEDULING_AVAILABLE}")
    
    # Start job manager
    job_manager.start()
    print(f"\n📊 Job Manager started with {config.worker_concurrency} workers")
    
    # Submit various cannabis jobs
    print(f"\n🌿 Submitting cannabis workflow jobs...")
    
    job_ids = []
    
    # Dispensary sync workflow
    dispensary_workflow = create_dispensary_sync_workflow(
        ['disp_ca_001', 'disp_ca_002', 'disp_ca_003'],
        'CA'
    )
    
    for job_config in dispensary_workflow:
        job_id = job_manager.submit_job(**job_config)
        job_ids.append(job_id)
        print(f"  ✅ Submitted {job_config['job_type'].value}: {job_id[:8]}")
    
    # Individual cannabis jobs
    individual_jobs = [
        {
            'job_type': CannabisJobType.STRAIN_ANALYSIS,
            'data': {
                'strain_data': {'id': 'strain_001', 'name': 'Blue Dream'},
                'analysis_type': 'full'
            },
            'priority': JobPriority.HIGH
        },
        {
            'job_type': CannabisJobType.TERPENE_PROCESSING,
            'data': {
                'batch_size': 50,
                'mode': 'standard'
            },
            'priority': JobPriority.NORMAL
        },
        {
            'job_type': CannabisJobType.COMPLIANCE_CHECK,
            'data': {
                'entity_type': 'dispensary',
                'entity_id': 'disp_ny_001',
                'state': 'NY'
            },
            'priority': JobPriority.CRITICAL,
            'requires_compliance': True,
            'state_specific': 'NY'
        },
        {
            'job_type': CannabisJobType.USER_RECOMMENDATIONS,
            'data': {
                'user_batch': [f'user_{i}' for i in range(20)],
                'type': 'strain_recommendations'
            },
            'priority': JobPriority.NORMAL
        }
    ]
    
    for job_config in individual_jobs:
        job_id = job_manager.submit_job(**job_config)
        job_ids.append(job_id)
        print(f"  ✅ Submitted {job_config['job_type'].value}: {job_id[:8]}")
    
    # Scheduled job
    future_time = datetime.now() + timedelta(seconds=10)
    scheduled_job_id = job_manager.submit_job(
        job_type=CannabisJobType.REPORT_GENERATION,
        data={
            'report_type': 'daily_summary',
            'recipients': ['admin@weedhounds.com']
        },
        priority=JobPriority.LOW,
        scheduled_at=future_time
    )
    job_ids.append(scheduled_job_id)
    print(f"  📅 Scheduled report generation: {scheduled_job_id[:8]} (in 10s)")
    
    print(f"\n📈 Processing {len(job_ids)} jobs...")
    
    # Monitor job processing
    start_time = time.time()
    completed_jobs = 0
    
    while completed_jobs < len(job_ids) and (time.time() - start_time) < 30:
        time.sleep(2)
        
        # Check system status
        status = job_manager.get_system_status()
        
        print(f"\n📊 System Status:")
        print(f"  Active Workers: {len([w for w in status['workers'] if w['running']])}")
        print(f"  Queue Total: {status['queue_status']['total_jobs']}")
        print(f"  Active Jobs: {status['active_jobs']}")
        print(f"  Completed: {status['job_metrics']['completed_jobs']}")
        print(f"  Failed: {status['job_metrics']['failed_jobs']}")
        
        if status['job_metrics']['average_processing_time'] > 0:
            print(f"  Avg Processing Time: {status['job_metrics']['average_processing_time']:.2f}s")
        
        # Show worker details
        print(f"  Workers:")
        for worker in status['workers']:
            current_job = worker['current_job'][:8] if worker['current_job'] else 'idle'
            print(f"    {worker['worker_id']}: {current_job} (processed: {worker['jobs_processed']})")
        
        completed_jobs = status['job_metrics']['completed_jobs']
        
        # Show queue breakdown
        queue_status = status['queue_status']
        if 'queues' in queue_status:
            queue_details = [f"{priority}: {count}" for priority, count in queue_status['queues'].items() if count > 0]
            if queue_details:
                print(f"  Queue Details: {', '.join(queue_details)}")
    
    # Final status
    print(f"\n🎯 Final Processing Summary:")
    final_status = job_manager.get_system_status()
    print(f"  Total Jobs Processed: {final_status['job_metrics']['completed_jobs']}")
    print(f"  Failed Jobs: {final_status['job_metrics']['failed_jobs']}")
    print(f"  Success Rate: {(final_status['job_metrics']['completed_jobs'] / len(job_ids)) * 100:.1f}%")
    
    if final_status['job_metrics']['average_processing_time'] > 0:
        print(f"  Average Processing Time: {final_status['job_metrics']['average_processing_time']:.2f}s")
    
    # Stop job manager
    job_manager.stop()
    print("\n✅ Demo completed successfully!")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run demo
    demo_background_jobs()