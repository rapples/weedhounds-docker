#!/usr/bin/env python3
"""
Real-Time Performance Monitoring Dashboard for Cannabis Data Platform
====================================================================

Comprehensive performance monitoring system with GPU utilization tracking,
cannabis-specific metrics, and distributed system health monitoring.
Provides real-time dashboards, alerting, and performance optimization insights.

Key Features:
- Real-time GPU and CPU utilization monitoring
- Cannabis-specific business metrics tracking
- Distributed system health monitoring
- Performance bottleneck detection and alerts
- API response time analytics
- Cache hit ratio and efficiency metrics
- Terpene analysis performance tracking
- Geographic distribution monitoring

Author: WeedHounds Monitoring Team
Created: November 2025
"""

import logging
import time
import threading
import asyncio
import json
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import deque, defaultdict
import statistics
import queue
from pathlib import Path
import pickle

try:
    import psutil
    SYSTEM_MONITORING_AVAILABLE = True
    print("✅ System monitoring available")
except ImportError:
    psutil = None
    SYSTEM_MONITORING_AVAILABLE = False
    print("⚠️ System monitoring not available")

try:
    import GPUtil
    GPU_MONITORING_AVAILABLE = True
    print("✅ GPU monitoring available")
except ImportError:
    GPUtil = None
    GPU_MONITORING_AVAILABLE = False
    print("⚠️ GPU monitoring not available")

try:
    import matplotlib.pyplot as plt
    import matplotlib.animation as animation
    from matplotlib.dates import DateFormatter
    import numpy as np
    PLOTTING_AVAILABLE = True
    print("✅ Plotting capabilities available")
except ImportError:
    plt = None
    animation = None
    DateFormatter = None
    np = None
    PLOTTING_AVAILABLE = False
    print("⚠️ Plotting not available")

try:
    import flask
    from flask import Flask, jsonify, render_template_string
    WEB_DASHBOARD_AVAILABLE = True
    print("✅ Web dashboard available")
except ImportError:
    flask = None
    Flask = None
    jsonify = None
    render_template_string = None
    WEB_DASHBOARD_AVAILABLE = False
    print("⚠️ Web dashboard not available")

@dataclass
class SystemMetrics:
    """System-level performance metrics."""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    disk_io_read: float
    disk_io_write: float
    network_bytes_sent: float
    network_bytes_recv: float
    gpu_utilization: List[float] = field(default_factory=list)
    gpu_memory: List[float] = field(default_factory=list)
    gpu_temperature: List[float] = field(default_factory=list)

@dataclass
class CannabisMetrics:
    """Cannabis-specific business metrics."""
    timestamp: datetime
    api_requests_per_minute: int
    terpene_analyses_completed: int
    strain_recommendations_served: int
    price_predictions_made: int
    dispensary_updates_processed: int
    cache_hit_ratio: float
    average_response_time: float
    active_user_sessions: int
    geographic_request_distribution: Dict[str, int] = field(default_factory=dict)
    strain_type_distribution: Dict[str, int] = field(default_factory=dict)

@dataclass
class PerformanceAlert:
    """Performance alert definition."""
    alert_id: str
    severity: str  # info, warning, critical
    metric_name: str
    threshold: float
    current_value: float
    message: str
    timestamp: datetime
    auto_resolve: bool = True

@dataclass
class MonitoringConfig:
    """Configuration for performance monitoring."""
    collection_interval: int = 10  # seconds
    retention_period: int = 86400  # 24 hours in seconds
    max_data_points: int = 8640  # 24 hours of 10-second intervals
    enable_gpu_monitoring: bool = True
    enable_web_dashboard: bool = True
    dashboard_port: int = 5000
    enable_alerts: bool = True
    alert_thresholds: Dict[str, float] = field(default_factory=lambda: {
        'cpu_percent': 85.0,
        'memory_percent': 90.0,
        'gpu_utilization': 95.0,
        'response_time': 5.0,
        'cache_hit_ratio': 0.7
    })
    cannabis_metrics_enabled: bool = True
    export_metrics: bool = True
    metrics_export_path: str = "metrics/"

class MetricsCollector:
    """Collects system and cannabis-specific performance metrics."""
    
    def __init__(self, config: MonitoringConfig):
        self.config = config
        self.system_metrics = deque(maxlen=config.max_data_points)
        self.cannabis_metrics = deque(maxlen=config.max_data_points)
        self.lock = threading.Lock()
        self.running = False
        self.collection_thread = None
        
        # Cannabis-specific counters
        self.api_request_counter = 0
        self.terpene_analysis_counter = 0
        self.strain_recommendation_counter = 0
        self.price_prediction_counter = 0
        self.dispensary_update_counter = 0
        self.response_times = deque(maxlen=1000)
        self.cache_hits = 0
        self.cache_misses = 0
        self.active_sessions = set()
        self.geographic_requests = defaultdict(int)
        self.strain_type_requests = defaultdict(int)
        
        # Last collection state for calculating deltas
        self.last_disk_io = None
        self.last_network_io = None
        self.last_collection_time = None
        
    def start_collection(self):
        """Start metrics collection."""
        self.running = True
        self.collection_thread = threading.Thread(target=self._collection_loop, daemon=True)
        self.collection_thread.start()
        logging.info("Metrics collection started")
    
    def stop_collection(self):
        """Stop metrics collection."""
        self.running = False
        if self.collection_thread:
            self.collection_thread.join(timeout=5)
        logging.info("Metrics collection stopped")
    
    def _collection_loop(self):
        """Main metrics collection loop."""
        while self.running:
            try:
                # Collect system metrics
                system_data = self._collect_system_metrics()
                if system_data:
                    with self.lock:
                        self.system_metrics.append(system_data)
                
                # Collect cannabis metrics
                cannabis_data = self._collect_cannabis_metrics()
                if cannabis_data:
                    with self.lock:
                        self.cannabis_metrics.append(cannabis_data)
                
                # Export metrics if enabled
                if self.config.export_metrics:
                    self._export_metrics()
                
                time.sleep(self.config.collection_interval)
                
            except Exception as e:
                logging.error(f"Metrics collection error: {e}")
                time.sleep(5)
    
    def _collect_system_metrics(self) -> Optional[SystemMetrics]:
        """Collect system-level metrics."""
        try:
            current_time = datetime.now()
            
            # CPU and Memory
            cpu_percent = psutil.cpu_percent() if SYSTEM_MONITORING_AVAILABLE else 0.0
            memory_percent = psutil.virtual_memory().percent if SYSTEM_MONITORING_AVAILABLE else 0.0
            
            # Disk I/O
            disk_read_rate = 0.0
            disk_write_rate = 0.0
            
            if SYSTEM_MONITORING_AVAILABLE:
                disk_io = psutil.disk_io_counters()
                if disk_io and self.last_disk_io and self.last_collection_time:
                    time_delta = (current_time - self.last_collection_time).total_seconds()
                    if time_delta > 0:
                        disk_read_rate = (disk_io.read_bytes - self.last_disk_io.read_bytes) / time_delta
                        disk_write_rate = (disk_io.write_bytes - self.last_disk_io.write_bytes) / time_delta
                self.last_disk_io = disk_io
            
            # Network I/O
            network_sent_rate = 0.0
            network_recv_rate = 0.0
            
            if SYSTEM_MONITORING_AVAILABLE:
                network_io = psutil.net_io_counters()
                if network_io and self.last_network_io and self.last_collection_time:
                    time_delta = (current_time - self.last_collection_time).total_seconds()
                    if time_delta > 0:
                        network_sent_rate = (network_io.bytes_sent - self.last_network_io.bytes_sent) / time_delta
                        network_recv_rate = (network_io.bytes_recv - self.last_network_io.bytes_recv) / time_delta
                self.last_network_io = network_io
            
            # GPU Metrics
            gpu_utilization = []
            gpu_memory = []
            gpu_temperature = []
            
            if GPU_MONITORING_AVAILABLE and self.config.enable_gpu_monitoring:
                try:
                    gpus = GPUtil.getGPUs()
                    for gpu in gpus:
                        gpu_utilization.append(gpu.load * 100)
                        gpu_memory.append(gpu.memoryUtil * 100)
                        gpu_temperature.append(gpu.temperature)
                except Exception as e:
                    logging.warning(f"GPU metrics collection failed: {e}")
            
            self.last_collection_time = current_time
            
            return SystemMetrics(
                timestamp=current_time,
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                disk_io_read=disk_read_rate,
                disk_io_write=disk_write_rate,
                network_bytes_sent=network_sent_rate,
                network_bytes_recv=network_recv_rate,
                gpu_utilization=gpu_utilization,
                gpu_memory=gpu_memory,
                gpu_temperature=gpu_temperature
            )
            
        except Exception as e:
            logging.error(f"System metrics collection failed: {e}")
            return None
    
    def _collect_cannabis_metrics(self) -> Optional[CannabisMetrics]:
        """Collect cannabis-specific business metrics."""
        try:
            current_time = datetime.now()
            
            # Calculate rates based on collection interval
            time_factor = 60.0 / self.config.collection_interval  # Convert to per-minute rates
            
            # Reset counters and calculate rates
            api_requests_per_minute = int(self.api_request_counter * time_factor)
            terpene_analyses = int(self.terpene_analysis_counter * time_factor)
            strain_recommendations = int(self.strain_recommendation_counter * time_factor)
            price_predictions = int(self.price_prediction_counter * time_factor)
            dispensary_updates = int(self.dispensary_update_counter * time_factor)
            
            # Reset counters
            self.api_request_counter = 0
            self.terpene_analysis_counter = 0
            self.strain_recommendation_counter = 0
            self.price_prediction_counter = 0
            self.dispensary_update_counter = 0
            
            # Calculate cache hit ratio
            total_cache_requests = self.cache_hits + self.cache_misses
            cache_hit_ratio = self.cache_hits / total_cache_requests if total_cache_requests > 0 else 1.0
            
            # Reset cache counters
            self.cache_hits = 0
            self.cache_misses = 0
            
            # Calculate average response time
            avg_response_time = statistics.mean(self.response_times) if self.response_times else 0.0
            
            # Clean up old response times
            self.response_times.clear()
            
            # Geographic and strain type distributions
            geo_distribution = dict(self.geographic_requests)
            strain_distribution = dict(self.strain_type_requests)
            
            # Reset distribution counters
            self.geographic_requests.clear()
            self.strain_type_requests.clear()
            
            return CannabisMetrics(
                timestamp=current_time,
                api_requests_per_minute=api_requests_per_minute,
                terpene_analyses_completed=terpene_analyses,
                strain_recommendations_served=strain_recommendations,
                price_predictions_made=price_predictions,
                dispensary_updates_processed=dispensary_updates,
                cache_hit_ratio=cache_hit_ratio,
                average_response_time=avg_response_time,
                active_user_sessions=len(self.active_sessions),
                geographic_request_distribution=geo_distribution,
                strain_type_distribution=strain_distribution
            )
            
        except Exception as e:
            logging.error(f"Cannabis metrics collection failed: {e}")
            return None
    
    def _export_metrics(self):
        """Export metrics to files for external analysis."""
        try:
            export_dir = Path(self.config.metrics_export_path)
            export_dir.mkdir(exist_ok=True)
            
            # Export system metrics
            system_file = export_dir / f"system_metrics_{datetime.now().strftime('%Y%m%d')}.json"
            with open(system_file, 'w') as f:
                system_data = [asdict(metric) for metric in list(self.system_metrics)]
                # Convert datetime objects to ISO strings
                for item in system_data:
                    item['timestamp'] = item['timestamp'].isoformat()
                json.dump(system_data, f, indent=2)
            
            # Export cannabis metrics
            cannabis_file = export_dir / f"cannabis_metrics_{datetime.now().strftime('%Y%m%d')}.json"
            with open(cannabis_file, 'w') as f:
                cannabis_data = [asdict(metric) for metric in list(self.cannabis_metrics)]
                # Convert datetime objects to ISO strings
                for item in cannabis_data:
                    item['timestamp'] = item['timestamp'].isoformat()
                json.dump(cannabis_data, f, indent=2)
            
        except Exception as e:
            logging.warning(f"Metrics export failed: {e}")
    
    # Public methods for incrementing cannabis metrics
    def record_api_request(self, endpoint: str = None, state: str = None):
        """Record an API request."""
        self.api_request_counter += 1
        if state:
            self.geographic_requests[state] += 1
    
    def record_terpene_analysis(self):
        """Record a terpene analysis completion."""
        self.terpene_analysis_counter += 1
    
    def record_strain_recommendation(self, strain_type: str = None):
        """Record a strain recommendation."""
        self.strain_recommendation_counter += 1
        if strain_type:
            self.strain_type_requests[strain_type] += 1
    
    def record_price_prediction(self):
        """Record a price prediction."""
        self.price_prediction_counter += 1
    
    def record_dispensary_update(self):
        """Record a dispensary update."""
        self.dispensary_update_counter += 1
    
    def record_response_time(self, response_time: float):
        """Record an API response time."""
        self.response_times.append(response_time)
    
    def record_cache_hit(self):
        """Record a cache hit."""
        self.cache_hits += 1
    
    def record_cache_miss(self):
        """Record a cache miss."""
        self.cache_misses += 1
    
    def add_user_session(self, session_id: str):
        """Add an active user session."""
        self.active_sessions.add(session_id)
    
    def remove_user_session(self, session_id: str):
        """Remove an active user session."""
        self.active_sessions.discard(session_id)
    
    def get_latest_metrics(self) -> Tuple[Optional[SystemMetrics], Optional[CannabisMetrics]]:
        """Get the most recent metrics."""
        with self.lock:
            latest_system = self.system_metrics[-1] if self.system_metrics else None
            latest_cannabis = self.cannabis_metrics[-1] if self.cannabis_metrics else None
            return latest_system, latest_cannabis
    
    def get_metrics_history(self, duration_minutes: int = 60) -> Tuple[List[SystemMetrics], List[CannabisMetrics]]:
        """Get metrics history for the specified duration."""
        cutoff_time = datetime.now() - timedelta(minutes=duration_minutes)
        
        with self.lock:
            recent_system = [m for m in self.system_metrics if m.timestamp >= cutoff_time]
            recent_cannabis = [m for m in self.cannabis_metrics if m.timestamp >= cutoff_time]
            
            return recent_system, recent_cannabis

class AlertManager:
    """Manages performance alerts and notifications."""
    
    def __init__(self, config: MonitoringConfig):
        self.config = config
        self.active_alerts = {}
        self.alert_history = deque(maxlen=1000)
        self.alert_handlers = []
        self.lock = threading.Lock()
        
    def add_alert_handler(self, handler: Callable[[PerformanceAlert], None]):
        """Add an alert handler function."""
        self.alert_handlers.append(handler)
    
    def check_thresholds(self, system_metrics: SystemMetrics, cannabis_metrics: CannabisMetrics):
        """Check metrics against alert thresholds."""
        if not self.config.enable_alerts:
            return
        
        alerts_to_trigger = []
        alerts_to_resolve = []
        
        # System metric alerts
        if system_metrics.cpu_percent > self.config.alert_thresholds.get('cpu_percent', 85.0):
            alerts_to_trigger.append(self._create_alert(
                'high_cpu_usage',
                'warning',
                'CPU Usage',
                self.config.alert_thresholds['cpu_percent'],
                system_metrics.cpu_percent,
                f"High CPU usage detected: {system_metrics.cpu_percent:.1f}%"
            ))
        else:
            alerts_to_resolve.append('high_cpu_usage')
        
        if system_metrics.memory_percent > self.config.alert_thresholds.get('memory_percent', 90.0):
            alerts_to_trigger.append(self._create_alert(
                'high_memory_usage',
                'warning',
                'Memory Usage',
                self.config.alert_thresholds['memory_percent'],
                system_metrics.memory_percent,
                f"High memory usage detected: {system_metrics.memory_percent:.1f}%"
            ))
        else:
            alerts_to_resolve.append('high_memory_usage')
        
        # GPU alerts
        if system_metrics.gpu_utilization:
            max_gpu_util = max(system_metrics.gpu_utilization)
            if max_gpu_util > self.config.alert_thresholds.get('gpu_utilization', 95.0):
                alerts_to_trigger.append(self._create_alert(
                    'high_gpu_usage',
                    'warning',
                    'GPU Utilization',
                    self.config.alert_thresholds['gpu_utilization'],
                    max_gpu_util,
                    f"High GPU utilization detected: {max_gpu_util:.1f}%"
                ))
            else:
                alerts_to_resolve.append('high_gpu_usage')
        
        # Cannabis-specific alerts
        if cannabis_metrics.average_response_time > self.config.alert_thresholds.get('response_time', 5.0):
            alerts_to_trigger.append(self._create_alert(
                'slow_response_time',
                'critical',
                'Response Time',
                self.config.alert_thresholds['response_time'],
                cannabis_metrics.average_response_time,
                f"Slow response time detected: {cannabis_metrics.average_response_time:.2f}s"
            ))
        else:
            alerts_to_resolve.append('slow_response_time')
        
        if cannabis_metrics.cache_hit_ratio < self.config.alert_thresholds.get('cache_hit_ratio', 0.7):
            alerts_to_trigger.append(self._create_alert(
                'low_cache_hit_ratio',
                'warning',
                'Cache Hit Ratio',
                self.config.alert_thresholds['cache_hit_ratio'],
                cannabis_metrics.cache_hit_ratio,
                f"Low cache hit ratio: {cannabis_metrics.cache_hit_ratio:.2%}"
            ))
        else:
            alerts_to_resolve.append('low_cache_hit_ratio')
        
        # Process alerts
        with self.lock:
            # Trigger new alerts
            for alert in alerts_to_trigger:
                if alert.alert_id not in self.active_alerts:
                    self.active_alerts[alert.alert_id] = alert
                    self.alert_history.append(alert)
                    self._notify_handlers(alert)
            
            # Resolve alerts
            for alert_id in alerts_to_resolve:
                if alert_id in self.active_alerts:
                    resolved_alert = self.active_alerts[alert_id]
                    resolved_alert.message += " (RESOLVED)"
                    del self.active_alerts[alert_id]
                    self.alert_history.append(resolved_alert)
                    self._notify_handlers(resolved_alert)
    
    def _create_alert(self, alert_id: str, severity: str, metric_name: str, 
                     threshold: float, current_value: float, message: str) -> PerformanceAlert:
        """Create a performance alert."""
        return PerformanceAlert(
            alert_id=alert_id,
            severity=severity,
            metric_name=metric_name,
            threshold=threshold,
            current_value=current_value,
            message=message,
            timestamp=datetime.now()
        )
    
    def _notify_handlers(self, alert: PerformanceAlert):
        """Notify all alert handlers."""
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                logging.error(f"Alert handler failed: {e}")
    
    def get_active_alerts(self) -> List[PerformanceAlert]:
        """Get currently active alerts."""
        with self.lock:
            return list(self.active_alerts.values())
    
    def get_alert_history(self, limit: int = 50) -> List[PerformanceAlert]:
        """Get recent alert history."""
        with self.lock:
            return list(self.alert_history)[-limit:]

class WebDashboard:
    """Web-based performance monitoring dashboard."""
    
    def __init__(self, metrics_collector: MetricsCollector, alert_manager: AlertManager, config: MonitoringConfig):
        self.metrics_collector = metrics_collector
        self.alert_manager = alert_manager
        self.config = config
        self.app = None
        self.dashboard_thread = None
        
        if WEB_DASHBOARD_AVAILABLE:
            self._create_flask_app()
    
    def _create_flask_app(self):
        """Create Flask web application."""
        self.app = Flask(__name__)
        
        @self.app.route('/')
        def dashboard():
            return render_template_string(DASHBOARD_HTML_TEMPLATE)
        
        @self.app.route('/api/metrics')
        def get_metrics():
            system_metrics, cannabis_metrics = self.metrics_collector.get_latest_metrics()
            
            response = {
                'timestamp': datetime.now().isoformat(),
                'system': asdict(system_metrics) if system_metrics else None,
                'cannabis': asdict(cannabis_metrics) if cannabis_metrics else None
            }
            
            # Convert datetime objects to ISO strings
            if response['system']:
                response['system']['timestamp'] = response['system']['timestamp'].isoformat()
            if response['cannabis']:
                response['cannabis']['timestamp'] = response['cannabis']['timestamp'].isoformat()
            
            return jsonify(response)
        
        @self.app.route('/api/alerts')
        def get_alerts():
            active_alerts = self.alert_manager.get_active_alerts()
            alert_history = self.alert_manager.get_alert_history()
            
            return jsonify({
                'active_alerts': [asdict(alert) for alert in active_alerts],
                'alert_history': [asdict(alert) for alert in alert_history]
            })
        
        @self.app.route('/api/history/<int:duration>')
        def get_metrics_history(duration):
            system_history, cannabis_history = self.metrics_collector.get_metrics_history(duration)
            
            response = {
                'system_history': [asdict(m) for m in system_history],
                'cannabis_history': [asdict(m) for m in cannabis_history]
            }
            
            # Convert datetime objects
            for item in response['system_history']:
                item['timestamp'] = item['timestamp'].isoformat()
            for item in response['cannabis_history']:
                item['timestamp'] = item['timestamp'].isoformat()
            
            return jsonify(response)
    
    def start_dashboard(self):
        """Start the web dashboard."""
        if not WEB_DASHBOARD_AVAILABLE or not self.app:
            logging.warning("Web dashboard not available")
            return
        
        def run_dashboard():
            try:
                self.app.run(
                    host='0.0.0.0',
                    port=self.config.dashboard_port,
                    debug=False,
                    use_reloader=False
                )
            except Exception as e:
                logging.error(f"Dashboard startup failed: {e}")
        
        self.dashboard_thread = threading.Thread(target=run_dashboard, daemon=True)
        self.dashboard_thread.start()
        
        logging.info(f"Web dashboard started on port {self.config.dashboard_port}")
    
    def stop_dashboard(self):
        """Stop the web dashboard."""
        # Flask doesn't have a clean shutdown method in dev server
        # In production, use a proper WSGI server
        logging.info("Web dashboard stopped")

class PerformanceMonitoringSystem:
    """Main performance monitoring system coordinator."""
    
    def __init__(self, config: Optional[MonitoringConfig] = None):
        self.config = config or MonitoringConfig()
        self.metrics_collector = MetricsCollector(self.config)
        self.alert_manager = AlertManager(self.config)
        self.web_dashboard = None
        self.monitoring_thread = None
        self.running = False
        
        # Setup alert handlers
        self.alert_manager.add_alert_handler(self._log_alert)
        
        # Setup web dashboard
        if self.config.enable_web_dashboard:
            self.web_dashboard = WebDashboard(
                self.metrics_collector,
                self.alert_manager,
                self.config
            )
        
        logging.info("Performance Monitoring System initialized")
    
    def start(self):
        """Start the performance monitoring system."""
        self.running = True
        
        # Start metrics collection
        self.metrics_collector.start_collection()
        
        # Start monitoring thread for alerts
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        
        # Start web dashboard
        if self.web_dashboard:
            self.web_dashboard.start_dashboard()
        
        logging.info("Performance Monitoring System started")
    
    def stop(self):
        """Stop the performance monitoring system."""
        self.running = False
        
        # Stop metrics collection
        self.metrics_collector.stop_collection()
        
        # Stop monitoring thread
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
        
        # Stop web dashboard
        if self.web_dashboard:
            self.web_dashboard.stop_dashboard()
        
        logging.info("Performance Monitoring System stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop for alert checking."""
        while self.running:
            try:
                system_metrics, cannabis_metrics = self.metrics_collector.get_latest_metrics()
                
                if system_metrics and cannabis_metrics:
                    self.alert_manager.check_thresholds(system_metrics, cannabis_metrics)
                
                time.sleep(self.config.collection_interval)
                
            except Exception as e:
                logging.error(f"Monitoring loop error: {e}")
                time.sleep(5)
    
    def _log_alert(self, alert: PerformanceAlert):
        """Log alert handler."""
        log_level = {
            'info': logging.INFO,
            'warning': logging.WARNING,
            'critical': logging.CRITICAL
        }.get(alert.severity, logging.INFO)
        
        logging.log(log_level, f"ALERT [{alert.severity.upper()}]: {alert.message}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        system_metrics, cannabis_metrics = self.metrics_collector.get_latest_metrics()
        active_alerts = self.alert_manager.get_active_alerts()
        
        status = {
            'monitoring_active': self.running,
            'collection_interval': self.config.collection_interval,
            'alerts_enabled': self.config.enable_alerts,
            'web_dashboard_enabled': self.config.enable_web_dashboard,
            'dashboard_url': f"http://localhost:{self.config.dashboard_port}" if self.config.enable_web_dashboard else None,
            'active_alerts_count': len(active_alerts),
            'system_health': 'healthy'
        }
        
        # Determine overall system health
        critical_alerts = [a for a in active_alerts if a.severity == 'critical']
        warning_alerts = [a for a in active_alerts if a.severity == 'warning']
        
        if critical_alerts:
            status['system_health'] = 'critical'
        elif warning_alerts:
            status['system_health'] = 'degraded'
        
        # Add latest metrics
        if system_metrics:
            status['latest_system_metrics'] = {
                'cpu_percent': system_metrics.cpu_percent,
                'memory_percent': system_metrics.memory_percent,
                'gpu_utilization': system_metrics.gpu_utilization,
                'timestamp': system_metrics.timestamp.isoformat()
            }
        
        if cannabis_metrics:
            status['latest_cannabis_metrics'] = {
                'api_requests_per_minute': cannabis_metrics.api_requests_per_minute,
                'cache_hit_ratio': cannabis_metrics.cache_hit_ratio,
                'average_response_time': cannabis_metrics.average_response_time,
                'active_user_sessions': cannabis_metrics.active_user_sessions,
                'timestamp': cannabis_metrics.timestamp.isoformat()
            }
        
        return status
    
    # Public methods for cannabis metric recording
    def record_api_request(self, endpoint: str = None, state: str = None):
        """Record an API request."""
        self.metrics_collector.record_api_request(endpoint, state)
    
    def record_terpene_analysis(self):
        """Record a terpene analysis completion."""
        self.metrics_collector.record_terpene_analysis()
    
    def record_strain_recommendation(self, strain_type: str = None):
        """Record a strain recommendation."""
        self.metrics_collector.record_strain_recommendation(strain_type)
    
    def record_price_prediction(self):
        """Record a price prediction."""
        self.metrics_collector.record_price_prediction()
    
    def record_response_time(self, response_time: float):
        """Record an API response time."""
        self.metrics_collector.record_response_time(response_time)
    
    def record_cache_hit(self):
        """Record a cache hit."""
        self.metrics_collector.record_cache_hit()
    
    def record_cache_miss(self):
        """Record a cache miss."""
        self.metrics_collector.record_cache_miss()

# HTML template for web dashboard
DASHBOARD_HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Cannabis Data Platform - Performance Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .dashboard { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .metric-card { border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
        .alert { padding: 10px; margin: 5px 0; border-radius: 3px; }
        .alert-warning { background-color: #fff3cd; border-color: #ffeaa7; }
        .alert-critical { background-color: #f8d7da; border-color: #f5c6cb; }
        .metric-value { font-size: 2em; font-weight: bold; color: #007bff; }
        .chart-container { width: 100%; height: 300px; }
    </style>
</head>
<body>
    <h1>🌿 Cannabis Data Platform - Performance Dashboard</h1>
    
    <div id="alerts-section">
        <h2>🚨 Active Alerts</h2>
        <div id="alerts-container"></div>
    </div>
    
    <div class="dashboard">
        <div class="metric-card">
            <h3>💻 System Metrics</h3>
            <div>CPU Usage: <span id="cpu-usage" class="metric-value">--</span>%</div>
            <div>Memory Usage: <span id="memory-usage" class="metric-value">--</span>%</div>
            <div>GPU Utilization: <span id="gpu-usage" class="metric-value">--</span>%</div>
        </div>
        
        <div class="metric-card">
            <h3>🌿 Cannabis Metrics</h3>
            <div>API Requests/min: <span id="api-requests" class="metric-value">--</span></div>
            <div>Cache Hit Ratio: <span id="cache-ratio" class="metric-value">--</span>%</div>
            <div>Response Time: <span id="response-time" class="metric-value">--</span>s</div>
            <div>Active Sessions: <span id="active-sessions" class="metric-value">--</span></div>
        </div>
        
        <div class="metric-card">
            <h3>📊 Performance Trends</h3>
            <div class="chart-container">
                <canvas id="performance-chart"></canvas>
            </div>
        </div>
        
        <div class="metric-card">
            <h3>🧪 Cannabis Analytics</h3>
            <div>Terpene Analyses: <span id="terpene-analyses" class="metric-value">--</span>/min</div>
            <div>Strain Recommendations: <span id="strain-recommendations" class="metric-value">--</span>/min</div>
            <div>Price Predictions: <span id="price-predictions" class="metric-value">--</span>/min</div>
        </div>
    </div>
    
    <script>
        // Initialize charts
        const ctx = document.getElementById('performance-chart').getContext('2d');
        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'CPU %',
                    data: [],
                    borderColor: 'rgb(75, 192, 192)',
                    tension: 0.1
                }, {
                    label: 'Memory %',
                    data: [],
                    borderColor: 'rgb(255, 99, 132)',
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: { beginAtZero: true, max: 100 }
                }
            }
        });
        
        // Update dashboard
        function updateDashboard() {
            fetch('/api/metrics')
                .then(response => response.json())
                .then(data => {
                    if (data.system) {
                        document.getElementById('cpu-usage').textContent = data.system.cpu_percent.toFixed(1);
                        document.getElementById('memory-usage').textContent = data.system.memory_percent.toFixed(1);
                        
                        const gpuUtil = data.system.gpu_utilization.length > 0 ? 
                            Math.max(...data.system.gpu_utilization).toFixed(1) : '0.0';
                        document.getElementById('gpu-usage').textContent = gpuUtil;
                    }
                    
                    if (data.cannabis) {
                        document.getElementById('api-requests').textContent = data.cannabis.api_requests_per_minute;
                        document.getElementById('cache-ratio').textContent = (data.cannabis.cache_hit_ratio * 100).toFixed(1);
                        document.getElementById('response-time').textContent = data.cannabis.average_response_time.toFixed(3);
                        document.getElementById('active-sessions').textContent = data.cannabis.active_user_sessions;
                        document.getElementById('terpene-analyses').textContent = data.cannabis.terpene_analyses_completed;
                        document.getElementById('strain-recommendations').textContent = data.cannabis.strain_recommendations_served;
                        document.getElementById('price-predictions').textContent = data.cannabis.price_predictions_made;
                    }
                })
                .catch(error => console.error('Error updating metrics:', error));
            
            // Update alerts
            fetch('/api/alerts')
                .then(response => response.json())
                .then(data => {
                    const alertsContainer = document.getElementById('alerts-container');
                    alertsContainer.innerHTML = '';
                    
                    if (data.active_alerts.length === 0) {
                        alertsContainer.innerHTML = '<div style="color: green;">✅ No active alerts</div>';
                    } else {
                        data.active_alerts.forEach(alert => {
                            const alertDiv = document.createElement('div');
                            alertDiv.className = `alert alert-${alert.severity}`;
                            alertDiv.textContent = alert.message;
                            alertsContainer.appendChild(alertDiv);
                        });
                    }
                })
                .catch(error => console.error('Error updating alerts:', error));
        }
        
        // Update every 10 seconds
        updateDashboard();
        setInterval(updateDashboard, 10000);
    </script>
</body>
</html>
"""

def create_monitoring_system(collection_interval: int = 10,
                           enable_web_dashboard: bool = True,
                           dashboard_port: int = 5000) -> PerformanceMonitoringSystem:
    """
    Factory function to create performance monitoring system.
    
    Args:
        collection_interval: Metrics collection interval in seconds
        enable_web_dashboard: Whether to enable web dashboard
        dashboard_port: Port for web dashboard
    
    Returns:
        Configured performance monitoring system
    """
    config = MonitoringConfig(
        collection_interval=collection_interval,
        enable_web_dashboard=enable_web_dashboard and WEB_DASHBOARD_AVAILABLE,
        dashboard_port=dashboard_port,
        enable_gpu_monitoring=GPU_MONITORING_AVAILABLE,
        cannabis_metrics_enabled=True
    )
    
    return PerformanceMonitoringSystem(config)

# Demo and Testing Functions
def demo_performance_monitoring():
    """Demonstrate performance monitoring capabilities."""
    print("\n🚀 Performance Monitoring Dashboard Demo")
    print("=" * 50)
    
    # Initialize monitoring system
    monitoring = create_monitoring_system(
        collection_interval=5,
        enable_web_dashboard=True,
        dashboard_port=5001
    )
    
    # Display system capabilities
    print(f"System Monitoring: {SYSTEM_MONITORING_AVAILABLE}")
    print(f"GPU Monitoring: {GPU_MONITORING_AVAILABLE}")
    print(f"Web Dashboard: {WEB_DASHBOARD_AVAILABLE}")
    
    # Start monitoring
    monitoring.start()
    
    # Simulate cannabis platform activity
    print(f"\n📊 Simulating cannabis platform activity...")
    
    # Simulate various activities
    import random
    states = ['CA', 'NY', 'CO', 'WA', 'OR']
    strain_types = ['indica', 'sativa', 'hybrid']
    
    for i in range(20):
        # Simulate API requests
        monitoring.record_api_request(f"/api/strains", random.choice(states))
        monitoring.record_api_request(f"/api/dispensaries", random.choice(states))
        
        # Simulate cannabis-specific operations
        if random.random() < 0.3:
            monitoring.record_terpene_analysis()
        
        if random.random() < 0.4:
            monitoring.record_strain_recommendation(random.choice(strain_types))
        
        if random.random() < 0.2:
            monitoring.record_price_prediction()
        
        # Simulate response times
        monitoring.record_response_time(random.uniform(0.1, 2.0))
        
        # Simulate cache operations
        if random.random() < 0.8:
            monitoring.record_cache_hit()
        else:
            monitoring.record_cache_miss()
        
        time.sleep(0.1)
    
    # Wait for metrics collection
    time.sleep(7)
    
    # Display system status
    print(f"\n📈 System Status:")
    status = monitoring.get_system_status()
    print(f"  Monitoring Active: {status['monitoring_active']}")
    print(f"  System Health: {status['system_health']}")
    print(f"  Active Alerts: {status['active_alerts_count']}")
    
    if status.get('dashboard_url'):
        print(f"  Dashboard URL: {status['dashboard_url']}")
    
    if status.get('latest_system_metrics'):
        sys_metrics = status['latest_system_metrics']
        print(f"  CPU Usage: {sys_metrics['cpu_percent']:.1f}%")
        print(f"  Memory Usage: {sys_metrics['memory_percent']:.1f}%")
    
    if status.get('latest_cannabis_metrics'):
        cannabis_metrics = status['latest_cannabis_metrics']
        print(f"  API Requests/min: {cannabis_metrics['api_requests_per_minute']}")
        print(f"  Cache Hit Ratio: {cannabis_metrics['cache_hit_ratio']:.2%}")
        print(f"  Avg Response Time: {cannabis_metrics['average_response_time']:.3f}s")
    
    print(f"\n⏱️  Monitoring for 10 seconds...")
    if status.get('dashboard_url'):
        print(f"🌐 Open {status['dashboard_url']} in your browser to view the dashboard")
    
    time.sleep(10)
    
    # Stop monitoring
    monitoring.stop()
    print("\n✅ Demo completed successfully!")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run demo
    demo_performance_monitoring()