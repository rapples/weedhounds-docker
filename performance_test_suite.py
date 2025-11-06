"""
Comprehensive Performance Testing Suite for WeedHounds Cannabis Data System
Load testing, stress testing, latency analysis, and scalability validation
for unlimited peer-to-peer cannabis data network
"""

import asyncio
import aiohttp
import time
import random
import statistics
import json
import csv
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt
import numpy as np
import psutil
import threading
import queue
import socket
import subprocess
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestType(Enum):
    """Performance test types"""
    LOAD_TEST = "load_test"
    STRESS_TEST = "stress_test"
    SPIKE_TEST = "spike_test"
    ENDURANCE_TEST = "endurance_test"
    LATENCY_TEST = "latency_test"
    THROUGHPUT_TEST = "throughput_test"
    SCALABILITY_TEST = "scalability_test"
    PEER_NETWORK_TEST = "peer_network_test"

class CannabisEndpoint(Enum):
    """Cannabis API endpoints for testing"""
    DISPENSARY_MENU = "/cannabis/menu"
    STRAIN_DATA = "/cannabis/strains"
    PRODUCT_PRICING = "/cannabis/pricing"
    TERPENE_PROFILES = "/cannabis/terpenes"
    LAB_RESULTS = "/cannabis/lab-results"
    ANALYTICS = "/cannabis/analytics"
    SEARCH = "/cannabis/search"
    CACHE_STATUS = "/system/cache-status"
    DHT_PEERS = "/system/dht-peers"
    HEALTH_CHECK = "/system/health"

@dataclass
class TestConfiguration:
    """Performance test configuration"""
    test_type: TestType
    duration_seconds: int
    concurrent_users: int
    requests_per_second: float
    ramp_up_time: int
    endpoints: List[CannabisEndpoint]
    payload_size: str  # "small", "medium", "large"
    cache_strategy: str  # "enabled", "disabled", "mixed"
    peer_count: int  # Number of DHT peers to simulate

@dataclass
class TestMetrics:
    """Performance test metrics"""
    test_id: str
    test_type: TestType
    start_time: datetime
    end_time: datetime
    total_requests: int
    successful_requests: int
    failed_requests: int
    avg_response_time: float
    min_response_time: float
    max_response_time: float
    p95_response_time: float
    p99_response_time: float
    requests_per_second: float
    bytes_transferred: int
    error_rate: float
    cpu_usage: List[float]
    memory_usage: List[float]
    network_io: List[float]
    cache_hit_rate: float
    dht_peer_count: int

@dataclass
class RequestResult:
    """Individual request result"""
    endpoint: str
    response_time: float
    status_code: int
    size_bytes: int
    success: bool
    error_message: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

class SystemMonitor:
    """System resource monitoring during tests"""
    
    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.monitoring = False
        self.monitor_thread = None
        self.metrics = {
            'cpu_usage': [],
            'memory_usage': [],
            'disk_io': [],
            'network_io': [],
            'timestamps': []
        }
    
    def start(self):
        """Start system monitoring"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.start()
        logger.info("System monitoring started")
    
    def stop(self):
        """Stop system monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        logger.info("System monitoring stopped")
    
    def get_metrics(self) -> Dict:
        """Get collected metrics"""
        return self.metrics.copy()
    
    def _monitor_loop(self):
        """Monitor system resources"""
        while self.monitoring:
            try:
                # CPU usage
                cpu_percent = psutil.cpu_percent(interval=None)
                
                # Memory usage
                memory = psutil.virtual_memory()
                memory_percent = memory.percent
                
                # Disk I/O
                disk_io = psutil.disk_io_counters()
                disk_io_total = (disk_io.read_bytes + disk_io.write_bytes) / (1024 * 1024)  # MB
                
                # Network I/O
                network_io = psutil.net_io_counters()
                network_io_total = (network_io.bytes_sent + network_io.bytes_recv) / (1024 * 1024)  # MB
                
                # Store metrics
                self.metrics['cpu_usage'].append(cpu_percent)
                self.metrics['memory_usage'].append(memory_percent)
                self.metrics['disk_io'].append(disk_io_total)
                self.metrics['network_io'].append(network_io_total)
                self.metrics['timestamps'].append(datetime.now())
                
                time.sleep(self.interval)
                
            except Exception as e:
                logger.error(f"System monitoring error: {e}")
                time.sleep(self.interval)

class CannabisDataGenerator:
    """Generate realistic cannabis data for testing"""
    
    def __init__(self):
        self.strain_names = [
            "Blue Dream", "Green Crack", "OG Kush", "Sour Diesel", "White Widow",
            "Girl Scout Cookies", "Jack Herer", "Purple Haze", "Bubba Kush", "AK-47",
            "Pineapple Express", "Northern Lights", "Granddaddy Purple", "Chemdawg", "Trainwreck"
        ]
        
        self.dispensary_names = [
            "Green Valley Dispensary", "Cannabis Corner", "The Hemp Store", "Leafy Greens",
            "Bud Brothers", "High Quality Cannabis", "Nature's Medicine", "The Green Room",
            "Cannabis Connection", "Premium Herb Co.", "Valley Greens", "The Bud Bar"
        ]
        
        self.terpenes = [
            "myrcene", "limonene", "pinene", "linalool", "caryophyllene",
            "humulene", "terpinolene", "ocimene", "bisabolol", "eucalyptol"
        ]
        
        self.effects = [
            "relaxed", "happy", "euphoric", "uplifted", "creative", "focused",
            "energetic", "sleepy", "hungry", "giggly", "talkative", "aroused"
        ]
    
    def generate_strain_data(self) -> Dict:
        """Generate strain data"""
        return {
            "id": f"strain_{random.randint(1000, 9999)}",
            "name": random.choice(self.strain_names),
            "type": random.choice(["indica", "sativa", "hybrid"]),
            "thc": round(random.uniform(10.0, 30.0), 1),
            "cbd": round(random.uniform(0.1, 15.0), 1),
            "terpenes": random.sample(self.terpenes, random.randint(3, 6)),
            "effects": random.sample(self.effects, random.randint(2, 5)),
            "description": f"Premium {random.choice(['indica', 'sativa', 'hybrid'])} strain",
            "genetics": "Unknown x Unknown"
        }
    
    def generate_dispensary_menu(self) -> Dict:
        """Generate dispensary menu"""
        products = []
        for _ in range(random.randint(10, 50)):
            product = {
                "id": f"product_{random.randint(1000, 9999)}",
                "name": random.choice(self.strain_names),
                "category": random.choice(["flower", "edibles", "concentrates", "pre-rolls"]),
                "price": round(random.uniform(5.0, 100.0), 2),
                "quantity": random.choice(["1g", "3.5g", "7g", "14g", "28g"]),
                "thc": round(random.uniform(10.0, 30.0), 1),
                "cbd": round(random.uniform(0.1, 15.0), 1),
                "brand": f"Brand {random.randint(1, 20)}",
                "available": random.choice([True, False])
            }
            products.append(product)
        
        return {
            "dispensary_id": f"disp_{random.randint(100, 999)}",
            "name": random.choice(self.dispensary_names),
            "products": products,
            "last_updated": datetime.now().isoformat()
        }
    
    def generate_pricing_data(self) -> Dict:
        """Generate pricing data"""
        return {
            "product_id": f"product_{random.randint(1000, 9999)}",
            "prices": {
                "1g": round(random.uniform(8.0, 20.0), 2),
                "3.5g": round(random.uniform(25.0, 65.0), 2),
                "7g": round(random.uniform(45.0, 120.0), 2),
                "14g": round(random.uniform(80.0, 220.0), 2),
                "28g": round(random.uniform(150.0, 400.0), 2)
            },
            "discounts": {
                "first_time": 0.15,
                "senior": 0.10,
                "veteran": 0.20
            },
            "last_updated": datetime.now().isoformat()
        }

class LoadTestEngine:
    """High-performance load testing engine"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.data_generator = CannabisDataGenerator()
        self.system_monitor = SystemMonitor()
        self.session = None
        
        # Test results storage
        self.results: List[RequestResult] = []
        self.results_lock = threading.Lock()
        
        # Payload templates
        self.payloads = {
            "small": lambda: {"query": "test", "limit": 10},
            "medium": lambda: self.data_generator.generate_strain_data(),
            "large": lambda: self.data_generator.generate_dispensary_menu()
        }
    
    async def run_test(self, config: TestConfiguration) -> TestMetrics:
        """Run performance test with given configuration"""
        test_id = f"{config.test_type.value}_{int(time.time())}"
        logger.info(f"Starting {config.test_type.value} test: {test_id}")
        
        # Initialize
        self.results.clear()
        start_time = datetime.now()
        
        # Start system monitoring
        self.system_monitor.start()
        
        try:
            # Create HTTP session
            timeout = aiohttp.ClientTimeout(total=60, connect=10)
            self.session = aiohttp.ClientSession(timeout=timeout)
            
            # Run test based on type
            if config.test_type == TestType.LOAD_TEST:
                await self._run_load_test(config)
            elif config.test_type == TestType.STRESS_TEST:
                await self._run_stress_test(config)
            elif config.test_type == TestType.SPIKE_TEST:
                await self._run_spike_test(config)
            elif config.test_type == TestType.ENDURANCE_TEST:
                await self._run_endurance_test(config)
            elif config.test_type == TestType.LATENCY_TEST:
                await self._run_latency_test(config)
            elif config.test_type == TestType.SCALABILITY_TEST:
                await self._run_scalability_test(config)
            elif config.test_type == TestType.PEER_NETWORK_TEST:
                await self._run_peer_network_test(config)
            else:
                raise ValueError(f"Unsupported test type: {config.test_type}")
            
        finally:
            # Cleanup
            if self.session:
                await self.session.close()
            self.system_monitor.stop()
        
        end_time = datetime.now()
        
        # Calculate metrics
        metrics = self._calculate_metrics(test_id, config, start_time, end_time)
        
        logger.info(f"Test {test_id} completed: {metrics.total_requests} requests, "
                   f"{metrics.avg_response_time:.3f}s avg response time, "
                   f"{metrics.error_rate:.1%} error rate")
        
        return metrics
    
    async def _run_load_test(self, config: TestConfiguration):
        """Run standard load test"""
        tasks = []
        
        # Calculate request timing
        total_requests = int(config.requests_per_second * config.duration_seconds)
        request_interval = 1.0 / config.requests_per_second
        
        logger.info(f"Load test: {total_requests} requests over {config.duration_seconds}s "
                   f"({config.requests_per_second} RPS)")
        
        # Ramp up
        for i in range(total_requests):
            if i < config.ramp_up_time * config.requests_per_second:
                # Gradual ramp up
                delay = i * request_interval * (config.ramp_up_time / config.duration_seconds)
            else:
                # Steady state
                delay = i * request_interval
            
            endpoint = random.choice(config.endpoints)
            task = asyncio.create_task(
                self._delayed_request(delay, endpoint, config.payload_size)
            )
            tasks.append(task)
        
        # Execute requests
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _run_stress_test(self, config: TestConfiguration):
        """Run stress test with increasing load"""
        logger.info(f"Stress test: increasing load up to {config.concurrent_users} users")
        
        # Gradually increase concurrent users
        max_users = config.concurrent_users
        step_duration = config.duration_seconds // 5  # 5 steps
        
        for step in range(5):
            users = int(max_users * (step + 1) / 5)
            logger.info(f"Stress test step {step + 1}: {users} concurrent users")
            
            tasks = []
            for _ in range(users):
                endpoint = random.choice(config.endpoints)
                task = asyncio.create_task(
                    self._continuous_requests(step_duration, endpoint, config.payload_size)
                )
                tasks.append(task)
            
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _run_spike_test(self, config: TestConfiguration):
        """Run spike test with sudden load increase"""
        logger.info(f"Spike test: sudden load spike to {config.concurrent_users} users")
        
        # Normal load for 1/3 of duration
        normal_duration = config.duration_seconds // 3
        await self._sustained_load(normal_duration, config.concurrent_users // 5, config)
        
        # Spike load for 1/3 of duration
        spike_duration = config.duration_seconds // 3
        await self._sustained_load(spike_duration, config.concurrent_users, config)
        
        # Return to normal for remaining duration
        remaining_duration = config.duration_seconds - normal_duration - spike_duration
        await self._sustained_load(remaining_duration, config.concurrent_users // 5, config)
    
    async def _run_endurance_test(self, config: TestConfiguration):
        """Run endurance test with sustained load"""
        logger.info(f"Endurance test: sustained load for {config.duration_seconds}s")
        
        await self._sustained_load(
            config.duration_seconds, 
            config.concurrent_users, 
            config
        )
    
    async def _run_latency_test(self, config: TestConfiguration):
        """Run latency-focused test"""
        logger.info("Latency test: measuring response times")
        
        # Sequential requests to measure latency accurately
        for endpoint in config.endpoints:
            for _ in range(100):  # 100 requests per endpoint
                await self._make_request(endpoint, config.payload_size)
                await asyncio.sleep(0.1)  # Small delay between requests
    
    async def _run_scalability_test(self, config: TestConfiguration):
        """Run scalability test with varying peer counts"""
        logger.info("Scalability test: testing with different peer counts")
        
        # Test with different numbers of simulated peers
        peer_counts = [1, 5, 10, 20, 50, config.peer_count]
        
        for peer_count in peer_counts:
            logger.info(f"Testing with {peer_count} simulated peers")
            
            # Simulate peer network setup
            await self._simulate_peer_network(peer_count)
            
            # Run test with current peer configuration
            await self._sustained_load(
                config.duration_seconds // len(peer_counts),
                config.concurrent_users,
                config
            )
    
    async def _run_peer_network_test(self, config: TestConfiguration):
        """Run DHT peer network specific tests"""
        logger.info("Peer network test: testing DHT operations")
        
        # Test different DHT operations
        dht_endpoints = [
            CannabisEndpoint.DHT_PEERS,
            CannabisEndpoint.CACHE_STATUS
        ]
        
        tasks = []
        for _ in range(config.concurrent_users):
            task = asyncio.create_task(
                self._continuous_dht_requests(config.duration_seconds, dht_endpoints)
            )
            tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _sustained_load(self, duration: int, users: int, config: TestConfiguration):
        """Generate sustained load"""
        tasks = []
        for _ in range(users):
            endpoint = random.choice(config.endpoints)
            task = asyncio.create_task(
                self._continuous_requests(duration, endpoint, config.payload_size)
            )
            tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _continuous_requests(self, duration: int, endpoint: CannabisEndpoint, payload_size: str):
        """Make continuous requests for specified duration"""
        end_time = time.time() + duration
        
        while time.time() < end_time:
            await self._make_request(endpoint, payload_size)
            # Random delay between 0.1 and 1.0 seconds
            await asyncio.sleep(random.uniform(0.1, 1.0))
    
    async def _continuous_dht_requests(self, duration: int, endpoints: List[CannabisEndpoint]):
        """Make continuous DHT-specific requests"""
        end_time = time.time() + duration
        
        while time.time() < end_time:
            endpoint = random.choice(endpoints)
            await self._make_request(endpoint, "small")
            await asyncio.sleep(random.uniform(0.5, 2.0))
    
    async def _delayed_request(self, delay: float, endpoint: CannabisEndpoint, payload_size: str):
        """Make request after specified delay"""
        await asyncio.sleep(delay)
        await self._make_request(endpoint, payload_size)
    
    async def _make_request(self, endpoint: CannabisEndpoint, payload_size: str):
        """Make HTTP request and record metrics"""
        start_time = time.time()
        url = f"{self.base_url}{endpoint.value}"
        
        try:
            # Generate payload
            payload = self.payloads[payload_size]()
            
            # Make request
            async with self.session.post(url, json=payload) as response:
                content = await response.read()
                response_time = time.time() - start_time
                
                result = RequestResult(
                    endpoint=endpoint.value,
                    response_time=response_time,
                    status_code=response.status,
                    size_bytes=len(content),
                    success=200 <= response.status < 400
                )
                
                with self.results_lock:
                    self.results.append(result)
                
        except asyncio.TimeoutError:
            response_time = time.time() - start_time
            result = RequestResult(
                endpoint=endpoint.value,
                response_time=response_time,
                status_code=408,
                size_bytes=0,
                success=False,
                error_message="Timeout"
            )
            
            with self.results_lock:
                self.results.append(result)
                
        except Exception as e:
            response_time = time.time() - start_time
            result = RequestResult(
                endpoint=endpoint.value,
                response_time=response_time,
                status_code=500,
                size_bytes=0,
                success=False,
                error_message=str(e)
            )
            
            with self.results_lock:
                self.results.append(result)
    
    async def _simulate_peer_network(self, peer_count: int):
        """Simulate DHT peer network setup"""
        try:
            # This would integrate with actual DHT peer setup
            # For now, just simulate the setup delay
            await asyncio.sleep(peer_count * 0.1)
            logger.debug(f"Simulated {peer_count} peer network setup")
        except Exception as e:
            logger.error(f"Error simulating peer network: {e}")
    
    def _calculate_metrics(self, test_id: str, config: TestConfiguration, 
                          start_time: datetime, end_time: datetime) -> TestMetrics:
        """Calculate comprehensive test metrics"""
        
        if not self.results:
            return TestMetrics(
                test_id=test_id,
                test_type=config.test_type,
                start_time=start_time,
                end_time=end_time,
                total_requests=0,
                successful_requests=0,
                failed_requests=0,
                avg_response_time=0.0,
                min_response_time=0.0,
                max_response_time=0.0,
                p95_response_time=0.0,
                p99_response_time=0.0,
                requests_per_second=0.0,
                bytes_transferred=0,
                error_rate=0.0,
                cpu_usage=[],
                memory_usage=[],
                network_io=[],
                cache_hit_rate=0.0,
                dht_peer_count=0
            )
        
        # Basic counts
        total_requests = len(self.results)
        successful_requests = sum(1 for r in self.results if r.success)
        failed_requests = total_requests - successful_requests
        
        # Response times
        response_times = [r.response_time for r in self.results]
        avg_response_time = statistics.mean(response_times)
        min_response_time = min(response_times)
        max_response_time = max(response_times)
        
        # Percentiles
        response_times_sorted = sorted(response_times)
        p95_index = int(len(response_times_sorted) * 0.95)
        p99_index = int(len(response_times_sorted) * 0.99)
        p95_response_time = response_times_sorted[p95_index] if p95_index < len(response_times_sorted) else max_response_time
        p99_response_time = response_times_sorted[p99_index] if p99_index < len(response_times_sorted) else max_response_time
        
        # Throughput
        duration = (end_time - start_time).total_seconds()
        requests_per_second = total_requests / duration if duration > 0 else 0
        
        # Data transfer
        bytes_transferred = sum(r.size_bytes for r in self.results)
        
        # Error rate
        error_rate = failed_requests / total_requests if total_requests > 0 else 0
        
        # System metrics
        system_metrics = self.system_monitor.get_metrics()
        
        return TestMetrics(
            test_id=test_id,
            test_type=config.test_type,
            start_time=start_time,
            end_time=end_time,
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            avg_response_time=avg_response_time,
            min_response_time=min_response_time,
            max_response_time=max_response_time,
            p95_response_time=p95_response_time,
            p99_response_time=p99_response_time,
            requests_per_second=requests_per_second,
            bytes_transferred=bytes_transferred,
            error_rate=error_rate,
            cpu_usage=system_metrics.get('cpu_usage', []),
            memory_usage=system_metrics.get('memory_usage', []),
            network_io=system_metrics.get('network_io', []),
            cache_hit_rate=0.0,  # Would be retrieved from cache system
            dht_peer_count=config.peer_count
        )

class PerformanceReporter:
    """Generate comprehensive performance reports"""
    
    def __init__(self, output_dir: str = "performance_reports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def generate_report(self, metrics: TestMetrics, detailed: bool = True) -> str:
        """Generate performance test report"""
        
        report_file = os.path.join(
            self.output_dir,
            f"{metrics.test_id}_report.json"
        )
        
        # Create comprehensive report data
        report_data = {
            "test_summary": asdict(metrics),
            "performance_analysis": self._analyze_performance(metrics),
            "recommendations": self._generate_recommendations(metrics),
            "charts": self._generate_charts(metrics) if detailed else None
        }
        
        # Save JSON report
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        # Generate text summary
        summary = self._generate_text_summary(metrics)
        
        summary_file = os.path.join(
            self.output_dir,
            f"{metrics.test_id}_summary.txt"
        )
        
        with open(summary_file, 'w') as f:
            f.write(summary)
        
        logger.info(f"Performance report saved: {report_file}")
        logger.info(f"Summary report saved: {summary_file}")
        
        return summary
    
    def _analyze_performance(self, metrics: TestMetrics) -> Dict:
        """Analyze performance metrics"""
        analysis = {
            "response_time_analysis": {
                "status": "good" if metrics.avg_response_time < 1.0 else "poor",
                "avg_response_time": metrics.avg_response_time,
                "p95_response_time": metrics.p95_response_time,
                "variability": metrics.max_response_time - metrics.min_response_time
            },
            "throughput_analysis": {
                "requests_per_second": metrics.requests_per_second,
                "target_rps": 100,  # Example target
                "throughput_achieved": metrics.requests_per_second >= 100
            },
            "error_analysis": {
                "error_rate": metrics.error_rate,
                "acceptable_error_rate": 0.01,  # 1%
                "error_rate_acceptable": metrics.error_rate <= 0.01
            },
            "resource_analysis": {
                "max_cpu_usage": max(metrics.cpu_usage) if metrics.cpu_usage else 0,
                "max_memory_usage": max(metrics.memory_usage) if metrics.memory_usage else 0,
                "resource_utilization_healthy": (
                    (max(metrics.cpu_usage) if metrics.cpu_usage else 0) < 80 and
                    (max(metrics.memory_usage) if metrics.memory_usage else 0) < 80
                )
            }
        }
        
        return analysis
    
    def _generate_recommendations(self, metrics: TestMetrics) -> List[str]:
        """Generate performance recommendations"""
        recommendations = []
        
        # Response time recommendations
        if metrics.avg_response_time > 2.0:
            recommendations.append(
                "High average response time detected. Consider optimizing database queries "
                "and implementing more aggressive caching."
            )
        
        if metrics.p95_response_time > 5.0:
            recommendations.append(
                "High P95 response time indicates performance issues for some requests. "
                "Investigate outliers and optimize slow endpoints."
            )
        
        # Error rate recommendations
        if metrics.error_rate > 0.05:
            recommendations.append(
                "High error rate detected. Review application logs and implement "
                "better error handling and retry mechanisms."
            )
        
        # Throughput recommendations
        if metrics.requests_per_second < 50:
            recommendations.append(
                "Low throughput detected. Consider scaling horizontally by adding "
                "more DHT peers or optimizing the unlimited peer network."
            )
        
        # Resource recommendations
        if metrics.cpu_usage and max(metrics.cpu_usage) > 80:
            recommendations.append(
                "High CPU usage detected. Consider optimizing computationally "
                "intensive operations or adding more processing power."
            )
        
        if metrics.memory_usage and max(metrics.memory_usage) > 80:
            recommendations.append(
                "High memory usage detected. Review memory leaks and optimize "
                "data structures and caching strategies."
            )
        
        # DHT peer recommendations
        if metrics.dht_peer_count < 10:
            recommendations.append(
                "Low DHT peer count. Consider adding more peers to improve "
                "data distribution and fault tolerance in the unlimited peer network."
            )
        
        if not recommendations:
            recommendations.append(
                "Performance metrics look good! System is operating within acceptable parameters."
            )
        
        return recommendations
    
    def _generate_charts(self, metrics: TestMetrics) -> Dict:
        """Generate performance charts"""
        charts = {}
        
        try:
            # Response time distribution
            if hasattr(self, 'results') and self.results:
                response_times = [r.response_time for r in self.results]
                
                plt.figure(figsize=(10, 6))
                plt.hist(response_times, bins=50, alpha=0.7, color='blue')
                plt.xlabel('Response Time (seconds)')
                plt.ylabel('Frequency')
                plt.title('Response Time Distribution')
                
                chart_file = os.path.join(self.output_dir, f"{metrics.test_id}_response_times.png")
                plt.savefig(chart_file)
                plt.close()
                
                charts['response_time_distribution'] = chart_file
            
            # System resource usage
            if metrics.cpu_usage:
                plt.figure(figsize=(12, 8))
                
                plt.subplot(2, 2, 1)
                plt.plot(metrics.cpu_usage, color='red')
                plt.xlabel('Time')
                plt.ylabel('CPU Usage (%)')
                plt.title('CPU Usage Over Time')
                
                plt.subplot(2, 2, 2)
                plt.plot(metrics.memory_usage, color='green')
                plt.xlabel('Time')
                plt.ylabel('Memory Usage (%)')
                plt.title('Memory Usage Over Time')
                
                plt.subplot(2, 2, 3)
                plt.plot(metrics.network_io, color='blue')
                plt.xlabel('Time')
                plt.ylabel('Network I/O (MB)')
                plt.title('Network I/O Over Time')
                
                plt.subplot(2, 2, 4)
                # Performance summary
                summary_data = [
                    metrics.avg_response_time,
                    metrics.requests_per_second / 100,  # Normalized
                    metrics.error_rate * 100
                ]
                labels = ['Avg Response\nTime (s)', 'RPS\n(normalized)', 'Error Rate\n(%)']
                plt.bar(labels, summary_data, color=['orange', 'purple', 'red'])
                plt.title('Performance Summary')
                
                plt.tight_layout()
                
                chart_file = os.path.join(self.output_dir, f"{metrics.test_id}_system_metrics.png")
                plt.savefig(chart_file)
                plt.close()
                
                charts['system_metrics'] = chart_file
        
        except Exception as e:
            logger.error(f"Error generating charts: {e}")
        
        return charts
    
    def _generate_text_summary(self, metrics: TestMetrics) -> str:
        """Generate text summary report"""
        
        duration = (metrics.end_time - metrics.start_time).total_seconds()
        
        summary = f"""
WEEDHOUNDS CANNABIS DATA PERFORMANCE TEST REPORT
{'=' * 50}

Test Information:
- Test ID: {metrics.test_id}
- Test Type: {metrics.test_type.value}
- Duration: {duration:.1f} seconds
- Start Time: {metrics.start_time}
- End Time: {metrics.end_time}

Request Statistics:
- Total Requests: {metrics.total_requests:,}
- Successful Requests: {metrics.successful_requests:,}
- Failed Requests: {metrics.failed_requests:,}
- Error Rate: {metrics.error_rate:.2%}

Response Time Metrics:
- Average Response Time: {metrics.avg_response_time:.3f}s
- Minimum Response Time: {metrics.min_response_time:.3f}s
- Maximum Response Time: {metrics.max_response_time:.3f}s
- 95th Percentile: {metrics.p95_response_time:.3f}s
- 99th Percentile: {metrics.p99_response_time:.3f}s

Throughput Metrics:
- Requests per Second: {metrics.requests_per_second:.2f}
- Data Transferred: {metrics.bytes_transferred / (1024*1024):.2f} MB

System Resource Usage:
- Peak CPU Usage: {max(metrics.cpu_usage) if metrics.cpu_usage else 0:.1f}%
- Peak Memory Usage: {max(metrics.memory_usage) if metrics.memory_usage else 0:.1f}%
- Network I/O: {max(metrics.network_io) if metrics.network_io else 0:.2f} MB

Cannabis Data System Metrics:
- Cache Hit Rate: {metrics.cache_hit_rate:.1%}
- DHT Peer Count: {metrics.dht_peer_count}

Performance Analysis:
{self._get_performance_status(metrics)}

Generated: {datetime.now()}
"""
        
        return summary
    
    def _get_performance_status(self, metrics: TestMetrics) -> str:
        """Get overall performance status"""
        
        issues = []
        
        if metrics.avg_response_time > 2.0:
            issues.append("High response times")
        
        if metrics.error_rate > 0.05:
            issues.append("High error rate")
        
        if metrics.requests_per_second < 50:
            issues.append("Low throughput")
        
        if metrics.cpu_usage and max(metrics.cpu_usage) > 80:
            issues.append("High CPU usage")
        
        if metrics.memory_usage and max(metrics.memory_usage) > 80:
            issues.append("High memory usage")
        
        if issues:
            return f"ISSUES DETECTED: {', '.join(issues)}"
        else:
            return "PERFORMANCE STATUS: GOOD - All metrics within acceptable ranges"

class PerformanceTestSuite:
    """Comprehensive performance test suite for cannabis data system"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.load_engine = LoadTestEngine(base_url)
        self.reporter = PerformanceReporter()
    
    async def run_comprehensive_tests(self) -> List[TestMetrics]:
        """Run comprehensive performance test suite"""
        
        logger.info("Starting comprehensive cannabis data performance tests")
        
        test_configs = [
            # Load test
            TestConfiguration(
                test_type=TestType.LOAD_TEST,
                duration_seconds=300,  # 5 minutes
                concurrent_users=50,
                requests_per_second=10.0,
                ramp_up_time=60,
                endpoints=[
                    CannabisEndpoint.DISPENSARY_MENU,
                    CannabisEndpoint.STRAIN_DATA,
                    CannabisEndpoint.PRODUCT_PRICING
                ],
                payload_size="medium",
                cache_strategy="enabled",
                peer_count=10
            ),
            
            # Stress test
            TestConfiguration(
                test_type=TestType.STRESS_TEST,
                duration_seconds=300,
                concurrent_users=200,
                requests_per_second=20.0,
                ramp_up_time=60,
                endpoints=[
                    CannabisEndpoint.DISPENSARY_MENU,
                    CannabisEndpoint.TERPENE_PROFILES,
                    CannabisEndpoint.ANALYTICS
                ],
                payload_size="large",
                cache_strategy="mixed",
                peer_count=20
            ),
            
            # Latency test
            TestConfiguration(
                test_type=TestType.LATENCY_TEST,
                duration_seconds=60,
                concurrent_users=1,
                requests_per_second=1.0,
                ramp_up_time=0,
                endpoints=[
                    CannabisEndpoint.HEALTH_CHECK,
                    CannabisEndpoint.CACHE_STATUS
                ],
                payload_size="small",
                cache_strategy="enabled",
                peer_count=5
            ),
            
            # Peer network test
            TestConfiguration(
                test_type=TestType.PEER_NETWORK_TEST,
                duration_seconds=180,
                concurrent_users=30,
                requests_per_second=5.0,
                ramp_up_time=30,
                endpoints=[
                    CannabisEndpoint.DHT_PEERS,
                    CannabisEndpoint.CACHE_STATUS
                ],
                payload_size="medium",
                cache_strategy="enabled",
                peer_count=50
            )
        ]
        
        results = []
        
        for config in test_configs:
            try:
                logger.info(f"Running {config.test_type.value} test...")
                metrics = await self.load_engine.run_test(config)
                results.append(metrics)
                
                # Generate report
                report = self.reporter.generate_report(metrics, detailed=True)
                print(f"\n{config.test_type.value.upper()} TEST SUMMARY:")
                print("=" * 50)
                print(report)
                
                # Brief pause between tests
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error running {config.test_type.value} test: {e}")
        
        # Generate comprehensive summary
        self._generate_comprehensive_summary(results)
        
        return results
    
    def _generate_comprehensive_summary(self, results: List[TestMetrics]):
        """Generate comprehensive test suite summary"""
        
        summary_file = os.path.join(
            self.reporter.output_dir,
            f"comprehensive_test_summary_{int(time.time())}.txt"
        )
        
        with open(summary_file, 'w') as f:
            f.write("WEEDHOUNDS CANNABIS DATA COMPREHENSIVE PERFORMANCE TEST SUMMARY\n")
            f.write("=" * 70 + "\n\n")
            
            for metrics in results:
                f.write(f"{metrics.test_type.value.upper()} TEST:\n")
                f.write(f"  Total Requests: {metrics.total_requests:,}\n")
                f.write(f"  Average Response Time: {metrics.avg_response_time:.3f}s\n")
                f.write(f"  Requests per Second: {metrics.requests_per_second:.2f}\n")
                f.write(f"  Error Rate: {metrics.error_rate:.2%}\n")
                f.write(f"  DHT Peers: {metrics.dht_peer_count}\n")
                f.write("\n")
            
            # Overall assessment
            avg_response_time = statistics.mean([m.avg_response_time for m in results])
            total_requests = sum([m.total_requests for m in results])
            avg_error_rate = statistics.mean([m.error_rate for m in results])
            
            f.write("OVERALL ASSESSMENT:\n")
            f.write(f"  Total Requests Across All Tests: {total_requests:,}\n")
            f.write(f"  Average Response Time: {avg_response_time:.3f}s\n")
            f.write(f"  Average Error Rate: {avg_error_rate:.2%}\n")
            
            if avg_response_time < 1.0 and avg_error_rate < 0.01:
                f.write("  STATUS: ✓ EXCELLENT - System performing optimally\n")
            elif avg_response_time < 2.0 and avg_error_rate < 0.05:
                f.write("  STATUS: ✓ GOOD - System performing well\n")
            else:
                f.write("  STATUS: ⚠ NEEDS IMPROVEMENT - Performance issues detected\n")
        
        logger.info(f"Comprehensive summary saved: {summary_file}")

if __name__ == "__main__":
    # Example usage
    async def run_performance_tests():
        # Initialize test suite
        test_suite = PerformanceTestSuite("http://localhost:8000")
        
        # Run comprehensive tests
        results = await test_suite.run_comprehensive_tests()
        
        print(f"\nCompleted {len(results)} performance tests")
        print("Check performance_reports/ directory for detailed reports")
    
    # Run tests
    asyncio.run(run_performance_tests())