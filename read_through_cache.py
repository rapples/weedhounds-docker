"""
Read-Through Cache Pattern for WeedHounds Cannabis Data System
Implements intelligent cache-through loading with MySQL persistence and DHT peer network
Combines local MySQL cache, intelligent TTL management, and unlimited peer-to-peer distribution
"""

import asyncio
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Union, Tuple
from dataclasses import dataclass
from enum import Enum
import hashlib
try:
    import aiohttp
except ImportError:
    aiohttp = None
    
import mysql.connector
from mysql.connector import Error

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from concurrent.futures import ThreadPoolExecutor
import pickle
import gzip

# Import our custom components - will be available when properly installed
try:
    from mysql_result_cache import MySQLResultCache, CannabisDataType
except ImportError:
    # Define stub classes for testing
    from enum import Enum
    class CannabisDataType(Enum):
        STRAIN_DATA = "strain_data"
        DISPENSARY_MENU = "dispensary_menu"
        PRODUCT_PRICING = "product_pricing"
        TERPENE_PROFILES = "terpene_profiles"
        LAB_RESULTS = "lab_results"
        DISPENSARY_INFO = "dispensary_info"
        ANALYTICS_DATA = "analytics_data"
        PROMOTIONS = "promotions"
    
    class MySQLResultCache:
        def __init__(self, **kwargs): pass
        async def initialize(self): return True
        async def get(self, key): return None
        async def set(self, key, data, data_type, **kwargs): return True
        async def get_cache_statistics(self): return {}

try:
    from intelligent_ttl_manager import IntelligentTTLManager, TTLStrategy
except ImportError:
    # Define stub classes for testing
    from enum import Enum
    class TTLStrategy(Enum):
        FIXED = "fixed"
        ADAPTIVE = "adaptive"
        BUSINESS_HOURS = "business_hours"
        VOLATILITY_BASED = "volatility"
        HYBRID = "hybrid"
    
    class IntelligentTTLManager:
        def __init__(self, **kwargs): pass
        async def initialize(self): return True
        async def calculate_ttl(self, data_type, cache_key, metadata=None): return 3600
        async def track_access(self, *args): pass
        async def get_ttl_statistics(self): return {}

try:
    from dht_storage import DHTStorage
except ImportError:
    # Define stub class for testing
    class DHTStorage:
        def __init__(self, **kwargs): 
            self.node_id = kwargs.get('node_id', 'test')
            self.peer_network = type('PeerNetwork', (), {'active_peers': []})()
        async def initialize(self): return True
        async def get(self, key): return None
        async def set(self, key, data, **kwargs): return True

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CacheStrategy(Enum):
    """Cache strategy for read-through operations"""
    LOCAL_FIRST = "local_first"      # Check local MySQL first
    DHT_FIRST = "dht_first"          # Check DHT peers first
    PARALLEL = "parallel"            # Check both simultaneously
    INTELLIGENT = "intelligent"     # AI-driven strategy selection

class DataSource(Enum):
    """Data source types for cannabis APIs"""
    DUTCHIE = "dutchie"
    JANE = "jane"
    LEAFLY = "leafly"
    WEEDMAPS = "weedmaps"
    IHEARTJANE = "iheartjane"
    DIRECT_API = "direct_api"
    SCRAPER = "scraper"

@dataclass
class CacheHit:
    """Cache hit information"""
    source: str           # 'mysql', 'dht', 'api'
    data: Any
    ttl_remaining: int
    hit_time: float
    compressed: bool = False
    peer_id: Optional[str] = None

@dataclass
class CacheMiss:
    """Cache miss information for analytics"""
    cache_key: str
    data_type: CannabisDataType
    sources_checked: List[str]
    total_check_time: float
    fallback_source: str

class ReadThroughCacheManager:
    """
    Advanced Read-Through Cache Manager for Cannabis Data
    Integrates MySQL persistence, intelligent TTL, and unlimited peer DHT network
    """
    
    def __init__(self, 
                 mysql_config: Dict,
                 dht_config: Dict,
                 api_config: Dict,
                 strategy: CacheStrategy = CacheStrategy.INTELLIGENT):
        """
        Initialize Read-Through Cache Manager
        
        Args:
            mysql_config: MySQL connection configuration
            dht_config: DHT network configuration
            api_config: API endpoints and credentials
            strategy: Default cache strategy
        """
        self.mysql_config = mysql_config
        self.dht_config = dht_config
        self.api_config = api_config
        self.strategy = strategy
        
        # Core components
        self.mysql_cache = None
        self.ttl_manager = None
        self.dht_storage = None
        
        # HTTP session for API calls
        self.session = None
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        # Performance tracking
        self.stats = {
            'total_requests': 0,
            'mysql_hits': 0,
            'dht_hits': 0,
            'api_calls': 0,
            'cache_misses': 0,
            'avg_response_time': 0.0,
            'strategy_switches': 0
        }
        
        # Cannabis-specific API mappings
        self.api_mappings = {
            CannabisDataType.DISPENSARY_MENU: {
                DataSource.DUTCHIE: self._fetch_dutchie_menu,
                DataSource.JANE: self._fetch_jane_menu,
                DataSource.IHEARTJANE: self._fetch_iheartjane_menu
            },
            CannabisDataType.STRAIN_DATA: {
                DataSource.LEAFLY: self._fetch_leafly_strain,
                DataSource.WEEDMAPS: self._fetch_weedmaps_strain
            },
            CannabisDataType.PRODUCT_PRICING: {
                DataSource.DUTCHIE: self._fetch_dutchie_pricing,
                DataSource.JANE: self._fetch_jane_pricing
            },
            CannabisDataType.TERPENE_PROFILES: {
                DataSource.LEAFLY: self._fetch_leafly_terpenes,
                DataSource.DIRECT_API: self._fetch_terpene_database
            }
        }
        
        # Strategy selection AI weights
        self.strategy_weights = {
            'mysql_availability': 0.3,
            'dht_peer_count': 0.25,
            'api_response_time': 0.2,
            'cache_hit_rate': 0.15,
            'network_latency': 0.1
        }
    
    async def initialize(self) -> bool:
        """Initialize all cache components"""
        try:
            # Initialize MySQL result cache
            self.mysql_cache = MySQLResultCache(**self.mysql_config)
            await self.mysql_cache.initialize()
            
            # Initialize TTL manager
            self.ttl_manager = IntelligentTTLManager(self.mysql_config)
            await self.ttl_manager.initialize()
            
            # Initialize DHT storage
            self.dht_storage = DHTStorage(
                node_id=self.dht_config.get('node_id', 'cache_manager'),
                port=self.dht_config.get('port', 9100)
            )
            await self.dht_storage.initialize()
            
            # Initialize HTTP session
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(timeout=timeout)
            
            # Start background tasks
            asyncio.create_task(self._strategy_optimization_loop())
            asyncio.create_task(self._cache_warming_loop())
            
            logger.info("Read-Through Cache Manager initialized successfully")
            logger.info(f"Strategy: {self.strategy.value}")
            logger.info(f"Supported data types: {len(self.api_mappings)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize cache manager: {e}")
            return False
    
    async def get(self, 
                  cache_key: str,
                  data_type: CannabisDataType,
                  source_preference: Optional[List[DataSource]] = None,
                  metadata: Optional[Dict] = None,
                  force_refresh: bool = False) -> Optional[Any]:
        """
        Get cannabis data with read-through caching
        
        Args:
            cache_key: Unique cache key
            data_type: Type of cannabis data
            source_preference: Preferred data sources in order
            metadata: Additional metadata for API calls
            force_refresh: Force API call ignoring cache
            
        Returns:
            Cannabis data or None if not found
        """
        start_time = time.time()
        self.stats['total_requests'] += 1
        
        try:
            if not force_refresh:
                # Try cache sources based on strategy
                cache_hit = await self._try_cache_sources(cache_key, data_type)
                if cache_hit:
                    await self._track_cache_hit(cache_hit, start_time)
                    return cache_hit.data
            
            # Cache miss - fetch from API
            logger.debug(f"Cache miss for {cache_key[:16]}..., fetching from API")
            self.stats['cache_misses'] += 1
            
            api_data = await self._fetch_from_api(
                cache_key, data_type, source_preference, metadata
            )
            
            if api_data:
                # Store in cache with intelligent TTL
                await self._store_in_cache(cache_key, data_type, api_data, metadata)
                
                response_time = time.time() - start_time
                self._update_avg_response_time(response_time)
                
                return api_data
            
            return None
            
        except Exception as e:
            logger.error(f"Error in read-through cache get: {e}")
            return None
    
    async def _try_cache_sources(self, cache_key: str, data_type: CannabisDataType) -> Optional[CacheHit]:
        """Try different cache sources based on strategy"""
        
        if self.strategy == CacheStrategy.LOCAL_FIRST:
            # Try MySQL first, then DHT
            mysql_result = await self._try_mysql_cache(cache_key)
            if mysql_result:
                return mysql_result
            
            return await self._try_dht_cache(cache_key, data_type)
        
        elif self.strategy == CacheStrategy.DHT_FIRST:
            # Try DHT first, then MySQL
            dht_result = await self._try_dht_cache(cache_key, data_type)
            if dht_result:
                return dht_result
            
            return await self._try_mysql_cache(cache_key)
        
        elif self.strategy == CacheStrategy.PARALLEL:
            # Try both simultaneously
            mysql_task = self._try_mysql_cache(cache_key)
            dht_task = self._try_dht_cache(cache_key, data_type)
            
            done, pending = await asyncio.wait(
                [mysql_task, dht_task],
                return_when=asyncio.FIRST_COMPLETED,
                timeout=1.0
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
            
            # Return first successful result
            for task in done:
                result = await task
                if result:
                    return result
            
            return None
        
        elif self.strategy == CacheStrategy.INTELLIGENT:
            # AI-driven strategy selection
            optimal_strategy = await self._select_optimal_strategy(cache_key, data_type)
            
            # Recursively call with optimal strategy
            old_strategy = self.strategy
            self.strategy = optimal_strategy
            result = await self._try_cache_sources(cache_key, data_type)
            self.strategy = old_strategy
            
            if result:
                self.stats['strategy_switches'] += 1
            
            return result
    
    async def _try_mysql_cache(self, cache_key: str) -> Optional[CacheHit]:
        """Try MySQL cache"""
        try:
            data = await self.mysql_cache.get(cache_key)
            if data:
                self.stats['mysql_hits'] += 1
                return CacheHit(
                    source='mysql',
                    data=data,
                    ttl_remaining=3600,  # Would need to calculate actual TTL
                    hit_time=time.time()
                )
        except Exception as e:
            logger.warning(f"MySQL cache error: {e}")
        
        return None
    
    async def _try_dht_cache(self, cache_key: str, data_type: CannabisDataType) -> Optional[CacheHit]:
        """Try DHT peer cache"""
        try:
            dht_key = f"{data_type.value}:{cache_key}"
            data = await self.dht_storage.get(dht_key)
            
            if data:
                self.stats['dht_hits'] += 1
                return CacheHit(
                    source='dht',
                    data=data,
                    ttl_remaining=1800,  # DHT TTL
                    hit_time=time.time(),
                    peer_id=self.dht_storage.node_id
                )
        except Exception as e:
            logger.warning(f"DHT cache error: {e}")
        
        return None
    
    async def _select_optimal_strategy(self, cache_key: str, data_type: CannabisDataType) -> CacheStrategy:
        """AI-driven strategy selection based on current conditions"""
        try:
            # Collect performance metrics
            mysql_latency = await self._measure_mysql_latency()
            dht_peer_count = len(self.dht_storage.peer_network.active_peers) if self.dht_storage else 0
            recent_hit_rate = await self._calculate_recent_hit_rate()
            
            # Score each strategy
            scores = {}
            
            # LOCAL_FIRST scoring
            local_score = (
                (1.0 - mysql_latency / 1000) * self.strategy_weights['mysql_availability'] +
                recent_hit_rate * self.strategy_weights['cache_hit_rate'] +
                0.8 * self.strategy_weights['api_response_time']  # MySQL is faster than API
            )
            scores[CacheStrategy.LOCAL_FIRST] = local_score
            
            # DHT_FIRST scoring
            dht_score = (
                min(dht_peer_count / 10, 1.0) * self.strategy_weights['dht_peer_count'] +
                (0.6 if dht_peer_count > 5 else 0.3) * self.strategy_weights['network_latency'] +
                recent_hit_rate * 0.8 * self.strategy_weights['cache_hit_rate']
            )
            scores[CacheStrategy.DHT_FIRST] = dht_score
            
            # PARALLEL scoring (higher overhead but potentially faster)
            parallel_score = (
                max(local_score, dht_score) * 0.9 +  # Best of both minus overhead
                0.8 * self.strategy_weights['cache_hit_rate']
            )
            scores[CacheStrategy.PARALLEL] = parallel_score
            
            # Select strategy with highest score
            optimal_strategy = max(scores, key=scores.get)
            
            logger.debug(f"Strategy selection scores: {scores}")
            logger.debug(f"Selected strategy: {optimal_strategy.value}")
            
            return optimal_strategy
            
        except Exception as e:
            logger.error(f"Error in strategy selection: {e}")
            return CacheStrategy.LOCAL_FIRST  # Safe default
    
    async def _measure_mysql_latency(self) -> float:
        """Measure MySQL response latency"""
        try:
            start = time.time()
            # Simple ping query
            connection = self.mysql_cache.connection_pool.get_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            latency = (time.time() - start) * 1000  # ms
            cursor.close()
            connection.close()
            return latency
        except:
            return 1000.0  # High latency if error
    
    async def _calculate_recent_hit_rate(self) -> float:
        """Calculate recent cache hit rate"""
        total_recent = self.stats['mysql_hits'] + self.stats['dht_hits'] + self.stats['cache_misses']
        if total_recent == 0:
            return 0.5  # Default
        
        hits = self.stats['mysql_hits'] + self.stats['dht_hits']
        return hits / total_recent
    
    async def _fetch_from_api(self, 
                             cache_key: str,
                             data_type: CannabisDataType,
                             source_preference: Optional[List[DataSource]],
                             metadata: Optional[Dict]) -> Optional[Any]:
        """Fetch data from cannabis APIs"""
        self.stats['api_calls'] += 1
        
        # Get available sources for this data type
        available_sources = self.api_mappings.get(data_type, {})
        if not available_sources:
            logger.warning(f"No API sources available for {data_type.value}")
            return None
        
        # Order sources by preference
        sources_to_try = []
        if source_preference:
            # Add preferred sources first
            for source in source_preference:
                if source in available_sources:
                    sources_to_try.append(source)
        
        # Add remaining sources
        for source in available_sources:
            if source not in sources_to_try:
                sources_to_try.append(source)
        
        # Try each source until successful
        for source in sources_to_try:
            try:
                logger.debug(f"Trying {source.value} for {data_type.value}")
                
                fetch_func = available_sources[source]
                data = await fetch_func(cache_key, metadata)
                
                if data:
                    logger.info(f"Successfully fetched {data_type.value} from {source.value}")
                    return data
                    
            except Exception as e:
                logger.warning(f"Failed to fetch from {source.value}: {e}")
                continue
        
        logger.error(f"Failed to fetch {data_type.value} from all sources")
        return None
    
    async def _store_in_cache(self, 
                             cache_key: str,
                             data_type: CannabisDataType,
                             data: Any,
                             metadata: Optional[Dict]):
        """Store data in both MySQL and DHT caches"""
        try:
            # Calculate intelligent TTL
            ttl = await self.ttl_manager.calculate_ttl(
                data_type.value, cache_key, metadata
            )
            
            # Store in MySQL cache
            mysql_success = await self.mysql_cache.set(
                cache_key, data, data_type, ttl_override=ttl, metadata=metadata
            )
            
            # Store in DHT network
            dht_key = f"{data_type.value}:{cache_key}"
            dht_success = await self.dht_storage.set(
                dht_key, data, ttl=min(ttl, 3600)  # DHT uses shorter TTL
            )
            
            logger.debug(f"Stored {cache_key[:16]}... in MySQL: {mysql_success}, DHT: {dht_success}")
            
        except Exception as e:
            logger.error(f"Error storing in cache: {e}")
    
    async def _track_cache_hit(self, cache_hit: CacheHit, start_time: float):
        """Track cache hit for analytics"""
        response_time = time.time() - start_time
        self._update_avg_response_time(response_time)
        
        # Track in TTL manager
        await self.ttl_manager.track_access(
            cache_hit.data.__class__.__name__,
            'unknown',  # Would need to determine data type
            True,  # Cache hit
            response_time * 1000
        )
    
    def _update_avg_response_time(self, response_time: float):
        """Update average response time"""
        current_avg = self.stats['avg_response_time']
        total_requests = self.stats['total_requests']
        
        # Weighted average
        self.stats['avg_response_time'] = (
            (current_avg * (total_requests - 1) + response_time) / total_requests
        )
    
    # Cannabis API fetch methods
    async def _fetch_dutchie_menu(self, cache_key: str, metadata: Dict) -> Optional[Dict]:
        """Fetch dispensary menu from Dutchie API"""
        try:
            dispensary_id = metadata.get('dispensary_id')
            if not dispensary_id:
                return None
            
            url = f"{self.api_config['dutchie']['base_url']}/dispensaries/{dispensary_id}/menu"
            headers = {
                'Authorization': f"Bearer {self.api_config['dutchie']['token']}",
                'Content-Type': 'application/json'
            }
            
            async with self.session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._normalize_dutchie_menu(data)
            
        except Exception as e:
            logger.error(f"Dutchie API error: {e}")
        
        return None
    
    async def _fetch_jane_menu(self, cache_key: str, metadata: Dict) -> Optional[Dict]:
        """Fetch dispensary menu from Jane API"""
        try:
            dispensary_id = metadata.get('dispensary_id')
            if not dispensary_id:
                return None
            
            url = f"{self.api_config['jane']['base_url']}/dispensaries/{dispensary_id}/products"
            headers = {
                'X-API-Key': self.api_config['jane']['api_key'],
                'Content-Type': 'application/json'
            }
            
            async with self.session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._normalize_jane_menu(data)
            
        except Exception as e:
            logger.error(f"Jane API error: {e}")
        
        return None
    
    async def _fetch_iheartjane_menu(self, cache_key: str, metadata: Dict) -> Optional[Dict]:
        """Fetch dispensary menu from iHeartJane API"""
        # Implementation would go here
        return None
    
    async def _fetch_leafly_strain(self, cache_key: str, metadata: Dict) -> Optional[Dict]:
        """Fetch strain data from Leafly API"""
        # Implementation would go here
        return None
    
    async def _fetch_weedmaps_strain(self, cache_key: str, metadata: Dict) -> Optional[Dict]:
        """Fetch strain data from Weedmaps API"""
        # Implementation would go here
        return None
    
    async def _fetch_dutchie_pricing(self, cache_key: str, metadata: Dict) -> Optional[Dict]:
        """Fetch pricing data from Dutchie API"""
        # Implementation would go here
        return None
    
    async def _fetch_jane_pricing(self, cache_key: str, metadata: Dict) -> Optional[Dict]:
        """Fetch pricing data from Jane API"""
        # Implementation would go here
        return None
    
    async def _fetch_leafly_terpenes(self, cache_key: str, metadata: Dict) -> Optional[Dict]:
        """Fetch terpene profiles from Leafly API"""
        # Implementation would go here
        return None
    
    async def _fetch_terpene_database(self, cache_key: str, metadata: Dict) -> Optional[Dict]:
        """Fetch terpene data from direct database"""
        # Implementation would go here
        return None
    
    def _normalize_dutchie_menu(self, raw_data: Dict) -> Dict:
        """Normalize Dutchie menu data to standard format"""
        # Implementation would normalize the data structure
        return raw_data
    
    def _normalize_jane_menu(self, raw_data: Dict) -> Dict:
        """Normalize Jane menu data to standard format"""
        # Implementation would normalize the data structure
        return raw_data
    
    async def _strategy_optimization_loop(self):
        """Background task for strategy optimization"""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                await self._optimize_strategies()
            except Exception as e:
                logger.error(f"Strategy optimization error: {e}")
    
    async def _cache_warming_loop(self):
        """Background task for cache warming"""
        while True:
            try:
                await asyncio.sleep(1800)  # Run every 30 minutes
                await self._warm_popular_caches()
            except Exception as e:
                logger.error(f"Cache warming error: {e}")
    
    async def _optimize_strategies(self):
        """Optimize strategy weights based on performance"""
        try:
            # Analyze recent performance
            hit_rate = await self._calculate_recent_hit_rate()
            avg_response = self.stats['avg_response_time']
            
            # Adjust weights based on performance
            if hit_rate < 0.6:  # Low hit rate
                self.strategy_weights['cache_hit_rate'] += 0.05
                self.strategy_weights['api_response_time'] -= 0.05
            
            if avg_response > 1.0:  # Slow response
                self.strategy_weights['mysql_availability'] += 0.05
                self.strategy_weights['dht_peer_count'] -= 0.05
            
            logger.debug(f"Updated strategy weights: {self.strategy_weights}")
            
        except Exception as e:
            logger.error(f"Strategy optimization error: {e}")
    
    async def _warm_popular_caches(self):
        """Warm caches for popular cannabis data"""
        try:
            # Would implement cache warming logic based on access patterns
            logger.debug("Cache warming completed")
        except Exception as e:
            logger.error(f"Cache warming error: {e}")
    
    async def get_cache_statistics(self) -> Dict:
        """Get comprehensive cache statistics"""
        try:
            mysql_stats = await self.mysql_cache.get_cache_statistics()
            ttl_stats = await self.ttl_manager.get_ttl_statistics()
            
            return {
                'read_through_stats': self.stats,
                'mysql_cache': mysql_stats,
                'ttl_manager': ttl_stats,
                'dht_peers': len(self.dht_storage.peer_network.active_peers) if self.dht_storage else 0
            }
        except Exception as e:
            logger.error(f"Error getting cache statistics: {e}")
            return {'error': str(e)}
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.session:
                await self.session.close()
            
            if self.executor:
                self.executor.shutdown(wait=True)
            
            logger.info("Read-Through Cache Manager cleaned up")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# Decorator for read-through caching
def read_through_cache(data_type: CannabisDataType, 
                      source_preference: Optional[List[DataSource]] = None,
                      cache_key_func: Optional[Callable] = None):
    """Decorator for read-through caching"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Initialize cache manager (would be singleton in real implementation)
            cache_manager = ReadThroughCacheManager(
                mysql_config={'host': 'localhost', 'port': 3307, 'user': 'root', 'password': '', 'database': 'terpene_database'},
                dht_config={'node_id': 'decorator_node', 'port': 9100},
                api_config={'dutchie': {'base_url': 'https://api.dutchie.com', 'token': 'token'}, 'jane': {'base_url': 'https://api.jane.com', 'api_key': 'key'}}
            )
            await cache_manager.initialize()
            
            # Generate cache key
            if cache_key_func:
                cache_key = cache_key_func(*args, **kwargs)
            else:
                cache_key = hashlib.sha256(str({'func': func.__name__, 'args': args, 'kwargs': kwargs}).encode()).hexdigest()
            
            # Try cache first
            result = await cache_manager.get(cache_key, data_type, source_preference)
            
            if result is None:
                # Execute function as fallback
                result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            return result
        
        return wrapper
    return decorator

if __name__ == "__main__":
    # Example usage
    async def test_read_through_cache():
        config = {
            'mysql_config': {
                'host': 'localhost',
                'port': 3307,
                'user': 'root',
                'password': '',
                'database': 'terpene_database'
            },
            'dht_config': {
                'node_id': 'test_cache_node',
                'port': 9100
            },
            'api_config': {
                'dutchie': {
                    'base_url': 'https://api.dutchie.com',
                    'token': 'test_token'
                },
                'jane': {
                    'base_url': 'https://api.jane.com',
                    'api_key': 'test_key'
                }
            }
        }
        
        cache_manager = ReadThroughCacheManager(**config)
        await cache_manager.initialize()
        
        # Test cache operations
        cache_key = "dispensary_123_menu"
        metadata = {
            'dispensary_id': '123',
            'location': 'california',
            'last_updated': '2024-01-18T10:00:00'
        }
        
        # First call - should fetch from API
        result1 = await cache_manager.get(
            cache_key, 
            CannabisDataType.DISPENSARY_MENU,
            [DataSource.DUTCHIE, DataSource.JANE],
            metadata
        )
        print(f"First call result: {result1}")
        
        # Second call - should hit cache
        result2 = await cache_manager.get(
            cache_key,
            CannabisDataType.DISPENSARY_MENU,
            metadata=metadata
        )
        print(f"Second call result: {result2}")
        
        # Get statistics
        stats = await cache_manager.get_cache_statistics()
        print(f"Cache statistics: {stats}")
        
        await cache_manager.cleanup()
    
    # Run test
    asyncio.run(test_read_through_cache())