"""
MySQL Result Cache for WeedHounds Cannabis Data System
Implements persistent database-based caching with intelligent TTL management
Integrates with the unlimited peer-to-peer network for enhanced performance
"""

import asyncio
import json
import hashlib
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
import mysql.connector
from mysql.connector import Error
import pickle
import gzip
import os
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CannabisDataType(Enum):
    """Cannabis-specific data types with optimized TTL"""
    STRAIN_DATA = "strain_data"           # TTL: 1 year
    DISPENSARY_MENU = "dispensary_menu"   # TTL: 1 hour
    PRODUCT_PRICING = "product_pricing"   # TTL: 10 minutes
    TERPENE_PROFILES = "terpene_profiles" # TTL: 6 months
    LAB_RESULTS = "lab_results"           # TTL: 1 year
    DISPENSARY_INFO = "dispensary_info"   # TTL: 1 week
    ANALYTICS_DATA = "analytics_data"     # TTL: 24 hours
    PROMOTIONS = "promotions"             # TTL: 30 minutes

@dataclass
class CacheEntry:
    """Cannabis data cache entry with metadata"""
    cache_key: str
    data_type: CannabisDataType
    data: Any
    created_at: datetime
    expires_at: datetime
    hit_count: int = 0
    compressed: bool = False
    source_api: Optional[str] = None
    dispensary_id: Optional[str] = None
    
class MySQLResultCache:
    """
    High-performance MySQL-based result cache for cannabis data
    Optimized for the unlimited peer-to-peer cannabis data network
    """
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 3307,
                 user: str = "root", 
                 password: str = "",
                 database: str = "terpene_database",
                 max_connections: int = 20,
                 compression_threshold: int = 1024):
        """
        Initialize MySQL result cache
        
        Args:
            host: Database host
            port: Database port (default 3307 for SSH tunnel)
            user: Database user
            password: Database password
            database: Database name
            max_connections: Maximum connection pool size
            compression_threshold: Compress data larger than this size (bytes)
        """
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'autocommit': True,
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci'
        }
        self.max_connections = max_connections
        self.compression_threshold = compression_threshold
        self.connection_pool = None
        self.stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'deletes': 0,
            'errors': 0
        }
        
        # Cannabis-specific TTL mapping (in seconds)
        self.ttl_mapping = {
            CannabisDataType.STRAIN_DATA: 365 * 24 * 3600,      # 1 year
            CannabisDataType.DISPENSARY_MENU: 3600,             # 1 hour
            CannabisDataType.PRODUCT_PRICING: 600,              # 10 minutes
            CannabisDataType.TERPENE_PROFILES: 180 * 24 * 3600, # 6 months
            CannabisDataType.LAB_RESULTS: 365 * 24 * 3600,      # 1 year
            CannabisDataType.DISPENSARY_INFO: 7 * 24 * 3600,    # 1 week
            CannabisDataType.ANALYTICS_DATA: 24 * 3600,         # 24 hours
            CannabisDataType.PROMOTIONS: 1800                   # 30 minutes
        }
        
    async def initialize(self) -> bool:
        """Initialize the MySQL result cache"""
        try:
            # Create connection pool
            from mysql.connector.pooling import MySQLConnectionPool
            
            pool_config = self.config.copy()
            pool_config['pool_name'] = 'cannabis_cache_pool'
            pool_config['pool_size'] = self.max_connections
            pool_config['pool_reset_session'] = True
            
            self.connection_pool = MySQLConnectionPool(**pool_config)
            
            # Create cache tables
            await self._create_cache_tables()
            
            # Start maintenance tasks
            asyncio.create_task(self._maintenance_loop())
            
            logger.info(f"MySQL Result Cache initialized with {self.max_connections} connections")
            logger.info(f"Cannabis data types configured: {len(self.ttl_mapping)}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize MySQL cache: {e}")
            self.stats['errors'] += 1
            return False
    
    async def _create_cache_tables(self):
        """Create optimized cache tables for cannabis data"""
        
        # Main cache table with cannabis-specific optimizations
        cache_table_sql = """
        CREATE TABLE IF NOT EXISTS cannabis_cache (
            cache_key VARCHAR(64) NOT NULL PRIMARY KEY,
            data_type ENUM('strain_data', 'dispensary_menu', 'product_pricing', 
                          'terpene_profiles', 'lab_results', 'dispensary_info', 
                          'analytics_data', 'promotions') NOT NULL,
            data_blob LONGBLOB NOT NULL,
            compressed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL,
            hit_count INT UNSIGNED DEFAULT 0,
            source_api VARCHAR(100),
            dispensary_id VARCHAR(50),
            data_size INT UNSIGNED,
            INDEX idx_data_type (data_type),
            INDEX idx_expires_at (expires_at),
            INDEX idx_dispensary_id (dispensary_id),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATION=utf8mb4_unicode_ci;
        """
        
        # Cache statistics table
        stats_table_sql = """
        CREATE TABLE IF NOT EXISTS cache_statistics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data_type VARCHAR(50) NOT NULL,
            operation ENUM('hit', 'miss', 'set', 'delete', 'expire') NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            response_time_ms FLOAT,
            cache_key_hash VARCHAR(64),
            INDEX idx_timestamp (timestamp),
            INDEX idx_data_type (data_type),
            INDEX idx_operation (operation)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATION=utf8mb4_unicode_ci;
        """
        
        # Cannabis-specific metadata table
        metadata_table_sql = """
        CREATE TABLE IF NOT EXISTS cannabis_metadata (
            cache_key VARCHAR(64) NOT NULL PRIMARY KEY,
            strain_name VARCHAR(200),
            dispensary_name VARCHAR(200),
            product_category VARCHAR(100),
            thc_content DECIMAL(5,2),
            cbd_content DECIMAL(5,2),
            price_per_gram DECIMAL(8,2),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_strain_name (strain_name),
            INDEX idx_dispensary_name (dispensary_name),
            INDEX idx_product_category (product_category),
            INDEX idx_thc_content (thc_content),
            INDEX idx_price_per_gram (price_per_gram),
            FOREIGN KEY (cache_key) REFERENCES cannabis_cache(cache_key) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATION=utf8mb4_unicode_ci;
        """
        
        connection = self.connection_pool.get_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(cache_table_sql)
            cursor.execute(stats_table_sql)
            cursor.execute(metadata_table_sql)
            connection.commit()
            logger.info("Cannabis cache tables created successfully")
        except Error as e:
            logger.error(f"Error creating cache tables: {e}")
            raise
        finally:
            cursor.close()
            connection.close()
    
    def _generate_cache_key(self, query: str, params: Dict = None) -> str:
        """Generate cache key for cannabis data query"""
        key_data = {
            'query': query,
            'params': params or {},
            'timestamp': int(time.time() / 3600)  # Hour-based for some cache invalidation
        }
        key_string = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_string.encode()).hexdigest()
    
    def _compress_data(self, data: bytes) -> bytes:
        """Compress data if larger than threshold"""
        if len(data) > self.compression_threshold:
            return gzip.compress(data)
        return data
    
    def _decompress_data(self, data: bytes, compressed: bool) -> bytes:
        """Decompress data if it was compressed"""
        if compressed:
            return gzip.decompress(data)
        return data
    
    async def get(self, cache_key: str) -> Optional[Any]:
        """
        Retrieve cannabis data from cache
        
        Args:
            cache_key: Cache key to retrieve
            
        Returns:
            Cached data or None if not found/expired
        """
        start_time = time.time()
        connection = None
        
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor(dictionary=True)
            
            # Get cache entry
            query = """
            SELECT data_blob, compressed, expires_at, data_type, hit_count
            FROM cannabis_cache 
            WHERE cache_key = %s AND expires_at > NOW()
            """
            cursor.execute(query, (cache_key,))
            result = cursor.fetchone()
            
            if result:
                # Update hit count
                update_query = "UPDATE cannabis_cache SET hit_count = hit_count + 1 WHERE cache_key = %s"
                cursor.execute(update_query, (cache_key,))
                
                # Decompress and deserialize data
                data_blob = self._decompress_data(result['data_blob'], result['compressed'])
                data = pickle.loads(data_blob)
                
                self.stats['hits'] += 1
                response_time = (time.time() - start_time) * 1000
                
                # Log cache hit statistics
                await self._log_cache_operation('hit', result['data_type'], response_time, cache_key)
                
                logger.debug(f"Cache HIT for key {cache_key[:16]}... (type: {result['data_type']})")
                return data
            else:
                self.stats['misses'] += 1
                response_time = (time.time() - start_time) * 1000
                await self._log_cache_operation('miss', 'unknown', response_time, cache_key)
                logger.debug(f"Cache MISS for key {cache_key[:16]}...")
                return None
                
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            self.stats['errors'] += 1
            return None
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    async def set(self, 
                  cache_key: str, 
                  data: Any, 
                  data_type: CannabisDataType,
                  ttl_override: Optional[int] = None,
                  metadata: Optional[Dict] = None) -> bool:
        """
        Store cannabis data in cache
        
        Args:
            cache_key: Cache key
            data: Data to cache
            data_type: Type of cannabis data
            ttl_override: Override default TTL (seconds)
            metadata: Cannabis-specific metadata
            
        Returns:
            True if successful, False otherwise
        """
        start_time = time.time()
        connection = None
        
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor()
            
            # Serialize and compress data
            data_blob = pickle.dumps(data)
            original_size = len(data_blob)
            compressed_data = self._compress_data(data_blob)
            compressed = len(compressed_data) < len(data_blob)
            
            # Calculate expiration time
            ttl = ttl_override or self.ttl_mapping.get(data_type, 3600)
            expires_at = datetime.now() + timedelta(seconds=ttl)
            
            # Insert/update cache entry
            query = """
            INSERT INTO cannabis_cache 
            (cache_key, data_type, data_blob, compressed, expires_at, data_size, source_api, dispensary_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            data_blob = VALUES(data_blob),
            compressed = VALUES(compressed),
            expires_at = VALUES(expires_at),
            data_size = VALUES(data_size),
            hit_count = 0
            """
            
            cursor.execute(query, (
                cache_key,
                data_type.value,
                compressed_data,
                compressed,
                expires_at,
                original_size,
                metadata.get('source_api') if metadata else None,
                metadata.get('dispensary_id') if metadata else None
            ))
            
            # Insert cannabis metadata if provided
            if metadata:
                await self._insert_cannabis_metadata(cursor, cache_key, metadata)
            
            connection.commit()
            self.stats['sets'] += 1
            
            response_time = (time.time() - start_time) * 1000
            await self._log_cache_operation('set', data_type.value, response_time, cache_key)
            
            compression_ratio = (original_size - len(compressed_data)) / original_size * 100 if compressed else 0
            logger.debug(f"Cache SET for key {cache_key[:16]}... (type: {data_type.value}, "
                        f"size: {original_size}B, compression: {compression_ratio:.1f}%)")
            
            return True
            
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            self.stats['errors'] += 1
            return False
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    async def _insert_cannabis_metadata(self, cursor, cache_key: str, metadata: Dict):
        """Insert cannabis-specific metadata"""
        try:
            metadata_query = """
            INSERT INTO cannabis_metadata 
            (cache_key, strain_name, dispensary_name, product_category, thc_content, cbd_content, price_per_gram)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            strain_name = VALUES(strain_name),
            dispensary_name = VALUES(dispensary_name),
            product_category = VALUES(product_category),
            thc_content = VALUES(thc_content),
            cbd_content = VALUES(cbd_content),
            price_per_gram = VALUES(price_per_gram)
            """
            
            cursor.execute(metadata_query, (
                cache_key,
                metadata.get('strain_name'),
                metadata.get('dispensary_name'),
                metadata.get('product_category'),
                metadata.get('thc_content'),
                metadata.get('cbd_content'),
                metadata.get('price_per_gram')
            ))
        except Exception as e:
            logger.warning(f"Failed to insert cannabis metadata: {e}")
    
    async def _log_cache_operation(self, operation: str, data_type: str, response_time: float, cache_key: str):
        """Log cache operation statistics"""
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor()
            
            cache_key_hash = hashlib.sha256(cache_key.encode()).hexdigest()[:16]
            
            query = """
            INSERT INTO cache_statistics (data_type, operation, response_time_ms, cache_key_hash)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (data_type, operation, response_time, cache_key_hash))
            connection.commit()
            
        except Exception as e:
            logger.warning(f"Failed to log cache statistics: {e}")
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    async def delete(self, cache_key: str) -> bool:
        """Delete cache entry"""
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor()
            
            query = "DELETE FROM cannabis_cache WHERE cache_key = %s"
            cursor.execute(query, (cache_key,))
            connection.commit()
            
            self.stats['deletes'] += 1
            return cursor.rowcount > 0
            
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            self.stats['errors'] += 1
            return False
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    async def clear_expired(self) -> int:
        """Clear expired cache entries"""
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor()
            
            query = "DELETE FROM cannabis_cache WHERE expires_at < NOW()"
            cursor.execute(query)
            connection.commit()
            
            expired_count = cursor.rowcount
            logger.info(f"Cleared {expired_count} expired cache entries")
            return expired_count
            
        except Exception as e:
            logger.error(f"Cache cleanup error: {e}")
            return 0
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    async def get_cache_statistics(self) -> Dict:
        """Get comprehensive cache statistics"""
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor(dictionary=True)
            
            # Overall cache stats
            stats_query = """
            SELECT 
                COUNT(*) as total_entries,
                SUM(data_size) as total_size_bytes,
                AVG(hit_count) as avg_hit_count,
                SUM(CASE WHEN compressed = 1 THEN 1 ELSE 0 END) as compressed_entries
            FROM cannabis_cache
            WHERE expires_at > NOW()
            """
            cursor.execute(stats_query)
            overall_stats = cursor.fetchone()
            
            # Stats by data type
            type_stats_query = """
            SELECT 
                data_type,
                COUNT(*) as count,
                SUM(data_size) as total_size,
                AVG(hit_count) as avg_hits,
                MIN(created_at) as oldest_entry,
                MAX(expires_at) as latest_expiry
            FROM cannabis_cache
            WHERE expires_at > NOW()
            GROUP BY data_type
            """
            cursor.execute(type_stats_query)
            type_stats = cursor.fetchall()
            
            # Recent performance stats
            perf_stats_query = """
            SELECT 
                operation,
                COUNT(*) as count,
                AVG(response_time_ms) as avg_response_time,
                MAX(response_time_ms) as max_response_time
            FROM cache_statistics
            WHERE timestamp > DATE_SUB(NOW(), INTERVAL 1 HOUR)
            GROUP BY operation
            """
            cursor.execute(perf_stats_query)
            perf_stats = cursor.fetchall()
            
            return {
                'overall': overall_stats,
                'by_type': type_stats,
                'performance': perf_stats,
                'runtime_stats': self.stats
            }
            
        except Exception as e:
            logger.error(f"Error getting cache statistics: {e}")
            return {'error': str(e)}
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    async def _maintenance_loop(self):
        """Background maintenance tasks"""
        while True:
            try:
                # Clear expired entries every hour
                await asyncio.sleep(3600)
                await self.clear_expired()
                
                # Log cache statistics
                stats = await self.get_cache_statistics()
                if 'overall' in stats and stats['overall']:
                    total_entries = stats['overall']['total_entries']
                    total_size_mb = stats['overall']['total_size_bytes'] / (1024 * 1024) if stats['overall']['total_size_bytes'] else 0
                    logger.info(f"Cache maintenance: {total_entries} entries, {total_size_mb:.1f}MB total")
                
            except Exception as e:
                logger.error(f"Cache maintenance error: {e}")

# Cache decorators for easy integration
def cache_cannabis_data(data_type: CannabisDataType, ttl: Optional[int] = None):
    """Decorator to cache cannabis data function results"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Generate cache key from function name and arguments
            cache_key_data = {
                'function': func.__name__,
                'args': str(args),
                'kwargs': str(sorted(kwargs.items()))
            }
            cache_key = hashlib.sha256(str(cache_key_data).encode()).hexdigest()
            
            # Try to get from cache first
            cache = MySQLResultCache()
            await cache.initialize()
            
            cached_result = await cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Execute function and cache result
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            if result is not None:
                await cache.set(cache_key, result, data_type, ttl)
            
            return result
        
        return wrapper
    return decorator

# Example usage functions
@cache_cannabis_data(CannabisDataType.STRAIN_DATA, ttl=365*24*3600)
async def get_strain_data(strain_name: str) -> Dict:
    """Example: Get strain data with 1-year caching"""
    # This would integrate with your existing strain API calls
    return {"strain": strain_name, "data": "cached for 1 year"}

@cache_cannabis_data(CannabisDataType.DISPENSARY_MENU, ttl=3600)
async def get_dispensary_menu(dispensary_id: str) -> Dict:
    """Example: Get dispensary menu with 1-hour caching"""
    # This would integrate with your existing dispensary API calls
    return {"dispensary": dispensary_id, "menu": "cached for 1 hour"}

if __name__ == "__main__":
    # Example usage
    async def test_cache():
        cache = MySQLResultCache()
        await cache.initialize()
        
        # Test cannabis data caching
        test_data = {
            "strain": "Blue Dream",
            "thc": 18.5,
            "cbd": 0.2,
            "terpenes": ["myrcene", "limonene", "pinene"],
            "effects": ["relaxed", "happy", "euphoric"]
        }
        
        metadata = {
            "strain_name": "Blue Dream",
            "dispensary_name": "Test Dispensary",
            "product_category": "flower",
            "thc_content": 18.5,
            "cbd_content": 0.2,
            "price_per_gram": 12.50
        }
        
        cache_key = cache._generate_cache_key("strain_lookup", {"strain": "Blue Dream"})
        
        # Set cache
        success = await cache.set(cache_key, test_data, CannabisDataType.STRAIN_DATA, metadata=metadata)
        print(f"Cache set: {success}")
        
        # Get from cache
        cached_data = await cache.get(cache_key)
        print(f"Cached data: {cached_data}")
        
        # Get statistics
        stats = await cache.get_cache_statistics()
        print(f"Cache stats: {stats}")
    
    # Run test
    asyncio.run(test_cache())