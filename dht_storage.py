"""
DHT Distributed Storage System for WeedHounds
Implements unlimited peer-to-peer caching with intelligent data management
"""

import asyncio
import json
import logging
import time
import hashlib
import gzip
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import aiofiles
import aioredis
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataType(Enum):
    """Types of cannabis data we cache"""
    DUTCHIE_MENU = "dutchie_menu"
    JANE_MENU = "jane_menu"
    TERPENE_PROFILE = "terpene_profile"
    STRAIN_INFO = "strain_info"
    DISPENSARY_INFO = "dispensary_info"
    PRICE_HISTORY = "price_history"
    INVENTORY_STATUS = "inventory_status"
    PRODUCT_REVIEW = "product_review"

@dataclass
class CacheEntry:
    """Represents a cached data entry in the DHT"""
    key: str
    data_type: DataType
    value: Any
    timestamp: datetime
    ttl_hours: int
    source_node: str
    api_source: str  # 'dutchie', 'jane', etc.
    checksum: str
    access_count: int = 0
    last_accessed: Optional[datetime] = None
    data_size: int = 0
    compression: str = "gzip"
    
    def __post_init__(self):
        if self.last_accessed is None:
            self.last_accessed = self.timestamp
        
        # Calculate data size and checksum
        serialized = pickle.dumps(self.value)
        self.data_size = len(serialized)
        self.checksum = hashlib.sha256(serialized).hexdigest()
    
    @property
    def expires_at(self) -> datetime:
        return self.timestamp + timedelta(hours=self.ttl_hours)
    
    @property
    def is_expired(self) -> bool:
        return datetime.now() > self.expires_at
    
    @property
    def age_hours(self) -> float:
        return (datetime.now() - self.timestamp).total_seconds() / 3600
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['last_accessed'] = self.last_accessed.isoformat() if self.last_accessed else None
        data['data_type'] = self.data_type.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CacheEntry':
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        if data['last_accessed']:
            data['last_accessed'] = datetime.fromisoformat(data['last_accessed'])
        data['data_type'] = DataType(data['data_type'])
        return cls(**data)

class DHTStorage:
    """Distributed Hash Table Storage with unlimited peer caching"""
    
    def __init__(self, node_id: str, storage_path: str = "./dht_cache", 
                 max_storage_gb: float = 50.0):
        self.node_id = node_id
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(exist_ok=True)
        
        # Storage limits
        self.max_storage_bytes = int(max_storage_gb * 1024 * 1024 * 1024)  # Convert GB to bytes
        self.current_storage_bytes = 0
        
        # In-memory cache for hot data
        self.memory_cache: Dict[str, CacheEntry] = {}
        self.memory_cache_max_entries = 10000
        
        # Redis connection for distributed coordination
        self.redis = None
        
        # Peer network
        self.known_peers: Set[str] = set()
        self.peer_cache_status: Dict[str, Dict] = {}
        
        # Data retention policies
        self.default_ttl_hours = {
            DataType.DUTCHIE_MENU: 24,      # Menu data expires in 24 hours
            DataType.JANE_MENU: 24,         # Menu data expires in 24 hours
            DataType.TERPENE_PROFILE: 8760,  # Terpene data lasts 1 year
            DataType.STRAIN_INFO: 8760,     # Strain info lasts 1 year
            DataType.DISPENSARY_INFO: 720,  # Dispensary info lasts 30 days
            DataType.PRICE_HISTORY: 8760,  # Price history lasts 1 year
            DataType.INVENTORY_STATUS: 1,   # Inventory expires in 1 hour
            DataType.PRODUCT_REVIEW: 8760   # Reviews last 1 year
        }
        
        # Statistics
        self.stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'peer_requests': 0,
            'peer_responses': 0,
            'api_calls_saved': 0,
            'data_shared': 0,
            'storage_ops': 0
        }
    
    async def initialize(self, redis_url: str = "redis://localhost:6379"):
        """Initialize the storage system"""
        try:
            # Connect to Redis for coordination
            self.redis = await aioredis.from_url(redis_url)
            
            # Load existing cache from disk
            await self.load_cache_from_disk()
            
            # Register this node in the peer network
            await self.register_peer()
            
            # Start background maintenance tasks
            asyncio.create_task(self.maintenance_loop())
            asyncio.create_task(self.peer_discovery_loop())
            
            logger.info(f"DHT Storage initialized for node {self.node_id}")
            logger.info(f"Storage limit: {self.max_storage_bytes / (1024**3):.1f} GB")
            logger.info(f"Current storage: {self.current_storage_bytes / (1024**3):.2f} GB")
            
        except Exception as e:
            logger.error(f"Failed to initialize DHT storage: {e}")
            raise
    
    async def store(self, key: str, value: Any, data_type: DataType, 
                   api_source: str, ttl_hours: Optional[int] = None) -> bool:
        """Store data in the distributed cache"""
        try:
            if ttl_hours is None:
                ttl_hours = self.default_ttl_hours.get(data_type, 24)
            
            # Create cache entry
            entry = CacheEntry(
                key=key,
                data_type=data_type,
                value=value,
                timestamp=datetime.now(),
                ttl_hours=ttl_hours,
                source_node=self.node_id,
                api_source=api_source,
                checksum=""  # Will be calculated in __post_init__
            )
            
            # Check storage limits
            if not await self.check_storage_limits(entry.data_size):
                await self.cleanup_old_data()
                if not await self.check_storage_limits(entry.data_size):
                    logger.warning(f"Cannot store {key}: storage limit exceeded")
                    return False
            
            # Store in memory cache
            self.memory_cache[key] = entry
            
            # Store to disk
            await self.store_to_disk(entry)
            
            # Announce to peers
            await self.announce_to_peers(entry)
            
            # Update statistics
            self.stats['storage_ops'] += 1
            self.current_storage_bytes += entry.data_size
            
            logger.debug(f"Stored {key} ({data_type.value}) from {api_source}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store {key}: {e}")
            return False
    
    async def get(self, key: str, check_peers: bool = True) -> Optional[CacheEntry]:
        """Retrieve data from the distributed cache"""
        try:
            # Check memory cache first
            if key in self.memory_cache:
                entry = self.memory_cache[key]
                if not entry.is_expired:
                    entry.access_count += 1
                    entry.last_accessed = datetime.now()
                    self.stats['cache_hits'] += 1
                    logger.debug(f"Memory cache hit for {key}")
                    return entry
                else:
                    # Remove expired entry
                    del self.memory_cache[key]
            
            # Check disk cache
            entry = await self.load_from_disk(key)
            if entry and not entry.is_expired:
                # Load into memory cache
                self.memory_cache[key] = entry
                entry.access_count += 1
                entry.last_accessed = datetime.now()
                self.stats['cache_hits'] += 1
                logger.debug(f"Disk cache hit for {key}")
                return entry
            
            # Check peer network if enabled
            if check_peers:
                entry = await self.request_from_peers(key)
                if entry:
                    # Store locally for future use
                    self.memory_cache[key] = entry
                    await self.store_to_disk(entry)
                    self.stats['peer_responses'] += 1
                    self.stats['api_calls_saved'] += 1
                    logger.debug(f"Peer cache hit for {key}")
                    return entry
            
            # Cache miss
            self.stats['cache_misses'] += 1
            logger.debug(f"Cache miss for {key}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get {key}: {e}")
            return None
    
    async def request_from_peers(self, key: str) -> Optional[CacheEntry]:
        """Request data from peer nodes"""
        try:
            if not self.known_peers:
                return None
            
            self.stats['peer_requests'] += 1
            
            # Query multiple peers in parallel
            tasks = []
            for peer in list(self.known_peers)[:5]:  # Limit to 5 peers for efficiency
                tasks.append(self.query_peer(peer, key))
            
            if not tasks:
                return None
            
            # Wait for first successful response
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
            
            # Get the first successful result
            for task in done:
                try:
                    result = await task
                    if result:
                        logger.debug(f"Retrieved {key} from peer network")
                        return result
                except Exception as e:
                    logger.debug(f"Peer query failed: {e}")
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to request from peers: {e}")
            return None
    
    async def query_peer(self, peer_address: str, key: str) -> Optional[CacheEntry]:
        """Query a specific peer for data"""
        try:
            # This would be implemented with actual HTTP/gRPC calls to peers
            # For now, simulate with Redis
            if self.redis:
                peer_key = f"peer:{peer_address}:cache:{key}"
                data = await self.redis.get(peer_key)
                if data:
                    entry_dict = json.loads(data)
                    return CacheEntry.from_dict(entry_dict)
            return None
            
        except Exception as e:
            logger.debug(f"Failed to query peer {peer_address}: {e}")
            return None
    
    async def announce_to_peers(self, entry: CacheEntry):
        """Announce new data to peer network"""
        try:
            if self.redis:
                # Announce data availability to peer network
                announcement = {
                    'node_id': self.node_id,
                    'key': entry.key,
                    'data_type': entry.data_type.value,
                    'timestamp': entry.timestamp.isoformat(),
                    'ttl_hours': entry.ttl_hours,
                    'api_source': entry.api_source,
                    'checksum': entry.checksum,
                    'data_size': entry.data_size
                }
                
                await self.redis.publish('dht:announcements', json.dumps(announcement))
                logger.debug(f"Announced {entry.key} to peer network")
                
        except Exception as e:
            logger.error(f"Failed to announce to peers: {e}")
    
    async def store_to_disk(self, entry: CacheEntry):
        """Store cache entry to disk"""
        try:
            # Create directory structure by data type
            type_dir = self.storage_path / entry.data_type.value
            type_dir.mkdir(exist_ok=True)
            
            # Compress and store data
            compressed_data = gzip.compress(pickle.dumps(entry))
            
            file_path = type_dir / f"{entry.key}.cache"
            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(compressed_data)
                
        except Exception as e:
            logger.error(f"Failed to store to disk: {e}")
    
    async def load_from_disk(self, key: str) -> Optional[CacheEntry]:
        """Load cache entry from disk"""
        try:
            # Search across all data type directories
            for data_type in DataType:
                file_path = self.storage_path / data_type.value / f"{key}.cache"
                if file_path.exists():
                    async with aiofiles.open(file_path, 'rb') as f:
                        compressed_data = await f.read()
                        entry = pickle.loads(gzip.decompress(compressed_data))
                        return entry
            return None
            
        except Exception as e:
            logger.error(f"Failed to load from disk: {e}")
            return None
    
    async def load_cache_from_disk(self):
        """Load existing cache entries from disk"""
        try:
            total_size = 0
            loaded_count = 0
            
            for data_type in DataType:
                type_dir = self.storage_path / data_type.value
                if type_dir.exists():
                    for cache_file in type_dir.glob("*.cache"):
                        try:
                            async with aiofiles.open(cache_file, 'rb') as f:
                                compressed_data = await f.read()
                                entry = pickle.loads(gzip.decompress(compressed_data))
                                
                                if not entry.is_expired:
                                    self.memory_cache[entry.key] = entry
                                    total_size += entry.data_size
                                    loaded_count += 1
                                else:
                                    # Delete expired files
                                    cache_file.unlink()
                                    
                        except Exception as e:
                            logger.warning(f"Failed to load {cache_file}: {e}")
            
            self.current_storage_bytes = total_size
            logger.info(f"Loaded {loaded_count} cache entries from disk")
            logger.info(f"Total storage: {total_size / (1024**2):.1f} MB")
            
        except Exception as e:
            logger.error(f"Failed to load cache from disk: {e}")
    
    async def register_peer(self):
        """Register this node in the peer network"""
        try:
            if self.redis:
                peer_info = {
                    'node_id': self.node_id,
                    'last_seen': datetime.now().isoformat(),
                    'storage_capacity': self.max_storage_bytes,
                    'current_storage': self.current_storage_bytes,
                    'cache_entries': len(self.memory_cache)
                }
                
                await self.redis.hset('dht:peers', self.node_id, json.dumps(peer_info))
                await self.redis.expire('dht:peers', 300)  # 5 minute TTL
                
        except Exception as e:
            logger.error(f"Failed to register peer: {e}")
    
    async def discover_peers(self) -> List[str]:
        """Discover other peers in the network"""
        try:
            if self.redis:
                peers = await self.redis.hgetall('dht:peers')
                discovered = []
                
                for peer_id, peer_data in peers.items():
                    if peer_id.decode() != self.node_id:
                        peer_info = json.loads(peer_data.decode())
                        discovered.append(peer_id.decode())
                        self.peer_cache_status[peer_id.decode()] = peer_info
                
                self.known_peers.update(discovered)
                return discovered
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to discover peers: {e}")
            return []
    
    async def check_storage_limits(self, additional_bytes: int) -> bool:
        """Check if we can store additional data"""
        return (self.current_storage_bytes + additional_bytes) <= self.max_storage_bytes
    
    async def cleanup_old_data(self):
        """Clean up old and least-used data"""
        try:
            # Sort entries by access patterns and age
            entries = list(self.memory_cache.values())
            
            # Priority cleanup: expired data first
            expired_keys = [entry.key for entry in entries if entry.is_expired]
            for key in expired_keys:
                await self.remove_entry(key)
            
            # If still over limit, remove least recently used
            if self.current_storage_bytes > self.max_storage_bytes * 0.9:
                entries = [entry for entry in entries if not entry.is_expired]
                entries.sort(key=lambda x: (x.access_count, x.last_accessed))
                
                # Remove 10% of entries
                remove_count = max(1, len(entries) // 10)
                for entry in entries[:remove_count]:
                    await self.remove_entry(entry.key)
            
            logger.info(f"Cleanup complete. Storage: {self.current_storage_bytes / (1024**2):.1f} MB")
            
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
    
    async def remove_entry(self, key: str):
        """Remove an entry from cache and disk"""
        try:
            # Remove from memory
            if key in self.memory_cache:
                entry = self.memory_cache[key]
                self.current_storage_bytes -= entry.data_size
                del self.memory_cache[key]
            
            # Remove from disk
            for data_type in DataType:
                file_path = self.storage_path / data_type.value / f"{key}.cache"
                if file_path.exists():
                    file_path.unlink()
                    break
                    
        except Exception as e:
            logger.error(f"Failed to remove entry {key}: {e}")
    
    async def maintenance_loop(self):
        """Background maintenance tasks"""
        while True:
            try:
                # Clean up expired entries
                await self.cleanup_old_data()
                
                # Update peer registration
                await self.register_peer()
                
                # Compress old data
                await self.compress_old_data()
                
                await asyncio.sleep(300)  # Run every 5 minutes
                
            except Exception as e:
                logger.error(f"Maintenance loop error: {e}")
                await asyncio.sleep(60)
    
    async def peer_discovery_loop(self):
        """Background peer discovery"""
        while True:
            try:
                new_peers = await self.discover_peers()
                if new_peers:
                    logger.info(f"Discovered {len(new_peers)} peers")
                
                await asyncio.sleep(60)  # Discover every minute
                
            except Exception as e:
                logger.error(f"Peer discovery error: {e}")
                await asyncio.sleep(60)
    
    async def compress_old_data(self):
        """Compress older data to save space"""
        try:
            # Find data older than 1 week for compression
            week_ago = datetime.now() - timedelta(days=7)
            
            for entry in self.memory_cache.values():
                if entry.timestamp < week_ago and entry.compression != "gzip_high":
                    # Re-compress with higher compression
                    entry.compression = "gzip_high"
                    await self.store_to_disk(entry)
                    
        except Exception as e:
            logger.error(f"Failed to compress old data: {e}")
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get detailed storage statistics"""
        data_type_stats = {}
        total_entries = 0
        
        for data_type in DataType:
            count = sum(1 for entry in self.memory_cache.values() 
                       if entry.data_type == data_type)
            data_type_stats[data_type.value] = count
            total_entries += count
        
        return {
            'node_id': self.node_id,
            'storage_stats': {
                'total_entries': total_entries,
                'memory_cache_entries': len(self.memory_cache),
                'current_storage_mb': self.current_storage_bytes / (1024**2),
                'max_storage_mb': self.max_storage_bytes / (1024**2),
                'storage_usage_percent': (self.current_storage_bytes / self.max_storage_bytes) * 100,
                'data_type_breakdown': data_type_stats
            },
            'peer_network': {
                'known_peers': len(self.known_peers),
                'peer_list': list(self.known_peers)
            },
            'performance_stats': self.stats,
            'cache_efficiency': {
                'hit_rate': (self.stats['cache_hits'] / 
                           max(self.stats['cache_hits'] + self.stats['cache_misses'], 1)) * 100,
                'api_calls_saved': self.stats['api_calls_saved'],
                'peer_success_rate': (self.stats['peer_responses'] / 
                                    max(self.stats['peer_requests'], 1)) * 100
            }
        }

# Utility functions for key generation
def generate_cache_key(data_type: DataType, identifier: str, **kwargs) -> str:
    """Generate a consistent cache key for data"""
    key_parts = [data_type.value, identifier]
    
    # Add relevant parameters based on data type
    if data_type in [DataType.DUTCHIE_MENU, DataType.JANE_MENU]:
        key_parts.extend([
            kwargs.get('menu_type', ''),
            kwargs.get('location', ''),
            datetime.now().strftime('%Y%m%d%H')  # Hour-based for freshness
        ])
    elif data_type == DataType.TERPENE_PROFILE:
        key_parts.extend([
            kwargs.get('strain_name', ''),
            kwargs.get('lab', '')
        ])
    elif data_type == DataType.PRICE_HISTORY:
        key_parts.extend([
            kwargs.get('product_id', ''),
            kwargs.get('timeframe', 'daily')
        ])
    
    # Create hash of key parts for consistent length
    key_string = ':'.join(str(part) for part in key_parts if part)
    return hashlib.md5(key_string.encode()).hexdigest()

def estimate_data_freshness_hours(data_type: DataType) -> int:
    """Estimate how fresh data should be for different types"""
    freshness_map = {
        DataType.DUTCHIE_MENU: 2,       # Very fresh menu data
        DataType.JANE_MENU: 2,          # Very fresh menu data  
        DataType.INVENTORY_STATUS: 1,   # Extremely fresh inventory
        DataType.PRICE_HISTORY: 24,     # Daily price updates
        DataType.TERPENE_PROFILE: 168,  # Weekly terpene updates
        DataType.STRAIN_INFO: 720,      # Monthly strain info updates
        DataType.DISPENSARY_INFO: 168,  # Weekly dispensary updates
        DataType.PRODUCT_REVIEW: 24     # Daily review updates
    }
    return freshness_map.get(data_type, 24)