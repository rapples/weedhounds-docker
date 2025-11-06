"""
High-Performance Data Serialization for Cannabis Data
Optimized serialization/deserialization for unlimited peer-to-peer network
Supports msgpack, protocol buffers, and custom binary formats
"""

import asyncio
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple, Type
from dataclasses import dataclass, asdict
from enum import Enum
import hashlib
import struct
import zlib
import pickle
from abc import ABC, abstractmethod

# Try to import optional high-performance serialization libraries
try:
    import msgpack
    MSGPACK_AVAILABLE = True
except ImportError:
    msgpack = None
    MSGPACK_AVAILABLE = False

try:
    import orjson
    ORJSON_AVAILABLE = True
except ImportError:
    orjson = None
    ORJSON_AVAILABLE = False

try:
    import lz4.frame
    LZ4_AVAILABLE = True
except ImportError:
    lz4 = None
    LZ4_AVAILABLE = False

try:
    import brotli
    BROTLI_AVAILABLE = True
except ImportError:
    brotli = None
    BROTLI_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SerializationFormat(Enum):
    """Supported serialization formats"""
    JSON = "json"
    ORJSON = "orjson"
    MSGPACK = "msgpack"
    PICKLE = "pickle"
    CUSTOM_BINARY = "custom_binary"
    PROTOBUF = "protobuf"

class CompressionType(Enum):
    """Supported compression types"""
    NONE = "none"
    ZLIB = "zlib"
    LZ4 = "lz4"
    BROTLI = "brotli"
    GZIP = "gzip"

class CannabisDataType(Enum):
    """Cannabis data types for optimized serialization"""
    STRAIN_DATA = "strain_data"
    DISPENSARY_MENU = "dispensary_menu"
    PRODUCT_PRICING = "product_pricing"
    TERPENE_PROFILES = "terpene_profiles"
    LAB_RESULTS = "lab_results"
    DISPENSARY_INFO = "dispensary_info"
    ANALYTICS_DATA = "analytics_data"
    PROMOTIONS = "promotions"

@dataclass
class SerializationMetrics:
    """Performance metrics for serialization operations"""
    format_used: SerializationFormat
    compression_used: CompressionType
    original_size: int
    serialized_size: int
    compression_ratio: float
    serialize_time: float
    deserialize_time: float
    memory_usage: int

class SerializationStrategy(ABC):
    """Abstract base class for serialization strategies"""
    
    @abstractmethod
    async def serialize(self, data: Any) -> bytes:
        """Serialize data to bytes"""
        pass
    
    @abstractmethod
    async def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to data"""
        pass
    
    @abstractmethod
    def get_format(self) -> SerializationFormat:
        """Get serialization format"""
        pass

class JSONStrategy(SerializationStrategy):
    """Standard JSON serialization"""
    
    def __init__(self, ensure_ascii: bool = False):
        self.ensure_ascii = ensure_ascii
    
    async def serialize(self, data: Any) -> bytes:
        try:
            json_str = json.dumps(data, ensure_ascii=self.ensure_ascii, separators=(',', ':'))
            return json_str.encode('utf-8')
        except Exception as e:
            logger.error(f"JSON serialization error: {e}")
            raise
    
    async def deserialize(self, data: bytes) -> Any:
        try:
            json_str = data.decode('utf-8')
            return json.loads(json_str)
        except Exception as e:
            logger.error(f"JSON deserialization error: {e}")
            raise
    
    def get_format(self) -> SerializationFormat:
        return SerializationFormat.JSON

class OrjsonStrategy(SerializationStrategy):
    """High-performance orjson serialization"""
    
    def __init__(self):
        if not ORJSON_AVAILABLE:
            raise ImportError("orjson not available")
    
    async def serialize(self, data: Any) -> bytes:
        try:
            return orjson.dumps(data)
        except Exception as e:
            logger.error(f"Orjson serialization error: {e}")
            raise
    
    async def deserialize(self, data: bytes) -> Any:
        try:
            return orjson.loads(data)
        except Exception as e:
            logger.error(f"Orjson deserialization error: {e}")
            raise
    
    def get_format(self) -> SerializationFormat:
        return SerializationFormat.ORJSON

class MsgpackStrategy(SerializationStrategy):
    """MessagePack serialization for binary efficiency"""
    
    def __init__(self, use_bin_type: bool = True):
        if not MSGPACK_AVAILABLE:
            raise ImportError("msgpack not available")
        self.use_bin_type = use_bin_type
    
    async def serialize(self, data: Any) -> bytes:
        try:
            return msgpack.packb(data, use_bin_type=self.use_bin_type)
        except Exception as e:
            logger.error(f"Msgpack serialization error: {e}")
            raise
    
    async def deserialize(self, data: bytes) -> Any:
        try:
            return msgpack.unpackb(data, raw=False)
        except Exception as e:
            logger.error(f"Msgpack deserialization error: {e}")
            raise
    
    def get_format(self) -> SerializationFormat:
        return SerializationFormat.MSGPACK

class PickleStrategy(SerializationStrategy):
    """Python pickle serialization (fastest but Python-only)"""
    
    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL):
        self.protocol = protocol
    
    async def serialize(self, data: Any) -> bytes:
        try:
            return pickle.dumps(data, protocol=self.protocol)
        except Exception as e:
            logger.error(f"Pickle serialization error: {e}")
            raise
    
    async def deserialize(self, data: bytes) -> Any:
        try:
            return pickle.loads(data)
        except Exception as e:
            logger.error(f"Pickle deserialization error: {e}")
            raise
    
    def get_format(self) -> SerializationFormat:
        return SerializationFormat.PICKLE

class CustomBinaryStrategy(SerializationStrategy):
    """Custom binary format optimized for cannabis data"""
    
    def __init__(self):
        # Cannabis data field mappings for binary optimization
        self.field_mappings = {
            # Common fields with short codes
            'id': b'\x01',
            'name': b'\x02',
            'strain': b'\x03',
            'thc': b'\x04',
            'cbd': b'\x05',
            'price': b'\x06',
            'quantity': b'\x07',
            'dispensary': b'\x08',
            'terpenes': b'\x09',
            'effects': b'\x0A',
            'flavors': b'\x0B',
            'category': b'\x0C',
            'brand': b'\x0D',
            'description': b'\x0E',
            'lab_results': b'\x0F',
            'availability': b'\x10',
            'updated_at': b'\x11',
            'created_at': b'\x12'
        }
        
        # Reverse mapping for deserialization
        self.reverse_mappings = {v: k for k, v in self.field_mappings.items()}
        
        # Data type codes
        self.type_codes = {
            str: b'\x20',
            int: b'\x21',
            float: b'\x22',
            bool: b'\x23',
            list: b'\x24',
            dict: b'\x25',
            type(None): b'\x26'
        }
        
        self.reverse_type_codes = {v: k for k, v in self.type_codes.items()}
    
    async def serialize(self, data: Any) -> bytes:
        """Serialize to custom binary format"""
        try:
            buffer = bytearray()
            buffer.extend(b'WEED')  # Magic header
            buffer.extend(struct.pack('!H', 1))  # Version
            
            self._serialize_value(data, buffer)
            
            return bytes(buffer)
        except Exception as e:
            logger.error(f"Custom binary serialization error: {e}")
            raise
    
    def _serialize_value(self, value: Any, buffer: bytearray):
        """Serialize a single value to buffer"""
        value_type = type(value)
        
        if value_type in self.type_codes:
            buffer.extend(self.type_codes[value_type])
        else:
            buffer.extend(self.type_codes[str])  # Fallback to string
        
        if value is None:
            pass  # No data for None
        elif isinstance(value, bool):
            buffer.extend(struct.pack('!?', value))
        elif isinstance(value, int):
            # Use variable-length encoding for integers
            self._encode_varint(value, buffer)
        elif isinstance(value, float):
            buffer.extend(struct.pack('!d', value))
        elif isinstance(value, str):
            encoded = value.encode('utf-8')
            self._encode_varint(len(encoded), buffer)
            buffer.extend(encoded)
        elif isinstance(value, list):
            self._encode_varint(len(value), buffer)
            for item in value:
                self._serialize_value(item, buffer)
        elif isinstance(value, dict):
            self._encode_varint(len(value), buffer)
            for key, val in value.items():
                # Use field mapping if available
                if key in self.field_mappings:
                    buffer.extend(self.field_mappings[key])
                else:
                    # Fallback to string key
                    self._serialize_value(key, buffer)
                self._serialize_value(val, buffer)
        else:
            # Fallback to string representation
            self._serialize_value(str(value), buffer)
    
    def _encode_varint(self, value: int, buffer: bytearray):
        """Encode integer using variable-length encoding"""
        if value < 0:
            raise ValueError("Negative values not supported in varint")
        
        while value >= 128:
            buffer.append((value & 0x7F) | 0x80)
            value >>= 7
        buffer.append(value & 0x7F)
    
    async def deserialize(self, data: bytes) -> Any:
        """Deserialize from custom binary format"""
        try:
            if len(data) < 6:
                raise ValueError("Data too short")
            
            if data[:4] != b'WEED':
                raise ValueError("Invalid magic header")
            
            version = struct.unpack('!H', data[4:6])[0]
            if version != 1:
                raise ValueError(f"Unsupported version: {version}")
            
            offset = [6]  # Use list for reference passing
            return self._deserialize_value(data, offset)
        except Exception as e:
            logger.error(f"Custom binary deserialization error: {e}")
            raise
    
    def _deserialize_value(self, data: bytes, offset: List[int]) -> Any:
        """Deserialize a single value from data"""
        if offset[0] >= len(data):
            raise ValueError("Unexpected end of data")
        
        type_code = data[offset[0]:offset[0]+1]
        offset[0] += 1
        
        if type_code not in self.reverse_type_codes:
            raise ValueError(f"Unknown type code: {type_code}")
        
        value_type = self.reverse_type_codes[type_code]
        
        if value_type == type(None):
            return None
        elif value_type == bool:
            value = struct.unpack('!?', data[offset[0]:offset[0]+1])[0]
            offset[0] += 1
            return value
        elif value_type == int:
            return self._decode_varint(data, offset)
        elif value_type == float:
            value = struct.unpack('!d', data[offset[0]:offset[0]+8])[0]
            offset[0] += 8
            return value
        elif value_type == str:
            length = self._decode_varint(data, offset)
            value = data[offset[0]:offset[0]+length].decode('utf-8')
            offset[0] += length
            return value
        elif value_type == list:
            length = self._decode_varint(data, offset)
            return [self._deserialize_value(data, offset) for _ in range(length)]
        elif value_type == dict:
            length = self._decode_varint(data, offset)
            result = {}
            for _ in range(length):
                # Check if it's a field mapping
                key_byte = data[offset[0]:offset[0]+1]
                if key_byte in self.reverse_mappings:
                    key = self.reverse_mappings[key_byte]
                    offset[0] += 1
                else:
                    key = self._deserialize_value(data, offset)
                
                value = self._deserialize_value(data, offset)
                result[key] = value
            return result
        else:
            raise ValueError(f"Unsupported type: {value_type}")
    
    def _decode_varint(self, data: bytes, offset: List[int]) -> int:
        """Decode variable-length integer"""
        result = 0
        shift = 0
        
        while offset[0] < len(data):
            byte = data[offset[0]]
            offset[0] += 1
            
            result |= (byte & 0x7F) << shift
            
            if (byte & 0x80) == 0:
                break
            
            shift += 7
            
            if shift >= 64:
                raise ValueError("Varint too long")
        
        return result
    
    def get_format(self) -> SerializationFormat:
        return SerializationFormat.CUSTOM_BINARY

class CompressionManager:
    """Manages data compression for serialized data"""
    
    def __init__(self):
        self.compression_methods = {
            CompressionType.NONE: (self._no_compression, self._no_decompression),
            CompressionType.ZLIB: (self._zlib_compress, self._zlib_decompress),
            CompressionType.GZIP: (self._gzip_compress, self._gzip_decompress)
        }
        
        if LZ4_AVAILABLE:
            self.compression_methods[CompressionType.LZ4] = (self._lz4_compress, self._lz4_decompress)
        
        if BROTLI_AVAILABLE:
            self.compression_methods[CompressionType.BROTLI] = (self._brotli_compress, self._brotli_decompress)
    
    def compress(self, data: bytes, compression_type: CompressionType) -> bytes:
        """Compress data using specified method"""
        if compression_type not in self.compression_methods:
            raise ValueError(f"Unsupported compression type: {compression_type}")
        
        compress_func, _ = self.compression_methods[compression_type]
        return compress_func(data)
    
    def decompress(self, data: bytes, compression_type: CompressionType) -> bytes:
        """Decompress data using specified method"""
        if compression_type not in self.compression_methods:
            raise ValueError(f"Unsupported compression type: {compression_type}")
        
        _, decompress_func = self.compression_methods[compression_type]
        return decompress_func(data)
    
    def _no_compression(self, data: bytes) -> bytes:
        return data
    
    def _no_decompression(self, data: bytes) -> bytes:
        return data
    
    def _zlib_compress(self, data: bytes) -> bytes:
        return zlib.compress(data, level=6)
    
    def _zlib_decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)
    
    def _gzip_compress(self, data: bytes) -> bytes:
        import gzip
        return gzip.compress(data, compresslevel=6)
    
    def _gzip_decompress(self, data: bytes) -> bytes:
        import gzip
        return gzip.decompress(data)
    
    def _lz4_compress(self, data: bytes) -> bytes:
        return lz4.frame.compress(data)
    
    def _lz4_decompress(self, data: bytes) -> bytes:
        return lz4.frame.decompress(data)
    
    def _brotli_compress(self, data: bytes) -> bytes:
        return brotli.compress(data, quality=6)
    
    def _brotli_decompress(self, data: bytes) -> bytes:
        return brotli.decompress(data)

class OptimizedSerializer:
    """
    High-performance serializer for cannabis data with automatic format selection
    """
    
    def __init__(self, 
                 compression_threshold: int = 1024,
                 prefer_speed: bool = True):
        """
        Initialize optimized serializer
        
        Args:
            compression_threshold: Minimum size in bytes to apply compression
            prefer_speed: Prefer speed over compression ratio
        """
        self.compression_threshold = compression_threshold
        self.prefer_speed = prefer_speed
        
        # Initialize available strategies
        self.strategies = {}
        self._init_strategies()
        
        # Initialize compression manager
        self.compression_manager = CompressionManager()
        
        # Performance tracking
        self.stats = {
            'total_operations': 0,
            'serializations': 0,
            'deserializations': 0,
            'compression_savings': 0,
            'format_usage': {},
            'compression_usage': {},
            'avg_serialize_time': 0.0,
            'avg_deserialize_time': 0.0
        }
        
        # Cannabis data type optimization preferences
        self.data_type_preferences = {
            CannabisDataType.STRAIN_DATA: {
                'format': SerializationFormat.MSGPACK,
                'compression': CompressionType.ZLIB
            },
            CannabisDataType.DISPENSARY_MENU: {
                'format': SerializationFormat.CUSTOM_BINARY,
                'compression': CompressionType.LZ4 if LZ4_AVAILABLE else CompressionType.ZLIB
            },
            CannabisDataType.PRODUCT_PRICING: {
                'format': SerializationFormat.CUSTOM_BINARY,
                'compression': CompressionType.NONE  # Often small data
            },
            CannabisDataType.TERPENE_PROFILES: {
                'format': SerializationFormat.MSGPACK,
                'compression': CompressionType.BROTLI if BROTLI_AVAILABLE else CompressionType.ZLIB
            },
            CannabisDataType.LAB_RESULTS: {
                'format': SerializationFormat.MSGPACK,
                'compression': CompressionType.ZLIB
            },
            CannabisDataType.ANALYTICS_DATA: {
                'format': SerializationFormat.ORJSON if ORJSON_AVAILABLE else SerializationFormat.JSON,
                'compression': CompressionType.LZ4 if LZ4_AVAILABLE else CompressionType.ZLIB
            }
        }
    
    def _init_strategies(self):
        """Initialize available serialization strategies"""
        # Always available
        self.strategies[SerializationFormat.JSON] = JSONStrategy()
        self.strategies[SerializationFormat.PICKLE] = PickleStrategy()
        self.strategies[SerializationFormat.CUSTOM_BINARY] = CustomBinaryStrategy()
        
        # Optional high-performance libraries
        if ORJSON_AVAILABLE:
            self.strategies[SerializationFormat.ORJSON] = OrjsonStrategy()
        
        if MSGPACK_AVAILABLE:
            self.strategies[SerializationFormat.MSGPACK] = MsgpackStrategy()
        
        logger.info(f"Initialized {len(self.strategies)} serialization strategies")
        logger.info(f"Available formats: {list(self.strategies.keys())}")
    
    async def serialize(self, 
                       data: Any, 
                       data_type: Optional[CannabisDataType] = None,
                       format_override: Optional[SerializationFormat] = None,
                       compression_override: Optional[CompressionType] = None) -> Tuple[bytes, SerializationMetrics]:
        """
        Serialize data with optimal format and compression
        
        Args:
            data: Data to serialize
            data_type: Cannabis data type for optimization
            format_override: Force specific serialization format
            compression_override: Force specific compression
            
        Returns:
            Tuple of serialized bytes and performance metrics
        """
        start_time = time.time()
        self.stats['total_operations'] += 1
        self.stats['serializations'] += 1
        
        try:
            # Select optimal format
            format_to_use = format_override or self._select_optimal_format(data, data_type)
            
            if format_to_use not in self.strategies:
                logger.warning(f"Format {format_to_use} not available, falling back to JSON")
                format_to_use = SerializationFormat.JSON
            
            # Serialize data
            strategy = self.strategies[format_to_use]
            serialized_data = await strategy.serialize(data)
            original_size = len(serialized_data)
            
            # Select and apply compression
            compression_to_use = compression_override or self._select_optimal_compression(
                serialized_data, data_type
            )
            
            compressed_data = self.compression_manager.compress(serialized_data, compression_to_use)
            final_size = len(compressed_data)
            
            # Calculate metrics
            serialize_time = time.time() - start_time
            compression_ratio = (original_size - final_size) / original_size if original_size > 0 else 0
            
            metrics = SerializationMetrics(
                format_used=format_to_use,
                compression_used=compression_to_use,
                original_size=original_size,
                serialized_size=final_size,
                compression_ratio=compression_ratio,
                serialize_time=serialize_time,
                deserialize_time=0.0,
                memory_usage=final_size
            )
            
            # Update statistics
            self._update_stats(metrics, 'serialize')
            
            # Create final payload with metadata
            payload = self._create_payload(compressed_data, format_to_use, compression_to_use)
            
            logger.debug(f"Serialized {original_size}B -> {final_size}B "
                        f"({compression_ratio:.1%} compression) using {format_to_use.value}/{compression_to_use.value}")
            
            return payload, metrics
            
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise
    
    async def deserialize(self, data: bytes) -> Tuple[Any, SerializationMetrics]:
        """
        Deserialize data with automatic format detection
        
        Args:
            data: Serialized data bytes
            
        Returns:
            Tuple of deserialized data and performance metrics
        """
        start_time = time.time()
        self.stats['total_operations'] += 1
        self.stats['deserializations'] += 1
        
        try:
            # Parse payload metadata
            payload_data, format_used, compression_used = self._parse_payload(data)
            
            # Decompress data
            decompressed_data = self.compression_manager.decompress(payload_data, compression_used)
            
            # Deserialize data
            if format_used not in self.strategies:
                raise ValueError(f"Unsupported format: {format_used}")
            
            strategy = self.strategies[format_used]
            deserialized_data = await strategy.deserialize(decompressed_data)
            
            # Calculate metrics
            deserialize_time = time.time() - start_time
            
            metrics = SerializationMetrics(
                format_used=format_used,
                compression_used=compression_used,
                original_size=len(decompressed_data),
                serialized_size=len(data),
                compression_ratio=0.0,  # Not applicable for deserialization
                serialize_time=0.0,
                deserialize_time=deserialize_time,
                memory_usage=len(decompressed_data)
            )
            
            # Update statistics
            self._update_stats(metrics, 'deserialize')
            
            logger.debug(f"Deserialized {len(data)}B using {format_used.value}/{compression_used.value} "
                        f"in {deserialize_time:.3f}s")
            
            return deserialized_data, metrics
            
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise
    
    def _select_optimal_format(self, data: Any, data_type: Optional[CannabisDataType]) -> SerializationFormat:
        """Select optimal serialization format"""
        # Use data type preference if available
        if data_type and data_type in self.data_type_preferences:
            preferred_format = self.data_type_preferences[data_type]['format']
            if preferred_format in self.strategies:
                return preferred_format
        
        # Automatic selection based on data characteristics
        if self.prefer_speed:
            # Prefer faster formats
            if SerializationFormat.PICKLE in self.strategies:
                return SerializationFormat.PICKLE
            elif SerializationFormat.ORJSON in self.strategies:
                return SerializationFormat.ORJSON
            elif SerializationFormat.MSGPACK in self.strategies:
                return SerializationFormat.MSGPACK
        else:
            # Prefer smaller output
            if SerializationFormat.CUSTOM_BINARY in self.strategies:
                return SerializationFormat.CUSTOM_BINARY
            elif SerializationFormat.MSGPACK in self.strategies:
                return SerializationFormat.MSGPACK
        
        # Fallback to JSON
        return SerializationFormat.JSON
    
    def _select_optimal_compression(self, 
                                   data: bytes, 
                                   data_type: Optional[CannabisDataType]) -> CompressionType:
        """Select optimal compression method"""
        # Skip compression for small data
        if len(data) < self.compression_threshold:
            return CompressionType.NONE
        
        # Use data type preference if available
        if data_type and data_type in self.data_type_preferences:
            preferred_compression = self.data_type_preferences[data_type]['compression']
            if preferred_compression in self.compression_manager.compression_methods:
                return preferred_compression
        
        # Automatic selection based on performance preferences
        if self.prefer_speed:
            if LZ4_AVAILABLE:
                return CompressionType.LZ4
            else:
                return CompressionType.ZLIB
        else:
            if BROTLI_AVAILABLE:
                return CompressionType.BROTLI
            else:
                return CompressionType.ZLIB
    
    def _create_payload(self, 
                       data: bytes, 
                       format_used: SerializationFormat, 
                       compression_used: CompressionType) -> bytes:
        """Create payload with metadata header"""
        # Create header: magic(4) + version(1) + format(1) + compression(1) + length(4)
        header = bytearray()
        header.extend(b'WEED')  # Magic
        header.append(1)  # Version
        header.append(list(SerializationFormat).index(format_used))  # Format index
        header.append(list(CompressionType).index(compression_used))  # Compression index
        header.extend(struct.pack('!I', len(data)))  # Data length
        
        return bytes(header) + data
    
    def _parse_payload(self, data: bytes) -> Tuple[bytes, SerializationFormat, CompressionType]:
        """Parse payload metadata"""
        if len(data) < 11:
            raise ValueError("Invalid payload: too short")
        
        if data[:4] != b'WEED':
            raise ValueError("Invalid payload: bad magic")
        
        version = data[4]
        if version != 1:
            raise ValueError(f"Unsupported version: {version}")
        
        format_index = data[5]
        compression_index = data[6]
        data_length = struct.unpack('!I', data[7:11])[0]
        
        if len(data) < 11 + data_length:
            raise ValueError("Invalid payload: truncated data")
        
        format_used = list(SerializationFormat)[format_index]
        compression_used = list(CompressionType)[compression_index]
        payload_data = data[11:11 + data_length]
        
        return payload_data, format_used, compression_used
    
    def _update_stats(self, metrics: SerializationMetrics, operation: str):
        """Update performance statistics"""
        format_name = metrics.format_used.value
        compression_name = metrics.compression_used.value
        
        # Update format usage
        if format_name not in self.stats['format_usage']:
            self.stats['format_usage'][format_name] = 0
        self.stats['format_usage'][format_name] += 1
        
        # Update compression usage
        if compression_name not in self.stats['compression_usage']:
            self.stats['compression_usage'][compression_name] = 0
        self.stats['compression_usage'][compression_name] += 1
        
        # Update timing averages
        if operation == 'serialize':
            current_avg = self.stats['avg_serialize_time']
            count = self.stats['serializations']
            self.stats['avg_serialize_time'] = (
                (current_avg * (count - 1) + metrics.serialize_time) / count
            )
        elif operation == 'deserialize':
            current_avg = self.stats['avg_deserialize_time']
            count = self.stats['deserializations']
            self.stats['avg_deserialize_time'] = (
                (current_avg * (count - 1) + metrics.deserialize_time) / count
            )
        
        # Update compression savings
        if metrics.compression_ratio > 0:
            savings = metrics.original_size - metrics.serialized_size
            self.stats['compression_savings'] += savings
    
    def get_statistics(self) -> Dict:
        """Get serialization performance statistics"""
        return {
            'operations': self.stats,
            'available_formats': list(self.strategies.keys()),
            'available_compression': list(self.compression_manager.compression_methods.keys()),
            'data_type_preferences': {dt.value: prefs for dt, prefs in self.data_type_preferences.items()}
        }
    
    async def benchmark_formats(self, test_data: Any, iterations: int = 100) -> Dict:
        """Benchmark different serialization formats"""
        results = {}
        
        for format_name, strategy in self.strategies.items():
            try:
                # Time serialization
                start_time = time.time()
                for _ in range(iterations):
                    serialized = await strategy.serialize(test_data)
                serialize_time = (time.time() - start_time) / iterations
                
                # Time deserialization
                start_time = time.time()
                for _ in range(iterations):
                    await strategy.deserialize(serialized)
                deserialize_time = (time.time() - start_time) / iterations
                
                results[format_name.value] = {
                    'serialize_time': serialize_time,
                    'deserialize_time': deserialize_time,
                    'serialized_size': len(serialized),
                    'total_time': serialize_time + deserialize_time
                }
                
            except Exception as e:
                results[format_name.value] = {'error': str(e)}
        
        return results

# Performance-optimized cannabis data classes
@dataclass
class CannabisProduct:
    """Optimized cannabis product data structure"""
    id: str
    name: str
    strain: str
    category: str
    thc: float
    cbd: float
    price: float
    quantity: int
    brand: str
    description: str
    terpenes: List[str]
    effects: List[str]
    flavors: List[str]
    lab_results: Dict[str, Any]
    availability: bool
    updated_at: datetime
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        data = asdict(self)
        # Convert datetime to ISO string
        data['updated_at'] = self.updated_at.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CannabisProduct':
        """Create from dictionary after deserialization"""
        # Convert ISO string back to datetime
        if isinstance(data['updated_at'], str):
            data['updated_at'] = datetime.fromisoformat(data['updated_at'])
        return cls(**data)

# Example usage functions
async def serialize_cannabis_data(serializer: OptimizedSerializer, 
                                 product: CannabisProduct) -> bytes:
    """Example: Serialize cannabis product data"""
    data, metrics = await serializer.serialize(
        product.to_dict(),
        CannabisDataType.DISPENSARY_MENU
    )
    logger.info(f"Serialized product: {metrics.compression_ratio:.1%} compression, "
               f"{metrics.serialize_time:.3f}s")
    return data

async def deserialize_cannabis_data(serializer: OptimizedSerializer, 
                                   data: bytes) -> CannabisProduct:
    """Example: Deserialize cannabis product data"""
    product_data, metrics = await serializer.deserialize(data)
    logger.info(f"Deserialized product: {metrics.deserialize_time:.3f}s")
    return CannabisProduct.from_dict(product_data)

if __name__ == "__main__":
    # Example usage and benchmarks
    async def test_serialization():
        serializer = OptimizedSerializer(prefer_speed=True)
        
        # Test data
        test_product = CannabisProduct(
            id="prod_123",
            name="Blue Dream",
            strain="Blue Dream",
            category="flower",
            thc=18.5,
            cbd=0.2,
            price=12.50,
            quantity=28,
            brand="Premium Cannabis",
            description="A balanced hybrid strain with sweet berry aroma",
            terpenes=["myrcene", "limonene", "pinene"],
            effects=["relaxed", "happy", "euphoric"],
            flavors=["berry", "sweet", "earthy"],
            lab_results={"pesticides": "pass", "heavy_metals": "pass"},
            availability=True,
            updated_at=datetime.now()
        )
        
        # Serialize
        serialized_data, metrics = await serializer.serialize(
            test_product.to_dict(),
            CannabisDataType.DISPENSARY_MENU
        )
        
        print(f"Serialization:")
        print(f"  Format: {metrics.format_used.value}")
        print(f"  Compression: {metrics.compression_used.value}")
        print(f"  Size: {metrics.original_size}B -> {metrics.serialized_size}B")
        print(f"  Compression ratio: {metrics.compression_ratio:.1%}")
        print(f"  Time: {metrics.serialize_time:.3f}s")
        
        # Deserialize
        deserialized_data, metrics = await serializer.deserialize(serialized_data)
        
        print(f"Deserialization:")
        print(f"  Time: {metrics.deserialize_time:.3f}s")
        print(f"  Success: {deserialized_data['name'] == test_product.name}")
        
        # Benchmark different formats
        print("\nBenchmarking formats...")
        benchmark_results = await serializer.benchmark_formats(test_product.to_dict())
        
        for format_name, results in benchmark_results.items():
            if 'error' not in results:
                print(f"  {format_name}: {results['total_time']:.3f}s total, "
                      f"{results['serialized_size']}B")
        
        # Get statistics
        stats = serializer.get_statistics()
        print(f"\nStatistics: {stats['operations']}")
    
    # Run test
    asyncio.run(test_serialization())