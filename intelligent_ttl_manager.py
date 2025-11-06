"""
Intelligent TTL Management for Cannabis Data Caching
Implements dynamic TTL policies based on data type, usage patterns, and business logic
Integrates with MySQL Result Cache and DHT peer network
"""

import asyncio
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import mysql.connector
from mysql.connector import Error
import numpy as np
from collections import defaultdict
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TTLStrategy(Enum):
    """TTL calculation strategies"""
    FIXED = "fixed"                    # Fixed TTL based on data type
    ADAPTIVE = "adaptive"              # Adaptive based on access patterns
    BUSINESS_HOURS = "business_hours"  # Different TTL for business vs off hours
    VOLATILITY_BASED = "volatility"    # Based on data change frequency
    HYBRID = "hybrid"                  # Combination of multiple strategies

class DataVolatility(Enum):
    """Data volatility classifications"""
    STATIC = "static"         # Rarely changes (strain genetics, lab results)
    SEMI_STATIC = "semi_static"  # Changes occasionally (dispensary info, terpene profiles)
    DYNAMIC = "dynamic"       # Changes regularly (menus, pricing)
    VOLATILE = "volatile"     # Changes frequently (promotions, inventory)

@dataclass
class TTLPolicy:
    """TTL policy configuration"""
    base_ttl: int  # Base TTL in seconds
    min_ttl: int   # Minimum TTL
    max_ttl: int   # Maximum TTL
    strategy: TTLStrategy
    volatility: DataVolatility
    business_multiplier: float = 1.0    # Multiplier during business hours
    access_multiplier: float = 1.0      # Multiplier based on access frequency
    freshness_weight: float = 0.5       # Weight for data freshness factor

class IntelligentTTLManager:
    """
    Intelligent TTL Management System for Cannabis Data
    Optimizes cache TTLs based on usage patterns, business logic, and data characteristics
    """
    
    def __init__(self, mysql_config: Dict):
        """
        Initialize TTL Manager
        
        Args:
            mysql_config: MySQL connection configuration
        """
        self.mysql_config = mysql_config
        self.connection_pool = None
        
        # Cannabis-specific TTL policies
        self.ttl_policies = {
            'strain_data': TTLPolicy(
                base_ttl=365*24*3600,  # 1 year
                min_ttl=30*24*3600,    # 30 days
                max_ttl=2*365*24*3600, # 2 years
                strategy=TTLStrategy.HYBRID,
                volatility=DataVolatility.STATIC,
                access_multiplier=1.2
            ),
            'dispensary_menu': TTLPolicy(
                base_ttl=3600,         # 1 hour
                min_ttl=300,           # 5 minutes
                max_ttl=4*3600,        # 4 hours
                strategy=TTLStrategy.BUSINESS_HOURS,
                volatility=DataVolatility.DYNAMIC,
                business_multiplier=0.5,  # Shorter TTL during business hours
                access_multiplier=0.8
            ),
            'product_pricing': TTLPolicy(
                base_ttl=600,          # 10 minutes
                min_ttl=60,            # 1 minute
                max_ttl=1800,          # 30 minutes
                strategy=TTLStrategy.VOLATILITY_BASED,
                volatility=DataVolatility.VOLATILE,
                freshness_weight=0.8
            ),
            'terpene_profiles': TTLPolicy(
                base_ttl=180*24*3600,  # 6 months
                min_ttl=7*24*3600,     # 1 week
                max_ttl=365*24*3600,   # 1 year
                strategy=TTLStrategy.ADAPTIVE,
                volatility=DataVolatility.SEMI_STATIC,
                access_multiplier=1.5
            ),
            'lab_results': TTLPolicy(
                base_ttl=365*24*3600,  # 1 year
                min_ttl=30*24*3600,    # 30 days
                max_ttl=3*365*24*3600, # 3 years
                strategy=TTLStrategy.FIXED,
                volatility=DataVolatility.STATIC
            ),
            'dispensary_info': TTLPolicy(
                base_ttl=7*24*3600,    # 1 week
                min_ttl=24*3600,       # 1 day
                max_ttl=30*24*3600,    # 30 days
                strategy=TTLStrategy.ADAPTIVE,
                volatility=DataVolatility.SEMI_STATIC,
                access_multiplier=1.3
            ),
            'analytics_data': TTLPolicy(
                base_ttl=24*3600,      # 24 hours
                min_ttl=3600,          # 1 hour
                max_ttl=7*24*3600,     # 1 week
                strategy=TTLStrategy.BUSINESS_HOURS,
                volatility=DataVolatility.DYNAMIC,
                business_multiplier=0.7
            ),
            'promotions': TTLPolicy(
                base_ttl=1800,         # 30 minutes
                min_ttl=300,           # 5 minutes
                max_ttl=3600,          # 1 hour
                strategy=TTLStrategy.VOLATILITY_BASED,
                volatility=DataVolatility.VOLATILE,
                freshness_weight=0.9
            )
        }
        
        # Access pattern tracking
        self.access_patterns = defaultdict(list)
        self.access_lock = threading.Lock()
        
        # Business hours configuration (PST/PDT)
        self.business_hours = {
            'start': 9,   # 9 AM
            'end': 21,    # 9 PM
            'timezone': 'US/Pacific'
        }
        
        # Statistics tracking
        self.stats = {
            'ttl_calculations': 0,
            'adaptive_adjustments': 0,
            'business_hour_adjustments': 0,
            'volatility_adjustments': 0
        }
    
    async def initialize(self) -> bool:
        """Initialize TTL Manager"""
        try:
            # Create connection pool
            from mysql.connector.pooling import MySQLConnectionPool
            
            pool_config = self.mysql_config.copy()
            pool_config['pool_name'] = 'ttl_manager_pool'
            pool_config['pool_size'] = 5
            
            self.connection_pool = MySQLConnectionPool(**pool_config)
            
            # Create TTL tracking tables
            await self._create_ttl_tables()
            
            # Start background tasks
            asyncio.create_task(self._pattern_analysis_loop())
            asyncio.create_task(self._ttl_optimization_loop())
            
            logger.info("Intelligent TTL Manager initialized")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize TTL Manager: {e}")
            return False
    
    async def _create_ttl_tables(self):
        """Create TTL tracking and optimization tables"""
        
        # TTL performance tracking
        ttl_tracking_sql = """
        CREATE TABLE IF NOT EXISTS ttl_tracking (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cache_key_hash VARCHAR(64) NOT NULL,
            data_type VARCHAR(50) NOT NULL,
            calculated_ttl INT NOT NULL,
            strategy_used VARCHAR(20) NOT NULL,
            base_ttl INT NOT NULL,
            multipliers JSON,
            calculation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            actual_lifetime INT,
            hit_count_during_lifetime INT DEFAULT 0,
            INDEX idx_data_type (data_type),
            INDEX idx_calculation_time (calculation_time),
            INDEX idx_cache_key_hash (cache_key_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATION=utf8mb4_unicode_ci;
        """
        
        # Access pattern tracking
        access_patterns_sql = """
        CREATE TABLE IF NOT EXISTS access_patterns (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cache_key_hash VARCHAR(64) NOT NULL,
            data_type VARCHAR(50) NOT NULL,
            access_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            hour_of_day TINYINT NOT NULL,
            day_of_week TINYINT NOT NULL,
            response_time_ms FLOAT,
            cache_hit BOOLEAN NOT NULL,
            INDEX idx_data_type (data_type),
            INDEX idx_access_time (access_time),
            INDEX idx_cache_key_hash (cache_key_hash),
            INDEX idx_hour_of_day (hour_of_day)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATION=utf8mb4_unicode_ci;
        """
        
        # TTL optimization results
        optimization_results_sql = """
        CREATE TABLE IF NOT EXISTS ttl_optimization_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data_type VARCHAR(50) NOT NULL,
            old_base_ttl INT NOT NULL,
            new_base_ttl INT NOT NULL,
            optimization_reason TEXT,
            performance_improvement FLOAT,
            optimization_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_data_type (data_type),
            INDEX idx_optimization_time (optimization_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATION=utf8mb4_unicode_ci;
        """
        
        connection = self.connection_pool.get_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(ttl_tracking_sql)
            cursor.execute(access_patterns_sql)
            cursor.execute(optimization_results_sql)
            connection.commit()
            logger.info("TTL tracking tables created successfully")
        except Error as e:
            logger.error(f"Error creating TTL tables: {e}")
            raise
        finally:
            cursor.close()
            connection.close()
    
    async def calculate_ttl(self, 
                           data_type: str, 
                           cache_key: str,
                           metadata: Optional[Dict] = None) -> int:
        """
        Calculate intelligent TTL for cannabis data
        
        Args:
            data_type: Type of cannabis data
            cache_key: Cache key for pattern tracking
            metadata: Additional metadata for TTL calculation
            
        Returns:
            Calculated TTL in seconds
        """
        self.stats['ttl_calculations'] += 1
        
        if data_type not in self.ttl_policies:
            logger.warning(f"Unknown data type: {data_type}, using default TTL")
            return 3600  # Default 1 hour
        
        policy = self.ttl_policies[data_type]
        calculated_ttl = policy.base_ttl
        multipliers = {}
        
        try:
            # Apply strategy-specific calculations
            if policy.strategy == TTLStrategy.FIXED:
                # Use base TTL as-is
                pass
                
            elif policy.strategy == TTLStrategy.ADAPTIVE:
                # Adjust based on access patterns
                access_multiplier = await self._calculate_access_multiplier(data_type, cache_key)
                calculated_ttl = int(calculated_ttl * access_multiplier)
                multipliers['access'] = access_multiplier
                self.stats['adaptive_adjustments'] += 1
                
            elif policy.strategy == TTLStrategy.BUSINESS_HOURS:
                # Adjust based on business hours
                business_multiplier = self._calculate_business_hours_multiplier(policy)
                calculated_ttl = int(calculated_ttl * business_multiplier)
                multipliers['business_hours'] = business_multiplier
                self.stats['business_hour_adjustments'] += 1
                
            elif policy.strategy == TTLStrategy.VOLATILITY_BASED:
                # Adjust based on data volatility and freshness
                volatility_multiplier = await self._calculate_volatility_multiplier(data_type, metadata)
                calculated_ttl = int(calculated_ttl * volatility_multiplier)
                multipliers['volatility'] = volatility_multiplier
                self.stats['volatility_adjustments'] += 1
                
            elif policy.strategy == TTLStrategy.HYBRID:
                # Combine multiple strategies
                access_multiplier = await self._calculate_access_multiplier(data_type, cache_key)
                business_multiplier = self._calculate_business_hours_multiplier(policy)
                volatility_multiplier = await self._calculate_volatility_multiplier(data_type, metadata)
                
                # Weighted combination
                combined_multiplier = (
                    access_multiplier * 0.4 +
                    business_multiplier * 0.3 +
                    volatility_multiplier * 0.3
                )
                
                calculated_ttl = int(calculated_ttl * combined_multiplier)
                multipliers = {
                    'access': access_multiplier,
                    'business_hours': business_multiplier,
                    'volatility': volatility_multiplier,
                    'combined': combined_multiplier
                }
                self.stats['adaptive_adjustments'] += 1
                self.stats['business_hour_adjustments'] += 1
                self.stats['volatility_adjustments'] += 1
            
            # Ensure TTL is within bounds
            calculated_ttl = max(policy.min_ttl, min(policy.max_ttl, calculated_ttl))
            
            # Track TTL calculation
            await self._track_ttl_calculation(
                cache_key, data_type, calculated_ttl, 
                policy.strategy.value, policy.base_ttl, multipliers
            )
            
            logger.debug(f"Calculated TTL for {data_type}: {calculated_ttl}s "
                        f"(base: {policy.base_ttl}s, strategy: {policy.strategy.value})")
            
            return calculated_ttl
            
        except Exception as e:
            logger.error(f"Error calculating TTL: {e}")
            return policy.base_ttl
    
    async def _calculate_access_multiplier(self, data_type: str, cache_key: str) -> float:
        """Calculate access pattern-based multiplier"""
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor()
            
            # Get recent access patterns for this data type
            cache_key_hash = self._hash_cache_key(cache_key)
            
            query = """
            SELECT AVG(cache_hit) as hit_rate, COUNT(*) as access_count
            FROM access_patterns
            WHERE data_type = %s AND access_time > DATE_SUB(NOW(), INTERVAL 7 DAY)
            """
            cursor.execute(query, (data_type,))
            result = cursor.fetchone()
            
            if result and result[1] > 10:  # Minimum 10 accesses for reliable data
                hit_rate = result[0] or 0
                access_count = result[1]
                
                # Higher hit rate and access count = longer TTL
                access_multiplier = 1.0 + (hit_rate * 0.5) + (min(access_count / 1000, 0.3))
                
                # Cap the multiplier
                return min(2.0, max(0.5, access_multiplier))
            
            return 1.0  # Default multiplier
            
        except Exception as e:
            logger.error(f"Error calculating access multiplier: {e}")
            return 1.0
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    def _calculate_business_hours_multiplier(self, policy: TTLPolicy) -> float:
        """Calculate business hours-based multiplier"""
        try:
            now = datetime.now()
            current_hour = now.hour
            
            # Check if current time is within business hours
            if self.business_hours['start'] <= current_hour < self.business_hours['end']:
                # During business hours - apply business multiplier
                return policy.business_multiplier
            else:
                # Off hours - use inverse multiplier (longer cache)
                return 1.0 / policy.business_multiplier if policy.business_multiplier > 0 else 1.0
                
        except Exception as e:
            logger.error(f"Error calculating business hours multiplier: {e}")
            return 1.0
    
    async def _calculate_volatility_multiplier(self, data_type: str, metadata: Optional[Dict]) -> float:
        """Calculate volatility-based multiplier"""
        try:
            policy = self.ttl_policies.get(data_type)
            if not policy:
                return 1.0
            
            # Base multiplier on volatility
            volatility_multipliers = {
                DataVolatility.STATIC: 1.5,      # Longer TTL for static data
                DataVolatility.SEMI_STATIC: 1.2,
                DataVolatility.DYNAMIC: 1.0,
                DataVolatility.VOLATILE: 0.7     # Shorter TTL for volatile data
            }
            
            base_multiplier = volatility_multipliers.get(policy.volatility, 1.0)
            
            # Adjust based on metadata if available
            if metadata:
                # Check for freshness indicators
                if 'last_updated' in metadata:
                    try:
                        last_updated = datetime.fromisoformat(metadata['last_updated'])
                        age_hours = (datetime.now() - last_updated).total_seconds() / 3600
                        
                        # Fresher data gets longer TTL
                        freshness_factor = max(0.5, 1.0 - (age_hours / 24) * 0.1)
                        base_multiplier *= freshness_factor
                    except:
                        pass
                
                # Check for change frequency indicators
                if 'change_frequency' in metadata:
                    change_freq = metadata['change_frequency']
                    if change_freq == 'hourly':
                        base_multiplier *= 0.5
                    elif change_freq == 'daily':
                        base_multiplier *= 0.8
                    elif change_freq == 'weekly':
                        base_multiplier *= 1.2
                    elif change_freq == 'monthly':
                        base_multiplier *= 1.5
            
            return max(0.3, min(2.0, base_multiplier))
            
        except Exception as e:
            logger.error(f"Error calculating volatility multiplier: {e}")
            return 1.0
    
    def _hash_cache_key(self, cache_key: str) -> str:
        """Create hash of cache key for tracking"""
        import hashlib
        return hashlib.sha256(cache_key.encode()).hexdigest()[:16]
    
    async def _track_ttl_calculation(self, 
                                    cache_key: str, 
                                    data_type: str, 
                                    calculated_ttl: int,
                                    strategy: str, 
                                    base_ttl: int, 
                                    multipliers: Dict):
        """Track TTL calculation for analysis"""
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor()
            
            cache_key_hash = self._hash_cache_key(cache_key)
            multipliers_json = json.dumps(multipliers)
            
            query = """
            INSERT INTO ttl_tracking 
            (cache_key_hash, data_type, calculated_ttl, strategy_used, base_ttl, multipliers)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                cache_key_hash, data_type, calculated_ttl, 
                strategy, base_ttl, multipliers_json
            ))
            connection.commit()
            
        except Exception as e:
            logger.warning(f"Failed to track TTL calculation: {e}")
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    async def track_access(self, 
                          cache_key: str, 
                          data_type: str, 
                          cache_hit: bool, 
                          response_time: float):
        """Track cache access patterns"""
        try:
            with self.access_lock:
                # Store in memory for immediate use
                self.access_patterns[data_type].append({
                    'timestamp': time.time(),
                    'cache_hit': cache_hit,
                    'response_time': response_time
                })
                
                # Keep only recent patterns (last 1000 per type)
                if len(self.access_patterns[data_type]) > 1000:
                    self.access_patterns[data_type] = self.access_patterns[data_type][-1000:]
            
            # Store in database for long-term analysis
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor()
            
            now = datetime.now()
            cache_key_hash = self._hash_cache_key(cache_key)
            
            query = """
            INSERT INTO access_patterns 
            (cache_key_hash, data_type, hour_of_day, day_of_week, response_time_ms, cache_hit)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                cache_key_hash, data_type, now.hour, now.weekday(),
                response_time, cache_hit
            ))
            connection.commit()
            
        except Exception as e:
            logger.warning(f"Failed to track access: {e}")
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    async def _pattern_analysis_loop(self):
        """Background task for pattern analysis"""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                await self._analyze_access_patterns()
            except Exception as e:
                logger.error(f"Pattern analysis error: {e}")
    
    async def _ttl_optimization_loop(self):
        """Background task for TTL optimization"""
        while True:
            try:
                await asyncio.sleep(24 * 3600)  # Run daily
                await self._optimize_ttl_policies()
            except Exception as e:
                logger.error(f"TTL optimization error: {e}")
    
    async def _analyze_access_patterns(self):
        """Analyze access patterns for insights"""
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor(dictionary=True)
            
            # Analyze patterns by hour and data type
            query = """
            SELECT 
                data_type,
                hour_of_day,
                AVG(cache_hit) as avg_hit_rate,
                COUNT(*) as access_count,
                AVG(response_time_ms) as avg_response_time
            FROM access_patterns
            WHERE access_time > DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY data_type, hour_of_day
            HAVING access_count > 10
            ORDER BY data_type, hour_of_day
            """
            
            cursor.execute(query)
            patterns = cursor.fetchall()
            
            # Log insights
            for pattern in patterns:
                if pattern['avg_hit_rate'] < 0.5:  # Low hit rate
                    logger.info(f"Low hit rate detected: {pattern['data_type']} "
                               f"at hour {pattern['hour_of_day']} ({pattern['avg_hit_rate']:.2f})")
                
                if pattern['avg_response_time'] > 1000:  # Slow response
                    logger.info(f"Slow response detected: {pattern['data_type']} "
                               f"at hour {pattern['hour_of_day']} ({pattern['avg_response_time']:.1f}ms)")
            
        except Exception as e:
            logger.error(f"Error analyzing access patterns: {e}")
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    async def _optimize_ttl_policies(self):
        """Optimize TTL policies based on performance data"""
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor(dictionary=True)
            
            # Analyze TTL effectiveness
            query = """
            SELECT 
                t.data_type,
                AVG(t.calculated_ttl) as avg_calculated_ttl,
                AVG(t.hit_count_during_lifetime) as avg_hits,
                COUNT(*) as calculation_count
            FROM ttl_tracking t
            WHERE t.calculation_time > DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY t.data_type
            HAVING calculation_count > 50
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            for result in results:
                data_type = result['data_type']
                avg_hits = result['avg_hits'] or 0
                
                # Optimize based on hit patterns
                if data_type in self.ttl_policies:
                    current_policy = self.ttl_policies[data_type]
                    
                    # If very low hits, consider shorter TTL
                    if avg_hits < 2 and current_policy.base_ttl > 3600:
                        new_ttl = max(current_policy.min_ttl, current_policy.base_ttl * 0.8)
                        logger.info(f"Optimizing {data_type}: reducing TTL from "
                                   f"{current_policy.base_ttl}s to {new_ttl}s (low hits)")
                        current_policy.base_ttl = int(new_ttl)
                    
                    # If very high hits, consider longer TTL
                    elif avg_hits > 10 and current_policy.base_ttl < current_policy.max_ttl:
                        new_ttl = min(current_policy.max_ttl, current_policy.base_ttl * 1.2)
                        logger.info(f"Optimizing {data_type}: increasing TTL from "
                                   f"{current_policy.base_ttl}s to {new_ttl}s (high hits)")
                        current_policy.base_ttl = int(new_ttl)
            
        except Exception as e:
            logger.error(f"Error optimizing TTL policies: {e}")
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    async def get_ttl_statistics(self) -> Dict:
        """Get TTL management statistics"""
        try:
            connection = self.connection_pool.get_connection()
            cursor = connection.cursor(dictionary=True)
            
            # TTL calculation stats
            stats_query = """
            SELECT 
                data_type,
                strategy_used,
                COUNT(*) as calculations,
                AVG(calculated_ttl) as avg_ttl,
                MIN(calculated_ttl) as min_ttl,
                MAX(calculated_ttl) as max_ttl
            FROM ttl_tracking
            WHERE calculation_time > DATE_SUB(NOW(), INTERVAL 24 HOUR)
            GROUP BY data_type, strategy_used
            """
            cursor.execute(stats_query)
            calculation_stats = cursor.fetchall()
            
            # Access pattern stats
            access_query = """
            SELECT 
                data_type,
                AVG(cache_hit) as hit_rate,
                COUNT(*) as total_accesses,
                AVG(response_time_ms) as avg_response_time
            FROM access_patterns
            WHERE access_time > DATE_SUB(NOW(), INTERVAL 24 HOUR)
            GROUP BY data_type
            """
            cursor.execute(access_query)
            access_stats = cursor.fetchall()
            
            return {
                'runtime_stats': self.stats,
                'calculation_stats': calculation_stats,
                'access_stats': access_stats,
                'policy_count': len(self.ttl_policies)
            }
            
        except Exception as e:
            logger.error(f"Error getting TTL statistics: {e}")
            return {'error': str(e)}
        finally:
            if connection:
                cursor.close()
                connection.close()

# TTL Manager decorator
def intelligent_ttl(data_type: str, metadata_func=None):
    """Decorator to apply intelligent TTL management"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Initialize TTL manager if not already done
            ttl_manager = IntelligentTTLManager({
                'host': 'localhost',
                'port': 3307,
                'user': 'root',
                'password': '',
                'database': 'terpene_database'
            })
            await ttl_manager.initialize()
            
            # Generate cache key
            import hashlib
            cache_key_data = {
                'function': func.__name__,
                'args': str(args),
                'kwargs': str(sorted(kwargs.items()))
            }
            cache_key = hashlib.sha256(str(cache_key_data).encode()).hexdigest()
            
            # Get metadata if function provided
            metadata = metadata_func(*args, **kwargs) if metadata_func else None
            
            # Calculate intelligent TTL
            ttl = await ttl_manager.calculate_ttl(data_type, cache_key, metadata)
            
            # Execute function (this would integrate with your caching system)
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            return result, ttl  # Return result and calculated TTL
        
        return wrapper
    return decorator

if __name__ == "__main__":
    # Example usage
    async def test_ttl_manager():
        mysql_config = {
            'host': 'localhost',
            'port': 3307,
            'user': 'root',
            'password': '',
            'database': 'terpene_database'
        }
        
        ttl_manager = IntelligentTTLManager(mysql_config)
        await ttl_manager.initialize()
        
        # Test TTL calculations for different data types
        test_cases = [
            ('strain_data', 'blue_dream_genetics', {'last_updated': '2024-01-15T10:00:00'}),
            ('dispensary_menu', 'dispensary_123_menu', {'change_frequency': 'hourly'}),
            ('product_pricing', 'product_456_price', {'last_updated': '2024-01-18T14:30:00'}),
            ('promotions', 'weekend_special', {'change_frequency': 'hourly'})
        ]
        
        for data_type, cache_key, metadata in test_cases:
            ttl = await ttl_manager.calculate_ttl(data_type, cache_key, metadata)
            print(f"{data_type}: {ttl}s ({ttl/3600:.1f} hours)")
            
            # Simulate access tracking
            await ttl_manager.track_access(cache_key, data_type, True, 150.5)
        
        # Get statistics
        stats = await ttl_manager.get_ttl_statistics()
        print(f"TTL Manager Stats: {stats}")
    
    # Run test
    asyncio.run(test_ttl_manager())