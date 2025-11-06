#!/usr/bin/env python3
"""
GPU Acceleration Engine for Cannabis Data Platform
==================================================

Provides GPU-accelerated computing for cannabis data processing with intelligent
fallback to CPU operations. Optimized for terpene analysis, strain calculations,
and large-scale dispensary data aggregation.

Key Features:
- CUDA/CuPy acceleration with CPU fallback
- Cannabis-specific matrix operations
- Parallel hash calculations for DHT operations
- GPU memory management and optimization
- Terpene correlation analysis acceleration
- Strain similarity calculations
- Price prediction model acceleration

Author: WeedHounds Performance Team
Created: November 2025
"""

import logging
import time
import numpy as np
import threading
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass
from contextlib import contextmanager
import gc
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed

# GPU Computing Imports with Fallback
try:
    import cupy as cp
    import cupy.cuda.memory
    CUDA_AVAILABLE = True
    print("✅ CUDA/CuPy available - GPU acceleration enabled")
except ImportError:
    cp = None
    CUDA_AVAILABLE = False
    print("⚠️ CUDA/CuPy not available - using CPU fallback")

try:
    import numba
    from numba import cuda, jit
    NUMBA_AVAILABLE = True
    print("✅ Numba available - JIT compilation enabled")
except ImportError:
    numba = None
    cuda = None
    jit = None
    NUMBA_AVAILABLE = False
    print("⚠️ Numba not available - standard Python execution")

@dataclass
class GPUConfiguration:
    """GPU configuration settings for cannabis data processing."""
    enable_gpu: bool = True
    gpu_memory_limit: float = 0.8  # Use 80% of GPU memory
    cpu_fallback: bool = True
    batch_size: int = 10000
    max_workers: int = None
    memory_pool_size: str = "2GB"
    enable_unified_memory: bool = True
    optimization_level: str = "balanced"  # conservative, balanced, aggressive

class GPUMemoryManager:
    """Advanced GPU memory management for cannabis data operations."""
    
    def __init__(self, config: GPUConfiguration):
        self.config = config
        self.memory_pool = None
        self.allocation_history = []
        self.peak_usage = 0
        self.lock = threading.Lock()
        
        if CUDA_AVAILABLE:
            self._setup_gpu_memory()
    
    def _setup_gpu_memory(self):
        """Setup GPU memory pool and configurations."""
        try:
            # Set memory pool
            if self.config.memory_pool_size:
                pool_size = self._parse_memory_size(self.config.memory_pool_size)
                self.memory_pool = cp.get_default_memory_pool()
                self.memory_pool.set_limit(size=pool_size)
            
            # Enable unified memory if requested
            if self.config.enable_unified_memory:
                cp.cuda.set_allocator(cp.cuda.MemoryPool().malloc)
            
            logging.info(f"GPU memory pool configured: {self.config.memory_pool_size}")
            
        except Exception as e:
            logging.warning(f"GPU memory setup failed: {e}")
    
    def _parse_memory_size(self, size_str: str) -> int:
        """Parse memory size string to bytes."""
        size_str = size_str.upper()
        if size_str.endswith('GB'):
            return int(float(size_str[:-2]) * 1024 * 1024 * 1024)
        elif size_str.endswith('MB'):
            return int(float(size_str[:-2]) * 1024 * 1024)
        elif size_str.endswith('KB'):
            return int(float(size_str[:-2]) * 1024)
        else:
            return int(size_str)
    
    @contextmanager
    def managed_allocation(self, operation_name: str):
        """Context manager for tracking GPU memory allocations."""
        start_time = time.time()
        initial_usage = self.get_memory_usage() if CUDA_AVAILABLE else 0
        
        try:
            with self.lock:
                self.allocation_history.append({
                    'operation': operation_name,
                    'start_time': start_time,
                    'initial_usage': initial_usage
                })
            
            yield
            
        finally:
            end_time = time.time()
            final_usage = self.get_memory_usage() if CUDA_AVAILABLE else 0
            peak_usage = max(initial_usage, final_usage)
            
            with self.lock:
                if self.allocation_history:
                    self.allocation_history[-1].update({
                        'end_time': end_time,
                        'duration': end_time - start_time,
                        'final_usage': final_usage,
                        'peak_usage': peak_usage,
                        'memory_delta': final_usage - initial_usage
                    })
                
                self.peak_usage = max(self.peak_usage, peak_usage)
            
            # Cleanup if memory usage is high
            if CUDA_AVAILABLE and final_usage > self.config.gpu_memory_limit:
                self.cleanup_memory()
    
    def get_memory_usage(self) -> float:
        """Get current GPU memory usage as percentage."""
        if not CUDA_AVAILABLE:
            return 0.0
        
        try:
            meminfo = cp.cuda.runtime.memGetInfo()
            free_memory, total_memory = meminfo
            used_memory = total_memory - free_memory
            return used_memory / total_memory
        except Exception:
            return 0.0
    
    def cleanup_memory(self):
        """Force GPU memory cleanup."""
        if CUDA_AVAILABLE:
            try:
                cp.get_default_memory_pool().free_all_blocks()
                gc.collect()
                logging.info("GPU memory cleanup completed")
            except Exception as e:
                logging.warning(f"GPU memory cleanup failed: {e}")

class CannabisGPUOperations:
    """GPU-accelerated operations for cannabis data processing."""
    
    def __init__(self, config: GPUConfiguration):
        self.config = config
        self.memory_manager = GPUMemoryManager(config)
        self.device_id = 0
        self.cache = {}
        
        if CUDA_AVAILABLE:
            self._setup_gpu_device()
    
    def _setup_gpu_device(self):
        """Setup GPU device and verify capabilities."""
        try:
            device_count = cp.cuda.runtime.getDeviceCount()
            logging.info(f"Found {device_count} GPU device(s)")
            
            if device_count > 0:
                cp.cuda.Device(self.device_id).use()
                device_props = cp.cuda.runtime.getDeviceProperties(self.device_id)
                logging.info(f"Using GPU: {device_props['name'].decode()}")
        except Exception as e:
            logging.warning(f"GPU device setup failed: {e}")
    
    def accelerated_terpene_correlation(self, terpene_data: List[Dict[str, float]]) -> np.ndarray:
        """
        GPU-accelerated terpene correlation analysis.
        
        Calculates correlation matrices for terpene profiles across cannabis strains
        using GPU acceleration for large datasets.
        """
        with self.memory_manager.managed_allocation("terpene_correlation"):
            try:
                if not terpene_data or not CUDA_AVAILABLE:
                    return self._cpu_terpene_correlation(terpene_data)
                
                # Convert to GPU arrays
                profiles = []
                for strain_data in terpene_data:
                    profile = [strain_data.get(terp, 0.0) for terp in 
                             ['myrcene', 'limonene', 'pinene', 'linalool', 'caryophyllene', 
                              'humulene', 'terpinolene', 'ocimene']]
                    profiles.append(profile)
                
                gpu_data = cp.array(profiles, dtype=cp.float32)
                
                # Calculate correlation matrix on GPU
                correlation_matrix = cp.corrcoef(gpu_data.T)
                
                # Convert back to CPU
                result = cp.asnumpy(correlation_matrix)
                
                # Cache result for future use
                cache_key = f"terpene_correlation_{len(terpene_data)}"
                self.cache[cache_key] = result
                
                logging.info(f"GPU terpene correlation completed for {len(terpene_data)} strains")
                return result
                
            except Exception as e:
                logging.warning(f"GPU terpene correlation failed: {e}, falling back to CPU")
                return self._cpu_terpene_correlation(terpene_data)
    
    def _cpu_terpene_correlation(self, terpene_data: List[Dict[str, float]]) -> np.ndarray:
        """CPU fallback for terpene correlation analysis."""
        if not terpene_data:
            return np.array([])
        
        profiles = []
        for strain_data in terpene_data:
            profile = [strain_data.get(terp, 0.0) for terp in 
                     ['myrcene', 'limonene', 'pinene', 'linalool', 'caryophyllene', 
                      'humulene', 'terpinolene', 'ocimene']]
            profiles.append(profile)
        
        return np.corrcoef(np.array(profiles).T)
    
    def accelerated_strain_similarity(self, strain_profiles: List[Dict[str, Any]], 
                                    target_profile: Dict[str, Any], 
                                    top_k: int = 10) -> List[Tuple[int, float]]:
        """
        GPU-accelerated strain similarity calculation.
        
        Finds the most similar strains to a target profile using cosine similarity
        with GPU acceleration for large strain databases.
        """
        with self.memory_manager.managed_allocation("strain_similarity"):
            try:
                if not strain_profiles or not CUDA_AVAILABLE:
                    return self._cpu_strain_similarity(strain_profiles, target_profile, top_k)
                
                # Extract features for similarity calculation
                feature_keys = ['thc', 'cbd', 'myrcene', 'limonene', 'pinene', 'linalool', 
                              'caryophyllene', 'humulene', 'price_per_gram']
                
                # Convert to GPU arrays
                strain_vectors = []
                for profile in strain_profiles:
                    vector = [profile.get(key, 0.0) for key in feature_keys]
                    strain_vectors.append(vector)
                
                target_vector = [target_profile.get(key, 0.0) for key in feature_keys]
                
                gpu_strains = cp.array(strain_vectors, dtype=cp.float32)
                gpu_target = cp.array(target_vector, dtype=cp.float32)
                
                # Normalize vectors
                strain_norms = cp.linalg.norm(gpu_strains, axis=1, keepdims=True)
                target_norm = cp.linalg.norm(gpu_target)
                
                normalized_strains = gpu_strains / (strain_norms + 1e-8)
                normalized_target = gpu_target / (target_norm + 1e-8)
                
                # Calculate cosine similarities
                similarities = cp.dot(normalized_strains, normalized_target)
                
                # Get top-k similar strains
                top_indices = cp.argpartition(-similarities, top_k)[:top_k]
                top_similarities = similarities[top_indices]
                
                # Convert to CPU and sort
                cpu_indices = cp.asnumpy(top_indices)
                cpu_similarities = cp.asnumpy(top_similarities)
                
                results = list(zip(cpu_indices, cpu_similarities))
                results.sort(key=lambda x: x[1], reverse=True)
                
                logging.info(f"GPU strain similarity completed for {len(strain_profiles)} strains")
                return results
                
            except Exception as e:
                logging.warning(f"GPU strain similarity failed: {e}, falling back to CPU")
                return self._cpu_strain_similarity(strain_profiles, target_profile, top_k)
    
    def _cpu_strain_similarity(self, strain_profiles: List[Dict[str, Any]], 
                             target_profile: Dict[str, Any], 
                             top_k: int = 10) -> List[Tuple[int, float]]:
        """CPU fallback for strain similarity calculation."""
        if not strain_profiles:
            return []
        
        feature_keys = ['thc', 'cbd', 'myrcene', 'limonene', 'pinene', 'linalool', 
                       'caryophyllene', 'humulene', 'price_per_gram']
        
        strain_vectors = []
        for profile in strain_profiles:
            vector = [profile.get(key, 0.0) for key in feature_keys]
            strain_vectors.append(vector)
        
        target_vector = [target_profile.get(key, 0.0) for key in feature_keys]
        
        # Calculate cosine similarities
        similarities = []
        target_norm = np.linalg.norm(target_vector)
        
        for i, strain_vector in enumerate(strain_vectors):
            strain_norm = np.linalg.norm(strain_vector)
            if strain_norm > 0 and target_norm > 0:
                similarity = np.dot(strain_vector, target_vector) / (strain_norm * target_norm)
            else:
                similarity = 0.0
            similarities.append((i, similarity))
        
        # Sort and return top-k
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]
    
    def accelerated_price_analysis(self, product_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        GPU-accelerated price analysis for cannabis products.
        
        Performs statistical analysis, trend detection, and price optimization
        calculations using GPU acceleration.
        """
        with self.memory_manager.managed_allocation("price_analysis"):
            try:
                if not product_data or not CUDA_AVAILABLE:
                    return self._cpu_price_analysis(product_data)
                
                # Extract price and feature data
                prices = []
                features = []
                
                for product in product_data:
                    if 'price' in product and product['price'] > 0:
                        prices.append(product['price'])
                        feature_set = [
                            product.get('thc', 0.0),
                            product.get('cbd', 0.0),
                            product.get('weight', 1.0),
                            product.get('quality_score', 5.0),
                            len(product.get('terpenes', [])),
                            1.0 if product.get('strain_type') == 'indica' else 0.0,
                            1.0 if product.get('strain_type') == 'sativa' else 0.0
                        ]
                        features.append(feature_set)
                
                if not prices:
                    return {'error': 'No price data available'}
                
                gpu_prices = cp.array(prices, dtype=cp.float32)
                gpu_features = cp.array(features, dtype=cp.float32)
                
                # Calculate price statistics
                price_stats = {
                    'mean': float(cp.mean(gpu_prices)),
                    'median': float(cp.median(gpu_prices)),
                    'std': float(cp.std(gpu_prices)),
                    'min': float(cp.min(gpu_prices)),
                    'max': float(cp.max(gpu_prices)),
                    'count': len(prices)
                }
                
                # Calculate percentiles
                percentiles = cp.percentile(gpu_prices, [25, 50, 75, 90, 95])
                price_stats.update({
                    'p25': float(percentiles[0]),
                    'p50': float(percentiles[1]),
                    'p75': float(percentiles[2]),
                    'p90': float(percentiles[3]),
                    'p95': float(percentiles[4])
                })
                
                # Feature correlations with price
                correlations = {}
                feature_names = ['thc', 'cbd', 'weight', 'quality', 'terpene_count', 'indica', 'sativa']
                
                for i, feature_name in enumerate(feature_names):
                    feature_col = gpu_features[:, i]
                    correlation = float(cp.corrcoef(gpu_prices, feature_col)[0, 1])
                    if not cp.isnan(correlation):
                        correlations[feature_name] = correlation
                
                result = {
                    'price_statistics': price_stats,
                    'feature_correlations': correlations,
                    'processing_time': time.time(),
                    'gpu_accelerated': True
                }
                
                logging.info(f"GPU price analysis completed for {len(product_data)} products")
                return result
                
            except Exception as e:
                logging.warning(f"GPU price analysis failed: {e}, falling back to CPU")
                return self._cpu_price_analysis(product_data)
    
    def _cpu_price_analysis(self, product_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """CPU fallback for price analysis."""
        if not product_data:
            return {'error': 'No product data available'}
        
        prices = []
        features = []
        
        for product in product_data:
            if 'price' in product and product['price'] > 0:
                prices.append(product['price'])
                feature_set = [
                    product.get('thc', 0.0),
                    product.get('cbd', 0.0),
                    product.get('weight', 1.0),
                    product.get('quality_score', 5.0),
                    len(product.get('terpenes', [])),
                    1.0 if product.get('strain_type') == 'indica' else 0.0,
                    1.0 if product.get('strain_type') == 'sativa' else 0.0
                ]
                features.append(feature_set)
        
        if not prices:
            return {'error': 'No price data available'}
        
        prices_array = np.array(prices)
        features_array = np.array(features)
        
        # Calculate price statistics
        price_stats = {
            'mean': float(np.mean(prices_array)),
            'median': float(np.median(prices_array)),
            'std': float(np.std(prices_array)),
            'min': float(np.min(prices_array)),
            'max': float(np.max(prices_array)),
            'count': len(prices)
        }
        
        # Calculate percentiles
        percentiles = np.percentile(prices_array, [25, 50, 75, 90, 95])
        price_stats.update({
            'p25': float(percentiles[0]),
            'p50': float(percentiles[1]),
            'p75': float(percentiles[2]),
            'p90': float(percentiles[3]),
            'p95': float(percentiles[4])
        })
        
        # Feature correlations with price
        correlations = {}
        feature_names = ['thc', 'cbd', 'weight', 'quality', 'terpene_count', 'indica', 'sativa']
        
        for i, feature_name in enumerate(feature_names):
            feature_col = features_array[:, i]
            correlation = np.corrcoef(prices_array, feature_col)[0, 1]
            if not np.isnan(correlation):
                correlations[feature_name] = float(correlation)
        
        return {
            'price_statistics': price_stats,
            'feature_correlations': correlations,
            'processing_time': time.time(),
            'gpu_accelerated': False
        }
    
    def accelerated_hash_calculation(self, data_chunks: List[bytes]) -> List[str]:
        """
        GPU-accelerated hash calculations for DHT operations.
        
        Parallel hash computation for large datasets used in distributed
        hash table operations.
        """
        with self.memory_manager.managed_allocation("hash_calculation"):
            try:
                if not data_chunks or not CUDA_AVAILABLE or not NUMBA_AVAILABLE:
                    return self._cpu_hash_calculation(data_chunks)
                
                # Use Numba CUDA for parallel hash calculation
                import hashlib
                
                @cuda.jit
                def parallel_hash_kernel(data, hashes, n):
                    idx = cuda.grid(1)
                    if idx < n:
                        # Simple hash calculation (replace with more sophisticated hash)
                        hash_val = 0
                        for i in range(len(data[idx])):
                            hash_val = (hash_val * 31 + data[idx][i]) % (2**32)
                        hashes[idx] = hash_val
                
                # Prepare data for GPU
                max_chunk_size = max(len(chunk) for chunk in data_chunks) if data_chunks else 0
                gpu_data = cp.zeros((len(data_chunks), max_chunk_size), dtype=cp.uint8)
                
                for i, chunk in enumerate(data_chunks):
                    chunk_array = cp.frombuffer(chunk, dtype=cp.uint8)
                    gpu_data[i, :len(chunk_array)] = chunk_array
                
                # Allocate output array
                gpu_hashes = cp.zeros(len(data_chunks), dtype=cp.uint32)
                
                # Launch kernel
                threads_per_block = 256
                blocks_per_grid = (len(data_chunks) + threads_per_block - 1) // threads_per_block
                parallel_hash_kernel[blocks_per_grid, threads_per_block](
                    gpu_data, gpu_hashes, len(data_chunks)
                )
                
                # Convert to hex strings
                cpu_hashes = cp.asnumpy(gpu_hashes)
                result = [f"{hash_val:08x}" for hash_val in cpu_hashes]
                
                logging.info(f"GPU hash calculation completed for {len(data_chunks)} chunks")
                return result
                
            except Exception as e:
                logging.warning(f"GPU hash calculation failed: {e}, falling back to CPU")
                return self._cpu_hash_calculation(data_chunks)
    
    def _cpu_hash_calculation(self, data_chunks: List[bytes]) -> List[str]:
        """CPU fallback for hash calculation."""
        import hashlib
        
        hashes = []
        for chunk in data_chunks:
            hash_obj = hashlib.sha256(chunk)
            hashes.append(hash_obj.hexdigest()[:8])  # First 8 characters
        
        return hashes
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics."""
        stats = {
            'gpu_available': CUDA_AVAILABLE,
            'numba_available': NUMBA_AVAILABLE,
            'memory_usage': self.memory_manager.get_memory_usage(),
            'peak_memory_usage': self.memory_manager.peak_usage,
            'cache_size': len(self.cache),
            'allocation_history': len(self.memory_manager.allocation_history)
        }
        
        if CUDA_AVAILABLE:
            try:
                device_props = cp.cuda.runtime.getDeviceProperties(self.device_id)
                stats.update({
                    'gpu_name': device_props['name'].decode(),
                    'gpu_memory_total': device_props['totalGlobalMem'],
                    'gpu_cores': device_props['multiProcessorCount']
                })
            except Exception:
                pass
        
        # System memory stats
        memory_info = psutil.virtual_memory()
        stats.update({
            'system_memory_total': memory_info.total,
            'system_memory_available': memory_info.available,
            'system_memory_percent': memory_info.percent
        })
        
        return stats
    
    def cleanup(self):
        """Cleanup GPU resources."""
        self.memory_manager.cleanup_memory()
        self.cache.clear()
        logging.info("GPU acceleration engine cleanup completed")

class GPUAccelerationEngine:
    """Main GPU acceleration engine for cannabis data platform."""
    
    def __init__(self, config: Optional[GPUConfiguration] = None):
        self.config = config or GPUConfiguration()
        self.gpu_ops = CannabisGPUOperations(self.config)
        self.is_initialized = True
        
        logging.info("GPU Acceleration Engine initialized")
        logging.info(f"GPU Available: {CUDA_AVAILABLE}")
        logging.info(f"Configuration: {self.config}")
    
    def process_cannabis_batch(self, batch_data: List[Dict[str, Any]], 
                              operation_type: str) -> Dict[str, Any]:
        """
        Process a batch of cannabis data using GPU acceleration.
        
        Supports multiple operation types:
        - terpene_analysis: Terpene correlation and clustering
        - strain_similarity: Strain recommendation and similarity
        - price_optimization: Price analysis and optimization
        - quality_assessment: Quality scoring and validation
        """
        start_time = time.time()
        
        try:
            if operation_type == "terpene_analysis":
                correlation_matrix = self.gpu_ops.accelerated_terpene_correlation(batch_data)
                result = {
                    'operation': 'terpene_analysis',
                    'correlation_matrix': correlation_matrix.tolist() if correlation_matrix.size > 0 else [],
                    'processed_strains': len(batch_data)
                }
                
            elif operation_type == "strain_similarity":
                if len(batch_data) < 2:
                    raise ValueError("Need at least 2 strains for similarity analysis")
                
                target_strain = batch_data[0]
                candidate_strains = batch_data[1:]
                
                similarities = self.gpu_ops.accelerated_strain_similarity(
                    candidate_strains, target_strain
                )
                
                result = {
                    'operation': 'strain_similarity',
                    'target_strain': target_strain.get('name', 'Unknown'),
                    'similar_strains': similarities,
                    'processed_candidates': len(candidate_strains)
                }
                
            elif operation_type == "price_optimization":
                price_analysis = self.gpu_ops.accelerated_price_analysis(batch_data)
                result = {
                    'operation': 'price_optimization',
                    'analysis': price_analysis,
                    'processed_products': len(batch_data)
                }
                
            else:
                raise ValueError(f"Unknown operation type: {operation_type}")
            
            # Add performance metrics
            processing_time = time.time() - start_time
            result.update({
                'processing_time': processing_time,
                'gpu_accelerated': CUDA_AVAILABLE,
                'performance_stats': self.gpu_ops.get_performance_stats()
            })
            
            logging.info(f"GPU batch processing completed: {operation_type} in {processing_time:.3f}s")
            return result
            
        except Exception as e:
            error_result = {
                'operation': operation_type,
                'error': str(e),
                'processing_time': time.time() - start_time,
                'gpu_accelerated': False
            }
            logging.error(f"GPU batch processing failed: {e}")
            return error_result
    
    def get_system_capabilities(self) -> Dict[str, Any]:
        """Get detailed system capabilities and configuration."""
        capabilities = {
            'gpu_acceleration': CUDA_AVAILABLE,
            'jit_compilation': NUMBA_AVAILABLE,
            'configuration': self.config.__dict__,
            'performance_stats': self.gpu_ops.get_performance_stats()
        }
        
        if CUDA_AVAILABLE:
            try:
                capabilities['gpu_devices'] = cp.cuda.runtime.getDeviceCount()
                for i in range(capabilities['gpu_devices']):
                    props = cp.cuda.runtime.getDeviceProperties(i)
                    capabilities[f'gpu_{i}_info'] = {
                        'name': props['name'].decode(),
                        'memory': props['totalGlobalMem'],
                        'cores': props['multiProcessorCount'],
                        'compute_capability': f"{props['major']}.{props['minor']}"
                    }
            except Exception as e:
                capabilities['gpu_info_error'] = str(e)
        
        return capabilities
    
    def shutdown(self):
        """Shutdown GPU acceleration engine."""
        self.gpu_ops.cleanup()
        self.is_initialized = False
        logging.info("GPU Acceleration Engine shutdown completed")

def create_gpu_engine(enable_gpu: bool = True, 
                     memory_limit: str = "2GB",
                     optimization_level: str = "balanced") -> GPUAccelerationEngine:
    """
    Factory function to create GPU acceleration engine with optimal configuration.
    
    Args:
        enable_gpu: Whether to enable GPU acceleration
        memory_limit: GPU memory limit (e.g., "2GB", "4GB")
        optimization_level: Optimization level ("conservative", "balanced", "aggressive")
    
    Returns:
        Configured GPU acceleration engine
    """
    config = GPUConfiguration(
        enable_gpu=enable_gpu and CUDA_AVAILABLE,
        memory_pool_size=memory_limit,
        optimization_level=optimization_level,
        cpu_fallback=True,
        enable_unified_memory=True
    )
    
    return GPUAccelerationEngine(config)

# Demo and Testing Functions
def demo_gpu_acceleration():
    """Demonstrate GPU acceleration capabilities with cannabis data."""
    print("\n🚀 GPU Acceleration Engine Demo")
    print("=" * 50)
    
    # Initialize GPU engine
    gpu_engine = create_gpu_engine()
    
    # Display system capabilities
    capabilities = gpu_engine.get_system_capabilities()
    print(f"GPU Available: {capabilities['gpu_acceleration']}")
    print(f"JIT Available: {capabilities['jit_compilation']}")
    
    # Generate sample cannabis data
    sample_strains = []
    strain_names = ["OG Kush", "Blue Dream", "Girl Scout Cookies", "Sour Diesel", "Purple Haze"]
    
    for i, name in enumerate(strain_names):
        strain = {
            'name': name,
            'thc': 15 + np.random.rand() * 15,
            'cbd': np.random.rand() * 5,
            'myrcene': np.random.rand() * 2,
            'limonene': np.random.rand() * 1.5,
            'pinene': np.random.rand() * 1,
            'linalool': np.random.rand() * 0.8,
            'caryophyllene': np.random.rand() * 1.2,
            'humulene': np.random.rand() * 0.6,
            'price': 8 + np.random.rand() * 12,
            'strain_type': 'indica' if i % 2 == 0 else 'sativa'
        }
        sample_strains.append(strain)
    
    print(f"\n📊 Testing with {len(sample_strains)} sample strains...")
    
    # Test terpene analysis
    print("\n🧪 Terpene Correlation Analysis:")
    terpene_result = gpu_engine.process_cannabis_batch(sample_strains, "terpene_analysis")
    print(f"  Processing time: {terpene_result['processing_time']:.3f}s")
    print(f"  GPU accelerated: {terpene_result['gpu_accelerated']}")
    
    # Test strain similarity
    print("\n🔍 Strain Similarity Analysis:")
    similarity_result = gpu_engine.process_cannabis_batch(sample_strains, "strain_similarity")
    print(f"  Processing time: {similarity_result['processing_time']:.3f}s")
    print(f"  Similar strains found: {len(similarity_result.get('similar_strains', []))}")
    
    # Test price optimization
    print("\n💰 Price Optimization Analysis:")
    price_result = gpu_engine.process_cannabis_batch(sample_strains, "price_optimization")
    print(f"  Processing time: {price_result['processing_time']:.3f}s")
    if 'analysis' in price_result and 'price_statistics' in price_result['analysis']:
        stats = price_result['analysis']['price_statistics']
        print(f"  Average price: ${stats['mean']:.2f}")
        print(f"  Price range: ${stats['min']:.2f} - ${stats['max']:.2f}")
    
    # Performance summary
    print(f"\n📈 Performance Summary:")
    perf_stats = gpu_engine.gpu_ops.get_performance_stats()
    print(f"  GPU Memory Usage: {perf_stats['memory_usage']:.1%}")
    print(f"  Cache Entries: {perf_stats['cache_size']}")
    print(f"  Memory Allocations: {perf_stats['allocation_history']}")
    
    # Cleanup
    gpu_engine.shutdown()
    print("\n✅ Demo completed successfully!")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run demo
    demo_gpu_acceleration()