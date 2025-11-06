#!/usr/bin/env python3
"""
GPU Data Aggregation Engine for Cannabis Analytics
=================================================

High-performance GPU-accelerated data aggregation system for large-scale
cannabis analytics, terpene analysis, and dispensary data processing.

Key Features:
- GPU-accelerated data aggregation and grouping operations
- Parallel cannabis data analytics (strain clustering, terpene analysis)
- High-performance time-series aggregation for price trends
- Multi-dimensional data cube operations for complex analytics
- Memory-efficient processing of large cannabis datasets
- Real-time aggregation for live dashboards

Author: WeedHounds Performance Team
Created: November 2025
"""

import logging
import time
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field
from collections import defaultdict, Counter
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pickle
from pathlib import Path

# GPU Computing Imports
try:
    import cupy as cp
    import cudf
    import cuml
    from cuml import DBSCAN, KMeans
    CUDF_AVAILABLE = True
    print("✅ CuDF/CuML available - GPU DataFrames enabled")
except ImportError:
    cp = None
    cudf = None
    cuml = None
    DBSCAN = None
    KMeans = None
    CUDF_AVAILABLE = False
    print("⚠️ CuDF/CuML not available - using Pandas fallback")

try:
    from gpu_acceleration_engine import GPUAccelerationEngine, GPUConfiguration
    GPU_ENGINE_AVAILABLE = True
except ImportError:
    GPU_ENGINE_AVAILABLE = False
    print("⚠️ GPU Acceleration Engine not available")

@dataclass
class AggregationConfig:
    """Configuration for GPU data aggregation operations."""
    enable_gpu: bool = True
    max_memory_usage: float = 0.8  # 80% of GPU memory
    batch_size: int = 50000
    cache_size: int = 1000
    parallel_workers: int = 4
    enable_streaming: bool = True
    compression_level: int = 3
    result_cache_ttl: int = 3600  # 1 hour
    aggregation_methods: List[str] = field(default_factory=lambda: [
        'sum', 'mean', 'median', 'std', 'min', 'max', 'count'
    ])

@dataclass
class CannabisDataPoint:
    """Standard cannabis data point structure."""
    strain_name: str
    dispensary: str
    price: float
    thc: Optional[float] = None
    cbd: Optional[float] = None
    terpenes: Optional[Dict[str, float]] = None
    strain_type: Optional[str] = None  # indica, sativa, hybrid
    category: Optional[str] = None  # flower, vape, edible, etc.
    weight: Optional[float] = None
    quality_score: Optional[float] = None
    timestamp: Optional[datetime] = None
    location: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None

class CannabisDataAggregator:
    """GPU-accelerated data aggregation for cannabis analytics."""
    
    def __init__(self, config: AggregationConfig):
        self.config = config
        self.cache = {}
        self.cache_timestamps = {}
        self.gpu_engine = None
        self.lock = threading.Lock()
        
        if GPU_ENGINE_AVAILABLE and config.enable_gpu:
            self.gpu_engine = GPUAccelerationEngine()
        
        logging.info(f"Cannabis Data Aggregator initialized (GPU: {CUDF_AVAILABLE})")
    
    def aggregate_strain_analytics(self, data_points: List[CannabisDataPoint]) -> Dict[str, Any]:
        """
        Comprehensive strain analytics aggregation.
        
        Aggregates strain data across multiple dimensions:
        - Price statistics by strain
        - THC/CBD distribution analysis
        - Terpene profile clustering
        - Geographic distribution
        - Quality assessments
        """
        cache_key = f"strain_analytics_{len(data_points)}_{hash(str(data_points[:5]))}"
        
        # Check cache
        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]
        
        start_time = time.time()
        
        try:
            if CUDF_AVAILABLE and self.config.enable_gpu:
                result = self._gpu_strain_analytics(data_points)
            else:
                result = self._cpu_strain_analytics(data_points)
            
            # Cache result
            self._cache_result(cache_key, result)
            
            result['processing_time'] = time.time() - start_time
            result['data_points_processed'] = len(data_points)
            result['gpu_accelerated'] = CUDF_AVAILABLE and self.config.enable_gpu
            
            logging.info(f"Strain analytics completed for {len(data_points)} points in {result['processing_time']:.3f}s")
            return result
            
        except Exception as e:
            logging.error(f"Strain analytics aggregation failed: {e}")
            return {'error': str(e), 'processing_time': time.time() - start_time}
    
    def _gpu_strain_analytics(self, data_points: List[CannabisDataPoint]) -> Dict[str, Any]:
        """GPU-accelerated strain analytics."""
        # Convert to CuDF DataFrame
        data_dict = {
            'strain_name': [dp.strain_name for dp in data_points],
            'dispensary': [dp.dispensary for dp in data_points],
            'price': [dp.price for dp in data_points],
            'thc': [dp.thc or 0.0 for dp in data_points],
            'cbd': [dp.cbd or 0.0 for dp in data_points],
            'strain_type': [dp.strain_type or 'unknown' for dp in data_points],
            'category': [dp.category or 'flower' for dp in data_points],
            'quality_score': [dp.quality_score or 5.0 for dp in data_points]
        }
        
        # Add terpene data
        terpene_names = ['myrcene', 'limonene', 'pinene', 'linalool', 'caryophyllene', 'humulene']
        for terp in terpene_names:
            data_dict[terp] = [
                (dp.terpenes or {}).get(terp, 0.0) for dp in data_points
            ]
        
        df = cudf.DataFrame(data_dict)
        
        # Strain-level aggregations
        strain_stats = df.groupby('strain_name').agg({
            'price': ['mean', 'median', 'std', 'min', 'max', 'count'],
            'thc': ['mean', 'std'],
            'cbd': ['mean', 'std'],
            'quality_score': ['mean', 'std'],
            **{terp: 'mean' for terp in terpene_names}
        }).to_pandas()
        
        # Flatten column names
        strain_stats.columns = ['_'.join(col).strip() for col in strain_stats.columns]
        
        # Dispensary analytics
        dispensary_stats = df.groupby('dispensary').agg({
            'price': ['mean', 'count'],
            'strain_name': 'nunique',
            'quality_score': 'mean'
        }).to_pandas()
        dispensary_stats.columns = ['_'.join(col).strip() for col in dispensary_stats.columns]
        
        # Strain type distribution
        strain_type_dist = df['strain_type'].value_counts().to_pandas()
        
        # Category distribution
        category_dist = df['category'].value_counts().to_pandas()
        
        # THC/CBD analysis
        thc_cbd_stats = {
            'thc_mean': float(df['thc'].mean()),
            'thc_std': float(df['thc'].std()),
            'cbd_mean': float(df['cbd'].mean()),
            'cbd_std': float(df['cbd'].std()),
            'thc_cbd_ratio_mean': float((df['thc'] / (df['cbd'] + 0.1)).mean())
        }
        
        # Terpene correlation analysis
        terpene_df = df[terpene_names]
        terpene_corr = terpene_df.corr().to_pandas()
        
        # Price trend analysis
        price_percentiles = df['price'].quantile([0.1, 0.25, 0.5, 0.75, 0.9]).to_pandas()
        
        return {
            'strain_statistics': strain_stats.to_dict('index'),
            'dispensary_analytics': dispensary_stats.to_dict('index'),
            'strain_type_distribution': strain_type_dist.to_dict(),
            'category_distribution': category_dist.to_dict(),
            'thc_cbd_analysis': thc_cbd_stats,
            'terpene_correlations': terpene_corr.to_dict(),
            'price_percentiles': price_percentiles.to_dict(),
            'total_strains': len(strain_stats),
            'total_dispensaries': len(dispensary_stats)
        }
    
    def _cpu_strain_analytics(self, data_points: List[CannabisDataPoint]) -> Dict[str, Any]:
        """CPU fallback for strain analytics."""
        # Convert to Pandas DataFrame
        data_dict = {
            'strain_name': [dp.strain_name for dp in data_points],
            'dispensary': [dp.dispensary for dp in data_points],
            'price': [dp.price for dp in data_points],
            'thc': [dp.thc or 0.0 for dp in data_points],
            'cbd': [dp.cbd or 0.0 for dp in data_points],
            'strain_type': [dp.strain_type or 'unknown' for dp in data_points],
            'category': [dp.category or 'flower' for dp in data_points],
            'quality_score': [dp.quality_score or 5.0 for dp in data_points]
        }
        
        # Add terpene data
        terpene_names = ['myrcene', 'limonene', 'pinene', 'linalool', 'caryophyllene', 'humulene']
        for terp in terpene_names:
            data_dict[terp] = [
                (dp.terpenes or {}).get(terp, 0.0) for dp in data_points
            ]
        
        df = pd.DataFrame(data_dict)
        
        # Strain-level aggregations
        strain_stats = df.groupby('strain_name').agg({
            'price': ['mean', 'median', 'std', 'min', 'max', 'count'],
            'thc': ['mean', 'std'],
            'cbd': ['mean', 'std'],
            'quality_score': ['mean', 'std'],
            **{terp: 'mean' for terp in terpene_names}
        })
        
        # Flatten column names
        strain_stats.columns = ['_'.join(col).strip() for col in strain_stats.columns]
        
        # Dispensary analytics
        dispensary_stats = df.groupby('dispensary').agg({
            'price': ['mean', 'count'],
            'strain_name': 'nunique',
            'quality_score': 'mean'
        })
        dispensary_stats.columns = ['_'.join(col).strip() for col in dispensary_stats.columns]
        
        # Distribution analysis
        strain_type_dist = df['strain_type'].value_counts()
        category_dist = df['category'].value_counts()
        
        # THC/CBD analysis
        thc_cbd_stats = {
            'thc_mean': float(df['thc'].mean()),
            'thc_std': float(df['thc'].std()),
            'cbd_mean': float(df['cbd'].mean()),
            'cbd_std': float(df['cbd'].std()),
            'thc_cbd_ratio_mean': float((df['thc'] / (df['cbd'] + 0.1)).mean())
        }
        
        # Terpene correlation analysis
        terpene_df = df[terpene_names]
        terpene_corr = terpene_df.corr()
        
        # Price analysis
        price_percentiles = df['price'].quantile([0.1, 0.25, 0.5, 0.75, 0.9])
        
        return {
            'strain_statistics': strain_stats.to_dict('index'),
            'dispensary_analytics': dispensary_stats.to_dict('index'),
            'strain_type_distribution': strain_type_dist.to_dict(),
            'category_distribution': category_dist.to_dict(),
            'thc_cbd_analysis': thc_cbd_stats,
            'terpene_correlations': terpene_corr.to_dict(),
            'price_percentiles': price_percentiles.to_dict(),
            'total_strains': len(strain_stats),
            'total_dispensaries': len(dispensary_stats)
        }
    
    def aggregate_terpene_profiles(self, data_points: List[CannabisDataPoint], 
                                 clustering: bool = True) -> Dict[str, Any]:
        """
        Advanced terpene profile aggregation and clustering.
        
        Performs:
        - Terpene distribution analysis
        - Profile clustering using GPU-accelerated algorithms
        - Dominant terpene identification
        - Strain grouping by terpene similarity
        """
        cache_key = f"terpene_profiles_{len(data_points)}_{clustering}"
        
        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Extract terpene data
            terpene_data = []
            strain_names = []
            
            for dp in data_points:
                if dp.terpenes:
                    terpene_data.append(dp.terpenes)
                    strain_names.append(dp.strain_name)
            
            if not terpene_data:
                return {'error': 'No terpene data available'}
            
            if CUDF_AVAILABLE and self.config.enable_gpu:
                result = self._gpu_terpene_analysis(terpene_data, strain_names, clustering)
            else:
                result = self._cpu_terpene_analysis(terpene_data, strain_names, clustering)
            
            result['processing_time'] = time.time() - start_time
            result['profiles_analyzed'] = len(terpene_data)
            result['gpu_accelerated'] = CUDF_AVAILABLE and self.config.enable_gpu
            
            self._cache_result(cache_key, result)
            
            logging.info(f"Terpene analysis completed for {len(terpene_data)} profiles in {result['processing_time']:.3f}s")
            return result
            
        except Exception as e:
            logging.error(f"Terpene aggregation failed: {e}")
            return {'error': str(e), 'processing_time': time.time() - start_time}
    
    def _gpu_terpene_analysis(self, terpene_data: List[Dict[str, float]], 
                            strain_names: List[str], clustering: bool) -> Dict[str, Any]:
        """GPU-accelerated terpene analysis."""
        # Common terpenes
        terpene_names = ['myrcene', 'limonene', 'pinene', 'linalool', 'caryophyllene', 
                        'humulene', 'terpinolene', 'ocimene', 'bisabolol', 'camphene']
        
        # Create terpene matrix
        terpene_matrix = []
        for profile in terpene_data:
            row = [profile.get(terp, 0.0) for terp in terpene_names]
            terpene_matrix.append(row)
        
        # Convert to CuDF DataFrame
        df_data = {terp: [row[i] for row in terpene_matrix] for i, terp in enumerate(terpene_names)}
        df_data['strain'] = strain_names
        df = cudf.DataFrame(df_data)
        
        # Statistical analysis
        terpene_stats = df[terpene_names].describe().to_pandas()
        
        # Correlation analysis
        correlation_matrix = df[terpene_names].corr().to_pandas()
        
        # Dominant terpene analysis
        terpene_matrix_gpu = cp.array(terpene_matrix)
        dominant_indices = cp.argmax(terpene_matrix_gpu, axis=1)
        dominant_terpenes = [terpene_names[idx] for idx in cp.asnumpy(dominant_indices)]
        
        dominant_distribution = Counter(dominant_terpenes)
        
        result = {
            'terpene_statistics': terpene_stats.to_dict(),
            'correlation_matrix': correlation_matrix.to_dict(),
            'dominant_terpene_distribution': dict(dominant_distribution),
            'total_profiles': len(terpene_data)
        }
        
        # Clustering analysis
        if clustering and len(terpene_data) > 5:
            try:
                # Use CuML for GPU clustering
                terpene_cudf = cudf.DataFrame(terpene_matrix, columns=terpene_names)
                
                # K-means clustering
                n_clusters = min(8, len(terpene_data) // 3)
                kmeans = KMeans(n_clusters=n_clusters, random_state=42)
                cluster_labels = kmeans.fit_predict(terpene_cudf).to_pandas()
                
                # DBSCAN clustering for outlier detection
                dbscan = DBSCAN(eps=0.3, min_samples=3)
                dbscan_labels = dbscan.fit_predict(terpene_cudf).to_pandas()
                
                # Cluster analysis
                cluster_analysis = {}
                for cluster_id in set(cluster_labels):
                    cluster_mask = cluster_labels == cluster_id
                    cluster_strains = [strain_names[i] for i, mask in enumerate(cluster_mask) if mask]
                    cluster_profiles = [terpene_data[i] for i, mask in enumerate(cluster_mask) if mask]
                    
                    # Calculate cluster centroid
                    if cluster_profiles:
                        centroid = {}
                        for terp in terpene_names:
                            centroid[terp] = np.mean([p.get(terp, 0.0) for p in cluster_profiles])
                        
                        cluster_analysis[f'cluster_{cluster_id}'] = {
                            'strains': cluster_strains,
                            'size': len(cluster_strains),
                            'centroid': centroid,
                            'dominant_terpene': max(centroid.items(), key=lambda x: x[1])[0]
                        }
                
                result.update({
                    'kmeans_clusters': cluster_analysis,
                    'kmeans_labels': cluster_labels.tolist(),
                    'dbscan_labels': dbscan_labels.tolist(),
                    'n_clusters': n_clusters,
                    'clustering_performed': True
                })
                
            except Exception as e:
                logging.warning(f"GPU clustering failed: {e}")
                result['clustering_error'] = str(e)
        
        return result
    
    def _cpu_terpene_analysis(self, terpene_data: List[Dict[str, float]], 
                            strain_names: List[str], clustering: bool) -> Dict[str, Any]:
        """CPU fallback for terpene analysis."""
        # Common terpenes
        terpene_names = ['myrcene', 'limonene', 'pinene', 'linalool', 'caryophyllene', 
                        'humulene', 'terpinolene', 'ocimene', 'bisabolol', 'camphene']
        
        # Create terpene matrix
        terpene_matrix = []
        for profile in terpene_data:
            row = [profile.get(terp, 0.0) for terp in terpene_names]
            terpene_matrix.append(row)
        
        # Convert to DataFrame
        df = pd.DataFrame(terpene_matrix, columns=terpene_names)
        df['strain'] = strain_names
        
        # Statistical analysis
        terpene_stats = df[terpene_names].describe()
        
        # Correlation analysis
        correlation_matrix = df[terpene_names].corr()
        
        # Dominant terpene analysis
        dominant_indices = np.argmax(terpene_matrix, axis=1)
        dominant_terpenes = [terpene_names[idx] for idx in dominant_indices]
        dominant_distribution = Counter(dominant_terpenes)
        
        result = {
            'terpene_statistics': terpene_stats.to_dict(),
            'correlation_matrix': correlation_matrix.to_dict(),
            'dominant_terpene_distribution': dict(dominant_distribution),
            'total_profiles': len(terpene_data)
        }
        
        # Clustering analysis (CPU version)
        if clustering and len(terpene_data) > 5:
            try:
                from sklearn.cluster import KMeans, DBSCAN
                from sklearn.preprocessing import StandardScaler
                
                # Standardize data
                scaler = StandardScaler()
                scaled_data = scaler.fit_transform(terpene_matrix)
                
                # K-means clustering
                n_clusters = min(8, len(terpene_data) // 3)
                kmeans = KMeans(n_clusters=n_clusters, random_state=42)
                cluster_labels = kmeans.fit_predict(scaled_data)
                
                # Cluster analysis
                cluster_analysis = {}
                for cluster_id in set(cluster_labels):
                    cluster_mask = cluster_labels == cluster_id
                    cluster_strains = [strain_names[i] for i, mask in enumerate(cluster_mask) if mask]
                    cluster_profiles = [terpene_data[i] for i, mask in enumerate(cluster_mask) if mask]
                    
                    # Calculate cluster centroid
                    if cluster_profiles:
                        centroid = {}
                        for terp in terpene_names:
                            centroid[terp] = np.mean([p.get(terp, 0.0) for p in cluster_profiles])
                        
                        cluster_analysis[f'cluster_{cluster_id}'] = {
                            'strains': cluster_strains,
                            'size': len(cluster_strains),
                            'centroid': centroid,
                            'dominant_terpene': max(centroid.items(), key=lambda x: x[1])[0]
                        }
                
                result.update({
                    'kmeans_clusters': cluster_analysis,
                    'kmeans_labels': cluster_labels.tolist(),
                    'n_clusters': n_clusters,
                    'clustering_performed': True
                })
                
            except ImportError:
                result['clustering_error'] = 'sklearn not available'
            except Exception as e:
                result['clustering_error'] = str(e)
        
        return result
    
    def aggregate_price_trends(self, data_points: List[CannabisDataPoint], 
                             time_window: str = 'daily') -> Dict[str, Any]:
        """
        Time-series price trend aggregation.
        
        Analyzes price trends over time with configurable windows:
        - hourly, daily, weekly, monthly aggregations
        - Strain-specific price trends
        - Dispensary price comparisons
        - Market trend analysis
        """
        cache_key = f"price_trends_{len(data_points)}_{time_window}"
        
        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Filter data points with timestamps and prices
            timestamped_data = [dp for dp in data_points if dp.timestamp and dp.price > 0]
            
            if not timestamped_data:
                return {'error': 'No timestamped price data available'}
            
            if CUDF_AVAILABLE and self.config.enable_gpu:
                result = self._gpu_price_trends(timestamped_data, time_window)
            else:
                result = self._cpu_price_trends(timestamped_data, time_window)
            
            result['processing_time'] = time.time() - start_time
            result['data_points_analyzed'] = len(timestamped_data)
            result['gpu_accelerated'] = CUDF_AVAILABLE and self.config.enable_gpu
            
            self._cache_result(cache_key, result)
            
            logging.info(f"Price trends analysis completed for {len(timestamped_data)} points in {result['processing_time']:.3f}s")
            return result
            
        except Exception as e:
            logging.error(f"Price trends aggregation failed: {e}")
            return {'error': str(e), 'processing_time': time.time() - start_time}
    
    def _gpu_price_trends(self, data_points: List[CannabisDataPoint], 
                         time_window: str) -> Dict[str, Any]:
        """GPU-accelerated price trend analysis."""
        # Prepare data
        data_dict = {
            'timestamp': [dp.timestamp for dp in data_points],
            'price': [dp.price for dp in data_points],
            'strain_name': [dp.strain_name for dp in data_points],
            'dispensary': [dp.dispensary for dp in data_points],
            'category': [dp.category or 'flower' for dp in data_points]
        }
        
        df = cudf.DataFrame(data_dict)
        df['timestamp'] = cudf.to_datetime(df['timestamp'])
        
        # Set time grouping based on window
        if time_window == 'hourly':
            df['time_group'] = df['timestamp'].dt.floor('H')
        elif time_window == 'daily':
            df['time_group'] = df['timestamp'].dt.floor('D')
        elif time_window == 'weekly':
            df['time_group'] = df['timestamp'].dt.floor('W')
        elif time_window == 'monthly':
            df['time_group'] = df['timestamp'].dt.floor('M')
        else:
            df['time_group'] = df['timestamp'].dt.floor('D')
        
        # Overall price trends
        price_trends = df.groupby('time_group')['price'].agg(['mean', 'median', 'count']).to_pandas()
        price_trends.index = price_trends.index.astype(str)
        
        # Strain-specific trends
        strain_trends = df.groupby(['strain_name', 'time_group'])['price'].agg(['mean', 'count']).to_pandas()
        strain_trends = strain_trends.reset_index()
        strain_trends['time_group'] = strain_trends['time_group'].astype(str)
        
        # Dispensary price comparisons
        dispensary_trends = df.groupby(['dispensary', 'time_group'])['price'].agg(['mean', 'count']).to_pandas()
        dispensary_trends = dispensary_trends.reset_index()
        dispensary_trends['time_group'] = dispensary_trends['time_group'].astype(str)
        
        # Category trends
        category_trends = df.groupby(['category', 'time_group'])['price'].agg(['mean', 'count']).to_pandas()
        category_trends = category_trends.reset_index()
        category_trends['time_group'] = category_trends['time_group'].astype(str)
        
        return {
            'overall_trends': price_trends.to_dict('index'),
            'strain_trends': strain_trends.to_dict('records'),
            'dispensary_trends': dispensary_trends.to_dict('records'),
            'category_trends': category_trends.to_dict('records'),
            'time_window': time_window,
            'date_range': {
                'start': str(df['timestamp'].min()),
                'end': str(df['timestamp'].max())
            }
        }
    
    def _cpu_price_trends(self, data_points: List[CannabisDataPoint], 
                         time_window: str) -> Dict[str, Any]:
        """CPU fallback for price trend analysis."""
        # Prepare data
        data_dict = {
            'timestamp': [dp.timestamp for dp in data_points],
            'price': [dp.price for dp in data_points],
            'strain_name': [dp.strain_name for dp in data_points],
            'dispensary': [dp.dispensary for dp in data_points],
            'category': [dp.category or 'flower' for dp in data_points]
        }
        
        df = pd.DataFrame(data_dict)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Set time grouping based on window
        if time_window == 'hourly':
            df['time_group'] = df['timestamp'].dt.floor('H')
        elif time_window == 'daily':
            df['time_group'] = df['timestamp'].dt.floor('D')
        elif time_window == 'weekly':
            df['time_group'] = df['timestamp'].dt.floor('W')
        elif time_window == 'monthly':
            df['time_group'] = df['timestamp'].dt.floor('M')
        else:
            df['time_group'] = df['timestamp'].dt.floor('D')
        
        # Overall price trends
        price_trends = df.groupby('time_group')['price'].agg(['mean', 'median', 'count'])
        price_trends.index = price_trends.index.astype(str)
        
        # Strain-specific trends
        strain_trends = df.groupby(['strain_name', 'time_group'])['price'].agg(['mean', 'count'])
        strain_trends = strain_trends.reset_index()
        strain_trends['time_group'] = strain_trends['time_group'].astype(str)
        
        # Dispensary price comparisons
        dispensary_trends = df.groupby(['dispensary', 'time_group'])['price'].agg(['mean', 'count'])
        dispensary_trends = dispensary_trends.reset_index()
        dispensary_trends['time_group'] = dispensary_trends['time_group'].astype(str)
        
        # Category trends
        category_trends = df.groupby(['category', 'time_group'])['price'].agg(['mean', 'count'])
        category_trends = category_trends.reset_index()
        category_trends['time_group'] = category_trends['time_group'].astype(str)
        
        return {
            'overall_trends': price_trends.to_dict('index'),
            'strain_trends': strain_trends.to_dict('records'),
            'dispensary_trends': dispensary_trends.to_dict('records'),
            'category_trends': category_trends.to_dict('records'),
            'time_window': time_window,
            'date_range': {
                'start': str(df['timestamp'].min()),
                'end': str(df['timestamp'].max())
            }
        }
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is valid."""
        if cache_key not in self.cache:
            return False
        
        timestamp = self.cache_timestamps.get(cache_key, 0)
        return time.time() - timestamp < self.config.result_cache_ttl
    
    def _cache_result(self, cache_key: str, result: Dict[str, Any]):
        """Cache aggregation result."""
        with self.lock:
            # Limit cache size
            if len(self.cache) >= self.config.cache_size:
                # Remove oldest entry
                oldest_key = min(self.cache_timestamps.keys(), 
                               key=lambda k: self.cache_timestamps[k])
                del self.cache[oldest_key]
                del self.cache_timestamps[oldest_key]
            
            self.cache[cache_key] = result
            self.cache_timestamps[cache_key] = time.time()
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get aggregation engine performance statistics."""
        return {
            'gpu_available': CUDF_AVAILABLE,
            'cache_entries': len(self.cache),
            'cache_hit_ratio': self._calculate_cache_hit_ratio(),
            'avg_processing_time': self._calculate_avg_processing_time(),
            'memory_usage': self._get_memory_usage()
        }
    
    def _calculate_cache_hit_ratio(self) -> float:
        """Calculate cache hit ratio (placeholder)."""
        return 0.8  # Placeholder - implement actual tracking
    
    def _calculate_avg_processing_time(self) -> float:
        """Calculate average processing time (placeholder)."""
        return 0.5  # Placeholder - implement actual tracking
    
    def _get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage."""
        import psutil
        
        memory_info = psutil.virtual_memory()
        return {
            'system_memory_percent': memory_info.percent,
            'cache_memory_mb': len(str(self.cache)) / (1024 * 1024)
        }
    
    def clear_cache(self):
        """Clear aggregation cache."""
        with self.lock:
            self.cache.clear()
            self.cache_timestamps.clear()
        logging.info("Aggregation cache cleared")

def create_cannabis_aggregator(enable_gpu: bool = True, 
                             batch_size: int = 50000,
                             cache_size: int = 1000) -> CannabisDataAggregator:
    """
    Factory function to create cannabis data aggregator.
    
    Args:
        enable_gpu: Whether to enable GPU acceleration
        batch_size: Processing batch size
        cache_size: Maximum cache entries
    
    Returns:
        Configured cannabis data aggregator
    """
    config = AggregationConfig(
        enable_gpu=enable_gpu and CUDF_AVAILABLE,
        batch_size=batch_size,
        cache_size=cache_size
    )
    
    return CannabisDataAggregator(config)

# Demo and Testing Functions
def demo_gpu_aggregation():
    """Demonstrate GPU data aggregation capabilities."""
    print("\n🚀 GPU Data Aggregation Engine Demo")
    print("=" * 50)
    
    # Initialize aggregator
    aggregator = create_cannabis_aggregator()
    
    # Generate sample cannabis data
    sample_data = []
    strain_names = ["OG Kush", "Blue Dream", "Girl Scout Cookies", "Sour Diesel", "Purple Haze",
                   "Wedding Cake", "Gorilla Glue", "Jack Herer", "White Widow", "AK-47"]
    dispensaries = ["Green Leaf", "Cannabis Corner", "Herb Haven", "Bud Boutique", "The Dispensary"]
    
    for i in range(1000):
        strain = np.random.choice(strain_names)
        dispensary = np.random.choice(dispensaries)
        
        data_point = CannabisDataPoint(
            strain_name=strain,
            dispensary=dispensary,
            price=8 + np.random.rand() * 15,
            thc=15 + np.random.rand() * 20,
            cbd=np.random.rand() * 8,
            terpenes={
                'myrcene': np.random.rand() * 2,
                'limonene': np.random.rand() * 1.5,
                'pinene': np.random.rand() * 1,
                'linalool': np.random.rand() * 0.8,
                'caryophyllene': np.random.rand() * 1.2,
                'humulene': np.random.rand() * 0.6
            },
            strain_type=np.random.choice(['indica', 'sativa', 'hybrid']),
            category=np.random.choice(['flower', 'vape', 'edible']),
            quality_score=3 + np.random.rand() * 4,
            timestamp=datetime.now() - timedelta(days=np.random.randint(0, 30))
        )
        sample_data.append(data_point)
    
    print(f"\n📊 Testing with {len(sample_data)} sample data points...")
    
    # Test strain analytics
    print("\n🧬 Strain Analytics:")
    strain_result = aggregator.aggregate_strain_analytics(sample_data)
    print(f"  Processing time: {strain_result['processing_time']:.3f}s")
    print(f"  GPU accelerated: {strain_result['gpu_accelerated']}")
    print(f"  Total strains analyzed: {strain_result.get('total_strains', 0)}")
    print(f"  Total dispensaries: {strain_result.get('total_dispensaries', 0)}")
    
    # Test terpene analysis
    print("\n🧪 Terpene Profile Analysis:")
    terpene_result = aggregator.aggregate_terpene_profiles(sample_data, clustering=True)
    print(f"  Processing time: {terpene_result['processing_time']:.3f}s")
    print(f"  Profiles analyzed: {terpene_result.get('profiles_analyzed', 0)}")
    print(f"  Clustering performed: {terpene_result.get('clustering_performed', False)}")
    
    # Test price trends
    print("\n💰 Price Trend Analysis:")
    price_result = aggregator.aggregate_price_trends(sample_data, time_window='daily')
    print(f"  Processing time: {price_result['processing_time']:.3f}s")
    print(f"  Data points analyzed: {price_result.get('data_points_analyzed', 0)}")
    if 'date_range' in price_result:
        print(f"  Date range: {price_result['date_range']['start']} to {price_result['date_range']['end']}")
    
    # Performance stats
    print(f"\n📈 Performance Statistics:")
    perf_stats = aggregator.get_performance_stats()
    print(f"  GPU Available: {perf_stats['gpu_available']}")
    print(f"  Cache Entries: {perf_stats['cache_entries']}")
    print(f"  Memory Usage: {perf_stats['memory_usage']['system_memory_percent']:.1f}%")
    
    print("\n✅ Demo completed successfully!")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run demo
    demo_gpu_aggregation()