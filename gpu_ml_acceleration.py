#!/usr/bin/env python3
"""
GPU-Accelerated Machine Learning Engine for Cannabis Analytics
============================================================

Advanced machine learning models optimized for cannabis data with GPU acceleration.
Provides high-performance inference and training for strain recommendation,
price prediction, terpene analysis, and quality assessment.

Key Features:
- GPU-accelerated neural networks for strain recommendation
- Real-time price prediction with market trend analysis
- Terpene profile similarity and clustering models
- Cannabis quality assessment and grading
- Customer preference learning and personalization
- Market demand forecasting
- Automated strain classification

Author: WeedHounds AI Team
Created: November 2025
"""

import logging
import time
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field
from pathlib import Path
import json
import pickle
import threading
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# GPU Computing Imports
try:
    import cupy as cp
    import cudf
    import cuml
    from cuml import RandomForestRegressor, RandomForestClassifier
    from cuml import LinearRegression, LogisticRegression
    from cuml import KMeans, DBSCAN
    from cuml.preprocessing import StandardScaler as CuMLScaler
    CUML_AVAILABLE = True
    print("✅ CuML available - GPU ML enabled")
except ImportError:
    cp = None
    cudf = None
    cuml = None
    RandomForestRegressor = None
    RandomForestClassifier = None
    LinearRegression = None
    LogisticRegression = None
    KMeans = None
    DBSCAN = None
    CuMLScaler = None
    CUML_AVAILABLE = False
    print("⚠️ CuML not available - using CPU ML fallback")

# Deep Learning Imports
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
    CUDA_DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"✅ PyTorch available - Device: {CUDA_DEVICE}")
except ImportError:
    torch = None
    nn = None
    optim = None
    DataLoader = None
    TensorDataset = None
    TORCH_AVAILABLE = False
    CUDA_DEVICE = None
    print("⚠️ PyTorch not available")

# Traditional ML Fallback
try:
    from sklearn.ensemble import RandomForestRegressor as SKRandomForestRegressor
    from sklearn.ensemble import RandomForestClassifier as SKRandomForestClassifier
    from sklearn.linear_model import LinearRegression as SKLinearRegression
    from sklearn.linear_model import LogisticRegression as SKLogisticRegression
    from sklearn.cluster import KMeans as SKKMeans
    from sklearn.preprocessing import StandardScaler as SKStandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_squared_error, accuracy_score, classification_report
    SKLEARN_AVAILABLE = True
    print("✅ Scikit-learn available")
except ImportError:
    SKLEARN_AVAILABLE = False
    print("⚠️ Scikit-learn not available")

try:
    from gpu_acceleration_engine import GPUAccelerationEngine
    from gpu_data_aggregation import CannabisDataPoint, create_cannabis_aggregator
    GPU_ENGINES_AVAILABLE = True
except ImportError:
    GPU_ENGINES_AVAILABLE = False
    print("⚠️ GPU engines not available")

@dataclass
class MLModelConfig:
    """Configuration for ML models."""
    enable_gpu: bool = True
    model_cache_size: int = 50
    batch_size: int = 1024
    learning_rate: float = 0.001
    max_epochs: int = 100
    early_stopping_patience: int = 10
    validation_split: float = 0.2
    random_state: int = 42
    model_save_path: str = "models/"
    enable_model_persistence: bool = True
    performance_threshold: float = 0.8

class CannabisNeuralNetwork(nn.Module):
    """
    Neural network for cannabis data analysis.
    
    Supports multiple tasks:
    - Strain recommendation
    - Price prediction  
    - Quality assessment
    - Terpene profile analysis
    """
    
    def __init__(self, input_size: int, hidden_sizes: List[int], 
                 output_size: int, task_type: str = 'regression'):
        super(CannabisNeuralNetwork, self).__init__()
        
        self.task_type = task_type
        self.layers = nn.ModuleList()
        
        # Build network architecture
        layer_sizes = [input_size] + hidden_sizes + [output_size]
        
        for i in range(len(layer_sizes) - 1):
            self.layers.append(nn.Linear(layer_sizes[i], layer_sizes[i + 1]))
            
            # Add batch normalization and dropout (except for output layer)
            if i < len(layer_sizes) - 2:
                self.layers.append(nn.BatchNorm1d(layer_sizes[i + 1]))
                self.layers.append(nn.ReLU())
                self.layers.append(nn.Dropout(0.2))
        
        # Output activation based on task
        if task_type == 'classification':
            self.layers.append(nn.Softmax(dim=1))
        elif task_type == 'binary_classification':
            self.layers.append(nn.Sigmoid())
        # For regression, no activation needed
    
    def forward(self, x):
        for layer in self.layers:
            x = layer(x)
        return x

class StrainRecommendationModel:
    """GPU-accelerated strain recommendation system."""
    
    def __init__(self, config: MLModelConfig):
        self.config = config
        self.model = None
        self.scaler = None
        self.feature_names = []
        self.strain_encoder = {}
        self.is_trained = False
        
    def prepare_features(self, data_points: List[CannabisDataPoint]) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare features for strain recommendation."""
        features = []
        labels = []
        
        # Create strain encoder
        unique_strains = list(set(dp.strain_name for dp in data_points))
        self.strain_encoder = {strain: idx for idx, strain in enumerate(unique_strains)}
        
        for dp in data_points:
            feature_vector = [
                dp.thc or 0.0,
                dp.cbd or 0.0,
                1.0 if dp.strain_type == 'indica' else 0.0,
                1.0 if dp.strain_type == 'sativa' else 0.0,
                1.0 if dp.strain_type == 'hybrid' else 0.0,
                dp.quality_score or 5.0,
                dp.price or 0.0
            ]
            
            # Add terpene features
            if dp.terpenes:
                terpene_features = [
                    dp.terpenes.get('myrcene', 0.0),
                    dp.terpenes.get('limonene', 0.0),
                    dp.terpenes.get('pinene', 0.0),
                    dp.terpenes.get('linalool', 0.0),
                    dp.terpenes.get('caryophyllene', 0.0),
                    dp.terpenes.get('humulene', 0.0)
                ]
                feature_vector.extend(terpene_features)
            else:
                feature_vector.extend([0.0] * 6)
            
            features.append(feature_vector)
            labels.append(self.strain_encoder[dp.strain_name])
        
        self.feature_names = [
            'thc', 'cbd', 'indica', 'sativa', 'hybrid', 'quality_score', 'price',
            'myrcene', 'limonene', 'pinene', 'linalool', 'caryophyllene', 'humulene'
        ]
        
        return np.array(features), np.array(labels)
    
    def train(self, data_points: List[CannabisDataPoint]) -> Dict[str, Any]:
        """Train strain recommendation model."""
        start_time = time.time()
        
        try:
            # Prepare data
            X, y = self.prepare_features(data_points)
            
            if TORCH_AVAILABLE and self.config.enable_gpu:
                return self._train_neural_network(X, y)
            elif CUML_AVAILABLE and self.config.enable_gpu:
                return self._train_cuml_model(X, y)
            elif SKLEARN_AVAILABLE:
                return self._train_sklearn_model(X, y)
            else:
                raise RuntimeError("No ML libraries available")
                
        except Exception as e:
            logging.error(f"Strain recommendation training failed: {e}")
            return {'error': str(e), 'training_time': time.time() - start_time}
    
    def _train_neural_network(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """Train PyTorch neural network."""
        # Prepare data
        X_tensor = torch.FloatTensor(X).to(CUDA_DEVICE)
        y_tensor = torch.LongTensor(y).to(CUDA_DEVICE)
        
        # Split data
        dataset = TensorDataset(X_tensor, y_tensor)
        train_size = int(0.8 * len(dataset))
        val_size = len(dataset) - train_size
        train_dataset, val_dataset = torch.utils.data.random_split(dataset, [train_size, val_size])
        
        train_loader = DataLoader(train_dataset, batch_size=self.config.batch_size, shuffle=True)
        val_loader = DataLoader(val_dataset, batch_size=self.config.batch_size, shuffle=False)
        
        # Create model
        input_size = X.shape[1]
        output_size = len(self.strain_encoder)
        hidden_sizes = [128, 64, 32]
        
        self.model = CannabisNeuralNetwork(
            input_size=input_size,
            hidden_sizes=hidden_sizes,
            output_size=output_size,
            task_type='classification'
        ).to(CUDA_DEVICE)
        
        # Training setup
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(self.model.parameters(), lr=self.config.learning_rate)
        scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=5)
        
        # Training loop
        train_losses = []
        val_accuracies = []
        best_val_acc = 0.0
        patience_counter = 0
        
        for epoch in range(self.config.max_epochs):
            # Training phase
            self.model.train()
            train_loss = 0.0
            
            for batch_X, batch_y in train_loader:
                optimizer.zero_grad()
                outputs = self.model(batch_X)
                loss = criterion(outputs, batch_y)
                loss.backward()
                optimizer.step()
                train_loss += loss.item()
            
            # Validation phase
            self.model.eval()
            val_correct = 0
            val_total = 0
            
            with torch.no_grad():
                for batch_X, batch_y in val_loader:
                    outputs = self.model(batch_X)
                    _, predicted = torch.max(outputs.data, 1)
                    val_total += batch_y.size(0)
                    val_correct += (predicted == batch_y).sum().item()
            
            val_acc = val_correct / val_total
            avg_train_loss = train_loss / len(train_loader)
            
            train_losses.append(avg_train_loss)
            val_accuracies.append(val_acc)
            
            # Learning rate scheduling
            scheduler.step(avg_train_loss)
            
            # Early stopping
            if val_acc > best_val_acc:
                best_val_acc = val_acc
                patience_counter = 0
                # Save best model
                if self.config.enable_model_persistence:
                    self._save_model()
            else:
                patience_counter += 1
                if patience_counter >= self.config.early_stopping_patience:
                    break
            
            if epoch % 10 == 0:
                logging.info(f"Epoch {epoch}: Train Loss: {avg_train_loss:.4f}, Val Acc: {val_acc:.4f}")
        
        self.is_trained = True
        
        return {
            'model_type': 'neural_network',
            'best_validation_accuracy': best_val_acc,
            'training_epochs': epoch + 1,
            'train_losses': train_losses,
            'validation_accuracies': val_accuracies,
            'feature_count': input_size,
            'strain_count': output_size,
            'gpu_accelerated': True
        }
    
    def _train_cuml_model(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """Train CuML model."""
        # Convert to CuDF
        X_cudf = cudf.DataFrame(X, columns=self.feature_names)
        y_cudf = cudf.Series(y)
        
        # Scale features
        self.scaler = CuMLScaler()
        X_scaled = self.scaler.fit_transform(X_cudf)
        
        # Train Random Forest
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=self.config.random_state
        )
        
        self.model.fit(X_scaled, y_cudf)
        
        # Calculate accuracy
        predictions = self.model.predict(X_scaled)
        accuracy = (predictions == y_cudf).sum() / len(y_cudf)
        
        self.is_trained = True
        
        return {
            'model_type': 'random_forest_cuml',
            'accuracy': float(accuracy),
            'feature_count': X.shape[1],
            'strain_count': len(self.strain_encoder),
            'gpu_accelerated': True
        }
    
    def _train_sklearn_model(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """Train scikit-learn model."""
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.preprocessing import StandardScaler
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=self.config.random_state
        )
        
        # Scale features
        self.scaler = StandardScaler()
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train model
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=self.config.random_state
        )
        
        self.model.fit(X_train_scaled, y_train)
        
        # Calculate accuracy
        predictions = self.model.predict(X_test_scaled)
        accuracy = accuracy_score(y_test, predictions)
        
        self.is_trained = True
        
        return {
            'model_type': 'random_forest_sklearn',
            'accuracy': accuracy,
            'feature_count': X.shape[1],
            'strain_count': len(self.strain_encoder),
            'gpu_accelerated': False
        }
    
    def predict_strains(self, user_preferences: Dict[str, Any], 
                       top_k: int = 5) -> List[Tuple[str, float]]:
        """Predict recommended strains based on user preferences."""
        if not self.is_trained:
            raise RuntimeError("Model must be trained before prediction")
        
        # Convert preferences to feature vector
        feature_vector = [
            user_preferences.get('desired_thc', 20.0),
            user_preferences.get('desired_cbd', 2.0),
            1.0 if user_preferences.get('preferred_type') == 'indica' else 0.0,
            1.0 if user_preferences.get('preferred_type') == 'sativa' else 0.0,
            1.0 if user_preferences.get('preferred_type') == 'hybrid' else 0.0,
            user_preferences.get('quality_threshold', 7.0),
            user_preferences.get('max_price', 15.0)
        ]
        
        # Add terpene preferences
        preferred_terpenes = user_preferences.get('preferred_terpenes', {})
        terpene_features = [
            preferred_terpenes.get('myrcene', 0.5),
            preferred_terpenes.get('limonene', 0.5),
            preferred_terpenes.get('pinene', 0.5),
            preferred_terpenes.get('linalool', 0.5),
            preferred_terpenes.get('caryophyllene', 0.5),
            preferred_terpenes.get('humulene', 0.5)
        ]
        feature_vector.extend(terpene_features)
        
        # Make prediction
        try:
            if TORCH_AVAILABLE and isinstance(self.model, CannabisNeuralNetwork):
                return self._predict_neural_network(feature_vector, top_k)
            elif CUML_AVAILABLE and hasattr(self.model, 'predict_proba'):
                return self._predict_cuml(feature_vector, top_k)
            else:
                return self._predict_sklearn(feature_vector, top_k)
        except Exception as e:
            logging.error(f"Strain prediction failed: {e}")
            return []
    
    def _predict_neural_network(self, feature_vector: List[float], top_k: int) -> List[Tuple[str, float]]:
        """Neural network prediction."""
        self.model.eval()
        
        with torch.no_grad():
            X_tensor = torch.FloatTensor([feature_vector]).to(CUDA_DEVICE)
            outputs = self.model(X_tensor)
            probabilities = torch.softmax(outputs, dim=1).cpu().numpy()[0]
        
        # Get top-k predictions
        top_indices = np.argsort(probabilities)[-top_k:][::-1]
        strain_names = {idx: strain for strain, idx in self.strain_encoder.items()}
        
        recommendations = []
        for idx in top_indices:
            strain_name = strain_names[idx]
            confidence = float(probabilities[idx])
            recommendations.append((strain_name, confidence))
        
        return recommendations
    
    def _predict_cuml(self, feature_vector: List[float], top_k: int) -> List[Tuple[str, float]]:
        """CuML prediction."""
        # Scale input
        X_cudf = cudf.DataFrame([feature_vector], columns=self.feature_names)
        X_scaled = self.scaler.transform(X_cudf)
        
        # Get probabilities
        probabilities = self.model.predict_proba(X_scaled).to_pandas().iloc[0]
        
        # Get top-k predictions
        top_indices = probabilities.argsort()[-top_k:][::-1]
        strain_names = {idx: strain for strain, idx in self.strain_encoder.items()}
        
        recommendations = []
        for idx in top_indices:
            strain_name = strain_names[idx]
            confidence = float(probabilities.iloc[idx])
            recommendations.append((strain_name, confidence))
        
        return recommendations
    
    def _predict_sklearn(self, feature_vector: List[float], top_k: int) -> List[Tuple[str, float]]:
        """Scikit-learn prediction."""
        # Scale input
        X_scaled = self.scaler.transform([feature_vector])
        
        # Get probabilities
        probabilities = self.model.predict_proba(X_scaled)[0]
        
        # Get top-k predictions
        top_indices = np.argsort(probabilities)[-top_k:][::-1]
        strain_names = {idx: strain for strain, idx in self.strain_encoder.items()}
        
        recommendations = []
        for idx in top_indices:
            strain_name = strain_names[idx]
            confidence = float(probabilities[idx])
            recommendations.append((strain_name, confidence))
        
        return recommendations
    
    def _save_model(self):
        """Save trained model."""
        if not self.config.enable_model_persistence:
            return
        
        model_dir = Path(self.config.model_save_path)
        model_dir.mkdir(exist_ok=True)
        
        if TORCH_AVAILABLE and isinstance(self.model, CannabisNeuralNetwork):
            torch.save(self.model.state_dict(), model_dir / "strain_recommendation_nn.pth")
        
        # Save metadata
        metadata = {
            'strain_encoder': self.strain_encoder,
            'feature_names': self.feature_names,
            'model_type': type(self.model).__name__,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(model_dir / "strain_recommendation_metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)

class PricePredictionModel:
    """GPU-accelerated price prediction model."""
    
    def __init__(self, config: MLModelConfig):
        self.config = config
        self.model = None
        self.scaler = None
        self.feature_names = []
        self.is_trained = False
    
    def train(self, data_points: List[CannabisDataPoint]) -> Dict[str, Any]:
        """Train price prediction model."""
        start_time = time.time()
        
        try:
            # Prepare features
            X, y = self._prepare_price_features(data_points)
            
            if CUML_AVAILABLE and self.config.enable_gpu:
                return self._train_cuml_regression(X, y)
            elif SKLEARN_AVAILABLE:
                return self._train_sklearn_regression(X, y)
            else:
                raise RuntimeError("No ML libraries available")
                
        except Exception as e:
            logging.error(f"Price prediction training failed: {e}")
            return {'error': str(e), 'training_time': time.time() - start_time}
    
    def _prepare_price_features(self, data_points: List[CannabisDataPoint]) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare features for price prediction."""
        features = []
        prices = []
        
        for dp in data_points:
            if dp.price is None or dp.price <= 0:
                continue
            
            feature_vector = [
                dp.thc or 0.0,
                dp.cbd or 0.0,
                dp.quality_score or 5.0,
                dp.weight or 1.0,
                1.0 if dp.strain_type == 'indica' else 0.0,
                1.0 if dp.strain_type == 'sativa' else 0.0,
                1.0 if dp.strain_type == 'hybrid' else 0.0,
                1.0 if dp.category == 'flower' else 0.0,
                1.0 if dp.category == 'vape' else 0.0,
                1.0 if dp.category == 'edible' else 0.0
            ]
            
            # Add terpene features
            if dp.terpenes:
                terpene_sum = sum(dp.terpenes.values())
                terpene_diversity = len([v for v in dp.terpenes.values() if v > 0])
                feature_vector.extend([terpene_sum, terpene_diversity])
            else:
                feature_vector.extend([0.0, 0.0])
            
            features.append(feature_vector)
            prices.append(dp.price)
        
        self.feature_names = [
            'thc', 'cbd', 'quality_score', 'weight', 'indica', 'sativa', 'hybrid',
            'flower', 'vape', 'edible', 'terpene_sum', 'terpene_diversity'
        ]
        
        return np.array(features), np.array(prices)
    
    def _train_cuml_regression(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """Train CuML regression model."""
        # Convert to CuDF
        X_cudf = cudf.DataFrame(X, columns=self.feature_names)
        y_cudf = cudf.Series(y)
        
        # Scale features
        self.scaler = CuMLScaler()
        X_scaled = self.scaler.fit_transform(X_cudf)
        
        # Train Random Forest Regressor
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=15,
            random_state=self.config.random_state
        )
        
        self.model.fit(X_scaled, y_cudf)
        
        # Calculate R² score
        predictions = self.model.predict(X_scaled)
        r2_score = self._calculate_r2_cudf(y_cudf, predictions)
        
        self.is_trained = True
        
        return {
            'model_type': 'random_forest_regressor_cuml',
            'r2_score': float(r2_score),
            'feature_count': X.shape[1],
            'training_samples': len(y),
            'gpu_accelerated': True
        }
    
    def _train_sklearn_regression(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """Train scikit-learn regression model."""
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.preprocessing import StandardScaler
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import r2_score, mean_squared_error
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=self.config.random_state
        )
        
        # Scale features
        self.scaler = StandardScaler()
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train model
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=15,
            random_state=self.config.random_state
        )
        
        self.model.fit(X_train_scaled, y_train)
        
        # Evaluate
        predictions = self.model.predict(X_test_scaled)
        r2 = r2_score(y_test, predictions)
        mse = mean_squared_error(y_test, predictions)
        
        self.is_trained = True
        
        return {
            'model_type': 'random_forest_regressor_sklearn',
            'r2_score': r2,
            'mse': mse,
            'feature_count': X.shape[1],
            'training_samples': len(y_train),
            'test_samples': len(y_test),
            'gpu_accelerated': False
        }
    
    def _calculate_r2_cudf(self, y_true, y_pred):
        """Calculate R² score for CuDF data."""
        ss_res = ((y_true - y_pred) ** 2).sum()
        ss_tot = ((y_true - y_true.mean()) ** 2).sum()
        return 1 - (ss_res / ss_tot)
    
    def predict_price(self, product_features: Dict[str, Any]) -> float:
        """Predict price for a cannabis product."""
        if not self.is_trained:
            raise RuntimeError("Model must be trained before prediction")
        
        # Convert features to vector
        feature_vector = [
            product_features.get('thc', 20.0),
            product_features.get('cbd', 2.0),
            product_features.get('quality_score', 7.0),
            product_features.get('weight', 1.0),
            1.0 if product_features.get('strain_type') == 'indica' else 0.0,
            1.0 if product_features.get('strain_type') == 'sativa' else 0.0,
            1.0 if product_features.get('strain_type') == 'hybrid' else 0.0,
            1.0 if product_features.get('category') == 'flower' else 0.0,
            1.0 if product_features.get('category') == 'vape' else 0.0,
            1.0 if product_features.get('category') == 'edible' else 0.0,
            product_features.get('terpene_sum', 0.0),
            product_features.get('terpene_diversity', 0.0)
        ]
        
        try:
            if CUML_AVAILABLE and hasattr(self.model, 'predict'):
                # CuML prediction
                X_cudf = cudf.DataFrame([feature_vector], columns=self.feature_names)
                X_scaled = self.scaler.transform(X_cudf)
                prediction = self.model.predict(X_scaled)
                return float(prediction.iloc[0])
            else:
                # Scikit-learn prediction
                X_scaled = self.scaler.transform([feature_vector])
                prediction = self.model.predict(X_scaled)
                return float(prediction[0])
        except Exception as e:
            logging.error(f"Price prediction failed: {e}")
            return 0.0

class MLModelManager:
    """Manager for all GPU-accelerated ML models."""
    
    def __init__(self, config: Optional[MLModelConfig] = None):
        self.config = config or MLModelConfig()
        self.models = {}
        self.model_stats = {}
        self.lock = threading.Lock()
        
        logging.info("ML Model Manager initialized")
        logging.info(f"GPU Acceleration: {CUML_AVAILABLE or TORCH_AVAILABLE}")
    
    def train_strain_recommendation(self, data_points: List[CannabisDataPoint]) -> Dict[str, Any]:
        """Train strain recommendation model."""
        model = StrainRecommendationModel(self.config)
        result = model.train(data_points)
        
        if 'error' not in result:
            with self.lock:
                self.models['strain_recommendation'] = model
                self.model_stats['strain_recommendation'] = result
        
        return result
    
    def train_price_prediction(self, data_points: List[CannabisDataPoint]) -> Dict[str, Any]:
        """Train price prediction model."""
        model = PricePredictionModel(self.config)
        result = model.train(data_points)
        
        if 'error' not in result:
            with self.lock:
                self.models['price_prediction'] = model
                self.model_stats['price_prediction'] = result
        
        return result
    
    def get_strain_recommendations(self, user_preferences: Dict[str, Any], 
                                 top_k: int = 5) -> List[Tuple[str, float]]:
        """Get strain recommendations."""
        if 'strain_recommendation' not in self.models:
            raise RuntimeError("Strain recommendation model not trained")
        
        model = self.models['strain_recommendation']
        return model.predict_strains(user_preferences, top_k)
    
    def predict_product_price(self, product_features: Dict[str, Any]) -> float:
        """Predict product price."""
        if 'price_prediction' not in self.models:
            raise RuntimeError("Price prediction model not trained")
        
        model = self.models['price_prediction']
        return model.predict_price(product_features)
    
    def get_model_statistics(self) -> Dict[str, Any]:
        """Get comprehensive model statistics."""
        stats = {
            'trained_models': list(self.models.keys()),
            'gpu_available': CUML_AVAILABLE or TORCH_AVAILABLE,
            'torch_available': TORCH_AVAILABLE,
            'cuml_available': CUML_AVAILABLE,
            'sklearn_available': SKLEARN_AVAILABLE,
            'model_performance': self.model_stats.copy()
        }
        
        if TORCH_AVAILABLE:
            stats['cuda_device'] = str(CUDA_DEVICE)
            stats['gpu_memory_allocated'] = torch.cuda.memory_allocated() if torch.cuda.is_available() else 0
        
        return stats
    
    def cleanup_models(self):
        """Cleanup model resources."""
        with self.lock:
            self.models.clear()
            self.model_stats.clear()
        
        if TORCH_AVAILABLE and torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        logging.info("ML models cleanup completed")

def create_ml_manager(enable_gpu: bool = True, 
                     model_cache_size: int = 50,
                     batch_size: int = 1024) -> MLModelManager:
    """
    Factory function to create ML model manager.
    
    Args:
        enable_gpu: Whether to enable GPU acceleration
        model_cache_size: Maximum cached models
        batch_size: Training batch size
    
    Returns:
        Configured ML model manager
    """
    config = MLModelConfig(
        enable_gpu=enable_gpu and (CUML_AVAILABLE or TORCH_AVAILABLE),
        model_cache_size=model_cache_size,
        batch_size=batch_size
    )
    
    return MLModelManager(config)

# Demo and Testing Functions
def demo_ml_acceleration():
    """Demonstrate GPU ML acceleration capabilities."""
    print("\n🚀 GPU ML Acceleration Engine Demo")
    print("=" * 50)
    
    # Initialize ML manager
    ml_manager = create_ml_manager()
    
    # Display capabilities
    stats = ml_manager.get_model_statistics()
    print(f"GPU Available: {stats['gpu_available']}")
    print(f"PyTorch Available: {stats['torch_available']}")
    print(f"CuML Available: {stats['cuml_available']}")
    
    # Generate sample cannabis data
    sample_data = []
    strain_names = ["OG Kush", "Blue Dream", "Girl Scout Cookies", "Sour Diesel", "Purple Haze"]
    dispensaries = ["Green Leaf", "Cannabis Corner", "Herb Haven"]
    
    for i in range(500):
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
            category='flower',
            quality_score=3 + np.random.rand() * 4,
            weight=1.0
        )
        sample_data.append(data_point)
    
    print(f"\n📊 Training with {len(sample_data)} sample data points...")
    
    # Train strain recommendation model
    print("\n🧬 Training Strain Recommendation Model:")
    strain_result = ml_manager.train_strain_recommendation(sample_data)
    if 'error' not in strain_result:
        print(f"  Model: {strain_result['model_type']}")
        print(f"  Accuracy: {strain_result.get('accuracy', strain_result.get('best_validation_accuracy', 'N/A'))}")
        print(f"  GPU Accelerated: {strain_result['gpu_accelerated']}")
        
        # Test recommendations
        user_prefs = {
            'desired_thc': 25.0,
            'desired_cbd': 1.0,
            'preferred_type': 'indica',
            'quality_threshold': 8.0,
            'max_price': 20.0
        }
        
        recommendations = ml_manager.get_strain_recommendations(user_prefs, top_k=3)
        print(f"  Sample recommendations: {[r[0] for r in recommendations]}")
    
    # Train price prediction model
    print("\n💰 Training Price Prediction Model:")
    price_result = ml_manager.train_price_prediction(sample_data)
    if 'error' not in price_result:
        print(f"  Model: {price_result['model_type']}")
        print(f"  R² Score: {price_result.get('r2_score', 'N/A')}")
        print(f"  GPU Accelerated: {price_result['gpu_accelerated']}")
        
        # Test price prediction
        product_features = {
            'thc': 22.0,
            'cbd': 1.5,
            'quality_score': 8.5,
            'strain_type': 'indica',
            'category': 'flower',
            'weight': 1.0,
            'terpene_sum': 5.0,
            'terpene_diversity': 4
        }
        
        predicted_price = ml_manager.predict_product_price(product_features)
        print(f"  Sample price prediction: ${predicted_price:.2f}")
    
    # Final statistics
    print(f"\n📈 Final Model Statistics:")
    final_stats = ml_manager.get_model_statistics()
    print(f"  Trained Models: {len(final_stats['trained_models'])}")
    for model_name in final_stats['trained_models']:
        print(f"    - {model_name}")
    
    # Cleanup
    ml_manager.cleanup_models()
    print("\n✅ Demo completed successfully!")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run demo
    demo_ml_acceleration()