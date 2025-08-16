import logging
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.linear_model import SGDRegressor, SGDClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, accuracy_score, mean_absolute_error
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.neural_network import MLPRegressor, MLPClassifier
import joblib
import os
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime, timedelta
import threading
import time
from collections import deque
import warnings
import shap # Import SHAP

from utils.feature_normalizer import normalize_features # Import the new normalizer
from exceptions import MLPredictionError

warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


class MLModel:
    """
    Улучшенная модель машинного обучения с поддержкой:
    - Онлайн обучения на потоковых данных
    - Нормализации данных разного масштаба
    - Инкрементального обучения
    - Автоматического сохранения и загрузки
    - Адаптивного обучения
    - Объяснимого ИИ (SHAP)
    """

    def __init__(self, model_path="ml_model.joblib",
                 incremental_learning=True,
                 auto_retrain_threshold=1000,
                 model_type="hybrid"):
        self.model_path = model_path
        self.incremental_learning = incremental_learning
        self.auto_retrain_threshold = auto_retrain_threshold
        self.model_type = model_type  # "hybrid", "random_forest", "neural_network"

        # Модели для разных задач
        self.price_model: Optional[Any] = None
        self.direction_model: Optional[Any] = None

        # Онлайн модели для инкрементального обучения
        self.online_price_model: Optional[SGDRegressor] = None
        self.online_direction_model: Optional[SGDClassifier] = None

        # Скейлеры для нормализации данных разного масштаба
        self.robust_scaler: Optional[RobustScaler] = None
        self.standard_scaler: Optional[StandardScaler] = None

        # Метаданные модели
        self.feature_names: Optional[List[str]] = None
        self.feature_importance: Optional[Dict[str, float]] = None
        self.training_stats: Dict[str, Any] = {
            'samples_processed': 0,
            'last_training_time': None,
            'model_version': 1,
            'accuracy_history': deque(maxlen=100),
            'error_history': deque(maxlen=100)
        }

        # Буфер для накопления данных
        self.training_buffer = deque(maxlen=10000)
        self.prediction_cache = {}

        # Блокировки для потокобезопасности
        self.model_lock = threading.RLock()
        self.buffer_lock = threading.Lock()

        # Инициализация
        self._load_models()
        self._initialize_online_models()
        # logger.info("MLModel initialized successfully.")

    def _initialize_online_models(self):
        """Инициализация моделей для онлайн обучения"""
        try:
            # SGD модели для инкрементального обучения
            self.online_price_model = SGDRegressor(
                learning_rate='adaptive',
                eta0=0.01,
                alpha=0.001,
                random_state=42,
                warm_start=True
            )

            self.online_direction_model = SGDClassifier(
                learning_rate='adaptive',
                eta0=0.01,
                alpha=0.001,
                random_state=42,
                warm_start=True,
                loss='log_loss'
            )

            # Робастный скейлер для данных разного масштаба
            self.robust_scaler = RobustScaler()
            self.standard_scaler = StandardScaler()

            # logger.info("Online models initialized.")

        except Exception as e:
            logger.error(f"Failed to initialize online models: {e}")
            raise MLPredictionError(f"Failed to initialize online models: {e}")

    def _load_models(self):
        """Загрузка предобученных моделей"""
        if os.path.exists(self.model_path):
            try:
                models_data = joblib.load(self.model_path)

                self.price_model = models_data.get('price_model')
                self.direction_model = models_data.get('direction_model')
                self.online_price_model = models_data.get('online_price_model')
                self.online_direction_model = models_data.get('online_direction_model')
                self.robust_scaler = models_data.get('robust_scaler')
                self.standard_scaler = models_data.get('standard_scaler')
                self.feature_names = models_data.get('feature_names')
                self.feature_importance = models_data.get('feature_importance')
                self.training_stats = models_data.get('training_stats', self.training_stats)

                # logger.info(f"Models loaded successfully. Version: {self.training_stats.get('model_version', 1)}")

            except Exception as e:
                logger.warning(f"Failed to load models: {e}. Initializing new models.", exc_info=True)
                self._initialize_new_models()
        else:
            # logger.info("No existing models found. Initializing new models.")
            self._initialize_new_models()

    def _initialize_new_models(self):
        """Инициализация новых моделей"""
        try:
            if self.model_type == "random_forest":
                self.price_model = RandomForestRegressor(
                    n_estimators=100,
                    max_depth=15,
                    min_samples_split=5,
                    min_samples_leaf=2,
                    random_state=42,
                    n_jobs=-1,
                    warm_start=True
                )

                self.direction_model = RandomForestClassifier(
                    n_estimators=100,
                    max_depth=15,
                    min_samples_split=5,
                    min_samples_leaf=2,
                    random_state=42,
                    n_jobs=-1,
                    warm_start=True
                )

            elif self.model_type == "neural_network":
                self.price_model = MLPRegressor(
                    hidden_layer_sizes=(100, 50, 25),
                    learning_rate_init=0.001,
                    max_iter=500,
                    random_state=42,
                    warm_start=True,
                    early_stopping=True
                )

                self.direction_model = MLPClassifier(
                    hidden_layer_sizes=(100, 50, 25),
                    learning_rate_init=0.001,
                    max_iter=500,
                    random_state=42,
                    warm_start=True,
                    early_stopping=True
                )

            else:  # hybrid
                # Используем Random Forest как основную модель
                self.price_model = RandomForestRegressor(
                    n_estimators=50,
                    max_depth=10,
                    random_state=42,
                    n_jobs=-1
                )

                self.direction_model = RandomForestClassifier(
                    n_estimators=50,
                    max_depth=10,
                    random_state=42,
                    n_jobs=-1
                )

            # logger.info(f"New {self.model_type} models initialized.")

        except Exception as e:
            logger.error(f"Failed to initialize new models: {e}")
            raise MLPredictionError(f"Failed to initialize new models: {e}")

    def add_training_data(self, features: Dict[str, float],
                          target_price: float,
                          target_direction: int):
        """Добавление данных в буфер для обучения"""
        try:
            with self.buffer_lock:
                # Нормализация данных для разных масштабов - теперь делается в FeatureEngineer
                # или перед вызовом этой функции, если данные приходят извне.
                # Здесь мы ожидаем, что features уже нормализованы.
                
                training_sample = {
                    'features': features, # Expecting already normalized features
                    'target_price': target_price,
                    'target_direction': target_direction,
                    'timestamp': time.time()
                }

                self.training_buffer.append(training_sample)
                self.training_stats['samples_processed'] += 1

                # Автоматическое обучение при достижении порога
                if (len(self.training_buffer) >= self.auto_retrain_threshold and
                        self.incremental_learning):
                    self._incremental_train()

        except Exception as e:
            logger.error(f"Error adding training data: {e}")
            raise MLPredictionError(f"Error adding training data: {e}")

    def _incremental_train(self):
        """Инкрементальное обучение на накопленных данных"""
        try:
            with self.model_lock:
                if len(self.training_buffer) < 10:
                    return

                # Подготовка данных
                features_list = []
                price_targets = []
                direction_targets = []

                # Ensure all features have the same keys and order
                if not self.feature_names:
                    # If feature_names not set, infer from first sample
                    if self.training_buffer:
                        first_features = self.training_buffer[0]['features']
                        # Check if features is a coroutine (async function result)
                        if hasattr(first_features, '__await__'):
                            logger.error(f"Features is a coroutine object: {type(first_features)}. This indicates an async function was called without await.")
                            raise MLPredictionError("Features is a coroutine - async function was not awaited")
                        if not isinstance(first_features, dict):
                            logger.error(f"Features is not a dict: {type(first_features)}, value: {first_features}")
                            raise MLPredictionError(f"Features must be a dict, got {type(first_features)}")
                        self.feature_names = sorted(list(first_features.keys()))
                    else:
                        logger.warning("No feature names available for incremental training.")
                        return

                for sample in list(self.training_buffer):
                    # Ensure features are in the correct order
                    feature_vector = [sample['features'].get(name, 0.0) for name in self.feature_names]
                    features_list.append(feature_vector)
                    price_targets.append(sample['target_price'])
                    direction_targets.append(sample['target_direction'])

                X = np.array(features_list)
                y_price = np.array(price_targets)
                y_direction = np.array(direction_targets)

                # Обновление скейлеров
                if self.robust_scaler is not None:
                    try:
                        # Try to transform first, if it fails, refit the scaler
                        X_scaled = self.robust_scaler.transform(X)
                    except Exception:
                        # If transform fails (scaler not fitted), refit on current data
                        X_scaled = self.robust_scaler.fit_transform(X)
                else:
                    self.robust_scaler = RobustScaler()
                    X_scaled = self.robust_scaler.fit_transform(X)

                # Инкрементальное обучение онлайн моделей
                if self.online_price_model is not None and self.online_direction_model is not None:
                    # Обучение модели предсказания цены
                    self.online_price_model.partial_fit(X_scaled, y_price)

                    # Обучение модели направления
                    unique_classes = np.unique(y_direction)
                    # Ensure all possible classes are known to the classifier
                    # For direction, we expect -1, 0, 1
                    all_possible_classes = np.array([-1, 0, 1])
                    if len(unique_classes) > 1:
                        self.online_direction_model.partial_fit(X_scaled, y_direction,
                                                                classes=all_possible_classes)
                    elif len(unique_classes) == 1 and unique_classes[0] in all_possible_classes:
                        # If only one class is present, partial_fit might fail without all classes
                        # This is a known issue with SGDClassifier.
                        # We can try to fit with all classes if it's the first fit, or skip if already fitted.
                        if not hasattr(self.online_direction_model, 'classes_') or \
                           not np.array_equal(self.online_direction_model.classes_, all_possible_classes):
                            # If not fitted or classes are different, try to fit with all classes
                            # This might require a dummy sample for other classes, or a full fit.
                            # For now, we rely on partial_fit's warm_start.
                            pass # partial_fit should handle this with warm_start=True

                # Обновление статистики
                self.training_stats['last_training_time'] = datetime.now().isoformat()

                # Очистка буфера
                self.training_buffer.clear()

                self.save_models()

                # logger.info(f"Incremental training completed. Samples processed: {len(features_list)}")

        except Exception as e:
            logger.error(f"Error in incremental training: {e}")
            raise MLPredictionError(f"Error in incremental training: {e}")

    def train(self, X: np.ndarray, y_price: np.ndarray, y_direction: np.ndarray, feature_names: List[str]) -> Dict[str, float]:
        """Полное обучение моделей"""
        try:
            with self.model_lock:
                if len(X) < 10:
                    logger.warning("Insufficient data for training")
                    return {}

                self.feature_names = feature_names

                # Разделение данных
                X_train, X_test, y_price_train, y_price_test, y_dir_train, y_dir_test = train_test_split(
                    X, y_price, y_direction, test_size=0.2, random_state=42, stratify=y_direction if len(np.unique(y_direction)) > 1 else None
                )

                # Масштабирование данных
                self.robust_scaler = RobustScaler()
                X_train_scaled = self.robust_scaler.fit_transform(X_train)
                X_test_scaled = self.robust_scaler.transform(X_test)

                # Обучение основных моделей
                price_mse = float('inf')
                price_mae = float('inf')
                if self.price_model is not None:
                    self.price_model.fit(X_train_scaled, y_price_train)
                    price_pred = self.price_model.predict(X_test_scaled)
                    price_mse = mean_squared_error(y_price_test, price_pred)
                    price_mae = mean_absolute_error(y_price_test, price_pred)
                    if hasattr(self.price_model, 'feature_importances_'):
                        self.feature_importance = dict(zip(self.feature_names, self.price_model.feature_importances_))
                else:
                    logger.warning("Price model not initialized for full training.")

                direction_acc = 0.0
                if self.direction_model is not None:
                    self.direction_model.fit(X_train_scaled, y_dir_train)
                    direction_pred = self.direction_model.predict(X_test_scaled)
                    direction_acc = accuracy_score(y_dir_test, direction_pred)
                else:
                    logger.warning("Direction model not initialized for full training.")

                # Инициализация и обучение онлайн моделей (полный фит для начального состояния)
                if self.online_price_model is not None:
                    self.online_price_model.fit(X_train_scaled, y_price_train)

                if self.online_direction_model is not None:
                    # Ensure all possible classes are known to the classifier
                    all_possible_classes = np.array([-1, 0, 1])
                    self.online_direction_model.fit(X_train_scaled, y_dir_train)
                    # After initial fit, ensure classes_ attribute is set correctly
                    if not np.array_equal(self.online_direction_model.classes_, all_possible_classes):
                        logger.warning("Online direction model classes mismatch after full fit. This might indicate an issue.")


                # Обновление статистики
                metrics = {
                    'price_mse': price_mse,
                    'price_mae': price_mae,
                    'direction_accuracy': direction_acc
                }

                self.training_stats['accuracy_history'].append(direction_acc)
                self.training_stats['error_history'].append(price_mse)
                self.training_stats['last_training_time'] = datetime.now().isoformat()
                self.training_stats['model_version'] += 1

                # Сохранение модели
                self.save_models()

                # logger.info(f"Training completed. Metrics: {metrics}")
                return metrics

        except Exception as e:
            logger.error(f"Error in training: {e}")
            raise MLPredictionError(f"Error in training: {e}")

    def predict(self, features: Dict[str, float]) -> Tuple[float, str, float]:
        """Предсказание цены и направления с доверительной оценкой"""
        try:
            with self.model_lock:
                # Features are expected to be already normalized by FeatureEngineer
                # Ensure feature order matches training order
                if not self.feature_names:
                    logger.warning("Feature names not set in MLModel. Cannot predict.")
                    return 0.0, "neutral", 0.0

                feature_vector = np.array([features.get(name, 0.0) for name in self.feature_names]).reshape(1, -1)

                # Применение скейлера
                if self.robust_scaler is not None:
                    feature_vector = self.robust_scaler.transform(feature_vector)
                else:
                    logger.warning("RobustScaler not fitted. Prediction might be inaccurate.")
                    # If scaler is not fitted, try to fit it with a dummy array or skip scaling
                    # For production, this should ideally not happen.
                    pass

                # Предсказание цены
                price_change = 0.0
                if self.online_price_model is not None and hasattr(self.online_price_model, 'coef_') and self.online_price_model.coef_.size > 0:
                    try:
                        price_change = self.online_price_model.predict(feature_vector)[0]
                    except Exception as e:
                        logger.warning(f"Online price model prediction failed: {e}. Falling back to main model if available.", exc_info=True)
                        if self.price_model is not None and hasattr(self.price_model, 'feature_importances_'):
                            price_change = self.price_model.predict(feature_vector)[0]
                elif self.price_model is not None and hasattr(self.price_model, 'feature_importances_'):
                    price_change = self.price_model.predict(feature_vector)[0]
                else:
                    logger.warning("No price prediction model available or trained.")

                # Предсказание направления
                direction = "neutral"
                confidence = 0.0

                if self.online_direction_model is not None and hasattr(self.online_direction_model, 'coef_') and self.online_direction_model.coef_.size > 0:
                    try:
                        direction_pred = self.online_direction_model.predict(feature_vector)[0]
                        if hasattr(self.online_direction_model, 'predict_proba'):
                            direction_proba = self.online_direction_model.predict_proba(feature_vector)[0]
                            confidence = np.max(direction_proba)
                        else:
                            confidence = 1.0 # Default confidence if proba not available

                        if direction_pred == 1:
                            direction = "up"
                        elif direction_pred == -1:
                            direction = "down"
                    except Exception as e:
                        logger.warning(f"Online direction model prediction failed: {e}. Falling back to main model if available.", exc_info=True)
                        if self.direction_model is not None and hasattr(self.direction_model, 'feature_importances_'):
                            direction_pred = self.direction_model.predict(feature_vector)[0]
                            if hasattr(self.direction_model, 'predict_proba'):
                                direction_proba = self.direction_model.predict_proba(feature_vector)[0]
                                confidence = np.max(direction_proba)
                            else:
                                confidence = 1.0

                            if direction_pred == 1:
                                direction = "up"
                            elif direction_pred == -1:
                                direction = "down"
                elif self.direction_model is not None and hasattr(self.direction_model, 'feature_importances_'):
                    direction_pred = self.direction_model.predict(feature_vector)[0]
                    if hasattr(self.direction_model, 'predict_proba'):
                        direction_proba = self.direction_model.predict_proba(feature_vector)[0]
                        confidence = np.max(direction_proba)
                    else:
                        confidence = 1.0

                    if direction_pred == 1:
                        direction = "up"
                    elif direction_pred == -1:
                        direction = "down"
                else:
                    logger.warning("No direction prediction model available or trained.")

                return price_change, direction, confidence

        except Exception as e:
            logger.error(f"Error in prediction: {e}", exc_info=True)
            raise MLPredictionError(f"Error in prediction: {e}")

    def get_shap_values(self, features: Dict[str, float]) -> Optional[Dict[str, float]]:
        """
        Вычисляет SHAP-значения для данного набора признаков.
        Использует обученную модель направления.
        """
        try:
            with self.model_lock:
                if self.direction_model is None or self.robust_scaler is None or self.feature_names is None:
                    logger.warning("Direction model, scaler or feature names not available for SHAP explanation.")
                    return None

                # Prepare features for SHAP
                feature_vector = np.array([features.get(name, 0.0) for name in self.feature_names]).reshape(1, -1)
                X_scaled = self.robust_scaler.transform(feature_vector)

                # Use the main direction model for SHAP explanation
                # For tree-based models, TreeExplainer is efficient
                # For other models, KernelExplainer or DeepExplainer might be needed
                if isinstance(self.direction_model, (RandomForestClassifier, RandomForestRegressor)):
                    explainer = shap.TreeExplainer(self.direction_model)
                elif isinstance(self.direction_model, (SGDClassifier, SGDRegressor, MLPClassifier, MLPRegressor)):
                    # For linear models or neural networks, use KernelExplainer (model-agnostic)
                    # Requires a background dataset for KernelExplainer
                    # For simplicity, we'll use a small sample from the training buffer if available
                    if not self.training_buffer:
                        logger.warning("No training buffer data for KernelExplainer background dataset.")
                        return None
                    
                    background_data = []
                    for sample in list(self.training_buffer)[:100]: # Use first 100 samples as background
                        background_data.append([sample['features'].get(name, 0.0) for name in self.feature_names])
                    
                    if not background_data:
                        logger.warning("Empty background data for KernelExplainer.")
                        return None

                    background_X_scaled = self.robust_scaler.transform(np.array(background_data))
                    explainer = shap.KernelExplainer(self.direction_model.predict_proba, background_X_scaled)
                else:
                    logger.warning(f"SHAP explainer not implemented for model type: {type(self.direction_model)}")
                    return None

                # Calculate SHAP values
                shap_values = explainer.shap_values(X_scaled)

                # For classification, shap_values is a list of arrays (one for each class)
                # We're interested in the SHAP values for the predicted class.
                # Assuming binary classification or multi-class where we care about the 'up' or 'down' class.
                # For simplicity, let's return SHAP values for the positive class (index 1) if available,
                # or the first class if not.
                if isinstance(shap_values, list) and len(shap_values) > 1:
                    # Find the index of the 'up' class (1) if it exists, otherwise default to 0
                    class_index = 0
                    if hasattr(self.direction_model, 'classes_'):
                        try:
                            class_index = list(self.direction_model.classes_).index(1) # Index of 'up' class
                        except ValueError:
                            pass # 'up' class not in classes, use default 0
                    shap_values_for_class = shap_values[class_index][0]
                else:
                    shap_values_for_class = shap_values[0] # For regression or single-class output

                # Map SHAP values to feature names
                shap_explanation = dict(zip(self.feature_names, shap_values_for_class))
                return shap_explanation

        except Exception as e:
            logger.error(f"Error calculating SHAP values: {e}", exc_info=True)
            raise MLPredictionError(f"Error calculating SHAP values: {e}")
        
    def save_models(self):
        """Сохранение всех моделей и метаданных"""
        try:
            with self.model_lock:
                models_data = {
                    'price_model': self.price_model,
                    'direction_model': self.direction_model,
                    'online_price_model': self.online_price_model,
                    'online_direction_model': self.online_direction_model,
                    'robust_scaler': self.robust_scaler,
                    'standard_scaler': self.standard_scaler,
                    'feature_names': self.feature_names,
                    'feature_importance': self.feature_importance,
                    'training_stats': self.training_stats
                }

                if os.path.exists(self.model_path):
                    backup_path = f"{self.model_path}.backup"
                    # Remove existing backup if it exists
                    if os.path.exists(backup_path):
                        os.remove(backup_path)
                    os.rename(self.model_path, backup_path)

                joblib.dump(models_data, self.model_path)

                # logger.info(f"Models saved to {self.model_path}")

        except Exception as e:
            logger.error(f"Error saving models: {e}", exc_info=True)
            raise MLPredictionError(f"Error saving models: {e}")

    def get_model_info(self) -> Dict[str, Any]:
        """Получение информации о модели"""
        # Convert deque objects to lists for JSON serialization
        training_stats_serializable = self.training_stats.copy()
        training_stats_serializable['accuracy_history'] = list(self.training_stats.get('accuracy_history', []))
        training_stats_serializable['error_history'] = list(self.training_stats.get('error_history', []))
        
        return {
            'model_type': self.model_type,
            'training_stats': training_stats_serializable,
            'feature_names': self.feature_names,
            'buffer_size': len(self.training_buffer),
            'model_path': self.model_path,
            'models_trained': {
                'price_model': self.price_model is not None and hasattr(self.price_model, 'feature_importances_'),
                'direction_model': self.direction_model is not None and hasattr(self.direction_model, 'feature_importances_'),
                'online_price_model': self.online_price_model is not None and hasattr(self.online_price_model, 'coef_') and self.online_price_model.coef_.size > 0,
                'online_direction_model': self.online_direction_model is not None and hasattr(self.online_direction_model, 'coef_') and self.online_direction_model.coef_.size > 0
            }
        }

    def get_model_status(self) -> Dict[str, Any]:
        """Получение статуса модели для совместимости"""
        return {
            'price_model_trained': self.price_model is not None and hasattr(self.price_model, 'feature_importances_'),
            'direction_model_trained': self.direction_model is not None and hasattr(self.direction_model, 'feature_importances_'),
            'online_models_trained': (self.online_price_model is not None and hasattr(self.online_price_model, 'coef_') and self.online_price_model.coef_.size > 0 and
                                      self.online_direction_model is not None and hasattr(self.online_direction_model, 'coef_') and self.online_direction_model.coef_.size > 0),
            'last_training': self.training_stats.get('last_training_time'),
            'samples_processed': self.training_stats.get('samples_processed', 0)
        }

    def reset_models(self):
        """Сброс и переинициализация моделей"""
        with self.model_lock:
            self.training_buffer.clear()
            self.prediction_cache.clear()
            self.training_stats = {
                'samples_processed': 0,
                'last_training_time': None,
                'model_version': 1,
                'accuracy_history': deque(maxlen=100),
                'error_history': deque(maxlen=100)
            }
            self._initialize_new_models()
            self._initialize_online_models()
            # logger.info("Models reset and reinitialized")




