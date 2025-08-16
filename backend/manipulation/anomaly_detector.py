import asyncio
import logging
import numpy as np
import pandas as pd
import joblib
import os
from collections import deque, defaultdict
from typing import Dict, Any, List, Optional
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import threading
import time

from manipulation.alert_manager import AlertManager
from feature_engineering import FeatureEngineer
from message_broker import MessageBroker
from exceptions import AnomalyDetectionError
from metrics import ANOMALIES_DETECTED_TOTAL, ANOMALY_DETECTION_LATENCY, ERRORS_TOTAL, ANOMALY_DETECTOR_TRAINING_TOTAL, ANOMALY_DETECTOR_MODEL_STATUS

logger = logging.getLogger(__name__)

class AnomalyDetector:
    def __init__(self, alert_manager: AlertManager, feature_engineer: FeatureEngineer, message_broker: MessageBroker,
                 model_path="anomaly_model.joblib",
                 training_buffer_size=5000, # Number of samples to train the anomaly detector
                 min_samples_for_training=100,
                 contamination=0.01, # Expected proportion of outliers in the data
                 retrain_interval_minutes=60): # How often to retrain the model

        self.alert_manager = alert_manager
        self.feature_engineer = feature_engineer
        self.message_broker = message_broker
        self.model_path = model_path
        self.training_buffer_size = training_buffer_size
        self.min_samples_for_training = min_samples_for_training
        self.contamination = contamination
        self.retrain_interval_minutes = retrain_interval_minutes

        self.model: Optional[IsolationForest] = None
        self.scaler: Optional[StandardScaler] = None
        self.feature_names: Optional[List[str]] = None
        self.training_buffer: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.training_buffer_size)) # symbol -> deque of features
        self.last_training_time: Dict[str, float] = defaultdict(float) # symbol -> timestamp

        self.buffer_lock = threading.RLock()
        self.model_lock = threading.RLock()

        self.is_running = False
        self.training_task: Optional[asyncio.Task] = None
        self.data_consumption_task: Optional[asyncio.Task] = None
        self.processed_features_queue_name = "processed_features_for_anomaly_detection" # Queue to consume from

        self._load_model()
        # logger.info("AnomalyDetector initialized.")

    async def start(self):
        """Запуск детектора аномалий."""
        if self.is_running:
            logger.warning("AnomalyDetector already running.")
            return

        self.is_running = True
        self.training_task = asyncio.create_task(self._periodic_training_loop())
        
        # Запускаем потребление сообщений в фоновой задаче
        self.data_consumption_task = asyncio.create_task(self._start_consuming())
        
        # logger.info("AnomalyDetector started.")

    async def _start_consuming(self):
        """Запуск потребления сообщений в фоновом режиме."""
        try:
            await self.message_broker.consume(self.processed_features_queue_name, self._add_features_for_training_and_detection)
            # logger.info(f"AnomalyDetector started consuming from {self.processed_features_queue_name}")
        except Exception as e:
            logger.error(f"Failed to start AnomalyDetector consumer: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='anomaly_detector', error_type='consumer_start_failed').inc()
            # Не поднимаем исключение, чтобы не блокировать запуск приложения

    async def stop(self):
        """Остановка детектора аномалий."""
        self.is_running = False
        if self.training_task:
            self.training_task.cancel()
            try:
                await self.training_task
            except asyncio.CancelledError:
                pass
        if self.data_consumption_task:
            self.data_consumption_task.cancel()
            try:
                await self.data_consumption_task
            except asyncio.CancelledError:
                pass
        # logger.info("AnomalyDetector stopped.")

    def _load_model(self):
        """Загрузка предобученной модели аномалий."""
        if os.path.exists(self.model_path):
            try:
                model_data = joblib.load(self.model_path)
                self.model = model_data.get('model')
                self.scaler = model_data.get('scaler')
                self.feature_names = model_data.get('feature_names')
                # logger.info("Anomaly detection model loaded successfully.")
                # Set gauge for loaded models
                for symbol in self.training_buffer.keys(): # Assuming symbols are known from previous training
                    ANOMALY_DETECTOR_MODEL_STATUS.labels(symbol=symbol).set(1)
            except Exception as e:
                logger.warning(f"Failed to load anomaly detection model: {e}. Initializing new model.", exc_info=True)
                self.model = None
                self.scaler = None
                self.feature_names = None
        else:
            # logger.info("No existing anomaly detection model found. Will train on first data.")
            self.model = None
            self.scaler = None
            self.feature_names = None

    def _save_model(self):
        """Сохранение модели аномалий."""
        try:
            with self.model_lock:
                if self.model and self.scaler and self.feature_names:
                    model_data = {
                        'model': self.model,
                        'scaler': self.scaler,
                        'feature_names': self.feature_names
                    }
                    joblib.dump(model_data, self.model_path)
                    # logger.info(f"Anomaly detection model saved to {self.model_path}")
                else:
                    logger.warning("Cannot save anomaly model: model, scaler or feature names are missing.")
        except Exception as e:
            logger.error(f"Error saving anomaly detection model: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='anomaly_detector', error_type='model_save_error').inc()

    async def _add_features_for_training_and_detection(self, data: Dict[str, Any]):
        """Добавление признаков в буфер для обучения и немедленное детектирование."""
        symbol = data.get('symbol')
        features = data.get('features')
        timestamp = data.get('timestamp')

        if not symbol or not features or not timestamp:
            logger.warning(f"Incomplete feature data received for anomaly detection: {data}")
            ERRORS_TOTAL.labels(component='anomaly_detector', error_type='incomplete_feature_data').inc()
            return

        with self.buffer_lock:
            self.training_buffer[symbol].append(features)

        # Attempt immediate anomaly detection
        await self.detect_anomaly(symbol, features, timestamp)

    async def _periodic_training_loop(self):
        """Периодический цикл обучения модели аномалий."""
        while self.is_running:
            try:
                for symbol in list(self.training_buffer.keys()):
                    current_time = time.time()
                    if (current_time - self.last_training_time[symbol] > self.retrain_interval_minutes * 60 and
                            len(self.training_buffer[symbol]) >= self.min_samples_for_training):
                        # logger.info(f"Initiating periodic training for anomaly detector for {symbol}.")
                        await self.train_model(symbol)
                        self.last_training_time[symbol] = current_time
                await asyncio.sleep(self.retrain_interval_minutes * 60 / 4) # Check every 15 minutes
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic anomaly training loop: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='anomaly_detector', error_type='periodic_training_loop_error').inc()
                await asyncio.sleep(60)

    async def train_model(self, symbol: str):
        """Обучение модели IsolationForest для конкретного символа."""
        with self.model_lock:
            if len(self.training_buffer[symbol]) < self.min_samples_for_training:
                logger.warning(f"Not enough data for anomaly detector training for {symbol}. Need {self.min_samples_for_training}, got {len(self.training_buffer[symbol])}")
                return

            try:
                # Convert deque of dicts to pandas DataFrame
                df = pd.DataFrame(list(self.training_buffer[symbol]))
                
                # Ensure feature names are consistent
                if self.feature_names is None:
                    self.feature_names = list(df.columns)
                else:
                    # Align columns if new features appear or old ones disappear
                    missing_cols = set(self.feature_names) - set(df.columns)
                    for c in missing_cols:
                        df[c] = 0.0 # Add missing columns with default value
                    df = df[self.feature_names] # Reorder columns

                X = df.values

                # Scale features
                self.scaler = StandardScaler()
                X_scaled = self.scaler.fit_transform(X)

                # Train Isolation Forest
                self.model = IsolationForest(
                    n_estimators=100,
                    contamination=self.contamination,
                    random_state=42,
                    n_jobs=-1
                )
                self.model.fit(X_scaled)
                self._save_model()
                # logger.info(f"Anomaly detection model trained for {symbol} with {len(self.training_buffer[symbol])} samples.")
                ANOMALY_DETECTOR_TRAINING_TOTAL.inc()
                ANOMALY_DETECTOR_MODEL_STATUS.labels(symbol=symbol).set(1)

            except Exception as e:
                logger.error(f"Error training anomaly detection model for {symbol}: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='anomaly_detector', error_type='model_training_error').inc()
                ANOMALY_DETECTOR_MODEL_STATUS.labels(symbol=symbol).set(0) # Mark as untrained if error

    async def detect_anomaly(self, symbol: str, features: Dict[str, float], timestamp: float) -> bool:
        """
        Детектирование аномалий для нового набора признаков.
        Возвращает True, если обнаружена аномалия.
        """
        start_time = time.perf_counter()
        try:
            with self.model_lock:
                if self.model is None or self.scaler is None or self.feature_names is None:
                    logger.debug(f"Anomaly detection model not trained for {symbol}. Skipping detection.")
                    return False

                # Prepare features for prediction
                feature_vector = np.array([features.get(name, 0.0) for name in self.feature_names]).reshape(1, -1)
                
                # Scale features
                X_scaled = self.scaler.transform(feature_vector)

                # Predict anomaly score (-1 for outlier, 1 for inlier)
                prediction = self.model.predict(X_scaled)
                
                if prediction[0] == -1:
                    # Anomaly detected
                    anomaly_score = self.model.decision_function(X_scaled)[0]
                    alert_data = {
                        "features": features,
                        "anomaly_score": anomaly_score,
                        "reason": f"IsolationForest detected an anomaly with score {anomaly_score:.4f}"
                    }
                    # Получаем торговую историю для алерта
                    trade_history = await self._get_recent_trade_history(symbol)
                    
                    await self.alert_manager.create_alert(
                        symbol, "AnomalyDetected", "High", "Unusual market behavior detected.", alert_data,
                        trade_history=trade_history
                    )
                    logger.warning(f"Anomaly detected for {symbol} at {datetime.fromtimestamp(timestamp)} with score {anomaly_score:.4f}")
                    ANOMALIES_DETECTED_TOTAL.labels(symbol=symbol, anomaly_type='IsolationForest').inc()
                    return True
                return False
        except Exception as e:
            logger.error(f"Error detecting anomaly for {symbol}: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='anomaly_detector', error_type='detection_error').inc()
            return False
        finally:
            ANOMALY_DETECTION_LATENCY.observe(time.perf_counter() - start_time)

    async def _get_recent_trade_history(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Получает недавнюю торговую историю для символа.
        Возвращает список сделок за последние 5 минут (до 100 сделок).
        """
        try:
            # Получаем торговую историю из TradeProcessor через message broker
            # Поскольку у нас нет прямого доступа к TradeProcessor,
            # возвращаем пустой список - это будет обработано в MLDataCollector
            return []
        except Exception as e:
            logger.error(f"Error getting trade history for {symbol}: {e}")
            return []

    def get_status(self) -> Dict[str, Any]:
        """Получение статуса детектора аномалий."""
        status = {
            'is_running': self.is_running,
            'model_trained': self.model is not None,
            'training_buffer_sizes': {s: len(b) for s, b in self.training_buffer.items()},
            'min_samples_for_training': self.min_samples_for_training,
            'last_training_time': {s: datetime.fromtimestamp(t).isoformat() for s, t in self.last_training_time.items()},
            'contamination': self.contamination,
            'retrain_interval_minutes': self.retrain_interval_minutes,
            'feature_names_count': len(self.feature_names) if self.feature_names else 0
        }
        return status



