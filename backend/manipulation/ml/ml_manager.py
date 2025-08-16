import logging
import asyncio
from typing import Dict, Any, Optional, List
from enum import Enum
from datetime import datetime, timedelta
import json
import threading
from collections import deque, defaultdict
import numpy as np

from manipulation.ml.ml_model import MLModel
from feature_engineering import FeatureEngineer
from manipulation.ml.data_collector import MLDataCollector
from manipulation.ml.ml_price_predictor import MLPricePredictor
from db_manager import DBManager # Import DBManager
from manipulation.trade_processor import TradeProcessor # Import TradeProcessor
from manipulation.orderbook_analyzer import OrderBookAnalyzer # Import OrderBookAnalyzer
from manipulation.anomaly_detector import AnomalyDetector # New: Import AnomalyDetector
from exceptions import MLPredictionError, AppError
from metrics import ML_MODEL_ACCURACY, ML_MODEL_LOSS, ML_MODEL_VERSION, ML_PREDICTIONS_TOTAL, ML_PREDICTIONS_VALIDATED_TOTAL, ERRORS_TOTAL

logger = logging.getLogger(__name__)


class MLMode(Enum):
    """Режимы работы ML системы"""
    TRAINING = "training"
    PREDICTION = "prediction"
    HYBRID = "hybrid"  # Обучение + прогнозирование


class MLManager:
    """Менеджер для управления ML системой"""

    def __init__(self, db_manager: DBManager, trade_processor: TradeProcessor, orderbook_analyzer: OrderBookAnalyzer, anomaly_detector: AnomalyDetector):
        self.db_manager = db_manager
        self.trade_processor = trade_processor
        self.orderbook_analyzer = orderbook_analyzer
        self.anomaly_detector = anomaly_detector # New: AnomalyDetector instance

        # Инициализация компонентов ML
        self.ml_model = MLModel()
        # FeatureEngineer now takes db_manager
        self.feature_engineer = FeatureEngineer(db_manager, trade_processor, orderbook_analyzer)
        # MLDataCollector now takes message_broker
        # It will be initialized in app.py with the global message_broker
        self.data_collector: Optional[MLDataCollector] = None 
        self.price_predictor = MLPricePredictor(
            db_manager, self.ml_model, self.feature_engineer,
            trade_processor, orderbook_analyzer
        )

        # Состояние системы
        self.current_mode = MLMode.HYBRID
        self.is_running = False
        self.training_active = False
        self.prediction_active = False

        # Статистика и мониторинг
        self.training_stats = { # This will be updated from ml_model.training_stats
            'total_samples': 0,
            'training_sessions': 0,
            'last_training': None,
            'model_accuracy': 0.0,
            'model_loss': 0.0,
            'overfitting_score': 0.0
        }

        # Прогнозы в реальном времени
        self.active_predictions: Dict[str, Dict[str, Any]] = {}  # symbol -> prediction_data
        self.prediction_history = deque(maxlen=1000)
        self.prediction_stats = defaultdict(lambda: {
            'total_predictions': 0,
            'correct_predictions': 0,
            'accuracy': 0.0
        })

        # Блокировки
        self.mode_lock = threading.RLock()
        self.prediction_lock = threading.RLock()

        # Задачи
        self.prediction_loop_task: Optional[asyncio.Task] = None
        self.validation_loop_task: Optional[asyncio.Task] = None
        self.target_calculation_loop_task: Optional[asyncio.Task] = None

        # logger.info("MLManager initialized")

    def set_data_collector(self, data_collector: MLDataCollector):
        """Устанавливает экземпляр MLDataCollector после его инициализации в app.py."""
        self.data_collector = data_collector
        # logger.info("MLDataCollector instance set in MLManager.")

    async def start(self, mode: MLMode = MLMode.HYBRID):
        """Запуск ML системы в указанном режиме"""
        async with asyncio.Lock(): # Use asyncio.Lock for async methods
            if self.is_running:
                logger.warning("MLManager is already running")
                return

            if not self.data_collector:
                raise AppError("MLDataCollector not set in MLManager. Cannot start.")

            self.current_mode = mode
            self.is_running = True

            logger.info(f"Starting MLManager in {mode.value} mode")

            # Запуск компонентов в зависимости от режима
            if mode in [MLMode.TRAINING, MLMode.HYBRID]:
                await self._start_training_mode()

            if mode in [MLMode.PREDICTION, MLMode.HYBRID]:
                await self._start_prediction_mode()

            logger.info(f"MLManager started successfully in {mode.value} mode")

    async def _start_training_mode(self):
        """Запуск режима обучения"""
        self.training_active = True
        await self.data_collector.start_collection()
        # Start retrospective target calculation loop
        self.target_calculation_loop_task = asyncio.create_task(self._retrospective_target_calculation_loop())
        logger.info("Training mode activated - starting MLDataCollector")

    async def _start_prediction_mode(self):
        """Запуск режима прогнозирования"""
        self.prediction_active = True

        # Запуск задач прогнозирования
        self.prediction_loop_task = asyncio.create_task(self._prediction_loop())
        self.validation_loop_task = asyncio.create_task(self._validate_predictions_loop())

        logger.info("Prediction mode activated")

    async def switch_mode(self, new_mode: MLMode):
        """Переключение режима работы"""
        with self.mode_lock:
            if new_mode == self.current_mode:
                # logger.info(f"Already in {new_mode.value} mode")
                return

            # logger.info(f"Switching from {self.current_mode.value} to {new_mode.value} mode")

            # Остановка текущих процессов
            if self.current_mode in [MLMode.TRAINING, MLMode.HYBRID]:
                await self._stop_training_mode()

            if self.current_mode in [MLMode.PREDICTION, MLMode.HYBRID]:
                await self._stop_prediction_mode()

            # Запуск новых процессов
            self.current_mode = new_mode

            if new_mode in [MLMode.TRAINING, MLMode.HYBRID]:
                await self._start_training_mode()

            if new_mode in [MLMode.PREDICTION, MLMode.HYBRID]:
                await self._start_prediction_mode()

            # logger.info(f"Successfully switched to {new_mode.value} mode")

    async def _stop_training_mode(self):
        """Остановка режима обучения"""
        self.training_active = False
        if self.target_calculation_loop_task:
            self.target_calculation_loop_task.cancel()
            try:
                await self.target_calculation_loop_task
            except asyncio.CancelledError:
                pass
        if self.data_collector:
            await self.data_collector.stop_collection()
        # logger.info("Training mode deactivated")

    async def _stop_prediction_mode(self):
        """Остановка режима прогнозирования"""
        self.prediction_active = False
        if self.prediction_loop_task:
            self.prediction_loop_task.cancel()
            try:
                await self.prediction_loop_task
            except asyncio.CancelledError:
                pass
        if self.validation_loop_task:
            self.validation_loop_task.cancel()
            try:
                await self.validation_loop_task
            except asyncio.CancelledError:
                pass
        # logger.info("Prediction mode deactivated")

    async def _prediction_loop(self):
        """Основной цикл прогнозирования"""
        while self.is_running and self.prediction_active:
            try:
                # Получаем активные символы из watchlist
                active_symbols = await self.db_manager.get_watchlist_symbols()

                for symbol in active_symbols:
                    await self._make_prediction_for_symbol(symbol)

                await asyncio.sleep(5)  # Прогнозы каждые 5 секунд

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in prediction loop: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_manager', error_type='prediction_loop_error').inc()
                await asyncio.sleep(10)

    async def _make_prediction_for_symbol(self, symbol: str):
        """Создание прогноза для символа"""
        try:
            prediction_data = await self.price_predictor.get_prediction_for_current_state(symbol)

            if 'error' not in prediction_data:
                with self.prediction_lock:
                    # Store the current price as reference for validation
                    reference_price = self._get_current_price_from_orderbook(symbol)
                    if reference_price is None:
                        logger.warning(f"Could not get reference price for {symbol} for prediction. Skipping.")
                        ERRORS_TOTAL.labels(component='ml_manager', error_type='missing_reference_price').inc()
                        return

                    # Сохраняем активный прогноз
                    self.active_predictions[symbol] = {
                        **prediction_data,
                        'created_at': datetime.now(),
                        'status': 'active',
                        'reference_price': reference_price # Store price at prediction time
                    }

                    # Добавляем в историю
                    self.prediction_history.append({
                        'symbol': symbol,
                        'prediction': prediction_data,
                        'timestamp': datetime.now()
                    })

                    # Обновляем статистику
                    self.prediction_stats[symbol]['total_predictions'] += 1
                    ML_PREDICTIONS_TOTAL.labels(symbol=symbol, predicted_direction=prediction_data['predicted_direction']).inc()

                # Сохраняем в БД
                await self._save_prediction_to_db(symbol, prediction_data, reference_price)

                logger.debug(f"Created prediction for {symbol}: {prediction_data['predicted_direction']}")

        except Exception as e:
            logger.error(f"Error making prediction for {symbol}: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_manager', error_type='make_prediction_error').inc()

    async def _save_prediction_to_db(self, symbol: str, prediction_data: Dict, reference_price: float):
        """Сохранение прогноза в базу данных."""
        try:
            # Определяем направление для БД
            direction_map = {
                'up': 'long',
                'down': 'short',
                'neutral': 'neutral'
            }

            db_direction = direction_map.get(prediction_data['predicted_direction'], 'neutral')

            prediction_id = await self.db_manager.save_ml_prediction(
                symbol=symbol,
                predicted_price_change=prediction_data['predicted_price_change'],
                predicted_direction=db_direction,
                confidence=prediction_data.get('confidence', 0.0),
                features=json.dumps(prediction_data.get('features', {})),
                model_version=self.ml_model.training_stats.get('model_version', 1),
                reference_price=reference_price
            )
            # Update the active prediction with the DB ID for later validation
            if symbol in self.active_predictions:
                self.active_predictions[symbol]['id'] = prediction_id

        except Exception as e:
            logger.error(f"Error saving prediction to DB for {symbol}: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_manager', error_type='save_prediction_to_db_error').inc()

    async def _validate_predictions_loop(self):
        """Цикл валидации прогнозов."""
        while self.is_running and self.prediction_active:
            try:
                await self._validate_active_predictions()
                await asyncio.sleep(self.data_collector.target_prediction_window_sec / 2) # Check every half of prediction window

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in prediction validation loop: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_manager', error_type='validation_loop_error').inc()
                await asyncio.sleep(60)

    async def _validate_active_predictions(self):
        """Валидация активных прогнозов."""
        current_time = datetime.now()
        predictions_to_remove = []

        with self.prediction_lock:
            for symbol, prediction in list(self.active_predictions.items()):
                # Check predictions that are older than the target prediction window
                target_validation_time = prediction['created_at'] + timedelta(seconds=self.data_collector.target_prediction_window_sec)
                
                if current_time >= target_validation_time:
                    await self._validate_prediction(symbol, prediction, target_validation_time)
                    predictions_to_remove.append(symbol)

            for symbol in predictions_to_remove:
                del self.active_predictions[symbol]

    async def _validate_prediction(self, symbol: str, prediction: Dict, actual_timestamp: datetime):
        """Валидация конкретного прогноза."""
        try:
            # Get the actual price at the target_validation_time
            actual_future_price = await self.data_collector._get_actual_price_at_timestamp(symbol, actual_timestamp.timestamp())
            if actual_future_price is None:
                logger.warning(f"Could not get actual future price for {symbol} at {actual_timestamp}. Cannot validate prediction {prediction.get('id')}.")
                ERRORS_TOTAL.labels(component='ml_manager', error_type='missing_actual_future_price').inc()
                return

            reference_price = prediction.get('reference_price')
            if reference_price is None or reference_price == 0:
                logger.warning(f"Reference price missing or zero for {symbol} prediction {prediction.get('id')}. Cannot validate.")
                ERRORS_TOTAL.labels(component='ml_manager', error_type='missing_reference_price_for_validation').inc()
                return

            # Calculate actual price change
            actual_change = (actual_future_price - reference_price) / reference_price * 100

            # Determine actual direction based on a small threshold
            if abs(actual_change) < 0.01: # e.g., 0.01% change is neutral
                actual_direction = 'neutral'
            elif actual_change > 0:
                actual_direction = 'up'
            else:
                actual_direction = 'down'

            predicted_direction = prediction['predicted_direction']
            is_correct = predicted_direction == actual_direction

            # Update statistics
            stats = self.prediction_stats[symbol]
            if is_correct:
                stats['correct_predictions'] += 1
            stats['accuracy'] = stats['correct_predictions'] / stats['total_predictions'] if stats['total_predictions'] > 0 else 0
            ML_PREDICTIONS_VALIDATED_TOTAL.labels(symbol=symbol, is_correct=str(is_correct)).inc()
            ML_MODEL_ACCURACY.labels(symbol=symbol).set(stats['accuracy'])

            # Save result to DB
            await self.db_manager.update_ml_prediction_result(
                prediction_id=prediction.get('id'), # Assuming prediction_data from _make_prediction_for_symbol has 'id'
                actual_price_change=actual_change,
                is_correct=is_correct,
                validated_at=datetime.now()
            )

            logger.debug(
                f"Validated prediction for {symbol} (ID: {prediction.get('id')}): predicted={predicted_direction}, actual={actual_direction}, correct={is_correct}, actual_change={actual_change:.4f}%")

        except Exception as e:
            logger.error(f"Error validating prediction for {symbol} (ID: {prediction.get('id', 'unknown')}): {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_manager', error_type='validate_prediction_error').inc()

    def _get_current_price_from_orderbook(self, symbol: str) -> Optional[float]:
        """Получение текущей цены символа из стакана ордеров."""
        try:
            if hasattr(self.orderbook_analyzer, 'current_orderbooks'):
                orderbook = self.orderbook_analyzer.current_orderbooks.get(symbol)
                if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                    bids = orderbook['bids']
                    asks = orderbook['asks']
                    if bids and asks:
                        best_bid = bids[0][0]
                        best_ask = asks[0][0]
                        if best_bid > 0 and best_ask > 0:
                            return (best_bid + best_ask) / 2
            return None
        except Exception as e:
            logger.error(f"Error getting current price from orderbook for {symbol}: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_manager', error_type='get_current_price_from_ob_error').inc()
            return None

    async def _retrospective_target_calculation_loop(self):
        """
        Цикл для периодического запуска ретроспективного расчета целевых значений
        и добавления их в буфер обучения ML-модели.
        """
        while self.is_running and self.training_active:
            try:
                if self.data_collector:
                    await self.data_collector._calculate_and_add_targets_retrospectively()
                await asyncio.sleep(self.data_collector.data_collection_window_sec) # Run as often as data is collected
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in retrospective target calculation loop: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_manager', error_type='retrospective_target_calc_error').inc()
                await asyncio.sleep(10)

    def get_system_status(self) -> Dict[str, Any]:
        """Получение статуса ML системы."""
        model_info = self.ml_model.get_model_info() # Get more detailed info
        collection_stats = self.data_collector.get_collection_stats() if self.data_collector else {}

        # Update ML_MODEL_VERSION gauge
        ML_MODEL_VERSION.set(model_info['training_stats'].get('model_version', 1))

        return {
            'mode': self.current_mode.value,
            'is_running': self.is_running,
            'training_active': self.training_active,
            'prediction_active': self.prediction_active,
            'model_status': self.ml_model.get_model_status(),
            'training_stats': model_info['training_stats'], # Use stats from MLModel
            'collection_stats': collection_stats,
            'active_predictions_count': len(self.active_predictions),
            'prediction_stats': dict(self.prediction_stats),
            'total_prediction_history': len(self.prediction_history),
            'model_info': model_info # Include full model info
        }

    def get_active_predictions(self) -> List[Dict[str, Any]]:
        """Получение активных прогнозов."""
        with self.prediction_lock:
            predictions_list = []
            for symbol, data in self.active_predictions.items():
                predictions_list.append({
                    'symbol': data['symbol'],
                    'predicted_direction': data['predicted_direction'],
                    'predicted_price_change': data['predicted_price_change'],
                    'confidence': data.get('confidence', 0.0),
                    'created_at': data['created_at'].isoformat(),
                    'age_minutes': (datetime.now() - data['created_at']).total_seconds() / 60,
                    'reference_price': data.get('reference_price'),
                    'id': data.get('id') # Include DB ID
                })
            return predictions_list

    def get_prediction_history(self, limit: int = 100) -> List[Dict]:
        """Получение истории прогнозов."""
        return list(self.prediction_history)[-limit:]

    def get_training_metrics(self) -> Dict[str, Any]:
        """Получение метрик обучения."""
        model_info = self.ml_model.get_model_info()

        # Расчет переобучения
        accuracy_history = list(self.ml_model.training_stats.get('accuracy_history', []))
        error_history = list(self.ml_model.training_stats.get('error_history', []))

        overfitting_score = 0.0
        if len(accuracy_history) > 10:
            # Compare recent accuracy to early accuracy
            recent_acc = np.mean(accuracy_history[-5:]) if len(accuracy_history) >= 5 else 0
            early_acc = np.mean(accuracy_history[:5]) if len(accuracy_history) >= 5 else 0
            overfitting_score = max(0, early_acc - recent_acc) # Positive if early is better than recent

        return {
            'model_info': model_info,
            'accuracy_history': accuracy_history,
            'error_history': error_history,
            'overfitting_score': overfitting_score,
            'is_overfitting': overfitting_score > 0.05, # Threshold for overfitting
            'training_recommendations': self._get_training_recommendations(overfitting_score)
        }

    def _get_training_recommendations(self, overfitting_score: float) -> List[str]:
        """Получение рекомендаций по обучению."""
        recommendations = []

        if overfitting_score > 0.05:
            recommendations.append(
                "Модель переобучается. Рекомендуется уменьшить сложность модели, увеличить регуляризацию или собрать больше разнообразных данных.")

        if len(self.ml_model.training_stats.get('accuracy_history', [])) < 10:
            recommendations.append("Недостаточно данных для оценки качества модели. Продолжайте сбор данных.")

        model_status = self.ml_model.get_model_status()
        if not model_status.get('online_models_trained'):
            recommendations.append("Онлайн модели прогнозирования не обучены или не готовы. Убедитесь, что достаточно данных для инкрементального обучения.")
        
        if model_status.get('samples_processed', 0) < self.ml_model.auto_retrain_threshold:
            recommendations.append(f"Накоплено недостаточно данных для автоматического инкрементального обучения. Требуется {self.ml_model.auto_retrain_threshold} образцов.")

        return recommendations

    async def stop(self):
        """Остановка ML системы."""
        # logger.info("Stopping MLManager...")

        self.is_running = False

        if self.training_active:
            await self._stop_training_mode()

        if self.prediction_active:
            await self._stop_prediction_mode()

        # Save models one last time
        self.ml_model.save_models()

        # logger.info("MLManager stopped")

    async def force_training(self):
        """Принудительный запуск обучения."""
        if not self.training_active:
            logger.warning("Training mode is not active. Cannot force training.")
            return False

        if not self.data_collector:
            logger.error("MLDataCollector not set. Cannot force training.")
            ERRORS_TOTAL.labels(component='ml_manager', error_type='data_collector_not_set').inc()
            return False

        try:
            # Trigger immediate target calculation and then training
            await self.data_collector._calculate_and_add_targets_retrospectively()
            
            # Fetch all available training data from DB for a full re-train
            training_data_from_db = await self.db_manager.get_ml_training_data(limit=10000) # Fetch more data
            if len(training_data_from_db) < self.data_collector.min_data_points:
                logger.warning(f"Insufficient data in DB for full force training: {len(training_data_from_db)} samples.")
                ERRORS_TOTAL.labels(component='ml_manager', error_type='insufficient_data_for_force_train').inc()
                return False

            features = []
            target_prices = []
            target_directions = []
            
            # Ensure feature names are consistent
            if not self.ml_model.feature_names:
                # Try to get feature names from feature_engineer
                self.ml_model.feature_names = self.feature_engineer.get_feature_names()
                if not self.ml_model.feature_names:
                    logger.error("Cannot perform full training: feature names not available.")
                    ERRORS_TOTAL.labels(component='ml_manager', error_type='feature_names_not_available').inc()
                    return False

            for row in training_data_from_db:
                try:
                    feature_dict = json.loads(row['features'])
                    # Ensure features are in the correct order and all expected features are present
                    feature_vector = [feature_dict.get(name, 0.0) for name in self.ml_model.feature_names]
                    features.append(feature_vector)
                    target_prices.append(row['target_price_change'])
                    # Convert direction string to int for ML model: 'up': 1, 'down': -1, 'neutral': 0
                    target_directions.append(1 if row['target_direction'] == 'up' else (-1 if row['target_direction'] == 'down' else 0))
                except Exception as e:
                    logger.warning(f"Error processing training data row from DB for force training: {e}")
                    ERRORS_TOTAL.labels(component='ml_manager', error_type='db_training_data_processing_error').inc()
                    continue

            if len(features) >= self.data_collector.min_data_points:
                X = np.array(features)
                y_price = np.array(target_prices)
                y_direction = np.array(target_directions)
                
                metrics = self.ml_model.train(X, y_price, y_direction, self.ml_model.feature_names)
                # logger.info(f"Full force training completed. Metrics: {metrics}")
                return True
            else:
                logger.warning(f"Not enough valid data points for full force training after processing: {len(features)} samples.")
                ERRORS_TOTAL.labels(component='ml_manager', error_type='not_enough_valid_data_for_force_train').inc()
                return False

        except Exception as e:
            logger.error(f"Error in force training: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_manager', error_type='force_training_error').inc()
            return False

    async def reset_models(self):
        """Сброс моделей."""
        self.ml_model.reset_models()
        # Reset MLManager's internal stats related to predictions
        with self.prediction_lock:
            self.active_predictions.clear()
            self.prediction_history.clear()
            self.prediction_stats.clear()
        # logger.info("Models reset successfully")

    async def get_shap_explanation_for_symbol(self, symbol: str) -> Optional[Dict[str, float]]:
        """
        Получает SHAP-объяснение для последнего прогноза по символу.
        """
        with self.prediction_lock:
            latest_prediction = self.active_predictions.get(symbol)
            if not latest_prediction or 'features' not in latest_prediction:
                logger.warning(f"No active prediction or features found for {symbol} to generate SHAP explanation.")
                return None
            
            features = json.loads(latest_prediction['features']) # Features are stored as JSON string
            
            try:
                shap_values = self.ml_model.get_shap_values(features)
                return shap_values
            except Exception as e:
                logger.error(f"Failed to get SHAP values for {symbol}: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_manager', error_type='shap_explanation_error').inc()
                return None




