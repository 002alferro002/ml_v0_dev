import asyncio
import logging
import json
import time
import os
import numpy as np
import pandas as pd
from collections import defaultdict, deque
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import threading
from dataclasses import dataclass
import aiofiles
import pickle
import aiohttp

from utils.feature_normalizer import normalize_features # Import the new normalizer
from exceptions import AppError, MLPredictionError
from metrics import ML_TRAINING_BUFFER_SIZE, PENDING_ALERTS_QUEUE_SIZE, ERRORS_TOTAL

logger = logging.getLogger(__name__)

@dataclass
class DataPoint:
    """Структура для хранения точки данных для ML обучения."""
    timestamp: float # Timestamp when features were collected
    symbol: str
    features: Dict[str, float]
    current_price_at_collection: float # Price at the moment of feature collection
    target_price: Optional[float] = None # Actual price at future_timestamp
    target_direction: Optional[str] = None # Actual direction at future_timestamp
    alert_data: Optional[Dict[str, Any]] = None # Original alert data if this point came from an alert

class MLDataCollector:
    def __init__(self, db_manager, ml_model, feature_engineer, message_broker,
                 trade_processor=None, orderbook_analyzer=None,
                 data_collection_window_sec=30, # How often to collect new data points
                 target_prediction_window_sec=300, # How far into the future to predict (5 minutes)
                 min_data_points=5, # Min data points in buffer to consider training
                 max_buffer_size=10000, # Max size of the data buffer
                 max_pending_alerts=100, # Max alerts in queue
                 persistence_file="ml_data_buffer.pkl"):

        self.db_manager = db_manager
        self.ml_model = ml_model
        self.feature_engineer = feature_engineer
        self.message_broker = message_broker # New: MessageBroker instance
        self.trade_processor = trade_processor
        self.orderbook_analyzer = orderbook_analyzer
        self.data_collection_window_sec = data_collection_window_sec
        self.target_prediction_window_sec = target_prediction_window_sec
        self.min_data_points = min_data_points
        self.max_buffer_size = max_buffer_size
        self.max_pending_alerts = max_pending_alerts
        self.persistence_file = persistence_file

        self.data_buffer = deque(maxlen=max_buffer_size) # Stores DataPoint objects
        self.processed_alerts = defaultdict(float) # alert_key -> last_processed_timestamp
        self.trade_cache = defaultdict(lambda: {'trades': [], 'timestamp': 0, 'expiry': 300})  # Cache for 5 minutes
        self.kline_cache = defaultdict(lambda: {'klines': [], 'timestamp': 0, 'expiry': 300}) # Cache for klines

        self.pending_alerts = asyncio.Queue(maxsize=max_pending_alerts)
        self.ml_alerts_queue_name = "ml_alerts_queue" # Queue for alerts specifically for ML

        self.collection_stats = {
            'total_samples': 0,
            'successful_predictions': 0, # This stat is for MLManager, not here
            'failed_predictions': 0, # This stat is for MLManager, not here
            'last_collection_time': None,
            'symbols_tracked': set(),
            'feature_importance': defaultdict(float) # This stat is for MLModel
        }

        self.buffer_lock = threading.RLock()
        self.stats_lock = threading.Lock()

        self.collection_task = None
        self.persistence_task = None
        self.training_task = None
        self.alert_processing_task = None
        self.consumer_task = None
        self.is_running = False

        self.http_session: Optional[aiohttp.ClientSession] = None

        self._load_persistent_data()

        logger.info("MLDataCollector initialized successfully.")

    async def start_collection(self):
        """Запуск процесса сбора данных"""
        if self.is_running:
            logger.warning("Data collection already running")
            return

        self.is_running = True
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.http_session = aiohttp.ClientSession(timeout=timeout)

        self.collection_task = asyncio.create_task(self._continuous_collection())
        self.persistence_task = asyncio.create_task(self._periodic_persistence())
        self.alert_processing_task = asyncio.create_task(self.process_pending_alerts_loop(5))
        
        # Запускаем потребление сообщений в фоновой задаче
        self.consumer_task = asyncio.create_task(self._start_consuming())
        
        logger.info("Data collection started - starting continuous collection task")

    async def _start_consuming(self):
        """Запуск потребления сообщений в фоновом режиме."""
        try:
            await self.message_broker.consume(self.ml_alerts_queue_name, self.add_alert_for_ml_processing)
            logger.info(f"MLDataCollector started consuming from {self.ml_alerts_queue_name}")
        except Exception as e:
            logger.error(f"Failed to start MLDataCollector consumer: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='consumer_start_failed').inc()
            # Не поднимаем исключение, чтобы не блокировать запуск приложения

    async def stop_collection(self):
        """Остановка процесса сбора данных"""
        self.is_running = False

        tasks = [
            self.collection_task,
            self.persistence_task,
            self.alert_processing_task,
            self.consumer_task
        ]
        for task in tasks:
            if task and not task.done():
                task.cancel()

        if tasks:
            try:
                await asyncio.gather(*[t for t in tasks if t], return_exceptions=True)
            except Exception as e:
                logger.error(f"Error during task cancellation: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='task_cancellation_error').inc()

        if self.http_session and not self.http_session.closed:
            try:
                await self.http_session.close()
            except Exception as e:
                logger.error(f"Error closing HTTP session: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='http_session_close_error').inc()
            finally:
                self.http_session = None

        await self._save_persistent_data()

        logger.info("Data collection stopped")

    async def _continuous_collection(self):
        """Непрерывный сбор данных"""
        while self.is_running:
            try:
                await self._collect_market_data()
                ML_TRAINING_BUFFER_SIZE.set(len(self.data_buffer))
                PENDING_ALERTS_QUEUE_SIZE.set(self.pending_alerts.qsize())
                await asyncio.sleep(self.data_collection_window_sec)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in continuous collection: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='continuous_collection_error').inc()
                await asyncio.sleep(5)

    async def _collect_market_data(self):
        """Сбор рыночных данных для всех отслеживаемых символов"""
        try:
            current_time = time.time()
            active_symbols = await self.db_manager.get_watchlist_symbols() # Get from DB
            if not active_symbols:
                logger.debug("No active symbols in watchlist to collect data for.")
                return

            for symbol in active_symbols:
                try:
                    data_point = await self._collect_symbol_data(symbol, current_time)

                    if data_point:
                        with self.buffer_lock:
                            self.data_buffer.append(data_point)

                        with self.stats_lock:
                            self.collection_stats['total_samples'] += 1
                            self.collection_stats['symbols_tracked'].add(symbol)
                            self.collection_stats['last_collection_time'] = datetime.now().isoformat()

                except Exception as e:
                    logger.error(f"Error collecting data for {symbol}: {e}", exc_info=True)
                    ERRORS_TOTAL.labels(component='ml_data_collector', error_type='symbol_data_collection_error').inc()

        except Exception as e:
            logger.error(f"Error in market data collection: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='market_data_collection_error').inc()

    async def _collect_symbol_data(self, symbol: str, timestamp: float) -> Optional[DataPoint]:
        """Сбор данных для конкретного символа."""
        try:
            order_book_snapshot = self.orderbook_analyzer.current_orderbooks.get(
                symbol) if self.orderbook_analyzer else None
            trade_history = list(
                self.trade_processor.trade_history.get(symbol, deque())) if self.trade_processor else []

            # Проверяем наличие валидных данных orderbook (не пустые списки)
            if (not order_book_snapshot or 
                not order_book_snapshot.get('bids') or 
                not order_book_snapshot.get('asks') or
                len(order_book_snapshot.get('bids', [])) == 0 or 
                len(order_book_snapshot.get('asks', [])) == 0):
                logger.debug(f"No valid order book for {symbol} - empty or missing bids/asks")
                return None

            features = await self.feature_engineer.extract_features(symbol, order_book_snapshot, trade_history, timestamp)
            if not features:
                logger.debug(f"No features extracted for {symbol}")
                return None

            current_price = self._get_current_price(symbol)
            if current_price is None:
                logger.warning(f"Could not get current price for {symbol} for data point collection.")
                return None

            # DataPoint is created without target values initially
            data_point = DataPoint(
                timestamp=timestamp,
                symbol=symbol,
                features=features, # Features are already normalized by FeatureEngineer
                current_price_at_collection=current_price
            )
            return data_point

        except Exception as e:
            logger.error(f"Error collecting data for {symbol}: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='collect_symbol_data_error').inc()
            return None

    async def add_alert_for_ml_processing(self, alert_data: Dict[str, Any]):
        """Добавление сообщения для обработки ML."""
        try:
            symbol = alert_data.get('symbol')
            alert_type = alert_data.get('alert_type')
            alert_id = alert_data.get('id')

            if not all([symbol, alert_type, alert_id is not None]):
                logger.warning(f"Incomplete alert data for ML processing: {alert_data}")
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='incomplete_alert_data').inc()
                return

            alert_key = f"{symbol}_{alert_type}_{alert_id}"
            current_time = time.time()

            # Cooldown for alerts to prevent flooding the queue
            if current_time - self.processed_alerts.get(alert_key, 0) < 30: # 30 seconds cooldown
                logger.debug(f"Alert {alert_id} for {symbol} on cooldown")
                return

            try:
                await self.pending_alerts.put(alert_data)
                self.processed_alerts[alert_key] = current_time
                logger.debug(f"Added alert {alert_id} for {symbol} to pending queue, size: {self.pending_alerts.qsize()}")
                PENDING_ALERTS_QUEUE_SIZE.set(self.pending_alerts.qsize())
            except asyncio.QueueFull:
                logger.warning(f"Pending alerts queue full, dropping alert {alert_id} for {symbol}")
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='pending_alerts_queue_full').inc()
                return

        except Exception as e:
            logger.error(f"Error adding alert {alert_data.get('id', 'unknown')} for ML: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='add_alert_for_ml_error').inc()

    async def _process_alert_for_ml(self, alert_data: Dict[str, Any]):
        """Обработка сообщения для машинного обучения."""
        try:
            symbol = alert_data['symbol']
            alert_id = alert_data.get('id')
            timestamp = alert_data.get('alert_timestamp_ms', time.time() * 1000) / 1000

            # Ensure order_book_snapshot and trade_history are present in alert_data
            # These should be provided by the AlertManager when the alert is created
            order_book_snapshot = alert_data.get('order_book_snapshot')
            trade_history = alert_data.get('trade_history', [])

            # Проверяем наличие валидных данных orderbook (не пустые списки)
            if (not order_book_snapshot or 
                not order_book_snapshot.get('bids') or 
                not order_book_snapshot.get('asks') or
                len(order_book_snapshot.get('bids', [])) == 0 or 
                len(order_book_snapshot.get('asks', [])) == 0):
                logger.warning(f"No valid order book snapshot in alert_data for {symbol}, alert {alert_id} - empty or missing bids/asks. Skipping ML data point.")
                return
            
            if not trade_history:
                logger.warning(f"No trade history in alert_data for {symbol}, alert {alert_id}. Attempting to fetch historical trades.")
                # Fetch historical trades from TimescaleDB
                end_dt = datetime.fromtimestamp(timestamp)
                start_dt = end_dt - timedelta(minutes=5) # Fetch trades for last 5 minutes
                trade_history = await self.db_manager.get_historical_trades(symbol, start_dt, end_dt)
                # Convert to expected format (timestamp in seconds)
                trade_history = [{**t, 'timestamp': t['time'].timestamp()} for t in trade_history]


            features = await self.feature_engineer.extract_features(symbol, order_book_snapshot, trade_history, timestamp)
            if not features:
                logger.warning(f"No features extracted for {symbol}, alert {alert_id}. Skipping ML data point.")
                return

            current_price = self._get_current_price(symbol)
            if current_price is None:
                logger.warning(f"Could not get current price for {symbol} for alert {alert_id}. Skipping ML data point.")
                return

            # Create DataPoint without target values initially
            data_point = DataPoint(
                timestamp=timestamp,
                symbol=symbol,
                features=features, # Features are already normalized by FeatureEngineer
                current_price_at_collection=current_price,
                alert_data=alert_data
            )

            with self.buffer_lock:
                self.data_buffer.append(data_point)
                logger.debug(f"Added data point to buffer for {symbol}, alert {alert_id}, buffer size: {len(self.data_buffer)}")
                ML_TRAINING_BUFFER_SIZE.set(len(self.data_buffer))

        except Exception as e:
            logger.error(f"Error processing alert for ML, alert {alert_data.get('id', 'unknown')}: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='process_alert_for_ml_error').inc()

    async def _calculate_and_add_targets_retrospectively(self):
        """
        Ретроспективно вычисляет целевые значения для точек данных в буфере
        и добавляет их в буфер обучения ML-модели.
        """
        logger.debug("Starting retrospective target calculation.")
        
        data_points_to_process = []
        with self.buffer_lock:
            # Collect data points that are old enough to have a future price
            current_time = time.time()
            # Iterate from the beginning of the deque (oldest points first)
            for i in range(len(self.data_buffer)):
                dp = self.data_buffer[i]
                future_timestamp_needed = dp.timestamp + self.target_prediction_window_sec
                if future_timestamp_needed <= current_time and dp.target_price is None:
                    data_points_to_process.append((i, dp))
                else:
                    # Since deque is ordered by time, if this one is too new, subsequent ones will be too.
                    break
        
        if not data_points_to_process:
            logger.debug("No data points old enough for retrospective target calculation.")
            return

        # logger.info(f"Processing {len(data_points_to_process)} data points for retrospective target calculation.")

        for index, dp in data_points_to_process:
            try:
                future_timestamp = dp.timestamp + self.target_prediction_window_sec
                actual_future_price = await self._get_actual_price_at_timestamp(dp.symbol, future_timestamp)

                if actual_future_price is None or dp.current_price_at_collection == 0:
                    logger.warning(f"Could not get actual future price or current price is zero for {dp.symbol} at {datetime.fromtimestamp(dp.timestamp)}. Skipping target calculation.")
                    ERRORS_TOTAL.labels(component='ml_data_collector', error_type='missing_future_price').inc()
                    continue

                target_price_change = (actual_future_price - dp.current_price_at_collection) / dp.current_price_at_collection * 100

                # Define direction based on a small threshold to avoid 'neutral' for tiny changes
                if abs(target_price_change) < 0.01: # e.g., 0.01% change is neutral
                    target_direction = 'neutral'
                elif target_price_change > 0:
                    target_direction = 'up'
                else:
                    target_direction = 'down'

                # Update the DataPoint in the buffer (or create a new one for training)
                # For simplicity, we'll update the existing one and then pass it to ML model
                dp.target_price = target_price_change
                dp.target_direction = target_direction

                # Add to ML model's training buffer
                # Convert direction string to int for ML model: 'up': 1, 'down': -1, 'neutral': 0
                ml_target_direction = 1 if target_direction == 'up' else (-1 if target_direction == 'down' else 0)
                self.ml_model.add_training_data(dp.features, dp.target_price, ml_target_direction)

                # Optionally, save to DB for persistent training data
                await self.db_manager.insert_ml_training_data(
                    dp.symbol,
                    dp.features,
                    dp.target_price,
                    dp.target_direction,
                    dp.alert_data.get('id') if dp.alert_data else None
                )
                logger.debug(f"Calculated and added target for {dp.symbol} at {datetime.fromtimestamp(dp.timestamp)}: change={target_price_change:.4f}%, dir={target_direction}")

            except Exception as e:
                logger.error(f"Error calculating target for {dp.symbol} at {datetime.fromtimestamp(dp.timestamp)}: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='target_calculation_error').inc()
        
        # Remove processed data points from the buffer (only those that had targets calculated)
        with self.buffer_lock:
            # Remove from the left (oldest)
            for _ in range(len(data_points_to_process)):
                self.data_buffer.popleft()
            logger.debug(f"Removed {len(data_points_to_process)} processed data points from buffer. New size: {len(self.data_buffer)}")
            ML_TRAINING_BUFFER_SIZE.set(len(self.data_buffer))


    async def _get_actual_price_at_timestamp(self, symbol: str, timestamp: float) -> Optional[float]:
        """
        Получает фактическую цену символа в указанный исторический момент времени.
        Использует Bybit KLine API или исторические данные из TimescaleDB.
        """
        # First, try to get from TimescaleDB raw_orderbooks
        end_dt = datetime.fromtimestamp(timestamp + 60) # Look up to 1 minute after target
        start_dt = datetime.fromtimestamp(timestamp - 60) # Look up to 1 minute before target
        
        try:
            historical_orderbooks = await self.db_manager.get_historical_orderbooks(symbol, start_dt, end_dt)
            for ob_entry in historical_orderbooks:
                ob_time = ob_entry['time'].timestamp()
                if ob_time <= timestamp < ob_time + 60: # Check if timestamp falls within this orderbook's minute
                    bids = ob_entry['bids']
                    asks = ob_entry['asks']
                    if bids and asks:
                        try:
                            bid_price = float(bids[0][0])
                            ask_price = float(asks[0][0])
                            mid_price = (bid_price + ask_price) / 2.0
                            return mid_price
                        except (ValueError, TypeError, IndexError) as e:
                            logger.warning(f"Error converting bid/ask prices to float for {symbol}: bids={bids[0] if bids else None}, asks={asks[0] if asks else None}, error={e}")
                            continue
            logger.debug(f"Timestamp {timestamp} not found in TimescaleDB orderbooks for {symbol}. Falling back to Bybit API.")
        except Exception as e:
            logger.warning(f"Error fetching historical orderbooks from DB for {symbol}: {e}. Falling back to Bybit API.", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='db_historical_price_fetch_error').inc()

        # Fallback to Bybit KLine API if not found in DB or error
        if not self.http_session or self.http_session.closed:
            logger.warning("HTTP session not initialized or closed, reinitializing for _get_actual_price_at_timestamp")
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.http_session = aiohttp.ClientSession(timeout=timeout)

        # Try to get from kline cache first
        cache_entry = self.kline_cache.get(symbol)
        if cache_entry and cache_entry['klines'] and time.time() - cache_entry['timestamp'] < cache_entry['expiry']:
            # Find the kline that contains the timestamp
            for kline in cache_entry['klines']:
                if kline['start_time'] <= timestamp < kline['start_time'] + 60: # Assuming 1-minute klines
                    return (kline['open'] + kline['close']) / 2 # Use mid-price of the kline
            logger.debug(f"Timestamp {timestamp} not found in cached klines for {symbol}.")

        # Fetch kline data from Bybit API
        max_attempts = 3
        retry_delay = 5
        
        # Fetch 1-minute klines around the target timestamp
        # Bybit kline API requires start and end in milliseconds
        end_ms = int(timestamp * 1000) + 60 * 1000 # Fetch up to 1 minute after target
        start_ms = int(timestamp * 1000) - 300 * 1000 # Fetch 5 minutes before target

        for attempt in range(max_attempts):
            try:
                url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval=1&start={start_ms}&end={end_ms}&limit=200"
                async with self.http_session.get(url) as response:
                    if response.status == 429:
                        logger.warning(f"Rate limit exceeded for kline {symbol}, retrying after {retry_delay} seconds (attempt {attempt + 1})")
                        ERRORS_TOTAL.labels(component='ml_data_collector', error_type='bybit_kline_rate_limit').inc()
                        await asyncio.sleep(retry_delay * (attempt + 1))
                        continue
                    if response.status != 200:
                        logger.error(f"Failed to fetch kline for {symbol} at {timestamp}: HTTP {response.status}")
                        ERRORS_TOTAL.labels(component='ml_data_collector', error_type='bybit_kline_http_error').inc()
                        await asyncio.sleep(retry_delay)
                        continue

                    data = await response.json()
                    if not isinstance(data, dict) or data.get('retCode') != 0 or not data.get('result', {}).get('list'):
                        logger.warning(f"Invalid kline response for {symbol} at {timestamp}: {data.get('retMsg', 'Unknown error')}")
                        ERRORS_TOTAL.labels(component='ml_data_collector', error_type='bybit_kline_invalid_response').inc()
                        await asyncio.sleep(retry_delay)
                        continue

                    klines_raw = data['result']['list']
                    klines = []
                    for k in klines_raw:
                        try:
                            klines.append({
                                'start_time': int(k[0]) / 1000, # Convert to seconds
                                'open': float(k[1]),
                                'high': float(k[2]),
                                'low': float(k[3]),
                                'close': float(k[4]),
                                'volume': float(k[5]),
                                'turnover': float(k[6])
                            })
                        except (ValueError, IndexError, TypeError) as e:
                            logger.warning(f"Invalid kline data format for {symbol}: {k}. Error: {e}")
                            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='invalid_kline_data_format').inc()
                            continue
                    
                    # Cache the klines
                    if klines:
                        self.kline_cache[symbol] = {
                            'klines': klines,
                            'timestamp': time.time(),
                            'expiry': 300 # Cache for 5 minutes
                        }

                    # Find the kline that contains the target timestamp
                    for kline in klines:
                        if kline['start_time'] <= timestamp < kline['start_time'] + 60: # Assuming 1-minute klines
                            return (kline['open'] + kline['close']) / 2 # Use mid-price of the kline

                    logger.warning(f"No kline found for {symbol} at exact timestamp {timestamp}. Klines fetched: {len(klines)}")
                    return None # No exact kline found

            except Exception as e:
                logger.error(f"Error fetching kline for {symbol} (attempt {attempt + 1}): {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='bybit_kline_fetch_error').inc()
                await asyncio.sleep(retry_delay)

        logger.error(f"Failed to fetch kline for {symbol} at {timestamp} after {max_attempts} attempts.")
        return None

    async def _fetch_historical_trades(self, symbol: str, timestamp: float, limit: int = 1000) -> List[Dict]:
        """
        Загрузка исторических сделок через Bybit API.
        Использует `market/recent-trade` для получения последних сделок.
        """
        if not self.http_session or self.http_session.closed:
            logger.warning("HTTP session not initialized or closed, reinitializing for _fetch_historical_trades")
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.http_session = aiohttp.ClientSession(timeout=timeout)

        # Check cache
        cache_entry = self.trade_cache.get(symbol)
        current_time = time.time()
        if cache_entry and cache_entry['trades'] and current_time - cache_entry['timestamp'] < cache_entry['expiry']:
            logger.debug(f"Using cached trades for {symbol}, {len(cache_entry['trades'])} trades")
            return cache_entry['trades']

        max_attempts = 5
        retry_delay = 10

        for attempt in range(max_attempts):
            try:
                # Bybit's recent-trade API does not support 'start' and 'end' timestamps.
                # It returns the most recent trades. We will fetch and then filter.
                url = f"https://api.bybit.com/v5/market/recent-trade?category=linear&symbol={symbol}&limit={limit}"

                async with self.http_session.get(url) as response:
                    if response.status == 429:
                        logger.warning(f"Rate limit exceeded for {symbol}, retrying after {retry_delay} seconds")
                        ERRORS_TOTAL.labels(component='ml_data_collector', error_type='bybit_trade_rate_limit').inc()
                        await asyncio.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                        continue
                    if response.status != 200:
                        logger.error(f"Failed to fetch historical trades for {symbol}: HTTP {response.status}")
                        ERRORS_TOTAL.labels(component='ml_data_collector', error_type='bybit_trade_http_error').inc()
                        await asyncio.sleep(retry_delay)
                        continue

                    data = await response.json()
                    if not isinstance(data, dict):
                        logger.error(f"Invalid response format for trades in {symbol}: {data}")
                        ERRORS_TOTAL.labels(component='ml_data_collector', error_type='bybit_trade_invalid_response_format').inc()
                        await asyncio.sleep(retry_delay)
                        continue
                    if data.get('retCode') != 0:
                        logger.warning(f"API error fetching trades for {symbol}: {data.get('retMsg', 'Unknown error')}")
                        ERRORS_TOTAL.labels(component='ml_data_collector', error_type='bybit_trade_api_error').inc()
                        await asyncio.sleep(retry_delay)
                        continue

                    trades = data.get('result', {}).get('list', [])
                    
                    formatted_trades = []
                    required_keys = ['time', 'price', 'qty', 'side', 'execId']
                    for trade in trades:
                        if not all(key in trade for key in required_keys):
                            logger.warning(f"Missing keys in trade data for {symbol}: {trade}")
                            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='missing_trade_keys').inc()
                            continue
                        try:
                            formatted_trades.append({
                                'timestamp': float(trade['time']) / 1000,
                                'price': float(trade['price']),
                                'size': float(trade['qty']),
                                'side': trade['side'],
                                'volume_usdt': float(trade['price']) * float(trade['qty']),
                                'trade_id': trade['execId']
                            })
                        except (KeyError, ValueError, TypeError) as e:
                            logger.warning(f"Invalid trade data for {symbol}: {e}, data: {trade}")
                            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='invalid_trade_data').inc()
                            continue

                    # Filter trades to be around the requested timestamp (e.g., within 5 minutes)
                    filtered_trades = [
                        trade for trade in formatted_trades
                        if abs(trade['timestamp'] - timestamp) <= 300 # within 5 minutes of target timestamp
                    ]

                    logger.debug(f"Fetched {len(formatted_trades)} raw trades, filtered to {len(filtered_trades)} for {symbol} around {datetime.fromtimestamp(timestamp)}")

                    # Cache the result
                    if filtered_trades:
                        self.trade_cache[symbol] = {
                            'trades': filtered_trades,
                            'timestamp': current_time,
                            'expiry': 300
                        }

                    return filtered_trades

            except Exception as e:
                logger.error(f"Error fetching historical trades for {symbol} (attempt {attempt + 1}): {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='bybit_trade_fetch_error').inc()
                await asyncio.sleep(retry_delay)

        logger.error(f"Failed to fetch historical trades for {symbol} after {max_attempts} attempts")
        return []

    def _get_current_price(self, symbol: str) -> Optional[float]:
        """Получение текущей цены из OrderBook или TradeProcessor."""
        try:
            if self.orderbook_analyzer and symbol in self.orderbook_analyzer.current_orderbooks:
                ob = self.orderbook_analyzer.current_orderbooks[symbol]
                if ob['bids'] and ob['asks']:
                    best_bid = ob['bids'][0][0] if ob['bids'] else 0
                    best_ask = ob['asks'][0][0] if ob['asks'] else 0
                    if best_bid > 0 and best_ask > 0:
                        return (best_bid + best_ask) / 2
            if self.trade_processor and symbol in self.trade_processor.trade_history:
                history = list(self.trade_processor.trade_history.get(symbol, deque()))
                if history:
                    return history[-1]['price']
            logger.warning(f"No current price for {symbol} from orderbook or trade history.")
            return None
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='get_current_price_error').inc()
            return None

    async def _periodic_persistence(self):
        """Периодическое сохранение данных."""
        while self.is_running:
            try:
                await asyncio.sleep(300) # Save every 5 minutes
                await self._save_persistent_data()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic persistence: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='persistence_error').inc()

    async def _save_persistent_data(self):
        """Сохранение данных в файл."""
        try:
            with self.buffer_lock:
                buffer_size = len(self.data_buffer)
                data_to_save = {
                    'data_buffer': list(self.data_buffer),
                    'collection_stats': self.collection_stats,
                    'processed_alerts': dict(self.processed_alerts)
                }

            logger.debug(f"Attempting to save {buffer_size} data points to {self.persistence_file}")

            directory = os.path.dirname(self.persistence_file) or '.'
            if not os.access(directory, os.W_OK):
                logger.error(f"No write permission for directory {directory}")
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='persistence_permission_denied').inc()
                return

            os.makedirs(directory, exist_ok=True)

            async with aiofiles.open(self.persistence_file, 'wb') as f:
                await f.write(pickle.dumps(data_to_save))

            # logger.info(f"Successfully saved {buffer_size} data points to {self.persistence_file}")

        except Exception as e:
            logger.error(f"Error saving persistent data to {self.persistence_file}: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='persistence_save_error').inc()

    def _load_persistent_data(self):
        """Загрузка сохраненных данных."""
        try:
            if os.path.exists(self.persistence_file):
                with open(self.persistence_file, 'rb') as f:
                    data = pickle.load(f)

                saved_buffer = data.get('data_buffer', [])
                # Only load a subset to avoid excessive memory usage on startup
                for item in saved_buffer[-self.max_buffer_size:]:
                    self.data_buffer.append(item)

                self.collection_stats.update(data.get('collection_stats', {}))
                self.processed_alerts.update(data.get('processed_alerts', {}))
                # logger.info(f"Loaded {len(self.data_buffer)} data points from persistence")
                ML_TRAINING_BUFFER_SIZE.set(len(self.data_buffer))

        except Exception as e:
            logger.warning(f"Could not load persistent data: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='ml_data_collector', error_type='persistence_load_error').inc()

    def get_collection_stats(self) -> Dict[str, Any]:
        """Получение статистики сбора данных."""
        with self.stats_lock:
            return {
                **self.collection_stats,
                'buffer_size': len(self.data_buffer),
                'is_running': self.is_running
            }

    async def cleanup(self):
        """Очистка ресурсов."""
        await self.stop_collection()
        self.data_buffer.clear()
        self.trade_cache.clear()
        self.kline_cache.clear()
        # logger.info("MLDataCollector cleanup completed")

    async def process_pending_alerts_loop(self, interval_sec: int):
        """Цикл обработки ожидающих сообщений."""
        while self.is_running:
            try:
                while not self.pending_alerts.empty():
                    alert_data = await self.pending_alerts.get()
                    await self._process_alert_for_ml(alert_data)
                    PENDING_ALERTS_QUEUE_SIZE.set(self.pending_alerts.qsize())
                    await asyncio.sleep(0.01)  # Small delay to prevent event loop overload
                await asyncio.sleep(interval_sec)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in pending alerts loop: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='ml_data_collector', error_type='pending_alerts_loop_error').inc()
                await asyncio.sleep(5)




