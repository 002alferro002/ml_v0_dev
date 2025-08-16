import asyncio
import logging
from typing import Dict, List, Optional, Callable, Any
from pybit.unified_trading import HTTP, WebSocket
from datetime import datetime
import json
import threading
import websocket
import aiohttp
from aiohttp import ClientConnectorError, ClientTimeout
import time
import random
from collections import deque

logger = logging.getLogger(__name__)

# Monkey patch для перехвата исключений WebSocket
original_websocket_send = websocket.WebSocket.send

def safe_websocket_send(self, data, opcode=websocket.ABNF.OPCODE_TEXT):
    """Безопасная отправка данных через WebSocket с обработкой исключений."""
    try:
        if self.sock is None:
            logger.warning("Попытка отправки данных в закрытый WebSocket")
            return
        return original_websocket_send(self, data, opcode)
    except websocket.WebSocketConnectionClosedException as e:
        logger.warning(f"WebSocket соединение уже закрыто при отправке: {e}")
        return
    except Exception as e:
        logger.error(f"Ошибка отправки WebSocket данных: {e}")
        return

# Применяем monkey patch
websocket.WebSocket.send = safe_websocket_send

# Monkey patch для _send_custom_ping в pybit
try:
    from pybit._websocket_stream import _WebSocketManager
    
    original_send_custom_ping = _WebSocketManager._send_custom_ping
    
    def safe_send_custom_ping(self):
        """Безопасная отправка custom ping с обработкой исключений."""
        try:
            if hasattr(self, 'ws') and self.ws and hasattr(self.ws, 'sock') and self.ws.sock:
                return original_send_custom_ping(self)
            else:
                logger.warning("WebSocket соединение недоступно для отправки ping")
        except websocket.WebSocketConnectionClosedException as e:
            logger.warning(f"WebSocket соединение уже закрыто при отправке ping: {e}")
        except Exception as e:
            logger.error(f"Ошибка отправки custom ping: {e}")
    
    # Применяем monkey patch
    _WebSocketManager._send_custom_ping = safe_send_custom_ping
    logger.info("Monkey patch для _send_custom_ping применен успешно")
except ImportError as e:
    logger.warning(f"Не удалось импортировать _WebSocketManager для monkey patch: {e}")
except Exception as e:
    logger.error(f"Ошибка применения monkey patch для _send_custom_ping: {e}")

class PybitAPIClient:
    """Клиент для работы с REST API Bybit через официальную библиотеку pybit."""
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.session = HTTP(
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet
        )
        
    async def get_all_futures_symbols(self) -> List[Dict[str, Any]]:
        """Получает список всех фьючерсных торговых пар с Bybit."""
        try:
            # Выполняем синхронный вызов в отдельном потоке
            response = await asyncio.to_thread(
                self.session.get_instruments_info,
                category="linear",
                status="Trading",
                limit=1000
            )
            
            if response["retCode"] == 0:
                instruments = response.get("result", {}).get("list", [])
                
                # Форматируем данные в том же формате, что и старый клиент
                symbols = []
                for instrument in instruments:
                    symbol_info = {
                        "symbol": instrument.get("symbol"),
                        "baseCoin": instrument.get("baseCoin"),
                        "quoteCoin": instrument.get("quoteCoin"),
                        "status": instrument.get("status"),
                        "contractType": instrument.get("contractType"),
                        "launchTime": instrument.get("launchTime"),
                        "deliveryTime": instrument.get("deliveryTime"),
                        "deliveryFeeRate": instrument.get("deliveryFeeRate"),
                        "priceScale": instrument.get("priceScale"),
                        "leverageFilter": instrument.get("leverageFilter"),
                        "priceFilter": instrument.get("priceFilter"),
                        "lotSizeFilter": instrument.get("lotSizeFilter")
                    }
                    symbols.append(symbol_info)
                
                logger.info(f"Получено {len(symbols)} торговых пар")
                return symbols
            else:
                logger.error(f"Ошибка получения торговых пар: {response}")
                return []
                
        except ConnectionError as e:
            logger.error(f"Ошибка подключения к Bybit API: {e}")
            return []
        except Exception as e:
            logger.error(f"Ошибка при получении торговых пар: {e}", exc_info=True)
            return []
    
    async def get_orderbook(self, symbol: str, limit: int = 50) -> Optional[Dict[str, Any]]:
        """Получает стакан заявок для символа."""
        try:
            response = await asyncio.to_thread(
                self.session.get_orderbook,
                category="linear",
                symbol=symbol,
                limit=limit
            )
            
            if response["retCode"] == 0:
                return response.get("result")
            else:
                logger.error(f"Ошибка получения стакана для {symbol}: {response}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при получении стакана для {symbol}: {e}")
            return None
    
    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Получает последние сделки для символа."""
        try:
            response = await asyncio.to_thread(
                self.session.get_public_trade_history,
                category="linear",
                symbol=symbol,
                limit=limit
            )
            
            if response["retCode"] == 0:
                return response.get("result", {}).get("list", [])
            else:
                logger.error(f"Ошибка получения сделок для {symbol}: {response}")
                return []
                
        except Exception as e:
            logger.error(f"Ошибка при получении сделок для {symbol}: {e}")
            return []
    
    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[Dict[str, Any]]:
        """Получает данные свечей для символа."""
        try:
            response = await asyncio.to_thread(
                self.session.get_kline,
                category="linear",
                symbol=symbol,
                interval=interval,
                limit=limit
            )
            
            if response["retCode"] == 0:
                return response.get("result", {}).get("list", [])
            else:
                logger.error(f"Ошибка получения свечей для {symbol}: {response}")
                return []
                
        except Exception as e:
            logger.error(f"Ошибка при получении свечей для {symbol}: {e}")
            return []
    
    async def get_24h_ticker(self, symbols: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """Получает 24-часовую статистику для символов.
        
        Args:
            symbols: Список символов для фильтрации. Если None, возвращает все тикеры.
            
        Returns:
            Словарь с данными тикеров, где ключ - символ, значение - данные тикера.
        """
        try:
            # Получаем все тикеры для категории linear (фьючерсы)
            response = await asyncio.to_thread(
                self.session.get_tickers,
                category="linear"
            )
            
            if response["retCode"] == 0:
                tickers = response.get("result", {}).get("list", [])
                result = {}
                
                # Создаем set для быстрого поиска, если нужна фильтрация
                symbols_set = set(symbols) if symbols else None
                
                for ticker in tickers:
                    symbol = ticker.get("symbol")
                    if not symbol:
                        continue
                        
                    # Фильтруем символы, если список передан
                    if symbols_set and symbol not in symbols_set:
                        continue
                        
                    result[symbol] = {
                        "symbol": symbol,
                        "lastPrice": float(ticker.get("lastPrice", 0)),
                        "price24hPcnt": float(ticker.get("price24hPcnt", 0)),
                        "volume24h": float(ticker.get("volume24h", 0)),
                        "turnover24h": float(ticker.get("turnover24h", 0)),
                        "highPrice24h": float(ticker.get("highPrice24h", 0)),
                        "lowPrice24h": float(ticker.get("lowPrice24h", 0))
                    }
                
                logger.info(f"Получено {len(result)} тикеров из {len(tickers)} доступных")
                return result
            else:
                logger.error(f"Ошибка получения тикеров: {response}")
                return {}
                
        except Exception as e:
            logger.error(f"Ошибка при получении тикеров: {e}", exc_info=True)
            return {}
    
    async def get_multi_timeframe_data(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Получает данные по множественным временным интервалам для символов.
        
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ: Ограничивает количество запросов для избежания таймаутов.
        Получает данные только для первых 50 символов и только для ключевых интервалов.
        
        Args:
            symbols: Список символов для получения данных
            
        Returns:
            Словарь с данными по интервалам для каждого символа
        """
        result = {}
        
        # Ограничиваем количество символов для обработки (первые 50)
        limited_symbols = symbols[:50] if len(symbols) > 50 else symbols
        
        # Интервалы для получения данных о волатильности и изменениях цены
        intervals = {
            '1m': {'interval': '1', 'limit': 2},
            '5m': {'interval': '5', 'limit': 2},
            '15m': {'interval': '15', 'limit': 2},
            '1h': {'interval': '60', 'limit': 2},
            '24h': {'interval': '1440', 'limit': 2}
        }
        
        logger.info(f"Получение данных по интервалам для {len(limited_symbols)} символов из {len(symbols)}")
        
        for symbol in limited_symbols:
            symbol_data = {}
            
            for timeframe, config in intervals.items():
                try:
                    # Используем метод get_kline из pybit
                    response = await asyncio.to_thread(
                        self.session.get_kline,
                        category="linear",
                        symbol=symbol,
                        interval=config['interval'],
                        limit=config['limit']
                    )
                    
                    if response.get("retCode") == 0:
                        klines = response.get("result", {}).get("list", [])
                        
                        if len(klines) >= 2:
                            # Берем последние две свечи для расчета изменений
                            current = klines[0]  # Последняя свеча
                            previous = klines[1]  # Предыдущая свеча
                            
                            current_close = float(current[4])
                            previous_close = float(previous[4])
                            current_high = float(current[2])
                            current_low = float(current[3])
                            current_volume = float(current[5])
                            
                            # Расчет изменения цены в процентах
                            price_change_percent = ((current_close - previous_close) / previous_close * 100) if previous_close > 0 else 0
                            
                            # Расчет волатильности (размах цены относительно цены закрытия)
                            volatility = ((current_high - current_low) / current_close * 100) if current_close > 0 else 0
                            
                            symbol_data[timeframe] = {
                                'price_change_percent': round(price_change_percent, 4),
                                'volatility': round(volatility, 4),
                                'volume': current_volume,
                                'close_price': current_close
                            }
                        else:
                            symbol_data[timeframe] = {
                                'price_change_percent': 0,
                                'volatility': 0,
                                'volume': 0,
                                'close_price': 0
                            }
                    else:
                        symbol_data[timeframe] = {
                            'price_change_percent': 0,
                            'volatility': 0,
                            'volume': 0,
                            'close_price': 0
                        }
                        
                except Exception as e:
                    logger.error(f"Ошибка при получении данных {timeframe} для {symbol}: {e}")
                    symbol_data[timeframe] = {
                        'price_change_percent': 0,
                        'volatility': 0,
                        'volume': 0,
                        'close_price': 0
                    }
                    
                # Небольшая задержка между запросами для одного символа
                await asyncio.sleep(0.05)
            
            result[symbol] = symbol_data
            
            # Задержка между символами для избежания превышения лимитов API
            await asyncio.sleep(0.1)
        
        logger.info(f"Получены данные по интервалам для {len(result)} символов")
        return result


class PybitWebSocketClient:
    """WebSocket клиент для Bybit через официальную библиотеку pybit."""
    
    def __init__(self, message_publisher: Callable, testnet: bool = False):
        self.message_publisher = message_publisher
        self.testnet = testnet
        self.ws = None
        self.is_running = False
        self.subscribed_symbols: set = set()
        self._connection_active = False
        
        # Параметры переподключения
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 10
        self._base_reconnect_delay = 1.0  # Базовая задержка в секундах
        self._max_reconnect_delay = 60.0  # Максимальная задержка
        self._last_disconnect_time = 0
        
        # Буферизация сообщений
        self._message_buffer = deque(maxlen=1000)
        self._buffer_enabled = True
        
        # Метрики соединения
        self._connection_stats = {
            'total_reconnects': 0,
            'last_reconnect_time': None,
            'connection_uptime_start': None,
            'messages_processed': 0,
            'messages_buffered': 0
        }
        
    def _handle_message(self, message):
        """Обработчик входящих WebSocket сообщений."""
        try:
            # Проверяем активность соединения
            if not self._connection_active:
                return
                
            # Проверяем, что сообщение не пустое
            if not message:
                return
                
            # Если сообщение - строка, парсим JSON
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    logger.warning(f"Не удалось распарсить JSON: {message}")
                    return
            else:
                data = message
            
            # Проверяем структуру данных
            if not isinstance(data, dict):
                logger.warning(f"Неожиданный формат данных: {type(data)}")
                return
                
            # Публикуем сообщение через message_publisher (теперь синхронная функция)
            try:
                self.message_publisher(data)
                self._connection_stats['messages_processed'] += 1
            except Exception as pub_error:
                logger.error(f"Ошибка публикации сообщения: {pub_error}")
                # Буферизуем сообщение при ошибке публикации
                if self._buffer_enabled:
                    self._buffer_message(data)
            
        except Exception as e:
            # Проверяем, связана ли ошибка с закрытым соединением
            if "Connection is already closed" in str(e) or "WebSocketConnectionClosedException" in str(e):
                logger.warning(f"WebSocket соединение закрыто во время обработки сообщения: {e}")
                self._connection_active = False
                self._last_disconnect_time = time.time()
                # Буферизуем сообщение для повторной обработки после переподключения
                if self._buffer_enabled and isinstance(message, dict):
                    self._buffer_message(message)
                return
            logger.error(f"Ошибка обработки WebSocket сообщения: {e}")
            # Буферизуем сообщение при других ошибках тоже
            if self._buffer_enabled and isinstance(message, dict):
                self._buffer_message(message)
    
    async def start(self):
        """Запускает WebSocket соединение."""
        if self.is_running:
            logger.warning("PybitWebSocketClient уже запущен")
            return
            
        try:
            # Создаем WebSocket соединение с дополнительными параметрами
            self.ws = WebSocket(
                testnet=self.testnet,
                channel_type="linear",
                ping_interval=30,  # Увеличиваем интервал ping
                ping_timeout=10    # Уменьшаем таймаут ping
            )
            
            # Переопределяем обработчик ошибок WebSocket
            original_on_error = getattr(self.ws, 'on_error', None)
            def safe_on_error(ws, error):
                if "Connection is already closed" in str(error):
                    logger.warning(f"WebSocket соединение уже закрыто: {error}")
                    self._connection_active = False
                    return
                if original_on_error:
                    original_on_error(ws, error)
                else:
                    logger.error(f"WebSocket ошибка: {error}")
            
            self.ws.on_error = safe_on_error
            
            self.is_running = True
            self._connection_active = True
            self._connection_stats['connection_uptime_start'] = time.time()
            self._reconnect_attempts = 0  # Сбрасываем счетчик при успешном запуске
            logger.info("PybitWebSocketClient запущен")
            
        except Exception as e:
            logger.error(f"Ошибка запуска PybitWebSocketClient: {e}")
            self.is_running = False
            self._connection_active = False
            raise
    
    async def stop(self):
        """Останавливает WebSocket соединение."""
        if not self.is_running:
            return
            
        try:
            # Сначала отмечаем соединение как неактивное
            self._connection_active = False
            
            if self.ws:
                # Отписываемся от всех символов
                for symbol in list(self.subscribed_symbols):
                    await self.unsubscribe_symbol(symbol)
                
                # Безопасно закрываем соединение
                try:
                    # Добавляем дополнительную проверку перед закрытием
                    if hasattr(self.ws, 'ws') and self.ws.ws and not self.ws.ws.closed:
                        self.ws.exit()
                    else:
                        logger.info("WebSocket уже закрыт")
                except Exception as exit_error:
                    # Игнорируем ошибки закрытия уже закрытого соединения
                    if "Connection is already closed" not in str(exit_error):
                        logger.warning(f"Ошибка при закрытии WebSocket: {exit_error}")
                
            self.is_running = False
            self.subscribed_symbols.clear()
            logger.info("PybitWebSocketClient остановлен")
            
        except Exception as e:
            logger.error(f"Ошибка остановки PybitWebSocketClient: {e}")
    
    async def subscribe_symbol(self, symbol: str):
        """Подписывается на обновления для символа."""
        if not self.is_running or not self.ws or not self._connection_active:
            logger.error("WebSocket не запущен или соединение неактивно")
            return False
            
        if symbol in self.subscribed_symbols:
            logger.debug(f"Уже подписан на {symbol}")
            return True
            
        try:
            # Подписываемся на orderbook с правильными параметрами
            try:
                if self._connection_active:
                    self.ws.orderbook_stream(
                        depth=50,
                        symbol=symbol,
                        callback=self._handle_message
                    )
                    logger.debug(f"Подписка на orderbook для {symbol} выполнена")
            except Exception as ob_error:
                logger.warning(f"Не удалось подписаться на orderbook для {symbol}: {ob_error}")
                # Если ошибка связана с закрытым соединением, отмечаем соединение как неактивное
                if "Connection is already closed" in str(ob_error):
                    self._connection_active = False
            
            # Подписываемся на trades
            try:
                if self._connection_active:
                    self.ws.trade_stream(
                        symbol=symbol,
                        callback=self._handle_message
                    )
                    logger.debug(f"Подписка на trades для {symbol} выполнена")
            except Exception as trade_error:
                logger.warning(f"Не удалось подписаться на trades для {symbol}: {trade_error}")
                # Если ошибка связана с закрытым соединением, отмечаем соединение как неактивное
                if "Connection is already closed" in str(trade_error):
                    self._connection_active = False
            
            if self._connection_active:
                self.subscribed_symbols.add(symbol)
                logger.info(f"Подписка на {symbol} успешна")
                return True
            else:
                logger.error(f"Соединение неактивно, не удалось подписаться на {symbol}")
                return False
            
        except Exception as e:
            logger.error(f"Ошибка подписки на {symbol}: {e}")
            # Проверяем, связана ли ошибка с закрытым соединением
            if "Connection is already closed" in str(e):
                self._connection_active = False
            return False
    
    async def unsubscribe_symbol(self, symbol: str):
        """Отписывается от обновлений для символа."""
        if symbol not in self.subscribed_symbols:
            logger.debug(f"Не подписан на {symbol}")
            return True
            
        try:
            # В pybit нет прямого метода отписки, поэтому просто удаляем из списка
            self.subscribed_symbols.discard(symbol)
            logger.info(f"Отписка от {symbol} успешна")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отписки от {symbol}: {e}")
            return False
    
    async def get_subscribed_symbols(self) -> List[str]:
        """Возвращает список подписанных символов."""
        return list(self.subscribed_symbols)
    
    def is_connected(self) -> bool:
        """Проверяет, подключен ли WebSocket."""
        if not self.is_running or not self.ws or not self._connection_active:
            return False
        
        # Дополнительная проверка состояния WebSocket
        try:
            if hasattr(self.ws, 'ws') and self.ws.ws:
                return not self.ws.ws.closed
            return True
        except Exception:
            self._connection_active = False
            return False
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Возвращает статистику подключения WebSocket."""
        current_stats = self._connection_stats.copy()
        current_stats.update({
            "is_connected": self.is_connected(),
            "is_running": self.is_running,
            "connection_active": self._connection_active,
            "subscribed_symbols_count": len(self.subscribed_symbols),
            "subscribed_symbols": list(self.subscribed_symbols),
            "reconnect_attempts": self._reconnect_attempts,
            "buffer_size": len(self._message_buffer)
        })
        return current_stats
    
    def _buffer_message(self, message: Dict[str, Any]):
        """Буферизует сообщение для последующей обработки."""
        if self._buffer_enabled:
            self._message_buffer.append({
                'message': message,
                'timestamp': time.time()
            })
            self._connection_stats['messages_buffered'] += 1
            logger.debug(f"Сообщение добавлено в буфер. Размер буфера: {len(self._message_buffer)}")
    
    def _process_buffered_messages(self):
        """Обрабатывает буферизованные сообщения."""
        processed_count = 0
        while self._message_buffer and self._connection_active:
            try:
                buffered_item = self._message_buffer.popleft()
                message = buffered_item['message']
                
                # Проверяем, не слишком ли старое сообщение (старше 30 секунд)
                if time.time() - buffered_item['timestamp'] > 30:
                    logger.debug("Пропускаем устаревшее буферизованное сообщение")
                    continue
                
                self.message_publisher(message)
                processed_count += 1
                self._connection_stats['messages_processed'] += 1
                
            except Exception as e:
                logger.error(f"Ошибка обработки буферизованного сообщения: {e}")
                break
        
        if processed_count > 0:
            logger.info(f"Обработано {processed_count} буферизованных сообщений")
    
    def _calculate_reconnect_delay(self) -> float:
        """Вычисляет задержку для переподключения с экспоненциальным backoff."""
        # Экспоненциальная задержка с джиттером
        delay = min(
            self._base_reconnect_delay * (2 ** self._reconnect_attempts),
            self._max_reconnect_delay
        )
        
        # Добавляем случайный джиттер (±20%)
        jitter = delay * 0.2 * (random.random() - 0.5)
        final_delay = max(0.1, delay + jitter)
        
        logger.info(f"Задержка переподключения: {final_delay:.2f} секунд (попытка {self._reconnect_attempts + 1})")
        return final_delay
    
    async def _attempt_reconnect(self):
        """Пытается переподключиться с экспоненциальной задержкой."""
        if self._reconnect_attempts >= self._max_reconnect_attempts:
            logger.error(f"Достигнуто максимальное количество попыток переподключения ({self._max_reconnect_attempts})")
            return False
        
        self._reconnect_attempts += 1
        delay = self._calculate_reconnect_delay()
        
        logger.info(f"Ожидание {delay:.2f} секунд перед попыткой переподключения...")
        await asyncio.sleep(delay)
        
        try:
            # Сохраняем список символов для повторной подписки
            symbols_to_resubscribe = list(self.subscribed_symbols)
            
            # Останавливаем текущее соединение
            await self.stop()
            
            # Запускаем новое соединение
            await self.start()
            
            if self.is_connected():
                logger.info("Переподключение успешно")
                self._connection_stats['total_reconnects'] += 1
                self._connection_stats['last_reconnect_time'] = datetime.now().isoformat()
                self._reconnect_attempts = 0  # Сбрасываем счетчик при успешном подключении
                
                # Повторно подписываемся на символы
                for symbol in symbols_to_resubscribe:
                    try:
                        await self.subscribe_symbol(symbol)
                    except Exception as sub_error:
                        logger.error(f"Ошибка повторной подписки на {symbol}: {sub_error}")
                
                # Обрабатываем буферизованные сообщения
                self._process_buffered_messages()
                
                return True
            else:
                logger.warning("Переподключение не удалось")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при попытке переподключения: {e}")
            return False