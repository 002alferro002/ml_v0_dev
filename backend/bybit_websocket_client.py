import asyncio
import websockets
import json
import logging
from typing import Dict, Any, List, Callable, Optional
from collections import defaultdict
from exceptions import WebSocketError
from metrics import WS_MESSAGES_RECEIVED_TOTAL, ACTIVE_WS_CONNECTIONS, ERRORS_TOTAL

logger = logging.getLogger(__name__)

class BybitWebSocketClient:
    def __init__(self, uri: str, message_publisher: Callable[[Dict[str, Any]], None],
                 on_connect: Optional[Callable[[], None]] = None,
                 on_disconnect: Optional[Callable[[], None]] = None):
        self.uri = uri
        self.message_publisher = message_publisher # Now takes a publisher function
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.is_connected = False
        self.reconnect_task: Optional[asyncio.Task] = None
        self.ping_task: Optional[asyncio.Task] = None
        self.subscriptions: Dict[str, List[str]] = defaultdict(list) # topic -> [symbol1, symbol2]
        self._subscription_counts: Dict[str, int] = defaultdict(int) # symbol_topic -> count
        self.lock = asyncio.Lock()
        # logger.info(f"BybitWebSocketClient initialized for URI: {uri}")

    async def connect(self):
        """Устанавливает соединение с WebSocket."""
        if self.is_connected:
            logger.warning("Already connected to WebSocket.")
            return

        try:
            # Улучшенные настройки WebSocket для стабильности соединения
            self.websocket = await websockets.connect(
                self.uri,
                ping_interval=20,  # Отправка ping каждые 20 секунд
                ping_timeout=10,   # Таймаут ожидания pong 10 секунд
                close_timeout=10,  # Таймаут закрытия соединения
                max_size=10**7,    # Максимальный размер сообщения 10MB
                compression=None   # Отключение сжатия для стабильности
            )
            self.is_connected = True
            ACTIVE_WS_CONNECTIONS.set(1)
            logger.info(f"Connected to Bybit WebSocket: {self.uri}")
            if self.on_connect:
                asyncio.create_task(self.on_connect())
            
            # Resubscribe to all topics after reconnection
            await self._resubscribe_all()

            self.ping_task = asyncio.create_task(self._periodic_ping())
            asyncio.create_task(self._listen_for_messages())

        except Exception as e:
            logger.error(f"Failed to connect to Bybit WebSocket: {e}", exc_info=True)
            self.is_connected = False
            ACTIVE_WS_CONNECTIONS.set(0)
            ERRORS_TOTAL.labels(component='websocket_client', error_type='connection_failed').inc()
            self._schedule_reconnect()
            raise WebSocketError(f"Failed to connect to Bybit WebSocket: {e}")

    async def disconnect(self):
        """Закрывает соединение с WebSocket."""
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
        if self.websocket and self.is_connected:
            try:
                await self.websocket.close()
                # logger.info("Disconnected from Bybit WebSocket.")
            except Exception as e:
                logger.error(f"Error during WebSocket disconnect: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='websocket_client', error_type='disconnect_error').inc()
            finally:
                self.is_connected = False
                self.websocket = None
                ACTIVE_WS_CONNECTIONS.set(0)
                if self.on_disconnect:
                    asyncio.create_task(self.on_disconnect())

    async def _listen_for_messages(self):
        """Слушает входящие сообщения от WebSocket."""
        while self.is_connected:
            try:
                message = await self.websocket.recv()
                data = json.loads(message)
                
                topic = data.get('topic', 'unknown')
                WS_MESSAGES_RECEIVED_TOTAL.labels(topic=topic).inc()

                if 'op' in data and data['op'] == 'pong':
                    logger.debug("Received pong from Bybit.")
                elif 'success' in data and data['success'] is False:
                    logger.error(f"Bybit WS error: {data.get('ret_msg', 'Unknown error')}, Request: {data.get('request')}")
                    ERRORS_TOTAL.labels(component='websocket_client', error_type='bybit_api_error').inc()
                else:
                    # Log received message for debugging
                    # logger.info(f"Received WebSocket message: topic={data.get('topic', 'unknown')}, data_size={len(str(data))}")
                    # Publish raw message to message broker
                    await self.message_publisher(data)
            except websockets.ConnectionClosedOK:
                logger.info("Bybit WebSocket соединение закрыто корректно.")
                break
            except websockets.ConnectionClosedError as e:
                if "keepalive ping timeout" in str(e):
                    logger.warning(f"Bybit WebSocket соединение закрыто из-за таймаута keepalive ping: {e}")
                else:
                    logger.error(f"Bybit WebSocket соединение закрыто с ошибкой: {e}")
                ERRORS_TOTAL.labels(component='websocket_client', error_type='connection_closed_error').inc()
                break
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка декодирования JSON сообщения: {message[:200] if 'message' in locals() else 'N/A'}... Ошибка: {e}")
                ERRORS_TOTAL.labels(component='websocket_client', error_type='json_decode_error').inc()
                # Продолжаем работу, не прерывая соединение из-за одного плохого сообщения
            except Exception as e:
                logger.error(f"Ошибка получения сообщения от Bybit WebSocket: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='websocket_client', error_type='message_receive_error').inc()
                break
        
        if self.is_connected: # If loop broke due to error, not graceful close
            self.is_connected = False
            self.websocket = None
            ACTIVE_WS_CONNECTIONS.set(0)
            if self.on_disconnect:
                asyncio.create_task(self.on_disconnect())
            self._schedule_reconnect()

    async def _periodic_ping(self):
        """Отправляет пинг-сообщения для поддержания соединения."""
        while self.is_connected:
            try:
                if self.websocket and self.is_connected:
                    await self.websocket.send(json.dumps({"op": "ping"}))
                    logger.debug("Sent ping to Bybit.")
                    await asyncio.sleep(15)  # Интервал ping 15 секунд
                else:
                    logger.warning("WebSocket is closed, stopping ping task.")
                    break
            except websockets.ConnectionClosed:
                logger.warning("WebSocket connection closed during ping.")
                break
            except Exception as e:
                logger.error(f"Error sending ping to Bybit WebSocket: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='websocket_client', error_type='ping_error').inc()
                break

    def _schedule_reconnect(self):
        """Планирует переподключение с экспоненциальной задержкой."""
        if self.reconnect_task and not self.reconnect_task.done():
            return # Reconnect already scheduled

        async def reconnect_attempt():
            delay = 2
            attempt = 1
            max_attempts = 10
            
            while not self.is_connected and attempt <= max_attempts:
                logger.info(f"Попытка переподключения #{attempt} к Bybit WebSocket через {delay} секунд...")
                await asyncio.sleep(delay)
                try:
                    await self.connect()
                    if self.is_connected:
                        logger.info(f"Успешно переподключились к Bybit WebSocket после {attempt} попыток")
                        break
                except WebSocketError: # Catch custom error
                    pass # Error already logged in connect()
                except Exception as e:
                    logger.error(f"Неожиданная ошибка при попытке переподключения #{attempt}: {e}", exc_info=True)
                    ERRORS_TOTAL.labels(component='websocket_client', error_type='reconnect_unexpected_error').inc()
                
                attempt += 1
                delay = min(delay * 1.5, 30) # Максимальная задержка 30 секунд
            
            if not self.is_connected:
                logger.error(f"Не удалось переподключиться к Bybit WebSocket после {max_attempts} попыток")
            
            self.reconnect_task = None # Clear task once connected or max attempts reached

        self.reconnect_task = asyncio.create_task(reconnect_attempt())

    async def subscribe(self, topic: str, symbols: List[str]):
        """Подписывается на указанные темы для символов."""
        async with self.lock:
            args_to_send = []
            for symbol in symbols:
                key = f"{symbol}_{topic}"
                self._subscription_counts[key] += 1
                if self._subscription_counts[key] == 1: # Only subscribe if it's the first client for this symbol+topic
                    self.subscriptions[topic].append(symbol)
                    args_to_send.append(f"{topic}.{symbol}")
                    # logger.info(f"Incremented subscription count for {key}. Now {self._subscription_counts[key]}.")
                else:
                    logger.debug(f"Already subscribed to {key}. Count: {self._subscription_counts[key]}.")

            if args_to_send:
                if self.websocket and self.is_connected:
                    try:
                        message = json.dumps({"op": "subscribe", "args": args_to_send})
                        await self.websocket.send(message)
                        # logger.info(f"Sent subscribe message for: {args_to_send}")
                    except websockets.ConnectionClosed:
                        logger.warning(f"WebSocket closed, cannot subscribe to {args_to_send}. Will resubscribe on reconnect.")
                        ERRORS_TOTAL.labels(component='websocket_client', error_type='subscribe_connection_closed').inc()
                    except Exception as e:
                        logger.error(f"Error sending subscribe message for {args_to_send}: {e}", exc_info=True)
                        ERRORS_TOTAL.labels(component='websocket_client', error_type='subscribe_error').inc()
                else:
                    logger.warning(f"WebSocket not connected, cannot subscribe to {args_to_send}. Will resubscribe on connect.")

    async def unsubscribe(self, topic: str, symbols: List[str]):
        """Отписывается от указанных тем для символов."""
        async with self.lock:
            args_to_send = []
            for symbol in symbols:
                key = f"{symbol}_{topic}"
                if self._subscription_counts[key] > 0:
                    self._subscription_counts[key] -= 1
                    if self._subscription_counts[key] == 0: # Only unsubscribe if no more clients for this symbol+topic
                        if symbol in self.subscriptions[topic]:
                            self.subscriptions[topic].remove(symbol)
                        args_to_send.append(f"{topic}.{symbol}")
                        # logger.info(f"Decremented subscription count for {key}. Now {self._subscription_counts[key]}.")
                    else:
                        logger.debug(f"Still subscribed to {key}. Count: {self._subscription_counts[key]}.")
                else:
                    logger.warning(f"Attempted to unsubscribe from {key} but count was already 0.")
                    ERRORS_TOTAL.labels(component='websocket_client', error_type='unsubscribe_count_zero').inc()

            if args_to_send:
                if self.websocket and self.is_connected:
                    try:
                        message = json.dumps({"op": "unsubscribe", "args": args_to_send})
                        await self.websocket.send(message)
                        # logger.info(f"Sent unsubscribe message for: {args_to_send}")
                    except websockets.ConnectionClosed:
                        logger.warning(f"WebSocket closed, cannot unsubscribe from {args_to_send}.")
                        ERRORS_TOTAL.labels(component='websocket_client', error_type='unsubscribe_connection_closed').inc()
                    except Exception as e:
                        logger.error(f"Error sending unsubscribe message for {args_to_send}: {e}", exc_info=True)
                        ERRORS_TOTAL.labels(component='websocket_client', error_type='unsubscribe_error').inc()
                else:
                    logger.warning(f"WebSocket not connected, cannot unsubscribe from {args_to_send}.")

    async def _resubscribe_all(self):
        """Переподписывается на все ранее активные темы."""
        async with self.lock:
            all_args = []
            for topic, symbols_list in self.subscriptions.items():
                for symbol in symbols_list:
                    # Only resubscribe if count is > 0 (meaning there's still an active local client)
                    if self._subscription_counts[f"{symbol}_{topic}"] > 0:
                        all_args.append(f"{topic}.{symbol}")
            
            if all_args:
                if self.websocket and self.is_connected:
                    try:
                        message = json.dumps({"op": "subscribe", "args": all_args})
                        await self.websocket.send(message)
                        # logger.info(f"Resubscribed to all active topics: {all_args}")
                    except websockets.ConnectionClosed:
                        logger.warning(f"WebSocket closed, cannot resubscribe to {all_args}.")
                        ERRORS_TOTAL.labels(component='websocket_client', error_type='resubscribe_connection_closed').inc()
                    except Exception as e:
                        logger.error(f"Error sending resubscribe message for {all_args}: {e}", exc_info=True)
                        ERRORS_TOTAL.labels(component='websocket_client', error_type='resubscribe_error').inc()
                else:
                    logger.warning(f"WebSocket not connected, cannot resubscribe to {all_args}.")
                    
    async def subscribe_to_symbol(self, symbol: str):
        """Подписывается на торговые данные и стакан ордеров для символа."""
        from utils.symbol_filter import is_symbol_allowed
        
        # Проверяем, разрешен ли символ
        if not is_symbol_allowed(symbol):
            logger.debug(f"Skipping subscription for excluded symbol: {symbol}")
            return
            
        topics = ["publicTrade", "orderbook.1"]
        await self.subscribe("publicTrade", [symbol])
        await self.subscribe("orderbook.1", [symbol])
        # logger.info(f"Subscribed to topics for symbol: {symbol}")
        
    async def unsubscribe_from_symbol(self, symbol: str):
        """Отписывается от торговых данных и стакана ордеров для символа."""
        topics = ["publicTrade", "orderbook.1"]
        await self.unsubscribe("publicTrade", [symbol])
        await self.unsubscribe("orderbook.1", [symbol])
        # logger.info(f"Unsubscribed from topics for symbol: {symbol}")


