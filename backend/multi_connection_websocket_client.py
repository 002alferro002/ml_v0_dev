import asyncio
import json
import logging
from typing import Dict, List, Set, Callable, Optional
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
from config_manager import ConfigManager
from metrics import (
    ACTIVE_WS_CONNECTIONS, WS_MESSAGES_RECEIVED_TOTAL, 
    ERRORS_TOTAL
)

logger = logging.getLogger(__name__)

class MultiConnectionBybitWebSocketClient:
    """
    WebSocket клиент для Bybit с поддержкой множественных соединений
    для распределения нагрузки подписок.
    """
    
    def __init__(self, message_publisher: Callable, max_subscriptions_per_connection: int = 200):
        self.config = ConfigManager()
        self.message_publisher = message_publisher
        self.max_subscriptions_per_connection = max_subscriptions_per_connection
        
        # Словарь соединений: connection_id -> connection_info
        self.connections: Dict[int, dict] = {}
        self.next_connection_id = 0
        
        # Отслеживание подписок: symbol -> connection_id
        self.symbol_to_connection: Dict[str, int] = {}
        
        # Общие настройки
        self.is_running = False
        self.reconnect_tasks: Dict[int, asyncio.Task] = {}
        
    async def start(self):
        """Запускает клиент с множественными соединениями."""
        if self.is_running:
            logger.warning("Multi-connection WebSocket client уже запущен")
            return
            
        self.is_running = True
        logger.info("Запуск Multi-connection WebSocket клиента")
        
        # Создаем первое соединение
        await self._create_connection()
        
    async def stop(self):
        """Останавливает все соединения."""
        self.is_running = False
        logger.info("Остановка Multi-connection WebSocket клиента")
        
        # Останавливаем все задачи переподключения
        for task in self.reconnect_tasks.values():
            if not task.done():
                task.cancel()
        
        # Останавливаем все задачи прослушивания
        for conn_info in self.connections.values():
            if conn_info['listen_task'] and not conn_info['listen_task'].done():
                conn_info['listen_task'].cancel()
        
        # Закрываем все соединения
        for conn_info in self.connections.values():
            if conn_info['websocket'] and conn_info['websocket'].close_code is None:
                await conn_info['websocket'].close()
                
        self.connections.clear()
        self.symbol_to_connection.clear()
        ACTIVE_WS_CONNECTIONS.set(0)
        
    async def _create_connection(self) -> int:
        """Создает новое WebSocket соединение."""
        connection_id = self.next_connection_id
        self.next_connection_id += 1
        
        connection_info = {
            'websocket': None,
            'is_connected': False,
            'subscriptions': set(),
            'ping_task': None,
            'listen_task': None,
            'reconnect_attempts': 0
        }
        
        self.connections[connection_id] = connection_info
        
        # Подключаемся
        await self._connect_single(connection_id)
        
        return connection_id
        
    async def _connect_single(self, connection_id: int):
        """Подключает одно WebSocket соединение."""
        conn_info = self.connections[connection_id]
        
        try:
            uri = self.config.get('BYBIT_WS_URI', 'wss://stream.bybit.com/v5/public/linear')
            
            # Увеличиваем таймауты для стабильности
            conn_info['websocket'] = await websockets.connect(
                uri,
                ping_interval=25,  # Bybit рекомендует ping каждые 20 секунд
                ping_timeout=10,
                close_timeout=10
            )
            
            conn_info['is_connected'] = True
            conn_info['reconnect_attempts'] = 0
            
            logger.info(f"Соединение {connection_id} подключено к Bybit WebSocket: {uri}")
            
            # Запускаем задачи для этого соединения
            conn_info['listen_task'] = asyncio.create_task(
                self._listen_for_messages(connection_id)
            )
            # Убираем ручной ping - используем встроенный механизм websockets
            conn_info['ping_task'] = None
            
            # Переподписываемся на все символы этого соединения
            await self._resubscribe_connection(connection_id)
            
            # Обновляем метрики
            ACTIVE_WS_CONNECTIONS.set(len([c for c in self.connections.values() if c['is_connected']]))
            
        except Exception as e:
            logger.error(f"Ошибка подключения соединения {connection_id}: {e}")
            conn_info['is_connected'] = False
            ERRORS_TOTAL.labels(component='multi_websocket_client', error_type='connection_error').inc()
            self._schedule_reconnect(connection_id)
            
    async def _listen_for_messages(self, connection_id: int):
        """Слушает сообщения от одного WebSocket соединения."""
        conn_info = self.connections[connection_id]
        
        while conn_info['is_connected'] and self.is_running:
            try:
                message = await conn_info['websocket'].recv()
                data = json.loads(message)
                
                topic = data.get('topic', 'unknown')
                WS_MESSAGES_RECEIVED_TOTAL.labels(topic=topic).inc()
                
                if 'op' in data and data['op'] == 'pong':
                    logger.debug(f"Получен pong от соединения {connection_id}")
                elif 'success' in data and data['success'] is False:
                    logger.error(f"Ошибка Bybit WS (соединение {connection_id}): {data.get('ret_msg', 'Unknown error')}")
                    ERRORS_TOTAL.labels(component='multi_websocket_client', error_type='bybit_api_error').inc()
                else:
                    # Публикуем сообщение
                    await self.message_publisher(data)
                    
            except ConnectionClosedOK:
                logger.info(f"Соединение {connection_id} закрыто корректно")
                break
            except ConnectionClosedError as e:
                if "keepalive ping timeout" in str(e):
                    logger.warning(f"Соединение {connection_id} закрыто из-за таймаута ping: {e}")
                else:
                    logger.error(f"Соединение {connection_id} закрыто с ошибкой: {e}")
                ERRORS_TOTAL.labels(component='multi_websocket_client', error_type='connection_closed_error').inc()
                break
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка JSON в соединении {connection_id}: {e}")
                ERRORS_TOTAL.labels(component='multi_websocket_client', error_type='json_decode_error').inc()
            except Exception as e:
                logger.error(f"Ошибка в соединении {connection_id}: {e}", exc_info=True)
                ERRORS_TOTAL.labels(component='multi_websocket_client', error_type='message_receive_error').inc()
                break
                
        # Соединение закрыто
        conn_info['is_connected'] = False
        if self.is_running:
            self._schedule_reconnect(connection_id)
            
    async def _periodic_ping(self, connection_id: int):
        """Отправляет ping для одного соединения."""
        # Метод оставлен для совместимости, но не используется
        # Ping обрабатывается встроенным механизмом websockets
        pass
                
    def _schedule_reconnect(self, connection_id: int):
        """Планирует переподключение для конкретного соединения."""
        if connection_id in self.reconnect_tasks and not self.reconnect_tasks[connection_id].done():
            return
            
        async def reconnect_attempt():
            conn_info = self.connections[connection_id]
            delay = min(2 ** conn_info['reconnect_attempts'], 60)  # Максимум 60 секунд
            
            while self.is_running and conn_info['reconnect_attempts'] < 10:
                conn_info['reconnect_attempts'] += 1
                logger.info(f"Попытка переподключения #{conn_info['reconnect_attempts']} для соединения {connection_id} через {delay} секунд")
                
                await asyncio.sleep(delay)
                
                if not self.is_running:
                    break
                    
                await self._connect_single(connection_id)
                
                if conn_info['is_connected']:
                    logger.info(f"Соединение {connection_id} успешно переподключено")
                    break
                    
                delay = min(delay * 2, 60)
                
        self.reconnect_tasks[connection_id] = asyncio.create_task(reconnect_attempt())
        
    async def _resubscribe_connection(self, connection_id: int):
        """Переподписывается на все символы для конкретного соединения."""
        conn_info = self.connections[connection_id]
        
        if not conn_info['is_connected'] or not conn_info['subscriptions']:
            return
            
        # Группируем подписки по топикам
        topics_to_subscribe = {}
        for subscription in conn_info['subscriptions']:
            topic, symbol = subscription.split(':')
            if topic not in topics_to_subscribe:
                topics_to_subscribe[topic] = []
            topics_to_subscribe[topic].append(symbol)
            
        # Подписываемся батчами
        for topic, symbols in topics_to_subscribe.items():
            for i in range(0, len(symbols), 10):  # Батчи по 10 символов
                batch = symbols[i:i+10]
                topics_list = [f"{topic}.{symbol}" for symbol in batch]
                
                subscribe_msg = {
                    "op": "subscribe",
                    "args": topics_list
                }
                
                try:
                    await conn_info['websocket'].send(json.dumps(subscribe_msg))
                    logger.debug(f"Переподписка соединения {connection_id} на {len(batch)} символов для {topic}")
                    await asyncio.sleep(0.1)  # Небольшая задержка между батчами
                except Exception as e:
                    logger.error(f"Ошибка переподписки соединения {connection_id}: {e}")
                    
    async def subscribe_to_symbol(self, symbol: str):
        """Подписывается на символ, выбирая подходящее соединение."""
        from utils.symbol_filter import is_symbol_allowed
        
        if not self.is_running:
            logger.warning(f"Клиент не запущен, пропускаем подписку на {symbol}")
            return
            
        # Проверяем, разрешен ли символ
        if not is_symbol_allowed(symbol):
            logger.debug(f"Skipping subscription for excluded symbol: {symbol}")
            return
            
        # Находим соединение с наименьшим количеством подписок
        target_connection_id = self._find_best_connection()
        
        if target_connection_id is None:
            # Создаем новое соединение
            target_connection_id = await self._create_connection()
            
        conn_info = self.connections[target_connection_id]
        
        if not conn_info['is_connected']:
            logger.warning(f"Соединение {target_connection_id} не подключено, откладываем подписку на {symbol}")
            return
            
        # Подписываемся на оба топика
        topics = [f"publicTrade.{symbol}", f"orderbook.1.{symbol}"]
        
        subscribe_msg = {
            "op": "subscribe",
            "args": topics
        }
        
        try:
            await conn_info['websocket'].send(json.dumps(subscribe_msg))
            
            # Обновляем отслеживание
            self.symbol_to_connection[symbol] = target_connection_id
            for topic in ["publicTrade", "orderbook.1"]:
                conn_info['subscriptions'].add(f"{topic}:{symbol}")
                
            logger.debug(f"Подписались на {symbol} через соединение {target_connection_id}")
            
        except Exception as e:
            logger.error(f"Ошибка подписки на {symbol}: {e}")
            ERRORS_TOTAL.labels(component='multi_websocket_client', error_type='subscription_error').inc()
            
    async def unsubscribe_from_symbol(self, symbol: str):
        """Отписывается от символа."""
        if symbol not in self.symbol_to_connection:
            logger.warning(f"Символ {symbol} не найден в подписках")
            return
            
        connection_id = self.symbol_to_connection[symbol]
        conn_info = self.connections.get(connection_id)
        
        if not conn_info or not conn_info['is_connected']:
            logger.warning(f"Соединение {connection_id} недоступно для отписки от {symbol}")
            return
            
        topics = [f"publicTrade.{symbol}", f"orderbook.1.{symbol}"]
        
        unsubscribe_msg = {
            "op": "unsubscribe",
            "args": topics
        }
        
        try:
            await conn_info['websocket'].send(json.dumps(unsubscribe_msg))
            
            # Обновляем отслеживание
            del self.symbol_to_connection[symbol]
            for topic in ["publicTrade", "orderbook.1"]:
                conn_info['subscriptions'].discard(f"{topic}:{symbol}")
                
            logger.debug(f"Отписались от {symbol} через соединение {connection_id}")
            
        except Exception as e:
            logger.error(f"Ошибка отписки от {symbol}: {e}")
            ERRORS_TOTAL.labels(component='multi_websocket_client', error_type='unsubscription_error').inc()
            
    def _find_best_connection(self) -> Optional[int]:
        """Находит соединение с наименьшим количеством подписок."""
        best_connection_id = None
        min_subscriptions = float('inf')
        
        for conn_id, conn_info in self.connections.items():
            if conn_info['is_connected'] and len(conn_info['subscriptions']) < self.max_subscriptions_per_connection:
                if len(conn_info['subscriptions']) < min_subscriptions:
                    min_subscriptions = len(conn_info['subscriptions'])
                    best_connection_id = conn_id
                    
        return best_connection_id
        
    async def subscribe_to_symbols(self, symbols: List[str]):
        """Подписывается на список символов."""
        from utils.symbol_filter import filter_symbols
        
        # Фильтруем символы перед подпиской
        filtered_symbols = filter_symbols(symbols)
        
        if len(filtered_symbols) != len(symbols):
            logger.info(f"Filtered symbols for WebSocket subscription: {len(symbols)} -> {len(filtered_symbols)}")
        
        logger.info(f"Подписка на {len(filtered_symbols)} символов")
        for symbol in filtered_symbols:
            await self.subscribe_to_symbol(symbol)
            await asyncio.sleep(0.01)  # Небольшая задержка между подписками
            
    async def unsubscribe_from_symbols(self, symbols: List[str]):
        """Отписывается от списка символов (старый метод - посимвольно)."""
        logger.info(f"Отписка от {len(symbols)} символов (посимвольно)")
        for symbol in symbols:
            await self.unsubscribe_from_symbol(symbol)
            await asyncio.sleep(0.01)  # Небольшая задержка между отписками
    
    async def unsubscribe_from_symbols_batch(self, symbols: List[str], batch_size: int = 50):
        """Пакетная отписка от символов (оптимизированный метод)."""
        from collections import defaultdict
        
        logger.info(f"Пакетная отписка от {len(symbols)} символов (батчи по {batch_size})")
        
        if not symbols:
            return
        
        # Группируем символы по соединениям
        symbols_by_connection = defaultdict(list)
        symbols_not_found = []
        
        for symbol in symbols:
            if symbol in self.symbol_to_connection:
                conn_id = self.symbol_to_connection[symbol]
                symbols_by_connection[conn_id].append(symbol)
            else:
                symbols_not_found.append(symbol)
        
        if symbols_not_found:
            logger.warning(f"Символы не найдены в подписках: {symbols_not_found}")
        
        # Отписываемся пакетами для каждого соединения
        total_unsubscribed = 0
        for conn_id, conn_symbols in symbols_by_connection.items():
            conn_info = self.connections.get(conn_id)
            if not conn_info or not conn_info['is_connected']:
                logger.warning(f"Соединение {conn_id} недоступно для отписки")
                continue
            
            # Разбиваем на батчи
            for i in range(0, len(conn_symbols), batch_size):
                batch = conn_symbols[i:i + batch_size]
                topics = []
                
                for symbol in batch:
                    topics.extend([f"publicTrade.{symbol}", f"orderbook.1.{symbol}"])
                    # Обновляем отслеживание
                    if symbol in self.symbol_to_connection:
                        del self.symbol_to_connection[symbol]
                    conn_info['subscriptions'].discard(f"publicTrade:{symbol}")
                    conn_info['subscriptions'].discard(f"orderbook.1:{symbol}")
                
                # Отправляем пакетный запрос отписки
                unsubscribe_msg = {
                    "op": "unsubscribe",
                    "args": topics
                }
                
                try:
                    await conn_info['websocket'].send(json.dumps(unsubscribe_msg))
                    total_unsubscribed += len(batch)
                    logger.debug(f"Отписались от {len(batch)} символов через соединение {conn_id}")
                    
                    # Небольшая задержка между батчами
                    if i + batch_size < len(conn_symbols):
                        await asyncio.sleep(0.1)
                        
                except Exception as e:
                    logger.error(f"Ошибка пакетной отписки от {len(batch)} символов: {e}")
                    ERRORS_TOTAL.labels(component='multi_websocket_client', error_type='batch_unsubscription_error').inc()
        
        logger.info(f"Пакетная отписка завершена: {total_unsubscribed}/{len(symbols)} символов")
    
    def get_connection_stats(self) -> dict:
        """Возвращает статистику соединений."""
        stats = {
            'total_connections': len(self.connections),
            'active_connections': len([c for c in self.connections.values() if c['is_connected']]),
            'total_subscriptions': sum(len(c['subscriptions']) for c in self.connections.values()),
            'connections_detail': []
        }
        
        for conn_id, conn_info in self.connections.items():
            stats['connections_detail'].append({
                'connection_id': conn_id,
                'is_connected': conn_info['is_connected'],
                'subscriptions_count': len(conn_info['subscriptions']),
                'reconnect_attempts': conn_info['reconnect_attempts']
            })
            
        return stats