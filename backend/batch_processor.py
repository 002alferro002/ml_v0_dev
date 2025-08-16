import asyncio
import logging
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
import json

logger = logging.getLogger(__name__)

@dataclass
class BatchConfig:
    """Конфигурация для пакетной обработки."""
    batch_size: int = 100  # Размер пакета
    batch_timeout_ms: int = 1000  # Таймаут пакета в миллисекундах
    max_workers: int = 4  # Максимальное количество потоков
    queue_max_size: int = 10000  # Максимальный размер очереди
    enable_metrics: bool = True  # Включить метрики

class BatchProcessor:
    """Пакетный многопоточный процессор для обработки данных."""
    
    def __init__(self, config: BatchConfig = None):
        self.config = config or BatchConfig()
        self.is_running = False
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        
        # Очереди для разных типов данных
        self.trade_queue = deque(maxlen=self.config.queue_max_size)
        self.orderbook_queue = deque(maxlen=self.config.queue_max_size)
        self.alert_queue = deque(maxlen=self.config.queue_max_size)
        
        # Обработчики для разных типов данных
        self.trade_handlers: List[Callable] = []
        self.orderbook_handlers: List[Callable] = []
        self.alert_handlers: List[Callable] = []
        
        # Метрики
        self.metrics = {
            'trades_processed': 0,
            'orderbooks_processed': 0,
            'alerts_processed': 0,
            'batches_processed': 0,
            'processing_errors': 0,
            'last_batch_time': 0,
            'avg_batch_size': 0,
            'total_processing_time': 0
        }
        
        # Блокировки для потокобезопасности
        self.trade_lock = threading.Lock()
        self.orderbook_lock = threading.Lock()
        self.alert_lock = threading.Lock()
        self.metrics_lock = threading.Lock()
        
        # Задачи обработки
        self.processing_tasks = []
        
        logger.info(f"BatchProcessor инициализирован с конфигурацией: {self.config}")
    
    def add_trade_handler(self, handler: Callable[[List[Dict[str, Any]]], None]):
        """Добавляет обработчик для пакетов торговых данных."""
        self.trade_handlers.append(handler)
        logger.info(f"Добавлен обработчик торговых данных: {handler.__name__}")
    
    def add_orderbook_handler(self, handler: Callable[[List[Dict[str, Any]]], None]):
        """Добавляет обработчик для пакетов данных стакана ордеров."""
        self.orderbook_handlers.append(handler)
        logger.info(f"Добавлен обработчик данных стакана: {handler.__name__}")
    
    def add_alert_handler(self, handler: Callable[[List[Dict[str, Any]]], None]):
        """Добавляет обработчик для пакетов алертов."""
        self.alert_handlers.append(handler)
        logger.info(f"Добавлен обработчик алертов: {handler.__name__}")
    
    async def add_trade(self, trade_data: Dict[str, Any]):
        """Добавляет торговые данные в очередь для пакетной обработки."""
        with self.trade_lock:
            self.trade_queue.append({
                'data': trade_data,
                'timestamp': time.time(),
                'type': 'trade'
            })
    
    async def add_orderbook(self, orderbook_data: Dict[str, Any]):
        """Добавляет данные стакана ордеров в очередь для пакетной обработки."""
        with self.orderbook_lock:
            self.orderbook_queue.append({
                'data': orderbook_data,
                'timestamp': time.time(),
                'type': 'orderbook'
            })
    
    async def add_alert(self, alert_data: Dict[str, Any]):
        """Добавляет алерт в очередь для пакетной обработки."""
        with self.alert_lock:
            self.alert_queue.append({
                'data': alert_data,
                'timestamp': time.time(),
                'type': 'alert'
            })
    
    async def start(self):
        """Запускает пакетную обработку."""
        if self.is_running:
            logger.warning("BatchProcessor уже запущен")
            return
        
        self.is_running = True
        logger.info("Запуск BatchProcessor...")
        
        # Запускаем задачи обработки для каждого типа данных
        self.processing_tasks = [
            asyncio.create_task(self._process_trade_batches()),
            asyncio.create_task(self._process_orderbook_batches()),
            asyncio.create_task(self._process_alert_batches()),
            asyncio.create_task(self._metrics_reporter())
        ]
        
        logger.info("BatchProcessor запущен успешно")
    
    async def stop(self):
        """Останавливает пакетную обработку."""
        if not self.is_running:
            logger.warning("BatchProcessor уже остановлен")
            return
        
        logger.info("Остановка BatchProcessor...")
        self.is_running = False
        
        # Отменяем все задачи
        for task in self.processing_tasks:
            task.cancel()
        
        # Ждем завершения задач
        await asyncio.gather(*self.processing_tasks, return_exceptions=True)
        
        # Закрываем пул потоков
        self.executor.shutdown(wait=True)
        
        logger.info("BatchProcessor остановлен")
    
    async def _process_trade_batches(self):
        """Обрабатывает пакеты торговых данных."""
        logger.info("Запущена обработка пакетов торговых данных")
        
        while self.is_running:
            try:
                batch = await self._collect_batch(self.trade_queue, self.trade_lock)
                if batch:
                    await self._process_batch_async(batch, self.trade_handlers, 'trades')
                else:
                    await asyncio.sleep(0.1)  # Небольшая пауза если нет данных
            except Exception as e:
                logger.error(f"Ошибка при обработке пакета торговых данных: {e}", exc_info=True)
                self._increment_error_count()
                await asyncio.sleep(1)  # Пауза при ошибке
    
    async def _process_orderbook_batches(self):
        """Обрабатывает пакеты данных стакана ордеров."""
        logger.info("Запущена обработка пакетов данных стакана ордеров")
        
        while self.is_running:
            try:
                batch = await self._collect_batch(self.orderbook_queue, self.orderbook_lock)
                if batch:
                    await self._process_batch_async(batch, self.orderbook_handlers, 'orderbooks')
                else:
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Ошибка при обработке пакета данных стакана: {e}", exc_info=True)
                self._increment_error_count()
                await asyncio.sleep(1)
    
    async def _process_alert_batches(self):
        """Обрабатывает пакеты алертов."""
        logger.info("Запущена обработка пакетов алертов")
        
        while self.is_running:
            try:
                batch = await self._collect_batch(self.alert_queue, self.alert_lock)
                if batch:
                    await self._process_batch_async(batch, self.alert_handlers, 'alerts')
                else:
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Ошибка при обработке пакета алертов: {e}", exc_info=True)
                self._increment_error_count()
                await asyncio.sleep(1)
    
    async def _collect_batch(self, queue: deque, lock: threading.Lock) -> List[Dict[str, Any]]:
        """Собирает пакет данных из очереди."""
        batch = []
        current_time = time.time()
        oldest_timestamp = None
        
        with lock:
            while len(batch) < self.config.batch_size and queue:
                item = queue.popleft()
                batch.append(item)
                
                if oldest_timestamp is None:
                    oldest_timestamp = item['timestamp']
        
        # Проверяем таймаут пакета
        if batch and oldest_timestamp:
            time_diff_ms = (current_time - oldest_timestamp) * 1000
            if time_diff_ms < self.config.batch_timeout_ms and len(batch) < self.config.batch_size:
                # Возвращаем элементы обратно в очередь если таймаут не истек
                with lock:
                    for item in reversed(batch):
                        queue.appendleft(item)
                return []
        
        return batch
    
    async def _process_batch_async(self, batch: List[Dict[str, Any]], handlers: List[Callable], data_type: str):
        """Асинхронно обрабатывает пакет данных."""
        if not batch or not handlers:
            return
        
        start_time = time.time()
        
        try:
            # Извлекаем только данные из пакета
            batch_data = [item['data'] for item in batch]
            
            # Обрабатываем пакет в отдельном потоке
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self.executor,
                self._process_batch_sync,
                batch_data,
                handlers,
                data_type
            )
            
            # Обновляем метрики
            processing_time = time.time() - start_time
            self._update_metrics(data_type, len(batch), processing_time)
            
            logger.debug(f"Обработан пакет {data_type}: {len(batch)} элементов за {processing_time:.3f}с")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке пакета {data_type}: {e}", exc_info=True)
            self._increment_error_count()
    
    def _process_batch_sync(self, batch_data: List[Dict[str, Any]], handlers: List[Callable], data_type: str):
        """Синхронно обрабатывает пакет данных в отдельном потоке."""
        for handler in handlers:
            try:
                handler(batch_data)
            except Exception as e:
                logger.error(f"Ошибка в обработчике {handler.__name__} для {data_type}: {e}", exc_info=True)
    
    def _update_metrics(self, data_type: str, batch_size: int, processing_time: float):
        """Обновляет метрики обработки."""
        with self.metrics_lock:
            if data_type == 'trades':
                self.metrics['trades_processed'] += batch_size
            elif data_type == 'orderbooks':
                self.metrics['orderbooks_processed'] += batch_size
            elif data_type == 'alerts':
                self.metrics['alerts_processed'] += batch_size
            
            self.metrics['batches_processed'] += 1
            self.metrics['last_batch_time'] = time.time()
            self.metrics['total_processing_time'] += processing_time
            
            # Вычисляем средний размер пакета
            total_items = (self.metrics['trades_processed'] + 
                          self.metrics['orderbooks_processed'] + 
                          self.metrics['alerts_processed'])
            if self.metrics['batches_processed'] > 0:
                self.metrics['avg_batch_size'] = total_items / self.metrics['batches_processed']
    
    def _increment_error_count(self):
        """Увеличивает счетчик ошибок."""
        with self.metrics_lock:
            self.metrics['processing_errors'] += 1
    
    async def _metrics_reporter(self):
        """Периодически выводит метрики обработки."""
        logger.info("Запущен репортер метрик")
        
        while self.is_running:
            try:
                await asyncio.sleep(30)  # Выводим метрики каждые 30 секунд
                
                if self.config.enable_metrics:
                    with self.metrics_lock:
                        metrics_copy = self.metrics.copy()
                    
                    logger.info(f"Метрики BatchProcessor: "
                              f"Торговые данные: {metrics_copy['trades_processed']}, "
                              f"Стакан ордеров: {metrics_copy['orderbooks_processed']}, "
                              f"Алерты: {metrics_copy['alerts_processed']}, "
                              f"Пакеты: {metrics_copy['batches_processed']}, "
                              f"Ошибки: {metrics_copy['processing_errors']}, "
                              f"Средний размер пакета: {metrics_copy['avg_batch_size']:.1f}")
                    
            except Exception as e:
                logger.error(f"Ошибка в репортере метрик: {e}", exc_info=True)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Возвращает текущие метрики."""
        with self.metrics_lock:
            return self.metrics.copy()
    
    def get_queue_sizes(self) -> Dict[str, int]:
        """Возвращает размеры очередей."""
        return {
            'trade_queue': len(self.trade_queue),
            'orderbook_queue': len(self.orderbook_queue),
            'alert_queue': len(self.alert_queue)
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Возвращает статус процессора."""
        return {
            'is_running': self.is_running,
            'config': {
                'batch_size': self.config.batch_size,
                'batch_timeout_ms': self.config.batch_timeout_ms,
                'max_workers': self.config.max_workers,
                'queue_max_size': self.config.queue_max_size
            },
            'queue_sizes': self.get_queue_sizes(),
            'metrics': self.get_metrics(),
            'handlers_count': {
                'trade_handlers': len(self.trade_handlers),
                'orderbook_handlers': len(self.orderbook_handlers),
                'alert_handlers': len(self.alert_handlers)
            }
        }