import asyncio
import logging
from typing import Dict, Any, List, Optional
from batch_processor import BatchProcessor, BatchConfig
from manipulation.trade_processor import TradeProcessor
from manipulation.orderbook_analyzer import OrderBookAnalyzer
from manipulation.alert_manager import AlertManager
from db_manager import DBManager
from message_broker import MessageBroker

logger = logging.getLogger(__name__)

class BatchIntegration:
    """Интеграция пакетной обработки с существующей системой."""
    
    def __init__(self, 
                 trade_processor: TradeProcessor,
                 orderbook_analyzer: OrderBookAnalyzer,
                 alert_manager: AlertManager,
                 db_manager: DBManager,
                 message_broker: MessageBroker,
                 batch_config: BatchConfig = None):
        
        self.trade_processor = trade_processor
        self.orderbook_analyzer = orderbook_analyzer
        self.alert_manager = alert_manager
        self.db_manager = db_manager
        self.message_broker = message_broker
        
        # Создаем BatchProcessor с конфигурацией
        self.batch_processor = BatchProcessor(batch_config or BatchConfig())
        
        # Регистрируем обработчики
        self._register_handlers()
        
        logger.info("BatchIntegration инициализирован")
    
    def _register_handlers(self):
        """Регистрирует обработчики для разных типов данных."""
        
        # Обработчики торговых данных
        self.batch_processor.add_trade_handler(self._batch_trade_handler)
        self.batch_processor.add_trade_handler(self._batch_trade_db_handler)
        self.batch_processor.add_trade_handler(self._batch_trade_analysis_handler)
        
        # Обработчики данных стакана ордеров
        self.batch_processor.add_orderbook_handler(self._batch_orderbook_handler)
        self.batch_processor.add_orderbook_handler(self._batch_orderbook_db_handler)
        self.batch_processor.add_orderbook_handler(self._batch_orderbook_analysis_handler)
        
        # Обработчики алертов
        self.batch_processor.add_alert_handler(self._batch_alert_handler)
        self.batch_processor.add_alert_handler(self._batch_alert_db_handler)
        
        logger.info("Обработчики зарегистрированы")
    
    def _batch_trade_handler(self, trades: List[Dict[str, Any]]):
        """Пакетная обработка торговых данных."""
        try:
            logger.debug(f"Обработка пакета из {len(trades)} торговых сделок")
            
            # Группируем сделки по символам для более эффективной обработки
            trades_by_symbol = {}
            for trade in trades:
                symbol = trade.get('symbol', 'UNKNOWN')
                if symbol not in trades_by_symbol:
                    trades_by_symbol[symbol] = []
                trades_by_symbol[symbol].append(trade)
            
            # Обрабатываем каждый символ
            for symbol, symbol_trades in trades_by_symbol.items():
                self._process_symbol_trades(symbol, symbol_trades)
                
        except Exception as e:
            logger.error(f"Ошибка в пакетной обработке торговых данных: {e}", exc_info=True)
    
    def _process_symbol_trades(self, symbol: str, trades: List[Dict[str, Any]]):
        """Обрабатывает торговые данные для конкретного символа."""
        try:
            # Вычисляем агрегированные метрики
            total_volume = sum(trade.get('volume_usdt', 0) for trade in trades)
            buy_volume = sum(trade.get('volume_usdt', 0) for trade in trades if trade.get('side') == 'Buy')
            sell_volume = sum(trade.get('volume_usdt', 0) for trade in trades if trade.get('side') == 'Sell')
            
            avg_price = sum(trade.get('price', 0) for trade in trades) / len(trades) if trades else 0
            min_price = min(trade.get('price', 0) for trade in trades) if trades else 0
            max_price = max(trade.get('price', 0) for trade in trades) if trades else 0
            
            # Логируем агрегированную информацию
            logger.debug(f"Символ {symbol}: {len(trades)} сделок, "
                        f"Объем: {total_volume:.2f} USDT, "
                        f"Покупки: {buy_volume:.2f}, Продажи: {sell_volume:.2f}, "
                        f"Цена: мин={min_price:.4f}, макс={max_price:.4f}, сред={avg_price:.4f}")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке торговых данных для {symbol}: {e}", exc_info=True)
    
    def _batch_trade_db_handler(self, trades: List[Dict[str, Any]]):
        """Пакетная запись торговых данных в базу данных."""
        try:
            logger.debug(f"Пакетная запись {len(trades)} торговых сделок в БД")
            
            # Здесь можно реализовать пакетную вставку в БД
            # Пока используем существующий метод для каждой сделки
            for trade in trades:
                # Синхронный вызов для записи в БД
                # В реальной реализации лучше использовать пакетную вставку
                pass
                
        except Exception as e:
            logger.error(f"Ошибка при пакетной записи торговых данных в БД: {e}", exc_info=True)
    
    def _batch_trade_analysis_handler(self, trades: List[Dict[str, Any]]):
        """Пакетный анализ торговых данных на предмет манипуляций."""
        try:
            logger.debug(f"Пакетный анализ {len(trades)} торговых сделок")
            
            # Группируем по символам для анализа
            trades_by_symbol = {}
            for trade in trades:
                symbol = trade.get('symbol', 'UNKNOWN')
                if symbol not in trades_by_symbol:
                    trades_by_symbol[symbol] = []
                trades_by_symbol[symbol].append(trade)
            
            # Анализируем каждый символ на предмет манипуляций
            for symbol, symbol_trades in trades_by_symbol.items():
                self._analyze_symbol_manipulation(symbol, symbol_trades)
                
        except Exception as e:
            logger.error(f"Ошибка при пакетном анализе торговых данных: {e}", exc_info=True)
    
    def _analyze_symbol_manipulation(self, symbol: str, trades: List[Dict[str, Any]]):
        """Анализирует торговые данные символа на предмет манипуляций."""
        try:
            if len(trades) < 2:
                return
            
            # Анализ на wash trading
            self._detect_batch_wash_trading(symbol, trades)
            
            # Анализ на ping-pong trading
            self._detect_batch_ping_pong(symbol, trades)
            
            # Анализ на ramping
            self._detect_batch_ramping(symbol, trades)
            
        except Exception as e:
            logger.error(f"Ошибка при анализе манипуляций для {symbol}: {e}", exc_info=True)
    
    def _detect_batch_wash_trading(self, symbol: str, trades: List[Dict[str, Any]]):
        """Пакетное детектирование wash trading."""
        try:
            # Простая эвристика: если много сделок с одинаковыми объемами
            volumes = [trade.get('volume_usdt', 0) for trade in trades]
            unique_volumes = set(volumes)
            
            if len(unique_volumes) < len(volumes) * 0.3:  # Менее 30% уникальных объемов
                logger.warning(f"Возможный wash trading для {symbol}: "
                             f"{len(trades)} сделок, {len(unique_volumes)} уникальных объемов")
                
        except Exception as e:
            logger.error(f"Ошибка при детектировании wash trading для {symbol}: {e}", exc_info=True)
    
    def _detect_batch_ping_pong(self, symbol: str, trades: List[Dict[str, Any]]):
        """Пакетное детектирование ping-pong trading."""
        try:
            # Анализируем последовательность покупок и продаж
            sides = [trade.get('side') for trade in trades]
            
            # Подсчитываем переключения между Buy и Sell
            switches = 0
            for i in range(1, len(sides)):
                if sides[i] != sides[i-1]:
                    switches += 1
            
            switch_ratio = switches / len(sides) if len(sides) > 0 else 0
            
            if switch_ratio > 0.7:  # Более 70% переключений
                logger.warning(f"Возможный ping-pong trading для {symbol}: "
                             f"{switches} переключений из {len(sides)} сделок ({switch_ratio:.2%})")
                
        except Exception as e:
            logger.error(f"Ошибка при детектировании ping-pong trading для {symbol}: {e}", exc_info=True)
    
    def _detect_batch_ramping(self, symbol: str, trades: List[Dict[str, Any]]):
        """Пакетное детектирование ramping."""
        try:
            if len(trades) < 5:
                return
            
            # Анализируем тренд цен
            prices = [trade.get('price', 0) for trade in trades]
            
            # Проверяем на последовательный рост цен
            increasing_count = 0
            for i in range(1, len(prices)):
                if prices[i] > prices[i-1]:
                    increasing_count += 1
            
            increasing_ratio = increasing_count / (len(prices) - 1) if len(prices) > 1 else 0
            
            if increasing_ratio > 0.8:  # Более 80% роста
                price_change = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
                logger.warning(f"Возможный ramping для {symbol}: "
                             f"{increasing_count} из {len(prices)-1} увеличений цены, "
                             f"изменение: {price_change:.2%}")
                
        except Exception as e:
            logger.error(f"Ошибка при детектировании ramping для {symbol}: {e}", exc_info=True)
    
    def _batch_orderbook_handler(self, orderbooks: List[Dict[str, Any]]):
        """Пакетная обработка данных стакана ордеров."""
        try:
            logger.debug(f"Обработка пакета из {len(orderbooks)} обновлений стакана")
            
            # Группируем по символам
            orderbooks_by_symbol = {}
            for orderbook in orderbooks:
                symbol = orderbook.get('symbol', 'UNKNOWN')
                if symbol not in orderbooks_by_symbol:
                    orderbooks_by_symbol[symbol] = []
                orderbooks_by_symbol[symbol].append(orderbook)
            
            # Обрабатываем каждый символ
            for symbol, symbol_orderbooks in orderbooks_by_symbol.items():
                self._process_symbol_orderbooks(symbol, symbol_orderbooks)
                
        except Exception as e:
            logger.error(f"Ошибка в пакетной обработке данных стакана: {e}", exc_info=True)
    
    def _process_symbol_orderbooks(self, symbol: str, orderbooks: List[Dict[str, Any]]):
        """Обрабатывает данные стакана для конкретного символа."""
        try:
            # Берем последнее обновление стакана для символа
            latest_orderbook = orderbooks[-1]
            
            bids = latest_orderbook.get('bids', [])
            asks = latest_orderbook.get('asks', [])
            
            if bids and asks:
                best_bid = max(bids, key=lambda x: x[0]) if bids else [0, 0]
                best_ask = min(asks, key=lambda x: x[0]) if asks else [0, 0]
                spread = best_ask[0] - best_bid[0] if best_bid[0] > 0 and best_ask[0] > 0 else 0
                
                logger.debug(f"Символ {symbol}: {len(orderbooks)} обновлений стакана, "
                            f"Лучший бид: {best_bid[0]:.4f}, Лучший аск: {best_ask[0]:.4f}, "
                            f"Спред: {spread:.4f}")
                
        except Exception as e:
            logger.error(f"Ошибка при обработке данных стакана для {symbol}: {e}", exc_info=True)
    
    def _batch_orderbook_db_handler(self, orderbooks: List[Dict[str, Any]]):
        """Пакетная запись данных стакана в базу данных."""
        try:
            logger.debug(f"Пакетная запись {len(orderbooks)} обновлений стакана в БД")
            # Реализация пакетной записи в БД
            
        except Exception as e:
            logger.error(f"Ошибка при пакетной записи данных стакана в БД: {e}", exc_info=True)
    
    def _batch_orderbook_analysis_handler(self, orderbooks: List[Dict[str, Any]]):
        """Пакетный анализ данных стакана на предмет манипуляций."""
        try:
            logger.debug(f"Пакетный анализ {len(orderbooks)} обновлений стакана")
            
            # Группируем по символам для анализа
            orderbooks_by_symbol = {}
            for orderbook in orderbooks:
                symbol = orderbook.get('symbol', 'UNKNOWN')
                if symbol not in orderbooks_by_symbol:
                    orderbooks_by_symbol[symbol] = []
                orderbooks_by_symbol[symbol].append(orderbook)
            
            # Анализируем каждый символ
            for symbol, symbol_orderbooks in orderbooks_by_symbol.items():
                self._analyze_orderbook_manipulation(symbol, symbol_orderbooks)
                
        except Exception as e:
            logger.error(f"Ошибка при пакетном анализе данных стакана: {e}", exc_info=True)
    
    def _analyze_orderbook_manipulation(self, symbol: str, orderbooks: List[Dict[str, Any]]):
        """Анализирует данные стакана на предмет манипуляций."""
        try:
            if len(orderbooks) < 2:
                return
            
            # Анализ на spoofing (фиктивные ордера)
            self._detect_batch_spoofing(symbol, orderbooks)
            
            # Анализ на layering
            self._detect_batch_layering(symbol, orderbooks)
            
        except Exception as e:
            logger.error(f"Ошибка при анализе манипуляций стакана для {symbol}: {e}", exc_info=True)
    
    def _detect_batch_spoofing(self, symbol: str, orderbooks: List[Dict[str, Any]]):
        """Пакетное детектирование spoofing."""
        try:
            # Анализируем изменения в больших ордерах
            large_orders_changes = 0
            
            for i in range(1, len(orderbooks)):
                prev_orderbook = orderbooks[i-1]
                curr_orderbook = orderbooks[i]
                
                # Простая эвристика: проверяем исчезновение больших ордеров
                # В реальной реализации нужен более сложный анализ
                
            if large_orders_changes > len(orderbooks) * 0.5:
                logger.warning(f"Возможный spoofing для {symbol}: "
                             f"{large_orders_changes} изменений больших ордеров")
                
        except Exception as e:
            logger.error(f"Ошибка при детектировании spoofing для {symbol}: {e}", exc_info=True)
    
    def _detect_batch_layering(self, symbol: str, orderbooks: List[Dict[str, Any]]):
        """Пакетное детектирование layering."""
        try:
            # Анализируем паттерны размещения ордеров
            logger.debug(f"Анализ layering для {symbol} на {len(orderbooks)} обновлениях")
            
        except Exception as e:
            logger.error(f"Ошибка при детектировании layering для {symbol}: {e}", exc_info=True)
    
    def _batch_alert_handler(self, alerts: List[Dict[str, Any]]):
        """Пакетная обработка алертов."""
        try:
            logger.debug(f"Обработка пакета из {len(alerts)} алертов")
            
            # Группируем алерты по типам и символам
            alerts_by_type = {}
            alerts_by_symbol = {}
            
            for alert in alerts:
                alert_type = alert.get('alert_type', 'UNKNOWN')
                symbol = alert.get('symbol', 'UNKNOWN')
                
                if alert_type not in alerts_by_type:
                    alerts_by_type[alert_type] = []
                alerts_by_type[alert_type].append(alert)
                
                if symbol not in alerts_by_symbol:
                    alerts_by_symbol[symbol] = []
                alerts_by_symbol[symbol].append(alert)
            
            # Логируем статистику
            logger.info(f"Пакет алертов: {len(alerts)} всего, "
                       f"Типы: {list(alerts_by_type.keys())}, "
                       f"Символы: {list(alerts_by_symbol.keys())}")
            
        except Exception as e:
            logger.error(f"Ошибка в пакетной обработке алертов: {e}", exc_info=True)
    
    def _batch_alert_db_handler(self, alerts: List[Dict[str, Any]]):
        """Пакетная запись алертов в базу данных."""
        try:
            logger.debug(f"Пакетная запись {len(alerts)} алертов в БД")
            # Реализация пакетной записи алертов в БД
            
        except Exception as e:
            logger.error(f"Ошибка при пакетной записи алертов в БД: {e}", exc_info=True)
    
    async def start(self):
        """Запускает пакетную обработку."""
        await self.batch_processor.start()
        logger.info("BatchIntegration запущен")
    
    async def stop(self):
        """Останавливает пакетную обработку."""
        await self.batch_processor.stop()
        logger.info("BatchIntegration остановлен")
    
    async def add_trade_data(self, trade_data: Dict[str, Any]):
        """Добавляет торговые данные для пакетной обработки."""
        await self.batch_processor.add_trade(trade_data)
    
    async def add_orderbook_data(self, orderbook_data: Dict[str, Any]):
        """Добавляет данные стакана для пакетной обработки."""
        await self.batch_processor.add_orderbook(orderbook_data)
    
    async def add_alert_data(self, alert_data: Dict[str, Any]):
        """Добавляет алерт для пакетной обработки."""
        await self.batch_processor.add_alert(alert_data)
    
    def get_status(self) -> Dict[str, Any]:
        """Возвращает статус пакетной обработки."""
        return self.batch_processor.get_status()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Возвращает метрики пакетной обработки."""
        return self.batch_processor.get_metrics()