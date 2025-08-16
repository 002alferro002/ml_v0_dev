import asyncio
import logging
from collections import defaultdict, deque
from typing import Dict, Any, List, Tuple, Optional
import time
from manipulation.alert_manager import AlertManager
from metrics import ORDERBOOK_UPDATES_TOTAL, ORDERBOOK_PROCESSING_LATENCY, ERRORS_TOTAL, ALERTS_GENERATED_TOTAL
from utils.orderbook_utils import depth_calculator, get_optimal_orderbook_config

logger = logging.getLogger(__name__)

class OrderBookAnalyzer:
    def __init__(self, alert_manager: AlertManager, message_broker, trade_processor=None, orderbook_depth: int = 50, snapshot_history_maxlen: int = 1000):
        self.alert_manager = alert_manager
        self.message_broker = message_broker # New: MessageBroker instance
        self.trade_processor = trade_processor # New: TradeProcessor instance for trade history access
        self.broadcast_callback = None # Callback for broadcasting streaming data
        self.current_orderbooks: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'bids': [], 'asks': [], 'timestamp': 0})
        self.orderbook_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=snapshot_history_maxlen))
        self.orderbook_depth = orderbook_depth
        self.processed_orderbooks_queue_name = "processed_orderbooks_queue" # Queue for processed orderbooks
        
        # Кэш для хранения информации о символах
        self.symbol_info_cache: Dict[str, Dict[str, Any]] = {}

        # Manipulation detection parameters
        self.iceberg_threshold_ratio = 0.1 # % of total volume hidden
        self.layering_spoofing_depth = 5 # Check top 5 levels
        self.layering_spoofing_volume_ratio = 0.5 # % of total volume in layering orders
        self.spread_manipulation_threshold = 0.005 # 0.5% spread increase

        # Alert tracking (for internal logic, AlertManager handles cooldowns)
        self.alert_counts: Dict[str, int] = defaultdict(int) # Generic counter for orderbook alerts

        # logger.info("OrderBookAnalyzer initialized.")
    
    def set_broadcast_callback(self, callback):
        """Устанавливает callback функцию для отправки потоковых данных."""
        self.broadcast_callback = callback
    
    def update_symbol_info(self, symbol: str, symbol_info: Dict[str, Any]):
        """Обновляет информацию о символе, включая tick size."""
        self.symbol_info_cache[symbol] = symbol_info
        # Обновляем информацию в калькуляторе глубины
        depth_calculator.update_symbol_info(symbol, symbol_info)
        logger.debug(f"Updated symbol info for {symbol}: tick_size={symbol_info.get('tickSize', 'unknown')}")
    
    def get_optimal_depth_for_symbol(self, symbol: str) -> int:
        """Получает оптимальную глубину стакана для символа на основе его характеристик."""
        try:
            current_ob = self.current_orderbooks[symbol]
            bids = current_ob.get('bids', [])
            asks = current_ob.get('asks', [])
            
            if not bids or not asks:
                return self.orderbook_depth
            
            # Получаем текущую цену
            current_price = (bids[0][0] + asks[0][0]) / 2
            total_orders = len(bids) + len(asks)
            
            # Рассчитываем оптимальную глубину
            optimal_depth = depth_calculator.calculate_optimal_depth(symbol, current_price, total_orders)
            
            # Ограничиваем максимальной настроенной глубиной
            return min(optimal_depth, self.orderbook_depth)
            
        except Exception as e:
            logger.warning(f"Error calculating optimal depth for {symbol}: {e}")
            return self.orderbook_depth

    async def process_orderbook_update(self, data: Dict[str, Any]):
        """Обрабатывает входящее обновление стакана ордеров."""
        start_time = time.perf_counter()
        try:
            symbol = data['s']
            logger.debug(f"Processing orderbook update for {symbol}")
            # Use current timestamp if 'ts' is not available in orderbook data
            timestamp_ms = int(time.time() * 1000)
            
            # Bybit v5 orderbook updates are usually full snapshots or deltas.
            # Assuming 'b' for bids, 'a' for asks, each is [price, size]
            bids_data = data.get('b', [])
            asks_data = data.get('a', [])

            # For simplicity, we'll assume full snapshots or merge deltas.
            # A real-time order book needs to handle 'u' (update) and 'd' (delete) events.
            # Here, we'll just update the current_orderbooks with the latest snapshot.
            # If it's a delta, we'd need to merge it with the existing orderbook.
            
            # Example: simple replacement for full snapshot
            current_bids = []
            for bid in bids_data:
                try:
                    price = float(bid[0])
                    size = float(bid[1])
                    if size > 0: # Only add if size is positive
                        current_bids.append([price, size])
                except (ValueError, IndexError):
                    logger.warning(f"Invalid bid data for {symbol}: {bid}")
                    ERRORS_TOTAL.labels(component='orderbook_analyzer', error_type='invalid_bid_data').inc()
                    continue
            
            current_asks = []
            for ask in asks_data:
                try:
                    price = float(ask[0])
                    size = float(ask[1])
                    if size > 0: # Only add if size is positive
                        current_asks.append([price, size])
                except (ValueError, IndexError):
                    logger.warning(f"Invalid ask data for {symbol}: {ask}")
                    ERRORS_TOTAL.labels(component='orderbook_analyzer', error_type='invalid_ask_data').inc()
                    continue

            # Sort bids descending by price, asks ascending by price
            current_bids.sort(key=lambda x: x[0], reverse=True)
            current_asks.sort(key=lambda x: x[0])

            # Получаем оптимальную глубину для символа
            optimal_depth = self.get_optimal_depth_for_symbol(symbol)

            processed_orderbook = {
                'timestamp': timestamp_ms / 1000, # Convert to seconds
                'symbol': symbol, # Add symbol for DB storage
                'bids': current_bids[:optimal_depth],
                'asks': current_asks[:optimal_depth],
            }
            self.current_orderbooks[symbol] = processed_orderbook
            
            # Add to history
            self.orderbook_history[symbol].append(processed_orderbook)

            # Publish processed orderbook to a queue for DB storage and other consumers
            await self.message_broker.publish(self.processed_orderbooks_queue_name, processed_orderbook)
            
            # Broadcast orderbook data to WebSocket clients
            if self.broadcast_callback:
                # Send top 25 levels for better visualization
                top_bids = current_bids[:25] if current_bids else []
                top_asks = current_asks[:25] if current_asks else []
                
                # Получаем конфигурацию стакана для фронтенда
                current_price = (current_bids[0][0] + current_asks[0][0]) / 2 if current_bids and current_asks else 0
                total_orders = len(current_bids) + len(current_asks)
                orderbook_config = get_optimal_orderbook_config(symbol, current_price, total_orders)
                
                await self.broadcast_callback("orderbook_update", {
                    "symbol": symbol,
                    "bids": top_bids,
                    "asks": top_asks,
                    "timestamp": timestamp_ms / 1000,
                    "config": orderbook_config  # Добавляем конфигурацию для фронтенда
                })

            # Run manipulation detection
            await self._detect_iceberg_orders(symbol)
            await self._detect_layering_spoofing(symbol)
            await self._detect_spread_manipulation(symbol)
            
            ORDERBOOK_UPDATES_TOTAL.inc()
        except Exception as e:
            logger.error(f"Error processing orderbook update: {data}. Error: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='orderbook_analyzer', error_type='processing_error').inc()
        finally:
            ORDERBOOK_PROCESSING_LATENCY.observe(time.perf_counter() - start_time)

    async def _detect_iceberg_orders(self, symbol: str):
        """Обнаруживает айсберг-ордера."""
        current_ob = self.current_orderbooks[symbol]
        bids = current_ob['bids']
        asks = current_ob['asks']

        if not bids or not asks:
            return

        # Iceberg detection: large hidden volume behind a small visible order.
        # This is hard to detect with just order book snapshots.
        # Heuristic: check if a large trade occurs at a price level with small visible volume,
        # and then the order reappears. This requires trade data.
        
        # Simplified heuristic: check for a large order at the top of the book
        # that is significantly smaller than the total liquidity behind it.
        
        best_bid_size = bids[0][1]
        best_ask_size = asks[0][1]
        
        total_bid_depth = sum(s for p, s in bids)
        total_ask_depth = sum(s for p, s in asks)

        # If best bid/ask is very small compared to total depth, it might be an iceberg.
        # This is a very rough heuristic.
        if (best_bid_size > 0 and best_bid_size / total_bid_depth < self.iceberg_threshold_ratio and total_bid_depth > 10000) or \
           (best_ask_size > 0 and best_ask_size / total_ask_depth < self.iceberg_threshold_ratio and total_ask_depth > 10000):
            
            self.alert_counts[symbol] += 1
            alert_data = {
                "best_bid_size": best_bid_size,
                "best_ask_size": best_ask_size,
                "total_bid_depth": total_bid_depth,
                "total_ask_depth": total_ask_depth,
                "reason": "Small visible order at top of book with large hidden depth"
            }
            await self.alert_manager.create_alert(
                symbol, "IcebergOrder", "Medium", "Potential iceberg order detected.", alert_data,
                order_book_snapshot=current_ob,
                trade_history=self._get_recent_trade_history(symbol)
            )
            logger.warning(f"Iceberg order detected for {symbol}")

    async def _detect_layering_spoofing(self, symbol: str):
        """Обнаруживает "наслаивание" и "спуфинг"."""
        current_ob = self.current_orderbooks[symbol]
        bids = current_ob['bids']
        asks = current_ob['asks']

        if not bids or not asks:
            return

        # Layering/Spoofing: placing large orders far from the best bid/ask to create false impression of depth,
        # then cancelling them before they are filled.
        # This requires tracking order changes, not just snapshots.
        # Heuristic: check for large orders at multiple levels far from the spread,
        # especially if they are disproportionately large compared to closer orders.
        
        # Check for large orders in deeper levels (e.g., beyond top 5)
        # that are significantly larger than orders closer to the spread.
        
        top_bids_volume = sum(s for p, s in bids[:self.layering_spoofing_depth])
        top_asks_volume = sum(s for p, s in asks[:self.layering_spoofing_depth])

        deeper_bids_volume = sum(s for p, s in bids[self.layering_spoofing_depth:])
        deeper_asks_volume = sum(s for p, s in asks[self.layering_spoofing_depth:])

        total_volume = top_bids_volume + top_asks_volume + deeper_bids_volume + deeper_asks_volume

        if total_volume == 0:
            return

        # If a significant portion of total volume is in deeper levels, it might be layering.
        if (deeper_bids_volume + deeper_asks_volume) / total_volume > self.layering_spoofing_volume_ratio:
            self.alert_counts[symbol] += 1
            alert_data = {
                "top_bids_volume": top_bids_volume,
                "top_asks_volume": top_asks_volume,
                "deeper_bids_volume": deeper_bids_volume,
                "deeper_asks_volume": deeper_asks_volume,
                "reason": "Disproportionately large orders in deeper order book levels"
            }
            await self.alert_manager.create_alert(
                symbol, "LayeringSpoofing", "High", "Potential layering/spoofing detected.", alert_data,
                order_book_snapshot=current_ob,
                trade_history=self._get_recent_trade_history(symbol)
            )
            logger.warning(f"Layering/Spoofing detected for {symbol}")

    async def _detect_spread_manipulation(self, symbol: str):
        """Обнаруживает манипуляции спред."""
        current_ob = self.current_orderbooks[symbol]
        bids = current_ob['bids']
        asks = current_ob['asks']

        if not bids or not asks:
            return

        best_bid_price = bids[0][0]
        best_ask_price = asks[0][0]

        spread = best_ask_price - best_bid_price
        mid_price = (best_bid_price + best_ask_price) / 2
        relative_spread = spread / mid_price if mid_price > 0 else 0

        # Check if relative spread is significantly wider than historical average
        # This requires tracking historical spread.
        # For simplicity, we'll use a fixed threshold.
        
        if relative_spread > self.spread_manipulation_threshold:
            self.alert_counts[symbol] += 1
            alert_data = {
                "current_spread": spread,
                "relative_spread": relative_spread,
                "reason": "Abnormally wide spread"
            }
            await self.alert_manager.create_alert(
                symbol, "SpreadManipulation", "High", "Abnormally wide spread detected.", alert_data,
                order_book_snapshot=current_ob,
                trade_history=self._get_recent_trade_history(symbol)
            )
            logger.warning(f"Spread manipulation detected for {symbol}: relative spread {relative_spread:.4f}")

    def _get_recent_trade_history(self, symbol: str, minutes: int = 5) -> List[Dict[str, Any]]:
        """Получает недавнюю торговую историю для символа."""
        try:
            if not self.trade_processor:
                logger.debug(f"No trade_processor available for {symbol}")
                return []
                
            if symbol not in self.trade_processor.trade_history:
                logger.debug(f"No trade history for symbol {symbol}. Available symbols: {list(self.trade_processor.trade_history.keys())}")
                return []
            
            # Получаем все сделки для символа
            all_trades = list(self.trade_processor.trade_history[symbol])
            logger.debug(f"Found {len(all_trades)} total trades for {symbol}")
            
            # Фильтруем сделки за последние N минут
            current_time = time.time()
            cutoff_time = current_time - (minutes * 60)
            
            recent_trades = [
                trade for trade in all_trades 
                if trade.get('timestamp', 0) >= cutoff_time
            ]
            
            logger.debug(f"Found {len(recent_trades)} recent trades for {symbol} in last {minutes} minutes")
            
            # Возвращаем последние 100 сделок (или меньше)
            result = recent_trades[-100:]
            logger.debug(f"Returning {len(result)} trades for {symbol}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting recent trade history for {symbol}: {e}")
            return []



