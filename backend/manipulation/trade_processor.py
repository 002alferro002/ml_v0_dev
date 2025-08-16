import asyncio
import logging
from collections import defaultdict, deque
from typing import Dict, Any, List, Optional
import time
from manipulation.alert_manager import AlertManager
from metrics import TRADES_PROCESSED_TOTAL, TRADE_PROCESSING_LATENCY, ERRORS_TOTAL, ALERTS_GENERATED_TOTAL
from datetime import datetime

logger = logging.getLogger(__name__)

class TradeProcessor:
    def __init__(self, alert_manager: AlertManager, message_broker, kline_interval_minutes: int = 1, trade_history_maxlen: int = 10000):
        self.alert_manager = alert_manager
        self.message_broker = message_broker # New: MessageBroker instance
        self.broadcast_callback = None # Callback for broadcasting streaming data
        self.trade_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=trade_history_maxlen))
        self.kline_data: Dict[str, Dict[str, Any]] = defaultdict(dict) # symbol -> {interval -> {timestamp -> kline_data}}
        self.kline_interval_minutes = kline_interval_minutes
        self.last_kline_update_time: Dict[str, float] = defaultdict(float)
        self.processed_trades_queue_name = "processed_trades_queue" # Queue for processed trades

        # Manipulation detection parameters (example values)
        self.wash_trade_threshold_ratio = 0.8 # % of volume that is wash trade
        self.ping_pong_threshold_seconds = 0.1 # time between buy/sell by same entity
        self.ramping_volume_multiplier = 5 # X times average volume
        self.ramping_price_change_percent = 0.5 # % price change
        self.consecutive_long_threshold = 5 # Number of consecutive long candles
        self.consecutive_long_volume_multiplier = 2 # Volume multiplier for consecutive long candles

        # Alert tracking (for internal logic, AlertManager handles cooldowns)
        self.wash_trade_alerts: Dict[str, int] = defaultdict(int)
        self.ping_pong_alerts: Dict[str, int] = defaultdict(int)
        self.ramping_alerts: Dict[str, int] = defaultdict(int)

        # logger.info("TradeProcessor initialized.")
    
    def set_broadcast_callback(self, callback):
        """Устанавливает callback функцию для отправки потоковых данных."""
        self.broadcast_callback = callback

    async def process_trade(self, trade: Dict[str, Any]):
        """Обрабатывает входящую сделку."""
        start_time = time.perf_counter()
        try:
            symbol = trade['s']
            price = float(trade['p'])
            size = float(trade['v'])  # Bybit v5 uses 'v' for volume/size
            side = 'Buy' if trade['S'] == 'Buy' else 'Sell'
            timestamp_ms = int(trade['T'])
            trade_id = trade['i'] # Bybit v5 uses 'i' for trade ID

            volume_usdt = price * size

            processed_trade = {
                'timestamp': timestamp_ms / 1000, # Convert to seconds
                'symbol': symbol, # Add symbol for DB storage
                'price': price,
                'size': size,
                'side': side,
                'volume_usdt': volume_usdt,
                'trade_id': trade_id
            }
            self.trade_history[symbol].append(processed_trade)

            # Publish processed trade to a queue for DB storage and other consumers
            await self.message_broker.publish(self.processed_trades_queue_name, processed_trade)
            
            # Broadcast trade data to WebSocket clients
            if self.broadcast_callback:
                await self.broadcast_callback("trade_update", {
                    "symbol": symbol,
                    "price": price,
                    "size": size,
                    "side": side,
                    "volume_usdt": volume_usdt,
                    "timestamp": timestamp_ms / 1000
                })

            # Update kline data
            await self._update_kline_data(symbol, processed_trade)

            # Run manipulation detection
            await self._detect_wash_trading(symbol, processed_trade)
            await self._detect_ping_pong(symbol, processed_trade)
            await self._detect_ramping(symbol, processed_trade)
            
            TRADES_PROCESSED_TOTAL.inc()
        except Exception as e:
            logger.error(f"Error processing trade: {trade}. Error: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='trade_processor', error_type='processing_error').inc()
        finally:
            TRADE_PROCESSING_LATENCY.observe(time.perf_counter() - start_time)

    async def _update_kline_data(self, symbol: str, trade: Dict[str, Any]):
        """Обновляет данные свечей (kline) на основе сделок."""
        interval_sec = self.kline_interval_minutes * 60
        current_bucket_start_sec = (int(trade['timestamp']) // interval_sec) * interval_sec
        
        kline = self.kline_data[symbol].setdefault(str(self.kline_interval_minutes) + 'min', {}) \
                                       .setdefault(current_bucket_start_sec, {
                                           'open': trade['price'],
                                           'high': trade['price'],
                                           'low': trade['price'],
                                           'close': trade['price'],
                                           'volume': 0.0,
                                           'buy_volume': 0.0,
                                           'sell_volume': 0.0,
                                           'start_time': current_bucket_start_sec
                                       })
        
        kline['high'] = max(kline['high'], trade['price'])
        kline['low'] = min(kline['low'], trade['price'])
        kline['close'] = trade['price']
        kline['volume'] += trade['volume_usdt']
        if trade['side'] == 'Buy':
            kline['buy_volume'] += trade['volume_usdt']
        else:
            kline['sell_volume'] += trade['volume_usdt']

        # Check for closed candle
        if trade['timestamp'] >= current_bucket_start_sec + interval_sec and \
           self.last_kline_update_time[symbol] < current_bucket_start_sec + interval_sec:
            
            closed_kline_timestamp = current_bucket_start_sec
            closed_kline = self.kline_data[symbol][str(self.kline_interval_minutes) + 'min'].get(closed_kline_timestamp)
            
            if closed_kline:
                logger.debug(f"Closed {self.kline_interval_minutes}min kline for {symbol} at {datetime.fromtimestamp(closed_kline_timestamp)}: {closed_kline}")
                # Trigger detection for closed candle
                asyncio.create_task(self._detect_consecutive_long(symbol, closed_kline))
                self.last_kline_update_time[symbol] = current_bucket_start_sec + interval_sec

    async def _detect_wash_trading(self, symbol: str, trade: Dict[str, Any]):
        """Обнаруживает признаки "отмывочной" торговли (wash trading)."""
        # Simplified detection: check for rapid buy/sell by same entity (not possible with public trades)
        # A more robust approach would require internal exchange data or advanced heuristics.
        # For public data, we can look for rapid reversals in trade direction with high volume.
        
        recent_trades = list(self.trade_history[symbol])[-10:] # Last 10 trades
        if len(recent_trades) < 2:
            return

        # Check for rapid buy-sell or sell-buy sequence with similar volume
        last_trade = recent_trades[-1]
        second_last_trade = recent_trades[-2]

        if (last_trade['side'] != second_last_trade['side'] and
            abs(last_trade['timestamp'] - second_last_trade['timestamp']) < 0.5 and # within 0.5 seconds
            abs(last_trade['volume_usdt'] - second_last_trade['volume_usdt']) / last_trade['volume_usdt'] < 0.1): # within 10% volume
            
            self.wash_trade_alerts[symbol] += 1
            alert_data = {
                "recent_trades": [last_trade, second_last_trade],
                "reason": "Rapid buy-sell/sell-buy with similar volume"
            }
            await self.alert_manager.create_alert(
                symbol, "WashTrading", "High", "Potential wash trading detected.", alert_data,
                trade_history=list(self.trade_history[symbol])
            )
            logger.warning(f"Wash trading detected for {symbol}")

    async def _detect_ping_pong(self, symbol: str, trade: Dict[str, Any]):
        """Обнаруживает "пинг-понг" торговлю."""
        # This typically involves a single entity rapidly buying and selling to create volume.
        # Similar to wash trading, hard to detect without entity IDs.
        # Heuristic: rapid sequence of small trades alternating buy/sell at same price level.
        
        recent_trades = list(self.trade_history[symbol])[-20:] # Last 20 trades
        if len(recent_trades) < 5:
            return

        ping_pong_count = 0
        for i in range(len(recent_trades) - 1):
            t1 = recent_trades[i]
            t2 = recent_trades[i+1]
            if (t1['side'] != t2['side'] and
                abs(t1['timestamp'] - t2['timestamp']) < self.ping_pong_threshold_seconds and
                abs(t1['price'] - t2['price']) < 0.001 * t1['price']): # Price within 0.1%
                ping_pong_count += 1
        
        if ping_pong_count >= 3: # 3 or more rapid alternations
            self.ping_pong_alerts[symbol] += 1
            alert_data = {
                "ping_pong_count": ping_pong_count,
                "recent_trades_sample": recent_trades[-5:],
                "reason": "Multiple rapid alternating trades at similar price"
            }
            await self.alert_manager.create_alert(
                symbol, "PingPong", "Medium", "Potential ping-pong trading detected.", alert_data,
                trade_history=list(self.trade_history[symbol])
            )
            logger.warning(f"Ping-pong trading detected for {symbol}")

    async def _detect_ramping(self, symbol: str, trade: Dict[str, Any]):
        """Обнаруживает "разгон" цены (ramping)."""
        # Ramping: rapid price increase/decrease with significant volume.
        
        recent_trades = list(self.trade_history[symbol])
        if len(recent_trades) < 10:
            return

        # Consider trades within a short window, e.g., last 30 seconds
        window_trades = [t for t in recent_trades if trade['timestamp'] - t['timestamp'] <= 30]
        if len(window_trades) < 5:
            return

        total_volume_window = sum(t['volume_usdt'] for t in window_trades)
        prices_window = [t['price'] for t in window_trades]

        if len(prices_window) < 2:
            return

        price_change_percent = (prices_window[-1] - prices_window[0]) / prices_window[0] * 100

        # Compare with average volume (requires historical average, simplified here)
        # For a real system, calculate average volume over a longer period (e.g., 1 hour)
        avg_volume_per_trade = sum(t['volume_usdt'] for t in recent_trades) / len(recent_trades) if recent_trades else 0
        
        if total_volume_window > self.ramping_volume_multiplier * avg_volume_per_trade * len(window_trades) and \
           abs(price_change_percent) > self.ramping_price_change_percent:
            
            self.ramping_alerts[symbol] += 1
            alert_data = {
                "price_change_percent": price_change_percent,
                "total_volume_window": total_volume_window,
                "reason": "Significant price change with high volume"
            }
            severity = "High" if abs(price_change_percent) > 1.0 else "Medium"
            await self.alert_manager.create_alert(
                symbol, "Ramping", severity, "Potential price ramping detected.", alert_data,
                trade_history=list(self.trade_history[symbol])
            )
            logger.warning(f"Ramping detected for {symbol}: {price_change_percent:.2f}% price change, {total_volume_window:.2f} volume")

    async def _detect_consecutive_long(self, symbol: str, closed_kline: Dict[str, Any]):
        """Обнаруживает последовательные длинные свечи (признак манипуляции)."""
        # A "long" candle is one with a significant body and high volume.
        # Consecutive long candles in one direction can indicate manipulation.
        
        if closed_kline['open'] == 0: # Avoid division by zero
            return

        price_change = closed_kline['close'] - closed_kline['open']
        body_percent = abs(price_change) / closed_kline['open'] * 100

        # Define what a "long" candle is (e.g., body > 0.1% of open price)
        if body_percent < 0.05: # Not a significant candle
            return

        # Check volume relative to average (simplified)
        # For a real system, calculate average volume over a longer period
        all_klines = self.kline_data[symbol].get(str(self.kline_interval_minutes) + 'min', {}).values()
        avg_kline_volume = sum(k['volume'] for k in all_klines) / len(all_klines) if all_klines else 0

        if closed_kline['volume'] < self.consecutive_long_volume_multiplier * avg_kline_volume:
            return # Not enough volume

        direction = "bullish" if price_change > 0 else "bearish"
        
        # Store recent candle directions and check for consecutive pattern
        # This requires storing a history of candle directions, which is not currently in kline_data
        # For simplicity, let's assume we check the last few candles from kline_data
        
        recent_klines = sorted(self.kline_data[symbol].get(str(self.kline_interval_minutes) + 'min', {}).values(), key=lambda x: x['start_time'])
        recent_klines = [k for k in recent_klines if closed_kline['start_time'] - k['start_time'] < self.kline_interval_minutes * 60 * self.consecutive_long_threshold]

        consecutive_count = 0
        for k in reversed(recent_klines):
            k_price_change = k['close'] - k['open']
            k_direction = "bullish" if k_price_change > 0 else "bearish"
            if k_direction == direction and abs(k_price_change) / k['open'] * 100 >= 0.05 and k['volume'] >= self.consecutive_long_volume_multiplier * avg_kline_volume:
                consecutive_count += 1
            else:
                break
        
        if consecutive_count >= self.consecutive_long_threshold:
            alert_data = {
                "direction": direction,
                "consecutive_count": consecutive_count,
                "last_kline": closed_kline,
                "reason": f"{consecutive_count} consecutive {direction} long candles with high volume"
            }
            await self.alert_manager.create_alert(
                symbol, "ConsecutiveLongCandles", "High", f"Potential manipulation: {consecutive_count} consecutive {direction} long candles.", alert_data,
                trade_history=list(self.trade_history[symbol])
            )
            logger.warning(f"Consecutive long candles detected for {symbol}: {consecutive_count} {direction}")



