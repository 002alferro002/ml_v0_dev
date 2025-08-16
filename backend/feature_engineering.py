import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional
import logging
from collections import deque
import time
from utils.feature_normalizer import normalize_features # Import the new normalizer
from db_manager import DBManager # Import DBManager to fetch historical data
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class FeatureEngineer:
    def __init__(self, db_manager: DBManager, trade_processor=None, orderbook_analyzer=None):
        self.db_manager = db_manager # New: DBManager instance
        self.trade_processor = trade_processor
        self.orderbook_analyzer = orderbook_analyzer
        logger.info("FeatureEngineer initialized.")

    async def extract_features(self, symbol: str, order_book_snapshot: Dict, trade_history: List[Dict],
                               timestamp_sec: float) -> Optional[Dict[str, Any]]:
        """
        Extracts features from order book and trade data for a given symbol at a specific timestamp.
        Args:
            symbol: The trading pair symbol (e.g., "BTCUSDT").
            order_book_snapshot: A dictionary with 'bids' and 'asks' lists.
            trade_history: A list of trade dictionaries.
            timestamp_sec: The timestamp (in seconds) around which to extract features.
        Returns:
            A dictionary of extracted features, or None if data is insufficient.
        """
        logger.debug(
            f"Extracting features for {symbol}: snapshot={bool(order_book_snapshot)}, trades={len(trade_history)}")

        features = {}

        # --- Order Book Features ---
        bids = order_book_snapshot.get('bids', []) if order_book_snapshot else []
        asks = order_book_snapshot.get('asks', []) if order_book_snapshot else []

        if not bids or not asks:
            logger.warning(f"Order book snapshot is empty or invalid for {symbol}.")
            return None  # Возвращаем None если нет данных orderbook
        else:
            try:
                best_bid_price = bids[0][0]
                best_bid_size = bids[0][1]
                best_ask_price = asks[0][0]
                best_ask_size = asks[0][1]
            except (IndexError, TypeError):
                logger.warning(f"Invalid orderbook structure for {symbol}")
                return None

            features['spread'] = best_ask_price - best_bid_price
            features['mid_price'] = (best_bid_price + best_ask_price) / 2
            features['relative_spread'] = features['spread'] / features['mid_price'] if features['mid_price'] > 0 else 0

            # Liquidity at different depths
            depth_levels = [5, 10, 20]
            for level in depth_levels:
                try:
                    features[f'bid_liquidity_depth_{level}'] = sum(s for p, s in bids[:level])
                    features[f'ask_liquidity_depth_{level}'] = sum(s for p, s in asks[:level])
                except (TypeError, ValueError):
                    features[f'bid_liquidity_depth_{level}'] = 0.0
                    features[f'ask_liquidity_depth_{level}'] = 0.0

                features[f'total_liquidity_depth_{level}'] = features[f'bid_liquidity_depth_{level}'] + features[
                    f'ask_liquidity_depth_{level}']
                features[f'liquidity_imbalance_depth_{level}'] = (features[f'bid_liquidity_depth_{level}'] - features[
                    f'ask_liquidity_depth_{level}']) / features[f'total_liquidity_depth_{level}'] if features[
                                                                                                         f'total_liquidity_depth_{level}'] > 0 else 0

            # Order book imbalance (overall)
            try:
                total_bid_volume = sum(s for p, s in bids)
                total_ask_volume = sum(s for p, s in asks)
            except (TypeError, ValueError):
                total_bid_volume = total_ask_volume = 0

            total_ob_volume = total_bid_volume + total_ask_volume
            features['orderbook_imbalance'] = (
                                                          total_bid_volume - total_ask_volume) / total_ob_volume if total_ob_volume > 0 else 0

        # --- Trade Features (recent history) ---
        # Увеличиваем окно для поиска недавних сделок
        recent_trades = [trade for trade in trade_history if
                         timestamp_sec - trade.get('timestamp', 0) <= 300] if trade_history else []

        if not recent_trades:
            # Если нет недавних сделок, используем исторические средние значения
            logger.debug(f"No recent trades for feature engineering for {symbol}, using historical averages.")

            # Используем все доступные сделки для расчета средних значений
            all_trades = trade_history if trade_history else []
            if all_trades:
                total_volume = sum(trade.get('volume_usdt', 0) for trade in all_trades)
                buy_volume = sum(trade.get('volume_usdt', 0) for trade in all_trades if trade.get('side') == 'Buy')
                sell_volume = sum(trade.get('volume_usdt', 0) for trade in all_trades if trade.get('side') == 'Sell')

                features['total_trade_volume_usd'] = total_volume / len(all_trades) if len(all_trades) > 0 else 0 # Средний объем
                features['buy_trade_volume_usd'] = buy_volume / len(all_trades) if len(all_trades) > 0 else 0
                features['sell_trade_volume_usd'] = sell_volume / len(all_trades) if len(all_trades) > 0 else 0
                features['trade_count'] = len(all_trades)
                features['trade_volume_imbalance'] = (
                                                                 buy_volume - sell_volume) / total_volume if total_volume > 0 else 0
                features['avg_trade_size_usd'] = total_volume / len(all_trades) if len(all_trades) > 0 else 0

                prices = [trade.get('price', 0) for trade in all_trades if trade.get('price', 0) > 0]
                if len(prices) > 1:
                    features['price_volatility'] = np.std(prices)
                    features['price_change_recent'] = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
                else:
                    features['price_volatility'] = 0.0
                    features['price_change_recent'] = 0.0
            else:
                # Полностью нулевые значения только если нет вообще никаких данных
                features['total_trade_volume_usd'] = 0.0
                features['buy_trade_volume_usd'] = 0.0
                features['sell_trade_volume_usd'] = 0.0
                features['trade_count'] = 0
                features['trade_volume_imbalance'] = 0.0
                features['avg_trade_size_usd'] = 0.0
                features['price_volatility'] = 0.0
                features['price_change_recent'] = 0.0
        else:
            total_trade_volume_usd = sum(trade.get('volume_usdt', 0) for trade in recent_trades)
            buy_volume_usd = sum(trade.get('volume_usdt', 0) for trade in recent_trades if trade.get('side') == 'Buy')
            sell_volume_usd = sum(trade.get('volume_usdt', 0) for trade in recent_trades if trade.get('side') == 'Sell')

            features['total_trade_volume_usd'] = total_trade_volume_usd
            features['buy_trade_volume_usd'] = buy_volume_usd
            features['sell_trade_volume_usd'] = sell_volume_usd
            features['trade_count'] = len(recent_trades)

            features['trade_volume_imbalance'] = (
                                                             buy_volume_usd - sell_volume_usd) / total_trade_volume_usd if total_trade_volume_usd > 0 else 0
            features['avg_trade_size_usd'] = total_trade_volume_usd / len(recent_trades) if len(
                recent_trades) > 0 else 0

            recent_prices = [trade.get('price', 0) for trade in recent_trades if trade.get('price', 0) > 0]
            if len(recent_prices) > 1:
                features['price_volatility'] = np.std(recent_prices)
                features['price_change_recent'] = (recent_prices[-1] - recent_prices[0]) / recent_prices[0] if \
                recent_prices[0] > 0 else 0
            else:
                features['price_volatility'] = 0.0
                features['price_change_recent'] = 0.0

        # --- Advanced Historical Features (using TimescaleDB) ---
        # Fetch historical kline data for more advanced indicators
        # For simplicity, let's fetch 1-minute klines for the last hour
        end_dt = datetime.fromtimestamp(timestamp_sec)
        start_dt = end_dt - timedelta(hours=1)
        
        historical_trades_db = await self.db_manager.get_historical_trades(symbol, start_dt, end_dt)
        historical_orderbooks_db = await self.db_manager.get_historical_orderbooks(symbol, start_dt, end_dt)

        # Convert DB trade data to DataFrame for easier processing
        if historical_trades_db:
            trades_df = pd.DataFrame([
                {'timestamp': t['time'].timestamp(), 'price': t['price'], 'volume': t['price'] * t['size'], 'side': t['side']}
                for t in historical_trades_db
            ])
            trades_df['timestamp'] = pd.to_datetime(trades_df['timestamp'], unit='s')
            trades_df = trades_df.set_index('timestamp').sort_index()

            # Example: Volume Weighted Average Price (VWAP) over last 5 minutes
            if len(trades_df) > 0:
                vwap_window = '5min'
                trades_df['price_volume'] = trades_df['price'] * trades_df['volume']
                vwap = trades_df['price_volume'].rolling(vwap_window).sum() / trades_df['volume'].rolling(vwap_window).sum()
                features['vwap_5min'] = vwap.iloc[-1] if not vwap.empty else 0.0
            else:
                features['vwap_5min'] = 0.0
        else:
            features['vwap_5min'] = 0.0

        # Example: Order Book Depth Ratio (sum of bids / sum of asks)
        if historical_orderbooks_db:
            # Get the latest orderbook from history for depth ratio
            latest_ob_db = sorted(historical_orderbooks_db, key=lambda x: x['time'], reverse=True)
            if latest_ob_db:
                latest_bids = latest_ob_db[0]['bids']
                latest_asks = latest_ob_db[0]['asks']
                # Safely extract volumes with validation
                total_bid_vol_db = 0.0
                total_ask_vol_db = 0.0
                
                for bid in latest_bids:
                    if isinstance(bid, (list, tuple)) and len(bid) >= 2:
                        try:
                            total_bid_vol_db += float(bid[1])
                        except (ValueError, TypeError):
                            continue
                            
                for ask in latest_asks:
                    if isinstance(ask, (list, tuple)) and len(ask) >= 2:
                        try:
                            total_ask_vol_db += float(ask[1])
                        except (ValueError, TypeError):
                            continue
                features['ob_depth_ratio'] = total_bid_vol_db / total_ask_vol_db if total_ask_vol_db > 0 else 0.0
            else:
                features['ob_depth_ratio'] = 0.0
        else:
            features['ob_depth_ratio'] = 0.0


        # --- Interaction Features ---
        # These signals are typically binary (0 or 1)
        if self.trade_processor and symbol in self.trade_processor.wash_trade_alerts:
            features['wash_trade_signal'] = int(self.trade_processor.wash_trade_alerts.get(symbol, 0) > 0)
        else:
            features['wash_trade_signal'] = 0

        if self.trade_processor and symbol in self.trade_processor.ping_pong_alerts:
            features['ping_pong_signal'] = int(self.trade_processor.ping_pong_alerts.get(symbol, 0) > 0)
        else:
            features['ping_pong_signal'] = 0

        if self.trade_processor and symbol in self.trade_processor.ramping_alerts:
            features['ramping_signal'] = int(self.trade_processor.ramping_alerts.get(symbol, 0) > 0)
        else:
            features['ramping_signal'] = 0

        if self.orderbook_analyzer and symbol in self.orderbook_analyzer.alert_counts:
            features['iceberg_signal'] = int(self.orderbook_analyzer.alert_counts.get(symbol, 0) > 0)
            features['layering_spoofing_signal'] = int(self.orderbook_analyzer.alert_counts.get(symbol, 0) > 0)
        else:
            features['iceberg_signal'] = 0
            features['layering_spoofing_signal'] = 0

        # Apply normalization using the centralized function
        normalized_features = normalize_features(features, symbol)

        logger.debug(f"Extracted and normalized features for {symbol}: {normalized_features}")
        return normalized_features

    def get_feature_names(self) -> List[str]:
        """Возвращает список всех возможных имен признаков."""
        # Generate dummy features to get all possible names
        dummy_symbol = "BTCUSDT"
        dummy_order_book_snapshot = {
            'bids': [[99.9, 100], [99.8, 200]],
            'asks': [[100.1, 150], [100.2, 250]]
        }
        dummy_trade_history = [
            {'timestamp': time.time() - 10, 'price': 100, 'size': 10, 'side': 'Buy', 'volume_usdt': 1000},
            {'timestamp': time.time() - 5, 'price': 100.01, 'size': 12, 'side': 'Sell', 'volume_usdt': 1200}
        ]
        dummy_timestamp_sec = time.time()

        # Temporarily mock db_manager methods for dummy feature extraction
        original_get_historical_trades = self.db_manager.get_historical_trades
        original_get_historical_orderbooks = self.db_manager.get_historical_orderbooks
        self.db_manager.get_historical_trades = lambda s, st, et: asyncio.Future() # Mock async
        self.db_manager.get_historical_trades.set_result([]) # Set empty result
        self.db_manager.get_historical_orderbooks = lambda s, st, et: asyncio.Future() # Mock async
        self.db_manager.get_historical_orderbooks.set_result([]) # Set empty result

        # Run extract_features in a new event loop for synchronous call
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        dummy_features = loop.run_until_complete(self.extract_features(dummy_symbol, dummy_order_book_snapshot, dummy_trade_history, dummy_timestamp_sec))
        loop.close()

        # Restore original db_manager methods
        self.db_manager.get_historical_trades = original_get_historical_trades
        self.db_manager.get_historical_orderbooks = original_get_historical_orderbooks

        if dummy_features:
            return sorted(list(dummy_features.keys()))
        return []


