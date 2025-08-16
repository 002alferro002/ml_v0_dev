import logging
from typing import Dict, Any, Tuple, Optional, List
from datetime import datetime
from collections import deque
import time
from manipulation.ml.ml_model import MLModel
from feature_engineering import FeatureEngineer
from db_manager import DBManager
from manipulation.trade_processor import TradeProcessor
from manipulation.orderbook_analyzer import OrderBookAnalyzer

logger = logging.getLogger(__name__)

class MLPricePredictor:
    def __init__(self, db_manager: DBManager, ml_model: MLModel, feature_engineer: FeatureEngineer,
                 trade_processor: TradeProcessor, orderbook_analyzer: OrderBookAnalyzer):
        self.db_manager = db_manager
        self.ml_model = ml_model
        self.feature_engineer = feature_engineer
        self.trade_processor = trade_processor
        self.orderbook_analyzer = orderbook_analyzer

        # Link managers to feature engineer for data access
        self.feature_engineer.trade_processor = self.trade_processor
        self.feature_engineer.orderbook_analyzer = self.orderbook_analyzer

        # logger.info("MLPricePredictor initialized and managers linked to FeatureEngineer.")

    async def predict_for_alert(self, symbol: str, alert_data: Dict[str, Any]) -> Tuple[float, str, float]:
        """
        Predicts price change and direction for a given alert.
        This function is called by MLDataCollector.
        It MUST use the order_book_snapshot and trade_history provided in alert_data
        to ensure the prediction is based on the market state at the time of the alert.
        """
        alert_timestamp_ms = alert_data.get('alert_timestamp_ms')
        if not alert_timestamp_ms:
            logger.warning(f"Alert data for {symbol} missing timestamp, cannot predict.")
            return 0.0, "neutral", 0.0

        # Retrieve relevant historical data from alert_data
        # It is crucial that alert_data contains the snapshot and history from the alert's context.
        order_book_snapshot = alert_data.get('order_book_snapshot')
        trade_history = alert_data.get('trade_history')

        if not order_book_snapshot or not trade_history:
            logger.warning(f"Insufficient historical data in alert_data for ML prediction for {symbol}. "
                           f"Snapshot present: {bool(order_book_snapshot)}, Trade history present: {bool(trade_history)}")
            return 0.0, "neutral", 0.0

        try:
            features = await self.feature_engineer.extract_features(
                symbol,
                order_book_snapshot,
                trade_history,
                alert_timestamp_ms / 1000 # Convert to seconds
            )
            if not features:
                logger.warning(f"No features extracted for {symbol} for alert prediction.")
                return 0.0, "neutral", 0.0

            predicted_price_change, predicted_direction, confidence = self.ml_model.predict(features)
            return predicted_price_change, predicted_direction, confidence

        except Exception as e:
            logger.error(f"Error during ML prediction for alert {symbol}: {e}", exc_info=True)
            return 0.0, "neutral", 0.0

    async def get_prediction_for_current_state(self, symbol: str) -> Dict[str, Any]:
        """
        Provides a real-time prediction based on the current market state.
        This can be used for API endpoints.
        """
        if not self.ml_model.get_model_status()['online_models_trained']:
            return {"error": "ML models are not trained yet or online models are not ready."}

        current_ob = self.orderbook_analyzer.current_orderbooks.get(symbol)
        trade_history = list(self.trade_processor.trade_history.get(symbol, deque()))
        current_timestamp = time.time()

        if not current_ob or not current_ob.get('bids') or not current_ob.get('asks') or not trade_history:
            return {"error": f"No real-time order book or trade data available for {symbol}."}

        # Pass the current order book and trade history directly
        current_ob_for_fe = {
            'bids': current_ob['bids'],
            'asks': current_ob['asks']
        }

        try:
            features = await self.feature_engineer.extract_features(
                symbol,
                current_ob_for_fe,
                trade_history,
                current_timestamp
            )
            if not features:
                return {"error": f"Could not extract features for {symbol}."}

            predicted_price_change, predicted_direction, confidence = self.ml_model.predict(features)

            return {
                "symbol": symbol,
                "predicted_price_change": predicted_price_change,
                "predicted_direction": predicted_direction,
                "confidence": confidence,
                "timestamp": datetime.fromtimestamp(current_timestamp).isoformat(),
                "model_status": self.ml_model.get_model_status(),
                "features": features # Include features for debugging/analysis
            }
        except Exception as e:
            logger.error(f"Error getting real-time prediction for {symbol}: {e}", exc_info=True)
            return {"error": f"Failed to get real-time prediction: {e}"}




