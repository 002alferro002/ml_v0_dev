import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from exceptions import AppError
from metrics import ALERTS_GENERATED_TOTAL, ERRORS_TOTAL

logger = logging.getLogger(__name__)

class AlertManager:
    def __init__(self, db_manager, message_broker, alert_grouping_seconds: int = 60, alert_cooldown_seconds: int = 30):
        self.db_manager = db_manager
        self.message_broker = message_broker # New: MessageBroker instance
        self.alert_grouping_seconds = alert_grouping_seconds
        self.alert_cooldown_seconds = alert_cooldown_seconds
        self.recent_alerts: Dict[str, Dict[str, Any]] = {}  # Stores last alert for each type+symbol
        self.grouped_alerts: Dict[str, Dict[str, Any]] = {} # Stores active grouped alerts
        self.alert_lock = asyncio.Lock()
        self.alert_queue_name = "alerts_queue" # Queue for incoming alerts
        # logger.info("AlertManager initialized.")

    async def start_consuming(self):
        """Начинает потребление алертов из очереди."""
        try:
            await self.message_broker.consume(self.alert_queue_name, self._process_incoming_alert)
            # logger.info(f"AlertManager started consuming from {self.alert_queue_name}")
        except Exception as e:
            logger.error(f"Failed to start AlertManager consumer: {e}", exc_info=True)
            ERRORS_TOTAL.labels(component='alert_manager', error_type='consumer_start_failed').inc()
            raise AppError(f"Failed to start AlertManager consumer: {e}")

    async def _process_incoming_alert(self, alert_data: Dict[str, Any]):
        """Обрабатывает входящий алерт из очереди."""
        symbol = alert_data.get('symbol')
        alert_type = alert_data.get('alert_type')
        severity = alert_data.get('severity')
        description = alert_data.get('description')
        data = alert_data.get('data', {})
        order_book_snapshot = alert_data.get('order_book_snapshot')
        trade_history = alert_data.get('trade_history')

        if not all([symbol, alert_type, severity, description]):
            logger.warning(f"Received incomplete alert data: {alert_data}")
            ERRORS_TOTAL.labels(component='alert_manager', error_type='incomplete_alert_data').inc()
            return

        await self.create_alert(symbol, alert_type, severity, description, data,
                                order_book_snapshot, trade_history)

    async def create_alert(self, symbol: str, alert_type: str, severity: str,
                           description: str, data: Dict[str, Any],
                           order_book_snapshot: Optional[Dict] = None,
                           trade_history: Optional[List[Dict]] = None) -> Optional[Dict[str, Any]]:
        """
        Создает или группирует алерт, применяя логику кулдауна и группировки.
        Возвращает созданный/обновленный алерт или None, если алерт на кулдауне.
        """
        alert_key = f"{symbol}_{alert_type}"
        current_time = time.time()
        alert_timestamp_ms = int(current_time * 1000)

        async with self.alert_lock:
            last_alert_info = self.recent_alerts.get(alert_key)

            # Проверка кулдауна
            if last_alert_info and (current_time - last_alert_info['timestamp']) < self.alert_cooldown_seconds:
                logger.debug(f"Alert {alert_key} on cooldown. Skipping.")
                return None

            # Подготовка данных для сохранения в БД (только сводка, не полные снимки)
            snapshot_summary = self._get_orderbook_summary(order_book_snapshot)
            trade_summary = self._get_trade_history_summary(trade_history)

            alert_data_for_db = {
                "symbol": symbol,
                "alert_type": alert_type,
                "severity": severity,
                "description": description,
                "alert_timestamp_ms": alert_timestamp_ms,
                "data": data,
                "snapshot_summary": snapshot_summary, # Summary for DB
                "trade_summary": trade_summary # Summary for DB
            }
            
            # Full snapshot and history are passed to MLDataCollector via alert_data
            # but not stored directly in the main alerts table to avoid bloat.
            # They are stored in raw_trades/raw_orderbooks hypertables.

            # Попытка сгруппировать алерт
            grouped_alert_id = None
            if alert_key in self.grouped_alerts:
                existing_group = self.grouped_alerts[alert_key]
                if (current_time - existing_group['alert_start_time_ms'] / 1000) < self.alert_grouping_seconds:
                    # Группируем алерт
                    grouped_alert_id = existing_group['id']
                    try:
                        await self.db_manager.update_grouped_alert(
                            alert_id=grouped_alert_id,
                            new_severity=severity,
                            new_description=description,
                            new_data=data,
                            alert_end_time_ms=alert_timestamp_ms,
                            snapshot_summary=snapshot_summary,
                            trade_summary=trade_summary
                        )
                        # logger.info(f"Grouped alert {alert_key} (ID: {grouped_alert_id}) updated.")
                        alert_data_for_db['id'] = grouped_alert_id # Add ID for ML processing
                    except Exception as e:
                        logger.error(f"Failed to update grouped alert {grouped_alert_id}: {e}", exc_info=True)
                        ERRORS_TOTAL.labels(component='alert_manager', error_type='update_grouped_alert_failed').inc()
                        return None
                else:
                    # Группа устарела, создаем новую
                    del self.grouped_alerts[alert_key]
                    try:
                        alert_data_for_db['id'] = await self.db_manager.insert_alert(alert_data_for_db)
                        self.grouped_alerts[alert_key] = {
                            'id': alert_data_for_db['id'],
                            'alert_start_time_ms': alert_timestamp_ms,
                            'last_update_time_ms': alert_timestamp_ms
                        }
                        # logger.info(f"New alert {alert_key} (ID: {alert_data_for_db['id']}) created after group expiry.")
                    except Exception as e:
                        logger.error(f"Failed to insert new alert after group expiry: {e}", exc_info=True)
                        ERRORS_TOTAL.labels(component='alert_manager', error_type='insert_new_alert_failed').inc()
                        return None
            else:
                # Создаем новый алерт
                try:
                    alert_data_for_db['id'] = await self.db_manager.insert_alert(alert_data_for_db)
                    self.grouped_alerts[alert_key] = {
                        'id': alert_data_for_db['id'],
                        'alert_start_time_ms': alert_timestamp_ms,
                        'last_update_time_ms': alert_timestamp_ms
                    }
                    # logger.info(f"New alert {alert_key} (ID: {alert_data_for_db['id']}) created.")
                except Exception as e:
                    logger.error(f"Failed to insert new alert: {e}", exc_info=True)
                    ERRORS_TOTAL.labels(component='alert_manager', error_type='insert_alert_failed').inc()
                    return None

            self.recent_alerts[alert_key] = {
                'timestamp': current_time,
                'id': alert_data_for_db['id']
            }
            
            ALERTS_GENERATED_TOTAL.labels(symbol=symbol, alert_type=alert_type, severity=severity).inc()

            # Pass the full alert data (including snapshots/history) to MLDataCollector
            # MLDataCollector will decide what to do with it (e.g., store in TimescaleDB)
            full_alert_data_for_ml = {
                **alert_data_for_db,
                "order_book_snapshot": order_book_snapshot,
                "trade_history": trade_history
            }
            # Publish to MLDataCollector's queue
            await self.message_broker.publish("ml_alerts_queue", full_alert_data_for_ml)

            return full_alert_data_for_ml

    def _get_orderbook_summary(self, order_book_snapshot: Optional[Dict]) -> Dict[str, Any]:
        """Создает краткую сводку стакана ордеров."""
        if not order_book_snapshot or not order_book_snapshot.get('bids') or not order_book_snapshot.get('asks'):
            return {}
        bids = order_book_snapshot['bids']
        asks = order_book_snapshot['asks']
        summary = {
            'best_bid': bids[0][0] if bids else None,
            'best_ask': asks[0][0] if asks else None,
            'bid_depth_5': sum(s for p, s in bids[:5]),
            'ask_depth_5': sum(s for p, s in asks[:5]),
        }
        return {k: v for k, v in summary.items() if v is not None}

    def _get_trade_history_summary(self, trade_history: Optional[List[Dict]]) -> Dict[str, Any]:
        """Создает краткую сводку истории сделок."""
        if not trade_history:
            return {}
        recent_trades = [t for t in trade_history if time.time() - t.get('timestamp', 0) <= 60] # Last 60 seconds
        if not recent_trades:
            return {}
        buy_volume = sum(t.get('volume_usdt', 0) for t in recent_trades if t.get('side') == 'Buy')
        sell_volume = sum(t.get('volume_usdt', 0) for t in recent_trades if t.get('side') == 'Sell')
        total_volume = buy_volume + sell_volume
        summary = {
            'trade_count_60s': len(recent_trades),
            'total_volume_60s': total_volume,
            'buy_volume_60s': buy_volume,
            'sell_volume_60s': sell_volume,
            'volume_imbalance_60s': (buy_volume - sell_volume) / total_volume if total_volume > 0 else 0
        }
        return summary

    def get_status(self) -> Dict[str, Any]:
        """Получение статуса менеджера алертов."""
        status = {
            'is_running': True,  # AlertManager всегда работает, если инициализирован
            'recent_alerts_count': len(self.recent_alerts),
            'grouped_alerts_count': len(self.grouped_alerts),
            'alert_grouping_seconds': self.alert_grouping_seconds,
            'alert_cooldown_seconds': self.alert_cooldown_seconds,
            'alert_queue_name': self.alert_queue_name,
            'recent_alert_types': list(set(key.split('_', 1)[1] for key in self.recent_alerts.keys() if '_' in key)),
            'active_symbols': list(set(key.split('_', 1)[0] for key in self.recent_alerts.keys() if '_' in key))
        }
        return status



