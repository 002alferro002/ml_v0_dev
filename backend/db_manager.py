import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from asyncpg import Connection, create_pool, connect, exceptions
from exceptions import DatabaseError
import time

logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.pool = None
        # logger.info("DBManager initialized.")

    async def connect(self):
        """Подключение к базе данных и создание таблиц"""
        try:
            # Сначала используем одиночное соединение для инициализации
            conn = await connect(self.db_url)
            try:
                await self._enable_timescaledb_extension_single(conn)
                await self._create_tables_single(conn)
                # logger.info("Database initialization completed successfully.")
            finally:
                await conn.close()
            
            # Теперь создаем пул соединений
            self.pool = await create_pool(
                self.db_url, 
                min_size=1, 
                max_size=10,
                command_timeout=60,
                max_cached_statement_lifetime=0,  # Отключаем кэширование подготовленных запросов
                server_settings={
                    'application_name': 'ml_bybit_app',
                    'tcp_keepalives_idle': '600',
                    'tcp_keepalives_interval': '30',
                    'tcp_keepalives_count': '3'
                }
            )
            # logger.info("Database connection pool created successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to database or create tables: {e}", exc_info=True)
            raise DatabaseError(f"Failed to connect to database: {e}")

    async def disconnect(self):
        """Закрывает соединение с базой данных."""
        if self.pool:
            await self.pool.close()
            # logger.info("Database connection pool closed.")

    async def recreate_pool(self):
        """Пересоздает пул соединений для очистки кэшированных метаданных."""
        if self.pool:
            await self.pool.close()
            # logger.info("Closed existing database connection pool.")
        
        # Создаем новый пул соединений
        self.pool = await create_pool(
            self.db_url, 
            min_size=1, 
            max_size=10,
            command_timeout=60,
            max_cached_statement_lifetime=0,  # Отключаем кэширование подготовленных запросов
            server_settings={
                'application_name': 'ml_bybit_app',
                'tcp_keepalives_idle': '600',
                'tcp_keepalives_interval': '30',
                'tcp_keepalives_count': '3'
            }
        )
        # logger.info("Recreated database connection pool successfully.")

    async def _execute(self, query: str, *args, fetch: bool = False, fetchval: bool = False, operation_name: str = "generic_query"):
        """Вспомогательная функция для выполнения запросов с метриками и обработкой ошибок."""
        from metrics import DB_OPERATION_LATENCY, DB_OPERATIONS_TOTAL, ERRORS_TOTAL
        start_time = time.perf_counter()
        try:
            if not self.pool:
                raise RuntimeError("Database pool not initialized. Call connect() first.")
            async with self.pool.acquire() as conn:
                result = None
                if fetch:
                    result = await conn.fetch(query, *args)
                elif fetchval:
                    result = await conn.fetchval(query, *args)
                else:
                    await conn.execute(query, *args)
                DB_OPERATIONS_TOTAL.labels(operation=operation_name, status='success').inc()
                return result
        except exceptions.PostgresError as e:
            logger.error(f"Database error executing query: {query} with args {args}. Error: {e}", exc_info=True)
            DB_OPERATIONS_TOTAL.labels(operation=operation_name, status='failure').inc()
            ERRORS_TOTAL.labels(component='db_manager', error_type='postgres_error').inc()
            raise DatabaseError(f"PostgreSQL error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error executing query: {query} with args {args}. Error: {e}", exc_info=True)
            DB_OPERATIONS_TOTAL.labels(operation=operation_name, status='failure').inc()
            ERRORS_TOTAL.labels(component='db_manager', error_type='unexpected_error').inc()
            raise DatabaseError(f"Unexpected database error: {e}")
        finally:
            DB_OPERATION_LATENCY.labels(operation=operation_name).observe(time.perf_counter() - start_time)

    async def _enable_timescaledb_extension(self):
        """Включает расширение TimescaleDB."""
        await self._execute("CREATE EXTENSION IF NOT EXISTS timescaledb;", operation_name="enable_timescaledb")
        # logger.info("TimescaleDB extension enabled.")

    async def _create_tables(self):
        """Создает необходимые таблицы в базе данных, если они не существуют, включая гипертаблицы."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS watchlist (
                symbol VARCHAR(20) PRIMARY KEY,
                added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                alert_type VARCHAR(50) NOT NULL,
                severity VARCHAR(20) NOT NULL,
                description TEXT,
                alert_start_time_ms BIGINT NOT NULL,
                alert_end_time_ms BIGINT NOT NULL,
                data JSONB,
                snapshot_summary JSONB,
                trade_summary JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ml_training_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                features JSONB NOT NULL,
                target_price_change DOUBLE PRECISION NOT NULL,
                target_direction VARCHAR(10) NOT NULL,
                alert_id INTEGER REFERENCES alerts(id) ON DELETE SET NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ml_predictions (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                predicted_price_change DOUBLE PRECISION NOT NULL,
                predicted_direction VARCHAR(10) NOT NULL,
                confidence DOUBLE PRECISION NOT NULL,
                features JSONB,
                model_version INTEGER,
                reference_price DOUBLE PRECISION, -- Price at the moment of prediction
                actual_price_change DOUBLE PRECISION,
                is_correct BOOLEAN,
                predicted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                validated_at TIMESTAMP WITH TIME ZONE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS raw_trades (
                id BIGSERIAL,
                symbol TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                size DOUBLE PRECISION NOT NULL,
                side TEXT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
                data JSONB,
                PRIMARY KEY (id, received_at)
            );
            """,
            """
            SELECT create_hypertable('raw_trades', 'received_at', if_not_exists => TRUE);
            """,
            """
            CREATE TABLE IF NOT EXISTS raw_orderbooks (
                id BIGSERIAL,
                symbol TEXT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
                bids JSONB NOT NULL,
                asks JSONB NOT NULL,
                data JSONB,
                PRIMARY KEY (id, received_at)
            );
            """,
            """
            SELECT create_hypertable('raw_orderbooks', 'received_at', if_not_exists => TRUE);
            """
        ]
        for query in queries:
            await self._execute(query, operation_name="create_table")
        # logger.info("Database tables checked/created.")

    async def _enable_timescaledb_extension_single(self, conn):
        """Включает расширение TimescaleDB используя одиночное соединение."""
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        # logger.info("TimescaleDB extension enabled via single connection.")

    async def _create_tables_single(self, conn):
        """Создает необходимые таблицы используя одиночное соединение."""
        # Сначала создаем все обычные таблицы
        table_queries = [
            """
            CREATE TABLE IF NOT EXISTS watchlist (
                symbol VARCHAR(20) PRIMARY KEY,
                added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                alert_type VARCHAR(50) NOT NULL,
                severity VARCHAR(20) NOT NULL,
                description TEXT,
                alert_start_time_ms BIGINT NOT NULL,
                alert_end_time_ms BIGINT NOT NULL,
                data JSONB,
                snapshot_summary JSONB,
                trade_summary JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ml_training_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                features JSONB NOT NULL,
                target_price_change DOUBLE PRECISION NOT NULL,
                target_direction VARCHAR(10) NOT NULL,
                alert_id INTEGER REFERENCES alerts(id) ON DELETE SET NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ml_predictions (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                predicted_price_change DOUBLE PRECISION NOT NULL,
                predicted_direction VARCHAR(10) NOT NULL,
                confidence DOUBLE PRECISION NOT NULL,
                features JSONB,
                model_version INTEGER,
                reference_price DOUBLE PRECISION, -- Price at the moment of prediction
                actual_price_change DOUBLE PRECISION,
                is_correct BOOLEAN,
                predicted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                validated_at TIMESTAMP WITH TIME ZONE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS futures_symbols (
                symbol VARCHAR(20) PRIMARY KEY,
                base_coin VARCHAR(20) NOT NULL,
                quote_coin VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL,
                contract_type VARCHAR(20),
                launch_time BIGINT,
                delivery_time BIGINT,
                min_price DECIMAL(20, 8),
                max_price DECIMAL(20, 8),
                tick_size DECIMAL(20, 8),
                min_order_qty DECIMAL(20, 8),
                max_order_qty DECIMAL(20, 8),
                is_enabled BOOLEAN DEFAULT true,
                last_updated TIMESTAMPTZ DEFAULT NOW()
            );
            
            -- Update existing table structure if needed
            ALTER TABLE futures_symbols ALTER COLUMN base_coin TYPE VARCHAR(20);
            ALTER TABLE futures_symbols ALTER COLUMN quote_coin TYPE VARCHAR(20);
            """,
            """
            CREATE TABLE IF NOT EXISTS symbol_filters (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL UNIQUE,
                description TEXT,
                filter_criteria JSONB NOT NULL,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS raw_trades (
                id BIGSERIAL,
                symbol TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                size DOUBLE PRECISION NOT NULL,
                side TEXT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
                data JSONB,
                PRIMARY KEY (id, received_at)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS raw_orderbooks (
                id BIGSERIAL,
                symbol TEXT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
                bids JSONB NOT NULL,
                asks JSONB NOT NULL,
                data JSONB,
                PRIMARY KEY (id, received_at)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS futures_symbols (
                symbol VARCHAR(20) PRIMARY KEY,
                base_coin VARCHAR(10) NOT NULL,
                quote_coin VARCHAR(10) NOT NULL,
                status VARCHAR(20) NOT NULL,
                contract_type VARCHAR(20),
                launch_time BIGINT,
                delivery_time BIGINT,
                min_price DECIMAL(20, 8),
                max_price DECIMAL(20, 8),
                tick_size DECIMAL(20, 8),
                min_order_qty DECIMAL(20, 8),
                max_order_qty DECIMAL(20, 8),
                is_enabled BOOLEAN DEFAULT true,
                last_updated TIMESTAMPTZ DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS symbol_filters (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL UNIQUE,
                description TEXT,
                filter_criteria JSONB NOT NULL,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        ]
        
        # Создаем обычные таблицы
        for query in table_queries:
            await conn.execute(query)
        
        # Затем создаем hypertables
        hypertable_queries = [
            "SELECT create_hypertable('raw_trades', 'received_at', if_not_exists => TRUE);",
            "SELECT create_hypertable('raw_orderbooks', 'received_at', if_not_exists => TRUE);"
        ]
        
        for query in hypertable_queries:
            try:
                await conn.execute(query)
            except Exception as e:
                logger.warning(f"Failed to create hypertable: {e}")
        
        # logger.info("Database tables checked/created via single connection.")

    async def add_to_watchlist(self, symbol: str):
        """Добавляет символ в список отслеживания."""
        await self._execute(
            "INSERT INTO watchlist (symbol) VALUES ($1) ON CONFLICT (symbol) DO NOTHING;",
            symbol, operation_name="add_to_watchlist"
        )
        # logger.info(f"Symbol {symbol} added to watchlist.")

    async def remove_from_watchlist(self, symbol: str):
        """Удаляет символ из списка отслеживания."""
        await self._execute("DELETE FROM watchlist WHERE symbol = $1;", symbol, operation_name="remove_from_watchlist")
        # logger.info(f"Symbol {symbol} removed from watchlist.")

    async def get_watchlist_symbols(self) -> List[str]:
        """Возвращает список всех отслеживаемых символов с фильтрацией."""
        from utils.symbol_filter import filter_symbols
        
        rows = await self._execute("SELECT symbol FROM watchlist;", fetch=True, operation_name="get_watchlist_symbols")
        symbols = [row['symbol'] for row in rows]
        
        # Фильтруем символы, исключая проблемные
        filtered_symbols = filter_symbols(symbols)
        
        if len(filtered_symbols) != len(symbols):
            logger.info(f"Filtered watchlist symbols: {len(symbols)} -> {len(filtered_symbols)}")
        
        return filtered_symbols

    async def insert_alert(self, alert_data: Dict[str, Any]) -> int:
        """Вставляет новый алерт в базу данных."""
        query = """
            INSERT INTO alerts (symbol, alert_type, severity, description, alert_start_time_ms, 
                                alert_end_time_ms, data, snapshot_summary, trade_summary)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id;
        """
        alert_id = await self._execute(
            query,
            alert_data['symbol'],
            alert_data['alert_type'],
            alert_data['severity'],
            alert_data['description'],
            alert_data['alert_timestamp_ms'],
            alert_data['alert_timestamp_ms'], # For new alerts, start and end are the same
            json.dumps(alert_data['data']),
            json.dumps(alert_data.get('snapshot_summary', {})),
            json.dumps(alert_data.get('trade_summary', {})),
            fetchval=True, operation_name="insert_alert"
        )
        logger.debug(f"Inserted new alert with ID: {alert_id}")
        return alert_id

    async def update_grouped_alert(self, alert_id: int, new_severity: str, new_description: str,
                                   new_data: Dict[str, Any], alert_end_time_ms: int,
                                   snapshot_summary: Dict[str, Any], trade_summary: Dict[str, Any]):
        """Обновляет существующий сгруппированный алерт."""
        query = """
            UPDATE alerts
            SET severity = $1,
                description = $2,
                data = data || $3::jsonb, -- Merge new data into existing JSONB
                alert_end_time_ms = $4,
                snapshot_summary = $5,
                trade_summary = $6,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $7;
        """
        await self._execute(
            query,
            new_severity,
            new_description,
            json.dumps(new_data),
            alert_end_time_ms,
            json.dumps(snapshot_summary),
            json.dumps(trade_summary),
            alert_id, operation_name="update_grouped_alert"
        )
        logger.debug(f"Updated grouped alert with ID: {alert_id}")

    async def get_alerts(self, limit: int = 100, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Возвращает последние алерты."""
        query = "SELECT * FROM alerts"
        params = []
        if symbol:
            query += " WHERE symbol = $1"
            params.append(symbol)
        query += " ORDER BY alert_start_time_ms DESC LIMIT $1 OFFSET 0;"
        params.append(limit)

        rows = await self._execute(query, *params, fetch=True, operation_name="get_alerts")
        return [dict(row) for row in rows]

    async def insert_ml_training_data(self, symbol: str, features: Dict[str, Any],
                                       target_price_change: float, target_direction: str,
                                       alert_id: Optional[int] = None):
        """Вставляет данные для обучения ML модели."""
        query = """
            INSERT INTO ml_training_data (symbol, features, target_price_change, target_direction, alert_id)
            VALUES ($1, $2, $3, $4, $5);
        """
        await self._execute(
            query,
            symbol,
            json.dumps(features),
            target_price_change,
            target_direction,
            alert_id, operation_name="insert_ml_training_data"
        )
        logger.debug(f"Inserted ML training data for {symbol}")

    async def get_ml_training_data(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Возвращает данные для обучения ML модели."""
        query = """
            SELECT id, symbol, features, target_price_change, target_direction, created_at
            FROM ml_training_data
            ORDER BY created_at DESC
            LIMIT $1;
        """
        rows = await self._execute(query, limit, fetch=True, operation_name="get_ml_training_data")
        return [dict(row) for row in rows]

    async def save_ml_prediction(self, symbol: str, predicted_price_change: float,
                                 predicted_direction: str, confidence: float,
                                 features: str, model_version: int, reference_price: float) -> int:
        """Сохраняет прогноз ML модели в базу данных."""
        query = """
            INSERT INTO ml_predictions (symbol, predicted_price_change, predicted_direction, confidence, features, model_version, reference_price)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id;
        """
        prediction_id = await self._execute(
            query,
            symbol,
            predicted_price_change,
            predicted_direction,
            confidence,
            features,
            model_version,
            reference_price,
            fetchval=True, operation_name="save_ml_prediction"
        )
        logger.debug(f"Saved ML prediction with ID: {prediction_id}")
        return prediction_id

    async def update_ml_prediction_result(self, prediction_id: int, actual_price_change: float,
                                          is_correct: bool, validated_at: datetime):
        """Обновляет результат валидации прогноза ML модели."""
        query = """
            UPDATE ml_predictions
            SET actual_price_change = $1,
                is_correct = $2,
                validated_at = $3
            WHERE id = $4;
        """
        await self._execute(
            query,
            actual_price_change,
            is_correct,
            validated_at,
            prediction_id, operation_name="update_ml_prediction_result"
        )
        logger.debug(f"Updated ML prediction result for ID: {prediction_id}")

    async def get_ml_predictions(self, limit: int = 100, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Возвращает историю прогнозов ML."""
        query = "SELECT * FROM ml_predictions"
        params = []
        if symbol:
            query += " WHERE symbol = $1"
            params.append(symbol)
        query += " ORDER BY predicted_at DESC LIMIT $1 OFFSET 0;"
        params.append(limit)

        rows = await self._execute(query, *params, fetch=True, operation_name="get_ml_predictions")
        return [dict(row) for row in rows]

    async def insert_raw_trade(self, trade: Dict[str, Any]) -> None:
        """Вставляет сырые данные сделки в гипертаблицу raw_trades."""
        # Convert timestamp to milliseconds
        timestamp_ms = int(trade['timestamp'] * 1000)
        
        # Используем прямое выполнение без подготовленных запросов
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    "INSERT INTO raw_trades (symbol, timestamp_ms, price, size, side, trade_id, data) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    trade['symbol'],
                    timestamp_ms,
                    float(trade['price']),
                    float(trade['size']),
                    trade['side'],
                    trade['trade_id'],
                    json.dumps(trade)
                )
                logger.debug(f"Inserted raw trade {trade['trade_id']} for {trade['symbol']}")
            except Exception as e:
                logger.error(f"Error inserting raw trade: {e}")
                raise DatabaseError(f"PostgreSQL error: {e}")

    async def insert_raw_orderbook(self, orderbook: Dict[str, Any]) -> None:
        """Вставляет сырые данные ордербука в гипертаблицу raw_orderbooks."""
        # Convert timestamp to milliseconds
        timestamp_ms = int(orderbook['timestamp'] * 1000)
        
        # Используем прямое выполнение без подготовленных запросов
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    "INSERT INTO raw_orderbooks (symbol, timestamp_ms, bids, asks, data) VALUES ($1, $2, $3, $4, $5)",
                    orderbook['symbol'],
                    timestamp_ms,
                    json.dumps(orderbook['bids']),
                    json.dumps(orderbook['asks']),
                    json.dumps(orderbook)
                )
                logger.debug(f"Inserted raw orderbook for {orderbook['symbol']} at timestamp {timestamp_ms}")
            except Exception as e:
                logger.error(f"Error inserting raw orderbook: {e}")
                raise DatabaseError(f"PostgreSQL error: {e}")

    async def get_historical_trades(self, symbol: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Получает исторические сделки из гипертаблицы raw_trades."""
        query = """
            SELECT received_at as time, symbol, price, size, side, timestamp_ms, trade_id
            FROM raw_trades
            WHERE symbol = $1 AND received_at >= $2 AND received_at <= $3
            ORDER BY received_at ASC;
        """
        rows = await self._execute(query, symbol, start_time, end_time, fetch=True, operation_name="get_historical_trades")
        return [dict(row) for row in rows]

    async def get_historical_orderbooks(self, symbol: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Получает исторические снимки стакана ордеров из гипертаблицы raw_orderbooks."""
        query = """
            SELECT received_at as time, symbol, bids, asks
            FROM raw_orderbooks
            WHERE symbol = $1 AND received_at >= $2 AND received_at <= $3
            ORDER BY received_at ASC;
        """
        rows = await self._execute(query, symbol, start_time, end_time, fetch=True, operation_name="get_historical_orderbooks")
        result = []
        for row in rows:
            row_dict = dict(row)
            try:
                # Parse JSON strings back to Python objects
                row_dict['bids'] = json.loads(row_dict['bids']) if isinstance(row_dict['bids'], str) else row_dict['bids']
                row_dict['asks'] = json.loads(row_dict['asks']) if isinstance(row_dict['asks'], str) else row_dict['asks']
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Error parsing bids/asks JSON for {symbol}: {e}")
                # Set empty lists as fallback
                row_dict['bids'] = []
                row_dict['asks'] = []
            result.append(row_dict)
        return result

    async def upsert_futures_symbol(self, symbol_data: Dict[str, Any]):
        """Вставляет или обновляет информацию о фьючерсном символе."""
        from utils.symbol_filter import is_symbol_allowed
        
        symbol = symbol_data['symbol']
        contract_type = symbol_data.get('contract_type')
        status = symbol_data['status']
        
        # Проверяем, разрешен ли символ
        if not is_symbol_allowed(symbol, contract_type, status):
            logger.info(f"Skipping upsert for excluded symbol: {symbol}")
            return
        
        query = """
            INSERT INTO futures_symbols (
                symbol, base_coin, quote_coin, status, contract_type,
                launch_time, delivery_time, min_price, max_price, tick_size,
                min_order_qty, max_order_qty, is_enabled
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (symbol) DO UPDATE SET
                base_coin = EXCLUDED.base_coin,
                quote_coin = EXCLUDED.quote_coin,
                status = EXCLUDED.status,
                contract_type = EXCLUDED.contract_type,
                launch_time = EXCLUDED.launch_time,
                delivery_time = EXCLUDED.delivery_time,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                tick_size = EXCLUDED.tick_size,
                min_order_qty = EXCLUDED.min_order_qty,
                max_order_qty = EXCLUDED.max_order_qty,
                is_enabled = EXCLUDED.is_enabled,
                last_updated = NOW();
        """
        await self._execute(
            query,
            symbol_data['symbol'],
            symbol_data['base_coin'],
            symbol_data['quote_coin'],
            symbol_data['status'],
            symbol_data.get('contract_type'),
            symbol_data.get('launch_time'),
            symbol_data.get('delivery_time'),
            symbol_data.get('min_price'),
            symbol_data.get('max_price'),
            symbol_data.get('tick_size'),
            symbol_data.get('min_order_qty'),
            symbol_data.get('max_order_qty'),
            symbol_data.get('is_enabled', True),
            operation_name="upsert_futures_symbol"
        )
        logger.debug(f"Upserted futures symbol: {symbol_data['symbol']}")
        
        # Автоматическая синхронизация watchlist
        await self._sync_watchlist_with_futures_symbols()

    async def get_futures_symbols(self, status: Optional[str] = None, enabled_only: bool = True, symbol_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получает список фьючерсных символов с фильтрацией."""
        query = "SELECT * FROM futures_symbols WHERE 1=1"
        params = []
        
        if enabled_only:
            query += " AND is_enabled = true"
        
        if status:
            query += f" AND status = ${len(params) + 1}"
            params.append(status)
        
        if symbol_filter:
            query += f" AND symbol = ${len(params) + 1}"
            params.append(symbol_filter)
        
        query += " ORDER BY symbol"
        
        rows = await self._execute(query, *params, fetch=True, operation_name="get_futures_symbols")
        return [dict(row) for row in rows]
    
    async def _sync_watchlist_with_futures_symbols(self):
        """Автоматически синхронизирует watchlist с включенными символами из futures_symbols.
        
        Returns:
            tuple: (symbols_added, symbols_removed) - списки добавленных и удаленных символов
        """
        try:
            from utils.symbol_filter import filter_symbols
            
            # Получаем все включенные символы из futures_symbols с дополнительными данными для фильтрации
            enabled_symbols_query = "SELECT symbol, contract_type, status FROM futures_symbols WHERE is_enabled = true AND status = 'Trading'"
            enabled_rows = await self._execute(enabled_symbols_query, fetch=True, operation_name="get_enabled_symbols")
            
            # Фильтруем символы, исключая проблемные
            symbols_data = [dict(row) for row in enabled_rows]
            all_enabled_symbols = [row['symbol'] for row in enabled_rows]
            filtered_enabled_symbols = filter_symbols(all_enabled_symbols, symbols_data)
            enabled_symbols = set(filtered_enabled_symbols)
            
            if len(filtered_enabled_symbols) != len(all_enabled_symbols):
                logger.info(f"Filtered enabled symbols for watchlist sync: {len(all_enabled_symbols)} -> {len(filtered_enabled_symbols)}")
            
            # Получаем текущие символы в watchlist
            watchlist_rows = await self._execute("SELECT symbol FROM watchlist", fetch=True, operation_name="get_current_watchlist")
            current_watchlist = {row['symbol'] for row in watchlist_rows}
            
            # Определяем символы для добавления и удаления
            symbols_to_add = enabled_symbols - current_watchlist
            symbols_to_remove = current_watchlist - enabled_symbols
            
            # Добавляем новые символы
            if symbols_to_add:
                for symbol in symbols_to_add:
                    await self._execute(
                        "INSERT INTO watchlist (symbol) VALUES ($1) ON CONFLICT (symbol) DO NOTHING",
                        symbol, operation_name="add_symbol_to_watchlist"
                    )
                # logger.info(f"Added {len(symbols_to_add)} symbols to watchlist: {list(symbols_to_add)}")
            
            # Удаляем отключенные символы
            if symbols_to_remove:
                for symbol in symbols_to_remove:
                    await self._execute(
                        "DELETE FROM watchlist WHERE symbol = $1",
                        symbol, operation_name="remove_symbol_from_watchlist"
                    )
                # logger.info(f"Removed {len(symbols_to_remove)} symbols from watchlist: {list(symbols_to_remove)}")
            
            if not symbols_to_add and not symbols_to_remove:
                logger.debug("Watchlist is already in sync with enabled futures symbols")
            
            return list(symbols_to_add), list(symbols_to_remove)
                
        except Exception as e:
            logger.error(f"Error syncing watchlist with futures symbols: {e}", exc_info=True)
            return [], []
    
    async def sync_watchlist_with_futures_symbols(self):
        """Публичный метод для ручной синхронизации watchlist."""
        await self._sync_watchlist_with_futures_symbols()
    
    async def remove_from_watchlist(self, symbol: str):
        """Удаляет символ из списка отслеживания."""
        await self._execute(
            "DELETE FROM watchlist WHERE symbol = $1",
            symbol, operation_name="remove_from_watchlist"
        )
        # logger.info(f"Symbol {symbol} removed from watchlist.")
    
    async def toggle_futures_symbol_status(self, symbol: str) -> bool:
        """Переключает статус символа и автоматически синхронизирует watchlist."""
        # Получаем текущий статус
        current_status_query = "SELECT is_enabled FROM futures_symbols WHERE symbol = $1"
        rows = await self._execute(current_status_query, symbol, fetch=True, operation_name="get_symbol_status")
        
        if not rows:
            raise DatabaseError(f"Symbol {symbol} not found in futures_symbols")
        
        current_status = rows[0]['is_enabled']
        new_status = not current_status
        
        # Обновляем статус
        await self._execute(
            "UPDATE futures_symbols SET is_enabled = $1, last_updated = NOW() WHERE symbol = $2",
            new_status, symbol, operation_name="toggle_symbol_status"
        )
        
        # Автоматически синхронизируем watchlist
        symbols_added, symbols_removed = await self._sync_watchlist_with_futures_symbols()
        
        # logger.info(f"Symbol {symbol} {'enabled' if new_status else 'disabled'} and watchlist synced")
        return new_status, symbols_added, symbols_removed

    async def create_symbol_filter(self, name: str, description: str, filter_criteria: Dict[str, Any]):
        """Создает новый фильтр символов."""
        query = """
            INSERT INTO symbol_filters (name, description, filter_criteria)
            VALUES ($1, $2, $3)
            ON CONFLICT (name) DO UPDATE SET
                description = EXCLUDED.description,
                filter_criteria = EXCLUDED.filter_criteria,
                updated_at = NOW();
        """
        await self._execute(
            query,
            name,
            description,
            json.dumps(filter_criteria),
            operation_name="create_symbol_filter"
        )
        logger.debug(f"Created/updated symbol filter: {name}")

    async def get_symbol_filters(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Получает список фильтров символов."""
        query = "SELECT * FROM symbol_filters"
        if active_only:
            query += " WHERE is_active = true"
        query += " ORDER BY name"
        
        rows = await self._execute(query, fetch=True, operation_name="get_symbol_filters")
        return [dict(row) for row in rows]

    async def apply_symbol_filter(self, filter_name: str) -> List[str]:
        """Применяет фильтр к символам и возвращает отфильтрованный список."""
        # Получаем критерии фильтра
        filter_query = "SELECT filter_criteria FROM symbol_filters WHERE name = $1 AND is_active = true"
        filter_row = await self._execute(filter_query, filter_name, fetch=True, operation_name="get_filter_criteria")
        
        if not filter_row:
            logger.warning(f"Filter '{filter_name}' not found or inactive")
            return []
        
        criteria = json.loads(filter_row[0]['filter_criteria'])
        
        # Строим динамический запрос на основе критериев
        query = "SELECT symbol FROM futures_symbols WHERE is_enabled = true"
        params = []
        
        if 'status' in criteria:
            query += f" AND status = ANY(${len(params) + 1})"
            params.append(criteria['status'])
        
        if 'base_coins' in criteria:
            query += f" AND base_coin = ANY(${len(params) + 1})"
            params.append(criteria['base_coins'])
        
        if 'quote_coins' in criteria:
            query += f" AND quote_coin = ANY(${len(params) + 1})"
            params.append(criteria['quote_coins'])
        
        if 'contract_types' in criteria:
            query += f" AND contract_type = ANY(${len(params) + 1})"
            params.append(criteria['contract_types'])
        
        if 'min_volume' in criteria:
            # Здесь можно добавить логику для фильтрации по объему торгов
            # Требует дополнительных данных о торговых объемах
            pass
        
        query += " ORDER BY symbol"
        
        rows = await self._execute(query, *params, fetch=True, operation_name="apply_symbol_filter")
        symbols = [row['symbol'] for row in rows]
        
        # logger.info(f"Filter '{filter_name}' returned {len(symbols)} symbols")
        return symbols


