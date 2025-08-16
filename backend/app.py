import asyncio
import logging
import os
import json
import time
import sys
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

# Принудительная очистка буферов для немедленного вывода логов
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)
from fastapi import FastAPI, HTTPException, status, WebSocket, WebSocketDisconnect, APIRouter, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest

from db_manager import DBManager
from pybit_client import PybitAPIClient, PybitWebSocketClient
from mcp_integration import MCPIntegration
from manipulation.trade_processor import TradeProcessor
from manipulation.orderbook_analyzer import OrderBookAnalyzer
from manipulation.alert_manager import AlertManager
from manipulation.ml.ml_manager import MLManager, MLMode
from manipulation.ml.data_collector import MLDataCollector
from manipulation.anomaly_detector import AnomalyDetector # New: Import AnomalyDetector
from message_broker import MessageBroker
from batch_integration import BatchIntegration # New: Import BatchIntegration
from exceptions import AppError, DatabaseError, WebSocketError, MLPredictionError, AnomalyDetectionError, DataProcessingError, ConfigurationError
from metrics import ACTIVE_WATCHLIST_SYMBOLS, ERRORS_TOTAL, get_metrics_text

# Load environment variables
load_dotenv()

# Configure logging с принудительной очисткой буфера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

# Принудительная очистка буферов после каждого лога
for handler in logging.root.handlers:
    if hasattr(handler, 'stream'):
        handler.stream.reconfigure(line_buffering=True)

# Настройка уровней логирования для внешних библиотек
logging.getLogger('aiormq').setLevel(logging.WARNING)
logging.getLogger('aio_pika').setLevel(logging.WARNING)
logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('pybit').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

# Настройка уровня для нашего приложения
logging.getLogger('__main__').setLevel(logging.INFO)
logging.getLogger('manipulation').setLevel(logging.INFO)
logging.getLogger('batch_integration').setLevel(logging.INFO)
logging.getLogger('batch_processor').setLevel(logging.INFO)
logging.getLogger('message_broker').setLevel(logging.INFO)
logging.getLogger('pybit_client').setLevel(logging.INFO)
logging.getLogger('db_manager').setLevel(logging.INFO)

logger = logging.getLogger(__name__)

# Global instances
db_manager: Optional[DBManager] = None
message_broker: Optional[MessageBroker] = None
bybit_ws_client: Optional[PybitWebSocketClient] = None
bybit_api_client: Optional[PybitAPIClient] = None
mcp_integration: Optional[MCPIntegration] = None
trade_processor: Optional[TradeProcessor] = None
orderbook_analyzer: Optional[OrderBookAnalyzer] = None
alert_manager: Optional[AlertManager] = None
ml_manager: Optional[MLManager] = None
ml_data_collector: Optional[MLDataCollector] = None
anomaly_detector: Optional[AnomalyDetector] = None
batch_integration: Optional[BatchIntegration] = None # New: BatchIntegration

# Helper function for WebSocket subscription sync
async def sync_websocket_subscriptions(symbols_added: List[str], symbols_removed: List[str]):
    """Синхронизирует WebSocket подписки с изменениями в watchlist."""
    try:
        # Подписываемся на новые символы
        if symbols_added:
            for symbol in symbols_added:
                await bybit_ws_client.subscribe_symbol(symbol)
            # logger.info(f"Subscribed to symbols: {symbols_added}")
        
        # Отписываемся от удаленных символов ПАКЕТНО
        if symbols_removed:
            # Используем пакетную отписку для лучшей производительности
            if hasattr(bybit_ws_client, 'unsubscribe_from_symbols_batch'):
                await bybit_ws_client.unsubscribe_from_symbols_batch(symbols_removed)
                logger.info(f"Batch unsubscribed from {len(symbols_removed)} symbols")
            else:
                # Fallback к старому методу если пакетный недоступен
                for symbol in symbols_removed:
                    await bybit_ws_client.unsubscribe_symbol(symbol)
                logger.info(f"Unsubscribed from {len(symbols_removed)} symbols (fallback)")
        
        # Обновляем метрику активных символов
        symbols = await db_manager.get_watchlist_symbols()
        ACTIVE_WATCHLIST_SYMBOLS.set(len(symbols))
        
    except Exception as e:
        logger.error(f"Error syncing WebSocket subscriptions: {e}", exc_info=True)

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        self.active_connections.append(websocket)
        logger.info(f"WebSocket added to manager. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        if websocket not in self.active_connections:
            return
        
        try:
            # Check if websocket is still connected
            if websocket.client_state.name != 'CONNECTED':
                self.disconnect(websocket)
                return
            
            await websocket.send_text(message)
        except Exception as e:
            logger.debug(f"Error sending personal message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: str):
        if not self.active_connections:
            return
            
        disconnected = []
        for connection in self.active_connections.copy():  # Use copy to avoid modification during iteration
            try:
                # More robust check for websocket connection state
                if (hasattr(connection, 'client_state') and 
                    hasattr(connection.client_state, 'name') and
                    connection.client_state.name != 'CONNECTED'):
                    disconnected.append(connection)
                    continue
                
                # Additional check for application state
                if (hasattr(connection, 'application_state') and 
                    hasattr(connection.application_state, 'name') and
                    connection.application_state.name != 'CONNECTED'):
                    disconnected.append(connection)
                    continue
                
                # Try to send a ping first to check if connection is alive
                try:
                    await connection.ping()
                except:
                    disconnected.append(connection)
                    continue
                    
                await connection.send_text(message)
            except Exception as e:
                logger.debug(f"Error broadcasting message to connection: {e}")
                disconnected.append(connection)
        
        # Remove disconnected connections
        for connection in disconnected:
            self.disconnect(connection)

manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Контекстный менеджер для управления жизненным циклом приложения.
    Инициализирует и очищает ресурсы.
    """
    global db_manager, message_broker, bybit_ws_client, bybit_api_client, trade_processor, \
           orderbook_analyzer, alert_manager, ml_manager, ml_data_collector, anomaly_detector, batch_integration

    # Принудительная очистка буферов
    sys.stdout.flush()
    sys.stderr.flush()
    
    logger.info("=== ЗАПУСК ПРИЛОЖЕНИЯ ===")
    print("Инициализация компонентов...")  # Прямой вывод для тестирования

    # Initialize DB Manager
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ConfigurationError("DATABASE_URL environment variable not set.")
    db_manager = DBManager(db_url)
    # Retry connection with delays for TimescaleDB initialization
    max_retries = 5
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            await db_manager.connect()
            # Пересоздаем пул соединений для очистки кэшированных метаданных
            await db_manager.recreate_pool()
            break
        except DatabaseError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.critical(f"Failed to connect to database after {max_retries} attempts: {e}")
                exit(1) # Exit if DB connection fails

    # Initialize Message Broker with retry logic (optional for local testing)
    rabbitmq_url = os.getenv("RABBITMQ_URL")
    message_broker = None
    if rabbitmq_url:
        message_broker = MessageBroker(rabbitmq_url)
        
        # Retry RabbitMQ connection
        max_retries = 5
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                await message_broker.connect()
                # Declare queues
                await message_broker.declare_queue("raw_bybit_messages")
                await message_broker.declare_queue("processed_trades_queue")
                await message_broker.declare_queue("processed_orderbooks_queue")
                await message_broker.declare_queue("alerts_queue") # Alerts from processors to AlertManager
                await message_broker.declare_queue("ml_alerts_queue") # Alerts from AlertManager to MLDataCollector
                await message_broker.declare_queue("processed_features_for_anomaly_detection") # Features for Anomaly Detector
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"RabbitMQ connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.warning(f"Failed to connect to RabbitMQ after {max_retries} attempts: {e}. Running without message broker.")
                    message_broker = None
                    break
    else:
        logger.info("RabbitMQ URL not provided. Running without message broker.")

    # Initialize Bybit API Client with pybit
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    testnet = os.getenv("BYBIT_TESTNET", "false").lower() == "true"
    bybit_api_client = PybitAPIClient(api_key=api_key, api_secret=api_secret, testnet=testnet)
    
    # Initialize Alert Manager
    alert_manager = AlertManager(db_manager, message_broker)

    # Initialize Trade Processor and Order Book Analyzer
    trade_processor = TradeProcessor(alert_manager, message_broker)
    orderbook_analyzer = OrderBookAnalyzer(alert_manager, message_broker, trade_processor)
    
    # Set broadcast callbacks for streaming data
    trade_processor.set_broadcast_callback(broadcast_streaming_data)
    orderbook_analyzer.set_broadcast_callback(broadcast_streaming_data)
    
    # Загружаем информацию о символах для OrderBookAnalyzer
    try:
        # logger.info("Загружаем информацию о символах из Bybit API...")
        instruments_list = await bybit_api_client.get_all_futures_symbols()
        instruments_info = {}
        for instrument in instruments_list:
            symbol = instrument.get("symbol")
            if symbol:
                instruments_info[symbol] = instrument
                orderbook_analyzer.update_symbol_info(symbol, instrument)
        # logger.info(f"Загружена информация о {len(instruments_info)} символах")
    except Exception as e:
        logger.warning(f"Не удалось загрузить информацию о символах: {e}")
        # Продолжаем работу без информации о символах

    # Initialize ML components (without anomaly_detector first)
    logger.info("Инициализация ML компонентов...")
    ml_manager = MLManager(db_manager, trade_processor, orderbook_analyzer, None) # Pass None for anomaly_detector initially
    logger.info("MLManager создан")
    
    ml_data_collector = MLDataCollector(db_manager, ml_manager.ml_model, ml_manager.feature_engineer,
                                        message_broker, trade_processor, orderbook_analyzer)
    logger.info("MLDataCollector создан")
    
    ml_manager.set_data_collector(ml_data_collector) # Set data collector in MLManager
    logger.info("MLDataCollector установлен в MLManager")

    # Initialize Anomaly Detector
    anomaly_detector = AnomalyDetector(alert_manager, ml_manager.feature_engineer, message_broker) # Pass feature_engineer
    
    # Set anomaly_detector in MLManager
    ml_manager.anomaly_detector = anomaly_detector

    # Initialize Batch Integration for batch and multi-threaded processing
    batch_integration = BatchIntegration(
        trade_processor=trade_processor,
        orderbook_analyzer=orderbook_analyzer,
        alert_manager=alert_manager,
        db_manager=db_manager,
        message_broker=message_broker
    )
    
    # Start batch processing
    await batch_integration.start()
    logger.info("Batch integration started successfully")

    # Initialize Bybit WebSocket Client with pybit
    # Сохраняем ссылку на главный event loop
    main_event_loop = asyncio.get_event_loop()
    
    # Create message publisher function for WebSocket client
    def publish_raw_message(data: dict):
        try:
            topic = data.get('topic', 'unknown')
            if topic == 'unknown':
                logger.info(f"Publishing message without topic: {data}")
            else:
                logger.debug(f"Publishing raw message: topic={topic}, data_size={len(str(data))}")
            
            # Используем сохраненный главный event loop
            try:
                if main_event_loop.is_running():
                    # Планируем выполнение корутины в главном потоке
                    future = asyncio.run_coroutine_threadsafe(
                        message_broker.publish("raw_bybit_messages", data), main_event_loop
                    )
                    # Не ждем результат, чтобы не блокировать WebSocket поток
                    logger.debug(f"Сообщение запланировано для публикации: {topic}")
                else:
                    logger.warning(f"Главный event loop не запущен для публикации: {topic}")
            except Exception as loop_error:
                logger.error(f"Ошибка при работе с event loop: {loop_error}")
                
        except Exception as e:
            logger.error(f"Ошибка публикации сообщения: {e}", exc_info=True)
    
    bybit_ws_client = PybitWebSocketClient(publish_raw_message, testnet=testnet)

    # Create message processor function for raw messages
    async def process_raw_message(data: dict):
        # Process different types of messages
        if 'topic' in data:
            topic = data['topic']
            logger.info(f"Processing raw message: topic={topic}, has_data={'data' in data}")
            if 'publicTrade' in topic:
                # Process trade data
                if 'data' in data:
                    logger.info(f"Processing {len(data['data'])} trades for topic {topic}")
                    for trade in data['data']:
                        await trade_processor.process_trade(trade)
                else:
                    logger.warning(f"Trade message without data: {data}")
            elif 'orderbook' in topic:
                # Process orderbook data
                if 'data' in data:
                    logger.info(f"Processing orderbook update for topic {topic}")
                    logger.debug(f"Orderbook data structure: {list(data['data'].keys()) if isinstance(data['data'], dict) else type(data['data'])}")
                    await orderbook_analyzer.process_orderbook_update(data['data'])
                else:
                    logger.warning(f"Orderbook message without data: {data}")
        else:
            logger.info(f"Message without topic: {data}")
            pass

    # Start consumers for raw data in background tasks
    try:
        logger.info("Starting message broker consumers...")
        
        # Create background tasks for consumers to avoid blocking
        consumer_tasks = []
        
        async def start_consumer(queue_name: str, callback):
            try:
                await message_broker.consume(queue_name, callback)
            except Exception as e:
                logger.error(f"Consumer for {queue_name} failed: {e}")
        
        consumer_tasks.append(asyncio.create_task(start_consumer("raw_bybit_messages", process_raw_message)))
        logger.info("Started consumer task for raw_bybit_messages")
        
        consumer_tasks.append(asyncio.create_task(start_consumer("processed_trades_queue", db_manager.insert_raw_trade)))
        logger.info("Started consumer task for processed_trades_queue")
        
        consumer_tasks.append(asyncio.create_task(start_consumer("processed_orderbooks_queue", db_manager.insert_raw_orderbook)))
        logger.info("Started consumer task for processed_orderbooks_queue")
        
        consumer_tasks.append(asyncio.create_task(start_consumer("alerts_queue", alert_manager._process_incoming_alert)))
        logger.info("Started consumer task for alerts_queue")
        
        # Store tasks for cleanup
        app.state.consumer_tasks = consumer_tasks
        
        logger.info("All message broker consumer tasks started successfully")
    except Exception as e:
        logger.critical(f"Failed to start message broker consumers: {e}")
        exit(1)

    # Start core components
    try:
        await bybit_ws_client.start()
        
        # Subscribe to all symbols in watchlist
        watchlist_symbols = await db_manager.get_watchlist_symbols()
        if watchlist_symbols:
            logger.info(f"Subscribing to {len(watchlist_symbols)} symbols from watchlist")
            for symbol in watchlist_symbols:
                await bybit_ws_client.subscribe_symbol(symbol)
            logger.info(f"Successfully subscribed to all watchlist symbols")
        else:
            logger.info("No symbols in watchlist to subscribe to")
        
        logger.info("Запуск MLManager в HYBRID режиме...")
        await ml_manager.start(MLMode.HYBRID) # Start ML in hybrid mode
        logger.info("MLManager запущен успешно")
        
        logger.info("Запуск AnomalyDetector...")
        await anomaly_detector.start() # Start Anomaly Detector
        logger.info("AnomalyDetector запущен успешно")
        
        # Start periodic market data broadcast
        try:
            task = asyncio.create_task(periodic_market_data_broadcast())
            logger.info("periodic_market_data_broadcast task created successfully")
        except Exception as e:
            logger.error(f"Failed to create periodic_market_data_broadcast task: {e}", exc_info=True)
    except AppError as e:
        logger.critical(f"Failed to start core components: {e.message}")
        exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during startup: {e}", exc_info=True)
        exit(1)

    # Initialize MCP Integration
    global mcp_integration
    try:
        mcp_integration = MCPIntegration(app)
        mcp_integration.integrate_with_app()
        await mcp_integration.start()
        logger.info("MCP Integration initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize MCP Integration: {e}")
        # Продолжаем работу без MCP

    # Set app state
    app.state.db_manager = db_manager
    app.state.message_broker = message_broker
    app.state.bybit_ws_client = bybit_ws_client
    app.state.bybit_api_client = bybit_api_client
    app.state.trade_processor = trade_processor
    app.state.orderbook_analyzer = orderbook_analyzer
    app.state.alert_manager = alert_manager
    app.state.ml_manager = ml_manager
    app.state.ml_data_collector = ml_data_collector
    app.state.anomaly_detector = anomaly_detector
    app.state.mcp_integration = mcp_integration
    app.state.batch_integration = batch_integration

    logger.info("Application startup complete.")
    yield
    logger.info("Shutting down application...")

    # Cleanup resources
    try:
        # Cancel consumer tasks
        if hasattr(app.state, 'consumer_tasks'):
            for task in app.state.consumer_tasks:
                if not task.done():
                    task.cancel()
            logger.info("Cancelled consumer tasks")
        
        if batch_integration:
            await batch_integration.stop()
        if mcp_integration:
            await mcp_integration.stop()
        if bybit_ws_client:
            await bybit_ws_client.stop()
        if ml_manager:
            await ml_manager.stop()
        if anomaly_detector:
            await anomaly_detector.stop()
        if message_broker:
            await message_broker.disconnect()
        if bybit_api_client:
            pass  # PybitAPIClient doesn't need explicit close
        if db_manager:
            await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Error during graceful shutdown: {e}", exc_info=True)
        ERRORS_TOTAL.labels(component='app', error_type='shutdown_error').inc()

    logger.info("Application shutdown complete.")

app = FastAPI(
    title="Crypto Manipulation Detection API",
    description="API for real-time cryptocurrency market manipulation detection.",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "http://localhost:5174", "http://127.0.0.1:5174", "http://localhost:3001", "http://127.0.0.1:3001"],  # Frontend URLs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket request logging middleware
@app.middleware("http")
async def websocket_logging_middleware(request, call_next):
    """Middleware для логирования WebSocket запросов."""
    if request.url.path.startswith("/ws/"):
        client_host = request.client.host if request.client else "unknown"
        logger.info(f"HTTP request to WebSocket endpoint {request.url.path} from {client_host}")
        logger.info(f"Request method: {request.method}")
        logger.info(f"Request headers: {dict(request.headers)}")
    
    response = await call_next(request)
    return response

# Inject dependencies into FastAPI's state
# App state is now set in lifespan function

# Import routers
from manipulation.ml.ml_api import router as ml_router, anomaly_router as ad_router # New: Import anomaly_router
from api.config_api import router as config_router

# Create API router with /api prefix
api_router = APIRouter(prefix="/api")
api_router.include_router(ml_router)
api_router.include_router(ad_router) # New: Include anomaly_router
api_router.include_router(config_router)

@app.exception_handler(AppError)
async def app_error_handler(request, exc: AppError):
    ERRORS_TOTAL.labels(component='app', error_type=exc.__class__.__name__).inc()
    return HTTPException(status_code=exc.status_code, detail=exc.message)

@app.exception_handler(Exception)
async def generic_exception_handler(request, exc: Exception):
    ERRORS_TOTAL.labels(component='app', error_type='unhandled_exception').inc()
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")

@app.get("/", summary="Root Endpoint")
async def read_root():
    return {"message": "Crypto Manipulation Detection API is running"}

@api_router.get("/watchlist", summary="Get Watchlist Symbols")
async def get_watchlist_symbols():
    """
    Возвращает список всех символов, находящихся в списке отслеживания.
    """
    try:
        symbols = await db_manager.get_watchlist_symbols()
        ACTIVE_WATCHLIST_SYMBOLS.set(len(symbols))
        return {"watchlist": symbols}
    except DatabaseError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error getting watchlist symbols: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get watchlist symbols: {e}")

@api_router.post("/watchlist/add", summary="Add Symbol to Watchlist")
async def add_symbol_to_watchlist(symbol: str):
    """
    Добавляет указанный символ в список отслеживания.
    """
    try:
        await db_manager.add_to_watchlist(symbol)
        await bybit_ws_client.subscribe_to_symbol(symbol)
        symbols = await db_manager.get_watchlist_symbols()
        ACTIVE_WATCHLIST_SYMBOLS.set(len(symbols))
        return {"message": f"Symbol {symbol} added to watchlist and subscribed."}
    except DatabaseError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except WebSocketError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error adding symbol {symbol} to watchlist: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to add symbol to watchlist: {e}")

@api_router.post("/watchlist/remove", summary="Remove Symbol from Watchlist")
async def remove_symbol_from_watchlist(symbol: str):
    """
    Удаляет указанный символ из списка отслеживания.
    """
    try:
        await bybit_ws_client.unsubscribe_from_symbol(symbol)
        await db_manager.remove_from_watchlist(symbol)
        symbols = await db_manager.get_watchlist_symbols()
        ACTIVE_WATCHLIST_SYMBOLS.set(len(symbols))
        return {"message": f"Symbol {symbol} removed from watchlist and unsubscribed."}
    except DatabaseError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except WebSocketError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error removing symbol {symbol} from watchlist: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to remove symbol from watchlist: {e}")

@api_router.get("/alerts", summary="Get Recent Alerts")
async def get_recent_alerts(limit: int = 100, symbol: Optional[str] = None):
    """
    Возвращает последние алерты, с возможностью фильтрации по символу.
    """
    try:
        alerts = await db_manager.get_alerts(limit=limit, symbol=symbol)
        return {"alerts": alerts}
    except DatabaseError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error getting recent alerts: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get recent alerts: {e}")

@api_router.get("/status", summary="Get Overall System Status")
async def get_overall_status():
    """
    Возвращает общий статус всех компонентов системы.
    """
    try:
        return {
            "db_status": "Connected" if db_manager and db_manager.pool else "Disconnected",
            "message_broker_status": "Connected" if message_broker and message_broker.connection else "Disconnected",
            "websocket_client_status": bybit_ws_client.get_connection_stats() if bybit_ws_client else "Not Initialized",
            "alert_manager_status": alert_manager.get_status() if alert_manager else "Not Initialized",
            "ml_system_status": ml_manager.get_system_status() if ml_manager else "Not Initialized",
            "anomaly_detector_status": anomaly_detector.get_status() if anomaly_detector else "Not Initialized", # New: Anomaly Detector status
            "active_watchlist_symbols_count": ACTIVE_WATCHLIST_SYMBOLS._value.get() # Get current value from Gauge
        }
    except Exception as e:
        logger.error(f"Error getting overall system status: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get overall system status: {e}")

@api_router.get("/metrics", summary="Prometheus Metrics", response_class=PlainTextResponse)
async def metrics():
    """
    Эндпоинт для сбора метрик Prometheus.
    """
    return get_metrics_text()

# Futures Symbols Management
@api_router.get("/futures-symbols", summary="Get All Futures Symbols")
async def get_futures_symbols(enabled_only: bool = False):
    """
    Возвращает список всех фьючерсных торговых пар.
    """
    try:
        symbols = await db_manager.get_futures_symbols(enabled_only=enabled_only)
        return {"symbols": symbols}
    except DatabaseError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error getting futures symbols: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get futures symbols: {e}")

@api_router.post("/futures-symbols/sync", summary="Sync Futures Symbols from Bybit")
async def sync_futures_symbols():
    """
    Синхронизирует список фьючерсных пар с биржей Bybit.
    """
    try:
        api_client = PybitAPIClient()
        symbols_data = await api_client.get_all_futures_symbols()
        
        synced_count = 0
        for symbol_info in symbols_data:
            # Преобразуем ключи для соответствия ожидаемому формату в db_manager
            # Конвертируем временные поля из строк в целые числа
            launch_time = symbol_info.get("launchTime")
            if launch_time and isinstance(launch_time, str):
                try:
                    launch_time = int(launch_time)
                except (ValueError, TypeError):
                    launch_time = None
            
            delivery_time = symbol_info.get("deliveryTime")
            if delivery_time and isinstance(delivery_time, str):
                try:
                    delivery_time = int(delivery_time)
                except (ValueError, TypeError):
                    delivery_time = None
            
            symbol_data = {
                "symbol": symbol_info["symbol"],
                "base_coin": symbol_info["baseCoin"],
                "quote_coin": symbol_info["quoteCoin"],
                "status": symbol_info["status"],
                "contract_type": symbol_info.get("contractType"),
                "launch_time": launch_time,
                "delivery_time": delivery_time,
                "min_price": symbol_info.get("minPrice"),
                "max_price": symbol_info.get("maxPrice"),
                "tick_size": symbol_info.get("tickSize"),
                "min_order_qty": symbol_info.get("minOrderQty"),
                "max_order_qty": symbol_info.get("maxOrderQty")
            }
            await db_manager.upsert_futures_symbol(symbol_data)
            synced_count += 1
            
        await api_client.close()
        
        logger.info(f"Синхронизировано {synced_count} фьючерсных пар")
        return {"message": f"Successfully synced {synced_count} futures symbols"}
        
    except Exception as e:
        logger.error(f"Error syncing futures symbols: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to sync futures symbols: {e}")

# Кэш для рыночных данных
market_data_cache = {}
market_data_cache_time = 0
MARKET_DATA_CACHE_DURATION = 15  # 15 секунд кэш

@api_router.get("/futures-symbols/market-data", summary="Get Futures Symbols with Market Data")
async def get_futures_symbols_with_market_data(enabled_only: bool = False):
    """
    Возвращает список фьючерсных торговых пар с рыночной статистикой по разным временным интервалам.
    Использует кэширование для ускорения повторных запросов.
    """
    global market_data_cache, market_data_cache_time
    
    try:
        # Проверяем кэш
        cache_key = f"market_data_{enabled_only}"
        current_time = time.time()
        
        if (cache_key in market_data_cache and 
            current_time - market_data_cache_time < MARKET_DATA_CACHE_DURATION):
            logger.info(f"Возвращаем данные из кэша для enabled_only={enabled_only}")
            return market_data_cache[cache_key]
        # Получаем базовую информацию о символах
        symbols = await db_manager.get_futures_symbols(enabled_only=enabled_only)
        
        if not symbols:
            return {"symbols": []}
        
        # Получаем список символов для API запросов
        symbol_names = [symbol["symbol"] for symbol in symbols]
        
        # Получаем рыночную статистику с Bybit
        api_client = PybitAPIClient()
        
        # Получаем 24-часовые тикеры
        market_data_24h = await api_client.get_24h_ticker()
        
        # Получаем данные по разным временным интервалам
        multi_timeframe_data = await api_client.get_multi_timeframe_data(symbol_names)
        
        # Объединяем данные
        enriched_symbols = []
        for symbol in symbols:
            symbol_name = symbol["symbol"]
            ticker_data = market_data_24h.get(symbol_name, {})
            timeframe_data = multi_timeframe_data.get(symbol_name, {})
            
            # Базовые данные из 24-часового тикера
            last_price = ticker_data.get("lastPrice", 0)
            high_24h = ticker_data.get("highPrice24h", 0)
            low_24h = ticker_data.get("lowPrice24h", 0)
            
            # Рассчитываем волатильность за 24ч как разность между максимальной и минимальной ценой
            volatility_24h = 0
            if last_price > 0:
                volatility_24h = ((high_24h - low_24h) / last_price) * 100
            
            enriched_symbol = {
                **symbol,
                "last_price": last_price,
                "turnover_24h": ticker_data.get("turnover24h", 0),
                "high_price_24h": high_24h,
                "low_price_24h": low_24h,
                
                # Данные по изменению цены для разных временных интервалов
                "price_change_1m_percent": timeframe_data.get('1m', {}).get('price_change_percent', 0),
                "price_change_5m_percent": timeframe_data.get('5m', {}).get('price_change_percent', 0),
                "price_change_15m_percent": timeframe_data.get('15m', {}).get('price_change_percent', 0),
                "price_change_1h_percent": timeframe_data.get('1h', {}).get('price_change_percent', 0),
                "price_change_24h_percent": ticker_data.get("price24hPcnt", 0) * 100,  # Используем точные данные из тикера
                
                # Данные по объему для разных временных интервалов
                "volume_1m": timeframe_data.get('1m', {}).get('volume', 0),
                "volume_5m": timeframe_data.get('5m', {}).get('volume', 0),
                "volume_15m": timeframe_data.get('15m', {}).get('volume', 0),
                "volume_1h": timeframe_data.get('1h', {}).get('volume', 0),
                "volume_24h": ticker_data.get("volume24h", 0),  # Используем точные данные из тикера
                
                # Данные по волатильности для разных временных интервалов
                "volatility_1m": timeframe_data.get('1m', {}).get('volatility', 0),
                "volatility_5m": timeframe_data.get('5m', {}).get('volatility', 0),
                "volatility_15m": timeframe_data.get('15m', {}).get('volatility', 0),
                "volatility_1h": timeframe_data.get('1h', {}).get('volatility', 0),
                "volatility_24h": volatility_24h
            }
            enriched_symbols.append(enriched_symbol)
        
        await api_client.close()
        
        # Сохраняем результат в кэш
        result = {"symbols": enriched_symbols}
        market_data_cache[cache_key] = result
        market_data_cache_time = current_time
        logger.info(f"Данные сохранены в кэш для enabled_only={enabled_only}")
        
        return result
        
    except DatabaseError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error getting futures symbols with market data: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get futures symbols with market data: {e}")

@api_router.post("/futures-symbols/{symbol}/toggle", summary="Toggle Symbol Enable/Disable")
async def toggle_symbol_status(symbol: str):
    """
    Переключает статус включения/отключения символа и автоматически синхронизирует watchlist.
    """
    try:
        # Используем новый метод, который автоматически синхронизирует watchlist
        new_status, symbols_added, symbols_removed = await db_manager.toggle_futures_symbol_status(symbol)
        
        # Синхронизируем WebSocket подписки
        await sync_websocket_subscriptions(symbols_added, symbols_removed)
        
        action = "enabled" if new_status else "disabled"
        return {"message": f"Symbol {symbol} {action} successfully and watchlist synced", "is_enabled": new_status}
        
    except DatabaseError as e:
        if "not found" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"Error toggling symbol {symbol} status: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to toggle symbol status: {e}")

# Symbol Filters Management
@api_router.get("/symbol-filters", summary="Get All Symbol Filters")
async def get_symbol_filters():
    """
    Возвращает список всех фильтров символов.
    """
    try:
        filters = await db_manager.get_symbol_filters()
        return {"filters": filters}
    except DatabaseError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error getting symbol filters: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get symbol filters: {e}")

@api_router.post("/symbol-filters", summary="Create Symbol Filter")
async def create_symbol_filter(filter_data: dict):
    """
    Создает новый фильтр символов.
    """
    try:
        name = filter_data.get("name")
        description = filter_data.get("description", "")
        criteria = filter_data.get("criteria", {})
        
        if not name or not criteria:
            raise HTTPException(status_code=400, detail="Name and criteria are required")
            
        filter_id = await db_manager.create_symbol_filter(name, description, criteria)
        return {"message": f"Filter '{name}' created successfully", "filter_id": filter_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating symbol filter: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to create symbol filter: {e}")

@api_router.post("/symbol-filters/{filter_name}/apply", summary="Apply Symbol Filter")
async def apply_symbol_filter(filter_name: str, action: str = "enable"):
    """
    Применяет фильтр к символам и включает/отключает их.
    action: 'enable' или 'disable'
    """
    try:
        if action not in ["enable", "disable"]:
            raise HTTPException(status_code=400, detail="Action must be 'enable' or 'disable'")
            
        # Получаем символы по фильтру
        symbols = await db_manager.apply_symbol_filter(filter_name)
        
        if not symbols:
            return {"message": f"No symbols match filter '{filter_name}'", "affected_count": 0}
            
        # Обновляем статус символов
        is_enabled = action == "enable"
        placeholders = ",".join([f"${i+1}" for i in range(len(symbols))])
        
        await db_manager._execute(
            f"UPDATE futures_symbols SET is_enabled = ${len(symbols)+1}, last_updated = NOW() WHERE symbol IN ({placeholders})",
            *symbols, is_enabled,
            operation_name="apply_filter_bulk_update"
        )
        
        # Автоматически синхронизируем watchlist после массового обновления
        symbols_added, symbols_removed = await db_manager._sync_watchlist_with_futures_symbols()
        
        # Синхронизируем WebSocket подписки
        await sync_websocket_subscriptions(symbols_added, symbols_removed)
        
        logger.info(f"Filter '{filter_name}' {action}d {len(symbols)} symbols and watchlist synced")
        return {
            "message": f"Filter '{filter_name}' applied successfully and watchlist synced",
            "action": action,
            "affected_count": len(symbols),
            "symbols": symbols
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error applying symbol filter {filter_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to apply symbol filter: {e}")

@api_router.post("/watchlist/sync", summary="Sync Watchlist with Futures Symbols")
async def sync_watchlist():
    """
    Ручная синхронизация watchlist с включенными символами из futures_symbols.
    """
    try:
        symbols_added, symbols_removed = await db_manager._sync_watchlist_with_futures_symbols()
        
        # Синхронизируем WebSocket подписки
        await sync_websocket_subscriptions(symbols_added, symbols_removed)
        
        return {
            "message": "Watchlist synchronized successfully with enabled futures symbols",
            "symbols_added": symbols_added,
            "symbols_removed": symbols_removed
        }
        
    except Exception as e:
        logger.error(f"Error syncing watchlist: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to sync watchlist: {e}")

# WebSocket endpoint for real-time streaming data
@app.websocket("/ws/streaming")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket эндпоинт для потоковой передачи данных в реальном времени.
    Отправляет обновления цен, объемов, книги ордеров и алертов.
    """
    client_host = websocket.client.host if websocket.client else "unknown"
    client_port = websocket.client.port if websocket.client else "unknown"
    logger.info(f"New WebSocket connection attempt to /ws/streaming from {client_host}:{client_port}")
    logger.info(f"WebSocket headers: {dict(websocket.headers)}")
    
    try:
        logger.info("Attempting to accept WebSocket connection...")
        await websocket.accept()
        logger.info("WebSocket connection accepted, adding to manager...")
        
        await manager.connect(websocket)
        logger.info(f"WebSocket connection established successfully from {client_host}:{client_port}")
        logger.info(f"Total active connections: {len(manager.active_connections)}")
        
        # Send initial data
        initial_data = {
            "type": "connection_established",
            "message": "Connected to streaming data",
            "timestamp": asyncio.get_event_loop().time()
        }
        try:
            await manager.send_personal_message(json.dumps(initial_data), websocket)
            logger.info("Initial data sent successfully")
        except Exception as e:
            logger.error(f"Error sending initial data: {e}")
            return
        
        # Keep connection alive and handle incoming messages (non-blocking)
        try:
            while True:
                try:
                    # Check if websocket is still connected before trying to receive
                    if (hasattr(websocket, 'client_state') and 
                        hasattr(websocket.client_state, 'name') and
                        websocket.client_state.name != 'CONNECTED'):
                        logger.debug("WebSocket client state is not CONNECTED, breaking loop")
                        break
                    
                    if (hasattr(websocket, 'application_state') and 
                        hasattr(websocket.application_state, 'name') and
                        websocket.application_state.name != 'CONNECTED'):
                        logger.debug("WebSocket application state is not CONNECTED, breaking loop")
                        break
                    
                    # Wait for client messages with timeout to avoid blocking
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                    logger.debug(f"Received WebSocket message: {data}")
                    message = json.loads(data)
                    logger.debug(f"Parsed WebSocket message: {message}")
                    
                    if message.get("type") == "ping":
                        pong_response = {
                            "type": "pong",
                            "timestamp": asyncio.get_event_loop().time()
                        }
                        await manager.send_personal_message(json.dumps(pong_response), websocket)
                        logger.debug("Sent pong response")
                    elif message.get("type") == "subscribe":
                        # Handle subscription requests
                        symbols = message.get("symbols", [])
                        subscription_response = {
                            "type": "subscription_confirmed",
                            "symbols": symbols,
                            "timestamp": asyncio.get_event_loop().time()
                        }
                        await manager.send_personal_message(json.dumps(subscription_response), websocket)
                        logger.debug(f"Sent subscription confirmation for symbols: {symbols}")
                    else:
                        logger.debug(f"Unknown message type: {message.get('type')}")
                        
                except asyncio.TimeoutError:
                    # No message received, continue to keep connection alive
                    continue
                except json.JSONDecodeError:
                    try:
                        error_response = {
                            "type": "error",
                            "message": "Invalid JSON format",
                            "timestamp": asyncio.get_event_loop().time()
                        }
                        await manager.send_personal_message(json.dumps(error_response), websocket)
                    except Exception:
                        # Connection already closed, ignore
                        break
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")
                    # Check if this is a disconnect-related error
                    if "disconnect" in str(e).lower() or "closed" in str(e).lower():
                        logger.debug("WebSocket disconnect detected, breaking loop")
                        break
                    try:
                        error_response = {
                            "type": "error",
                            "message": "Internal server error",
                            "timestamp": asyncio.get_event_loop().time()
                        }
                        await manager.send_personal_message(json.dumps(error_response), websocket)
                    except Exception:
                        # Connection already closed, ignore
                        break
        except WebSocketDisconnect as e:
            logger.info(f"WebSocket client disconnected normally: {e}")
            logger.info(f"Disconnect code: {e.code if hasattr(e, 'code') else 'unknown'}")
            logger.info(f"Disconnect reason: {e.reason if hasattr(e, 'reason') else 'unknown'}")
                
    except WebSocketDisconnect as e:
        logger.info(f"WebSocket client disconnected during handshake: {e}")
        logger.info(f"Disconnect code: {e.code if hasattr(e, 'code') else 'unknown'}")
        logger.info(f"Disconnect reason: {e.reason if hasattr(e, 'reason') else 'unknown'}")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket endpoint: {e}")
        logger.error(f"Exception type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        # Ensure cleanup
        try:
            manager.disconnect(websocket)
            logger.info("WebSocket connection cleaned up successfully")
        except Exception as cleanup_error:
            logger.debug(f"Error during WebSocket cleanup: {cleanup_error}")

@app.websocket("/ws/alerts")
async def websocket_alerts_endpoint(websocket: WebSocket):
    """
    WebSocket эндпоинт для алертов.
    """
    logger.info("New WebSocket connection attempt to /ws/alerts")
    
    try:
        await manager.connect(websocket)
        logger.info("WebSocket alerts connection established successfully")
        
        # Send initial data
        initial_data = {
            "type": "connection_established",
            "message": "Connected to alerts stream",
            "timestamp": asyncio.get_event_loop().time()
        }
        await manager.send_personal_message(json.dumps(initial_data), websocket)
        
        # Keep connection alive
        try:
            while True:
                try:
                    # Check if websocket is still connected before trying to receive
                    if (hasattr(websocket, 'client_state') and 
                        hasattr(websocket.client_state, 'name') and
                        websocket.client_state.name != 'CONNECTED'):
                        logger.debug("WebSocket alerts client state is not CONNECTED, breaking loop")
                        break
                    
                    if (hasattr(websocket, 'application_state') and 
                        hasattr(websocket.application_state, 'name') and
                        websocket.application_state.name != 'CONNECTED'):
                        logger.debug("WebSocket alerts application state is not CONNECTED, breaking loop")
                        break
                    
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                    # Handle any incoming messages if needed
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    # Check if this is a disconnect-related error
                    if "disconnect" in str(e).lower() or "closed" in str(e).lower():
                        logger.debug("WebSocket alerts disconnect detected, breaking loop")
                        break
                    logger.debug(f"WebSocket alerts error: {e}")
                    break
        except WebSocketDisconnect:
            logger.info("WebSocket alerts client disconnected normally")
                
    except WebSocketDisconnect:
        logger.info("WebSocket alerts client disconnected during handshake")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket alerts endpoint: {e}")
    finally:
        try:
            manager.disconnect(websocket)
        except Exception as cleanup_error:
            logger.debug(f"Error during WebSocket alerts cleanup: {cleanup_error}")

@app.websocket("/ws/predictions")
async def websocket_predictions_endpoint(websocket: WebSocket):
    """
    WebSocket эндпоинт для предсказаний ML модели.
    """
    logger.info("New WebSocket connection attempt to /ws/predictions")
    
    try:
        await manager.connect(websocket)
        logger.info("WebSocket predictions connection established successfully")
        
        # Send initial data
        initial_data = {
            "type": "connection_established",
            "message": "Connected to predictions stream",
            "timestamp": asyncio.get_event_loop().time()
        }
        await manager.send_personal_message(json.dumps(initial_data), websocket)
        
        # Keep connection alive
        try:
            while True:
                try:
                    # Check if websocket is still connected before trying to receive
                    if (hasattr(websocket, 'client_state') and 
                        hasattr(websocket.client_state, 'name') and
                        websocket.client_state.name != 'CONNECTED'):
                        logger.debug("WebSocket predictions client state is not CONNECTED, breaking loop")
                        break
                    
                    if (hasattr(websocket, 'application_state') and 
                        hasattr(websocket.application_state, 'name') and
                        websocket.application_state.name != 'CONNECTED'):
                        logger.debug("WebSocket predictions application state is not CONNECTED, breaking loop")
                        break
                    
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                    # Handle any incoming messages if needed
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    # Check if this is a disconnect-related error
                    if "disconnect" in str(e).lower() or "closed" in str(e).lower():
                        logger.debug("WebSocket predictions disconnect detected, breaking loop")
                        break
                    logger.debug(f"WebSocket predictions error: {e}")
                    break
        except WebSocketDisconnect:
            logger.info("WebSocket predictions client disconnected normally")
                
    except WebSocketDisconnect:
        logger.info("WebSocket predictions client disconnected during handshake")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket predictions endpoint: {e}")
    finally:
        try:
            manager.disconnect(websocket)
        except Exception as cleanup_error:
            logger.debug(f"Error during WebSocket predictions cleanup: {cleanup_error}")

@app.websocket("/ws/training_metrics")
async def websocket_training_metrics_endpoint(websocket: WebSocket):
    """
    WebSocket эндпоинт для метрик обучения.
    """
    logger.info("New WebSocket connection attempt to /ws/training_metrics")
    
    try:
        await manager.connect(websocket)
        logger.info("WebSocket training metrics connection established successfully")
        
        # Send initial data
        initial_data = {
            "type": "connection_established",
            "message": "Connected to training metrics stream",
            "timestamp": asyncio.get_event_loop().time()
        }
        await manager.send_personal_message(json.dumps(initial_data), websocket)
        
        # Keep connection alive
        try:
            while True:
                try:
                    # Check if websocket is still connected before trying to receive
                    if (hasattr(websocket, 'client_state') and 
                        hasattr(websocket.client_state, 'name') and
                        websocket.client_state.name != 'CONNECTED'):
                        logger.debug("WebSocket training metrics client state is not CONNECTED, breaking loop")
                        break
                    
                    if (hasattr(websocket, 'application_state') and 
                        hasattr(websocket.application_state, 'name') and
                        websocket.application_state.name != 'CONNECTED'):
                        logger.debug("WebSocket training metrics application state is not CONNECTED, breaking loop")
                        break
                    
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                    # Handle any incoming messages if needed
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    # Check if this is a disconnect-related error
                    if "disconnect" in str(e).lower() or "closed" in str(e).lower():
                        logger.debug("WebSocket training metrics disconnect detected, breaking loop")
                        break
                    logger.debug(f"WebSocket training metrics error: {e}")
                    break
        except WebSocketDisconnect:
            logger.info("WebSocket training metrics client disconnected normally")
                
    except WebSocketDisconnect:
        logger.info("WebSocket training metrics client disconnected during handshake")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket training metrics endpoint: {e}")
    finally:
        try:
            manager.disconnect(websocket)
        except Exception as cleanup_error:
            logger.debug(f"Error during WebSocket training metrics cleanup: {cleanup_error}")

@app.websocket("/ws/futures")
async def websocket_futures_endpoint(websocket: WebSocket):
    """
    WebSocket эндпоинт для фьючерсов.
    """
    logger.info("New WebSocket connection attempt to /ws/futures")
    
    try:
        await manager.connect(websocket)
        logger.info("WebSocket futures connection established successfully")
        
        # Send initial data
        initial_data = {
            "type": "connection_established",
            "message": "Connected to futures stream",
            "timestamp": asyncio.get_event_loop().time()
        }
        await manager.send_personal_message(json.dumps(initial_data), websocket)
        
        # Keep connection alive
        try:
            while True:
                try:
                    # Check if websocket is still connected before trying to receive
                    if (hasattr(websocket, 'client_state') and 
                        hasattr(websocket.client_state, 'name') and
                        websocket.client_state.name != 'CONNECTED'):
                        logger.debug("WebSocket futures client state is not CONNECTED, breaking loop")
                        break
                    
                    if (hasattr(websocket, 'application_state') and 
                        hasattr(websocket.application_state, 'name') and
                        websocket.application_state.name != 'CONNECTED'):
                        logger.debug("WebSocket futures application state is not CONNECTED, breaking loop")
                        break
                    
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                    # Handle any incoming messages if needed
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    # Check if this is a disconnect-related error
                    if "disconnect" in str(e).lower() or "closed" in str(e).lower():
                        logger.debug("WebSocket futures disconnect detected, breaking loop")
                        break
                    logger.debug(f"WebSocket futures error: {e}")
                    break
        except WebSocketDisconnect:
            logger.info("WebSocket futures client disconnected normally")
                
    except WebSocketDisconnect:
        logger.info("WebSocket futures client disconnected during handshake")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket futures endpoint: {e}")
    finally:
        try:
            manager.disconnect(websocket)
        except Exception as cleanup_error:
            logger.debug(f"Error during WebSocket futures cleanup: {cleanup_error}")

# Function to broadcast streaming data to all connected clients
async def broadcast_streaming_data(data_type: str, data: Dict[str, Any]):
    """
    Отправляет потоковые данные всем подключенным WebSocket клиентам.
    """
    if manager.active_connections:
        message = {
            "type": data_type,
            "data": data,
            "timestamp": asyncio.get_event_loop().time()
        }
        await manager.broadcast(json.dumps(message))

# Periodic task to broadcast market data updates
async def periodic_market_data_broadcast():
    """
    Периодически отправляет обновления рыночных данных всем подключенным клиентам.
    """
    global manager, db_manager, ml_manager, alert_manager
    
    logger.info("Starting periodic_market_data_broadcast task...")
    # Ждем инициализации приложения
    await asyncio.sleep(5)
    logger.info("periodic_market_data_broadcast: Initialization wait complete, starting main loop")
    
    while True:
        try:
            logger.info(f"Periodic broadcast check: {len(manager.active_connections)} active connections")
            if manager.active_connections:
                # Отправляем системный статус
                try:
                    system_status = {
                        "status": "running",
                        "uptime": time.time() - (getattr(periodic_market_data_broadcast, 'start_time', time.time())),
                        "cpu_usage": 0.0,  # Можно добавить реальный мониторинг CPU
                        "memory_usage": 0.0,  # Можно добавить реальный мониторинг памяти
                        "active_connections": len(manager.active_connections)
                    }
                    await broadcast_streaming_data("system_status", system_status)
                except Exception as e:
                    logger.error(f"Error broadcasting system status: {e}")
                
                # Отправляем ML статус
                try:
                    if ml_manager:
                        ml_status = {
                            "training_active": ml_manager.is_training_active(),
                            "prediction_active": ml_manager.is_prediction_active(),
                            "model_accuracy": getattr(ml_manager, 'last_accuracy', None),
                            "last_training": getattr(ml_manager, 'last_training_time', None),
                            "current_symbol": getattr(ml_manager, 'current_symbol', None)
                        }
                        await broadcast_streaming_data("ml_status", ml_status)
                except Exception as e:
                    logger.error(f"Error broadcasting ML status: {e}")
                
                # Отправляем количество алертов
                try:
                    if alert_manager:
                        alerts_count = len(getattr(alert_manager, 'recent_alerts', []))
                        await broadcast_streaming_data("alerts_count", {"count": alerts_count})
                except Exception as e:
                    logger.error(f"Error broadcasting alerts count: {e}")
                
                # Получаем обновленные рыночные данные
                symbols = await db_manager.get_futures_symbols(enabled_only=True)
                logger.info(f"Got {len(symbols) if symbols else 0} symbols from database")
                if symbols:
                    symbol_names = [symbol["symbol"] for symbol in symbols]  # Используем все символы
                    
                    api_client = BybitAPIClient()
                    logger.info("Fetching market data from Bybit API...")
                    market_data_24h = await api_client.get_24h_ticker()
                    multi_timeframe_data = await api_client.get_multi_timeframe_data(symbol_names)
                    await api_client.close()
                    logger.info(f"API data received: {len(market_data_24h)} tickers, {len(multi_timeframe_data)} timeframe data")
                    
                    # Формируем данные для отправки как price_update и streaming_update
                    for symbol in symbols:
                        symbol_name = symbol["symbol"]
                        ticker_data = market_data_24h.get(symbol_name, {})
                        timeframe_data = multi_timeframe_data.get(symbol_name, {})
                        
                        if ticker_data or timeframe_data:
                            # Отправляем price_update для каждого символа
                            price_update = {
                                "symbol": symbol_name,
                                "price": float(ticker_data.get("lastPrice", 0)),
                                "change_24h": float(ticker_data.get("price24hPcnt", 0)) * 100,
                                "volume_24h": float(ticker_data.get("volume24h", 0))
                            }
                            await broadcast_streaming_data("price_update", price_update)
                            
                            # Отправляем streaming_update с подробными данными
                            streaming_update = {
                                "symbol": symbol_name,
                                "last_price": float(ticker_data.get("lastPrice", 0)),
                                "price_change_24h_percent": float(ticker_data.get("price24hPcnt", 0)) * 100,
                                "volume_24h": float(ticker_data.get("volume24h", 0)),
                                "volatility_1m": timeframe_data.get('1m', {}).get('volatility', 0),
                                "volatility_5m": timeframe_data.get('5m', {}).get('volatility', 0),
                                "volatility_15m": timeframe_data.get('15m', {}).get('volatility', 0),
                                "volatility_1h": timeframe_data.get('1h', {}).get('volatility', 0),
                                "price_change_1m_percent": timeframe_data.get('1m', {}).get('price_change_percent', 0),
                                "price_change_5m_percent": timeframe_data.get('5m', {}).get('price_change_percent', 0),
                                "price_change_15m_percent": timeframe_data.get('15m', {}).get('price_change_percent', 0),
                                "price_change_1h_percent": timeframe_data.get('1h', {}).get('price_change_percent', 0)
                            }
                            await broadcast_streaming_data("streaming_update", streaming_update)
                    
                    logger.info(f"Broadcasted market data for {len(symbols)} symbols")
                        
            # Ждем 30 секунд перед следующим обновлением
            await asyncio.sleep(30)
            
        except Exception as e:
            logger.error(f"Error in periodic market data broadcast: {e}")
            await asyncio.sleep(60)  # Ждем дольше при ошибке

# Устанавливаем время старта для расчета uptime
if not hasattr(periodic_market_data_broadcast, 'start_time'):
    periodic_market_data_broadcast.start_time = time.time()


# Database View API Endpoints
@api_router.get("/database/raw-trades", summary="Get Raw Trades Data")
async def get_raw_trades(
    symbol: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1000
):
    """Получить исторические данные сделок из базы данных"""
    try:
        if not db_manager:
            raise HTTPException(status_code=500, detail="Database manager not initialized")
        
        # Построение SQL запроса
        query = "SELECT received_at as time, symbol, price, size, side, timestamp_ms, trade_id FROM raw_trades WHERE 1=1"
        params = []
        
        if symbol:
            query += " AND symbol = $" + str(len(params) + 1)
            params.append(symbol)
        
        if start_time:
            query += " AND received_at >= $" + str(len(params) + 1)
            params.append(start_time)
        
        if end_time:
            query += " AND received_at <= $" + str(len(params) + 1)
            params.append(end_time)
        
        query += " ORDER BY received_at DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        rows = await db_manager._execute(query, *params, fetch=True, operation_name="get_raw_trades")
        trades = [dict(row) for row in rows]
        
        return {
            "status": "success",
            "data": trades,
            "count": len(trades),
            "filters": {
                "symbol": symbol,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit
            }
        }
    except Exception as e:
        logger.error(f"Error fetching raw trades: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/database/raw-orderbooks", summary="Get Raw Orderbooks Data")
async def get_raw_orderbooks(
    symbol: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1000
):
    """Получить исторические данные стаканов ордеров из базы данных"""
    try:
        if not db_manager:
            raise HTTPException(status_code=500, detail="Database manager not initialized")
        
        # Построение SQL запроса
        query = "SELECT received_at as time, symbol, bids, asks FROM raw_orderbooks WHERE 1=1"
        params = []
        
        if symbol:
            query += " AND symbol = $" + str(len(params) + 1)
            params.append(symbol)
        
        if start_time:
            query += " AND received_at >= $" + str(len(params) + 1)
            params.append(start_time)
        
        if end_time:
            query += " AND received_at <= $" + str(len(params) + 1)
            params.append(end_time)
        
        query += " ORDER BY received_at DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        rows = await db_manager._execute(query, *params, fetch=True, operation_name="get_raw_orderbooks")
        orderbooks = []
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
            orderbooks.append(row_dict)
        
        return {
            "status": "success",
            "data": orderbooks,
            "count": len(orderbooks),
            "filters": {
                "symbol": symbol,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit
            }
        }
    except Exception as e:
        logger.error(f"Error fetching raw orderbooks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/database/ml-training-data", summary="Get ML Training Data")
async def get_ml_training_data(
    symbol: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1000
):
    """Получить данные для обучения ML из базы данных"""
    try:
        if not db_manager:
            raise HTTPException(status_code=500, detail="Database manager not initialized")
        
        # Выполнить SQL запрос для получения ML training data
        query = """
        SELECT id, symbol, features, target_price_change, target_direction, created_at, alert_id
        FROM ml_training_data
        WHERE 1=1
        """
        
        params = []
        if symbol:
            query += " AND symbol = $" + str(len(params) + 1)
            params.append(symbol)
        
        if start_time:
            query += " AND created_at >= $" + str(len(params) + 1)
            params.append(start_time)
        
        if end_time:
            query += " AND created_at <= $" + str(len(params) + 1)
            params.append(end_time)
        
        query += " ORDER BY created_at DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        rows = await db_manager._execute(query, *params, fetch=True, operation_name="get_ml_training_data")
        
        # Преобразовать в список словарей
        training_data = []
        for row in rows:
            row_dict = dict(row)
            # Преобразовать JSON поля
            if row_dict.get('features'):
                row_dict['features'] = json.loads(row_dict['features']) if isinstance(row_dict['features'], str) else row_dict['features']
            training_data.append(row_dict)
        
        return {
            "status": "success",
            "data": training_data,
            "count": len(training_data),
            "filters": {
                "symbol": symbol,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit
            }
        }
    except Exception as e:
        logger.error(f"Error fetching ML training data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/database/ml-predictions", summary="Get ML Predictions")
async def get_ml_predictions(
    symbol: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1000
):
    """Получить ML прогнозы из базы данных"""
    try:
        if not db_manager:
            raise HTTPException(status_code=500, detail="Database manager not initialized")
        
        # Выполнить SQL запрос для получения ML predictions
        query = """
        SELECT id, symbol, predicted_price_change, predicted_direction, 
               confidence, features, model_version, reference_price, created_at,
               actual_price_change, is_correct, validated_at
        FROM ml_predictions
        WHERE 1=1
        """
        
        params = []
        if symbol:
            query += " AND symbol = $" + str(len(params) + 1)
            params.append(symbol)
        
        if start_time:
            query += " AND created_at >= $" + str(len(params) + 1)
            params.append(start_time)
        
        if end_time:
            query += " AND created_at <= $" + str(len(params) + 1)
            params.append(end_time)
        
        query += " ORDER BY created_at DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        rows = await db_manager._execute(query, *params, fetch=True, operation_name="get_ml_predictions")
        
        # Преобразовать в список словарей
        predictions = []
        for row in rows:
            row_dict = dict(row)
            # Преобразовать JSON поля
            if row_dict.get('features'):
                row_dict['features'] = json.loads(row_dict['features']) if isinstance(row_dict['features'], str) else row_dict['features']
            predictions.append(row_dict)
        
        return {
            "status": "success",
            "data": predictions,
            "count": len(predictions),
            "filters": {
                "symbol": symbol,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit
            }
        }
    except Exception as e:
        logger.error(f"Error fetching ML predictions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/database/alerts", summary="Get Historical Alerts")
async def get_historical_alerts(
    symbol: Optional[str] = None,
    alert_type: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1000
):
    """Получить исторические алерты из базы данных"""
    try:
        if not db_manager:
            raise HTTPException(status_code=500, detail="Database manager not initialized")
        
        # Пересоздадим пул соединений для очистки кэша
        try:
            await db_manager.recreate_pool()
            logger.info("Database pool recreated")
        except Exception as e:
            logger.error(f"Error recreating pool: {e}")
        
        # Проверим, существует ли таблица alerts
        try:
            table_exists_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'alerts'
            );
            """
            table_exists = await db_manager._execute(table_exists_query, fetchval=True)
            logger.info(f"Alerts table exists: {table_exists}")
            
            if not table_exists:
                return {"data": [], "total": 0, "message": "Alerts table does not exist"}
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return {"data": [], "total": 0, "message": f"Error checking table: {e}"}
        
        # Сначала проверим, какие колонки существуют в таблице alerts
        try:
            columns_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'alerts' AND table_schema = 'public'
            ORDER BY ordinal_position;
            """
            columns_result = await db_manager._execute(columns_query, fetch=True)
            available_columns = [row['column_name'] for row in columns_result]
            logger.info(f"Available columns in alerts table: {available_columns}")
            
            # Построим запрос только с существующими колонками
            base_columns = ['id', 'symbol', 'alert_type', 'severity', 'description', 'created_at']
            select_columns = [col for col in base_columns if col in available_columns]
            
            if not select_columns:
                return {"data": [], "total": 0, "message": "No valid columns found in alerts table"}
                
        except Exception as e:
            logger.error(f"Error checking columns: {e}")
            return {"data": [], "total": 0, "message": f"Error checking columns: {e}"}
        
        # Выполнить SQL запрос для получения alerts
        query = f"""
        SELECT {', '.join(select_columns)}
        FROM alerts
        WHERE 1=1
        """
        
        params = []
        if symbol:
            query += " AND symbol = $" + str(len(params) + 1)
            params.append(symbol)
        
        if alert_type:
            query += " AND alert_type = $" + str(len(params) + 1)
            params.append(alert_type)
        
        if start_time:
            query += " AND created_at >= $" + str(len(params) + 1)
            params.append(start_time)
        
        if end_time:
            query += " AND created_at <= $" + str(len(params) + 1)
            params.append(end_time)
        
        query += " ORDER BY created_at DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        rows = await db_manager._execute(query, *params, fetch=True, operation_name="get_alerts")
        
        # Преобразовать в список словарей
        alerts = []
        for row in rows:
            row_dict = dict(row)
            # Преобразовать JSON поля
            if row_dict.get('metadata'):
                row_dict['metadata'] = json.loads(row_dict['metadata']) if isinstance(row_dict['metadata'], str) else row_dict['metadata']
            alerts.append(row_dict)
        
        return {
            "status": "success",
            "data": alerts,
            "count": len(alerts),
            "filters": {
                "symbol": symbol,
                "alert_type": alert_type,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit
            }
        }
    except Exception as e:
        logger.error(f"Error fetching historical alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/batch/status", summary="Get Batch Processing Status")
async def get_batch_status():
    """Получить статус пакетной обработки данных."""
    try:
        if not batch_integration:
            raise HTTPException(status_code=503, detail="Batch integration not initialized")
        
        status = await batch_integration.get_status()
        return {
            "success": True,
            "data": status
        }
    except Exception as e:
        logger.error(f"Error getting batch status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/batch/metrics", summary="Get Batch Processing Metrics")
async def get_batch_metrics():
    """Получить метрики пакетной обработки."""
    try:
        if not batch_integration:
            raise HTTPException(status_code=503, detail="Batch integration not initialized")
        
        metrics = await batch_integration.get_metrics()
        return {
            "success": True,
            "data": metrics
        }
    except Exception as e:
        logger.error(f"Error getting batch metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.post("/batch/config", summary="Update Batch Processing Configuration")
async def update_batch_config(config_data: dict):
    """Обновить конфигурацию пакетной обработки."""
    try:
        if not batch_integration:
            raise HTTPException(status_code=503, detail="Batch integration not initialized")
        
        await batch_integration.update_config(config_data)
        return {
            "success": True,
            "message": "Batch configuration updated successfully"
        }
    except Exception as e:
        logger.error(f"Error updating batch config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/database/stats", summary="Get Database Statistics")
async def get_database_stats():
    """Получить статистику базы данных"""
    try:
        if not db_manager:
            raise HTTPException(status_code=500, detail="Database manager not initialized")
        
        stats = {}
        
        # Получить статистику для каждой таблицы
        tables = ['raw_trades', 'raw_orderbooks', 'ml_training_data', 'ml_predictions', 'alerts']
        
        for table in tables:
            # Общее количество записей
            total_count = await db_manager._execute(f"SELECT COUNT(*) FROM {table}", fetchval=True, operation_name=f"count_{table}")
            
            # Количество уникальных символов
            unique_symbols = await db_manager._execute(f"SELECT COUNT(DISTINCT symbol) FROM {table} WHERE symbol IS NOT NULL", fetchval=True, operation_name=f"unique_symbols_{table}")
            
            # Диапазон дат - используем правильные названия колонок для каждой таблицы
            if table in ['raw_trades', 'raw_orderbooks']:
                # Для этих таблиц используем received_at
                date_range = await db_manager._execute(f"SELECT MIN(received_at), MAX(received_at) FROM {table} WHERE received_at IS NOT NULL", fetch=True, operation_name=f"date_range_{table}")
            elif table == 'alerts':
                # Для alerts используем created_at
                date_range = await db_manager._execute(f"SELECT MIN(created_at), MAX(created_at) FROM {table} WHERE created_at IS NOT NULL", fetch=True, operation_name=f"date_range_{table}")
            elif table in ['ml_training_data', 'ml_predictions']:
                # Для ML таблиц используем created_at
                date_range = await db_manager._execute(f"SELECT MIN(created_at), MAX(created_at) FROM {table} WHERE created_at IS NOT NULL", fetch=True, operation_name=f"date_range_{table}")
            else:
                # Fallback для других таблиц
                date_range = [(None, None)]
            
            date_range_row = date_range[0] if date_range else (None, None)
            
            stats[table] = {
                "total_records": total_count,
                "unique_symbols": unique_symbols,
                "date_range": {
                    "earliest": date_range_row[0].isoformat() if date_range_row[0] else None,
                    "latest": date_range_row[1].isoformat() if date_range_row[1] else None
                }
            }
        
        return {
            "status": "success",
            "data": stats
        }
    except Exception as e:
        logger.error(f"Error fetching database stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Include API router in the main app
app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn
    import logging
    
    # Настройка uvicorn для корректного отображения логов приложения
    # Устанавливаем уровень логирования для uvicorn
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.setLevel(logging.INFO)
    
    # Запускаем с минимальными настройками, чтобы не перехватывать логи приложения
    uvicorn.run(app, host="0.0.0.0", port=5000, ws="auto", 
                log_level="info",  # Изменено на info для уменьшения количества логов
                log_config=None)  # Используем настройки логирования приложения


