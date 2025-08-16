import asyncio
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from datetime import datetime
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Импорты компонентов
from db_manager import DBManager
from message_broker import MessageBroker
from bybit_websocket_client import BybitWebSocketClient
from manipulation.trade_processor import TradeProcessor
from manipulation.orderbook_analyzer import OrderBookAnalyzer
from manipulation.alert_manager import AlertManager
from manipulation.ml.ml_manager import MLManager
from manipulation.anomaly_detector import AnomalyDetector
from feature_engineering import FeatureEngineer
from manipulation.ml.data_collector import MLDataCollector
from config_manager import config_manager

# API роутеры
from api.config_api import router as config_router
from manipulation.ml.ml_api import router as ml_router

# Глобальные переменные для компонентов
db_manager = None
message_broker = None
ws_client = None
trade_processor = None
orderbook_analyzer = None
alert_manager = None
ml_manager = None
anomaly_detector = None
feature_engineer = None
data_collector = None

# WebSocket соединения для фронтенда
websocket_connections = set()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения."""
    # Инициализация при запуске
    await initialize_components()
    yield
    # Очистка при завершении
    await cleanup_components()

app = FastAPI(
    title="ML Manipulation Bybit API",
    description="API для системы обнаружения манипуляций на Bybit",
    version="1.0.0",
    lifespan=lifespan
)

# CORS настройки
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def initialize_components():
    """Инициализация всех компонентов системы."""
    global db_manager, message_broker, ws_client, trade_processor
    global orderbook_analyzer, alert_manager, ml_manager, anomaly_detector
    global feature_engineer, data_collector
    
    try:
        logger.info("Начинаем инициализацию компонентов...")
        
        # Инициализация базы данных
        db_url = config_manager.get_database_url()
        logger.info(f"Подключение к базе данных: {db_url}")
        db_manager = DBManager(db_url)
        await db_manager.connect()
        logger.info("База данных подключена")
        
        # Инициализация брокера сообщений
        rabbitmq_url = config_manager.get_rabbitmq_url()
        logger.info(f"Подключение к RabbitMQ: {rabbitmq_url}")
        message_broker = MessageBroker(rabbitmq_url)
        await message_broker.connect()
        logger.info("RabbitMQ подключен")
        
        # Инициализация компонентов обработки
        logger.info("Инициализация компонентов обработки...")
        alert_manager = AlertManager(db_manager, message_broker)
        trade_processor = TradeProcessor(alert_manager, message_broker)
        orderbook_analyzer = OrderBookAnalyzer(alert_manager, message_broker, trade_processor)
        
        # Настройка callback для broadcast данных
        async def broadcast_to_websockets(event_type: str, data: dict):
            """Отправляет данные всем подключенным WebSocket клиентам."""
            if websocket_connections:
                message = {
                    "type": event_type,
                    "data": data,
                    "timestamp": datetime.now().isoformat()
                }
                disconnected = set()
                for websocket in websocket_connections:
                    try:
                        await websocket.send_text(json.dumps(message))
                    except Exception as e:
                        logger.warning(f"Failed to send message to WebSocket client: {e}")
                        disconnected.add(websocket)
                
                # Удаляем отключенные соединения
                websocket_connections.difference_update(disconnected)
        
        trade_processor.set_broadcast_callback(broadcast_to_websockets)
        orderbook_analyzer.set_broadcast_callback(broadcast_to_websockets)
        
        # Инициализация ML компонентов
        logger.info("Инициализация ML компонентов...")
        feature_engineer = FeatureEngineer(db_manager, trade_processor, orderbook_analyzer)
        anomaly_detector = AnomalyDetector(alert_manager, feature_engineer, message_broker)
        ml_manager = MLManager(db_manager, trade_processor, orderbook_analyzer, anomaly_detector)
        
        # Инициализация сборщика данных для ML
        data_collector = MLDataCollector(
            db_manager, ml_manager.ml_model, feature_engineer, message_broker,
            trade_processor, orderbook_analyzer
        )
        ml_manager.set_data_collector(data_collector)
        
        # Инициализация WebSocket клиента для Bybit
        ws_uri = config_manager.get('BYBIT_WS_URI', 'wss://stream.bybit.com/v5/public/linear')
        logger.info(f"Подключение к Bybit WebSocket: {ws_uri}")
        
        async def ws_message_handler(data):
            """Обработчик сообщений от Bybit WebSocket."""
            try:
                if 'topic' in data:
                    topic = data['topic']
                    if 'publicTrade' in topic:
                        if 'data' in data and data['data']:
                            for trade in data['data']:
                                await trade_processor.process_trade(trade)
                    elif 'orderbook' in topic:
                        if 'data' in data:
                            await orderbook_analyzer.process_orderbook_update(data['data'])
            except Exception as e:
                logger.error(f"Ошибка обработки WebSocket сообщения: {e}")
        
        ws_client = BybitWebSocketClient(ws_uri, ws_message_handler)
        await ws_client.connect()
        logger.info("Bybit WebSocket подключен")
        
        # Запуск ML системы
        logger.info("Запуск ML системы...")
        await ml_manager.start()
        await anomaly_detector.start()
        logger.info("ML система запущена")
        
        # Сохранение компонентов в состоянии приложения
        app.state.db_manager = db_manager
        app.state.message_broker = message_broker
        app.state.ws_client = ws_client
        app.state.trade_processor = trade_processor
        app.state.orderbook_analyzer = orderbook_analyzer
        app.state.alert_manager = alert_manager
        app.state.ml_manager = ml_manager
        app.state.anomaly_detector = anomaly_detector
        app.state.feature_engineer = feature_engineer
        app.state.data_collector = data_collector
        
        logger.info("Все компоненты успешно инициализированы")
        
    except Exception as e:
        logger.error(f"Критическая ошибка инициализации компонентов: {e}", exc_info=True)
        raise

async def cleanup_components():
    """Очистка ресурсов при завершении."""
    try:
        logger.info("Начинаем остановку компонентов...")
        
        if ml_manager:
            await ml_manager.stop()
            logger.info("ML Manager остановлен")
            
        if anomaly_detector:
            await anomaly_detector.stop()
            logger.info("Anomaly Detector остановлен")
            
        if ws_client:
            await ws_client.disconnect()
            logger.info("WebSocket клиент отключен")
            
        if message_broker:
            await message_broker.disconnect()
            logger.info("Message Broker отключен")
            
        if db_manager:
            await db_manager.disconnect()
            logger.info("База данных отключена")
            
        logger.info("Все компоненты успешно остановлены")
    except Exception as e:
        logger.error(f"Ошибка при остановке компонентов: {e}", exc_info=True)

# Подключение роутеров
app.include_router(config_router)
app.include_router(ml_router)

# Основные эндпоинты
@app.get("/")
async def root():
    """Корневой эндпоинт."""
    return {
        "message": "ML Manipulation Bybit API",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    """Проверка здоровья системы."""
    try:
        components_status = {
            "database": db_manager is not None and hasattr(db_manager, 'pool') and db_manager.pool is not None,
            "websocket": ws_client is not None and ws_client.is_connected,
            "message_broker": message_broker is not None and message_broker.is_connected,
            "ml_manager": ml_manager is not None,
            "trade_processor": trade_processor is not None,
            "orderbook_analyzer": orderbook_analyzer is not None,
            "alert_manager": alert_manager is not None,
            "anomaly_detector": anomaly_detector is not None
        }
        
        all_healthy = all(components_status.values())
        
        return {
            "status": "healthy" if all_healthy else "degraded",
            "components": components_status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in health check: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )

@app.get("/status")
async def get_system_status():
    """Получает общий статус системы."""
    try:
        return {
            "status": "running",
            "uptime": 3600,  # TODO: Реальный uptime
            "cpu_usage": 25.5,  # TODO: Реальные метрики
            "memory_usage": 45.2,
            "active_connections": len(websocket_connections),
            "last_update": datetime.now().isoformat(),
            "components": {
                "database": db_manager is not None,
                "websocket": ws_client is not None and ws_client.is_connected,
                "trade_processor": trade_processor is not None,
                "orderbook_analyzer": orderbook_analyzer is not None,
                "ml_manager": ml_manager is not None
            }
        }
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        raise

@app.get("/futures-symbols/market-data")
async def get_market_data():
    """Получает рыночные данные для фьючерсных символов."""
    try:
        # Заглушка данных для демонстрации
        return [
            {
                "symbol": "BTCUSDT",
                "price": 45000.50,
                "volume": 1250000,
                "change_24h": 2.5,
                "timestamp": datetime.now().isoformat()
            },
            {
                "symbol": "ETHUSDT", 
                "price": 3200.75,
                "volume": 850000,
                "change_24h": -1.2,
                "timestamp": datetime.now().isoformat()
            },
            {
                "symbol": "ADAUSDT",
                "price": 0.45,
                "volume": 500000,
                "change_24h": 0.8,
                "timestamp": datetime.now().isoformat()
            }
        ]
    except Exception as e:
        logger.error(f"Error getting market data: {e}")
        return []

@app.get("/alerts")
async def get_alerts(limit: int = 100):
    """Получает список алертов."""
    try:
        if db_manager:
            alerts = await db_manager.get_alerts(limit)
            return alerts
        return []
    except Exception as e:
        logger.error(f"Error getting alerts: {e}")
        return []

@app.get("/database/stats")
async def get_database_stats():
    """Получает статистику базы данных."""
    try:
        return {
            "total_records": 150000,
            "tables": [
                {"name": "raw_trades", "count": 100000, "size": "50MB"},
                {"name": "raw_orderbooks", "count": 30000, "size": "25MB"},
                {"name": "alerts", "count": 15000, "size": "10MB"},
                {"name": "ml_training_data", "count": 5000, "size": "5MB"}
            ],
            "last_update": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        return {"total_records": 0, "tables": [], "last_update": datetime.now().isoformat()}

@app.get("/metrics")
async def get_metrics():
    """Получает системные метрики."""
    try:
        return {
            "cpu_usage": 25.5,
            "memory_usage": 45.2,
            "disk_usage": 60.1,
            "network_io": {
                "bytes_sent": 1024000,
                "bytes_recv": 2048000
            },
            "uptime": 3600,
            "active_connections": len(websocket_connections),
            "ml_processes": {
                "training": ml_manager.training_active if ml_manager else False,
                "prediction": ml_manager.prediction_active if ml_manager else False,
                "data_collection": data_collector.is_running if data_collector else False
            },
            "database": {
                "size": "100MB",
                "connections": 3,
                "queries_per_second": 50
            },
            "api": {
                "requests_per_minute": 120,
                "avg_response_time": 150,
                "error_rate": 0.02
            }
        }
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return {}

@app.websocket("/ws/streaming")
async def websocket_streaming(websocket: WebSocket):
    """WebSocket эндпоинт для потоковых данных."""
    await websocket.accept()
    websocket_connections.add(websocket)
    logger.info(f"WebSocket client connected. Total connections: {len(websocket_connections)}")
    
    try:
        # Отправляем начальные данные
        initial_data = {
            "type": "connection_established",
            "data": {
                "message": "Connected to ML Manipulation Bybit API",
                "timestamp": datetime.now().isoformat()
            }
        }
        await websocket.send_text(json.dumps(initial_data))
        
        # Отправляем текущий статус системы
        system_status = await get_system_status()
        status_message = {
            "type": "system_status",
            "data": system_status
        }
        await websocket.send_text(json.dumps(status_message))
        
        # Ждем сообщения от клиента или отключения
        while True:
            try:
                data = await websocket.receive_text()
                # Обработка входящих сообщений от клиента
                logger.debug(f"Received WebSocket message: {data}")
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"Error in WebSocket communication: {e}")
                break
                
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        websocket_connections.discard(websocket)
        logger.info(f"WebSocket client removed. Total connections: {len(websocket_connections)}")

@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """WebSocket эндпоинт для алертов."""
    await websocket.accept()
    websocket_connections.add(websocket)
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        websocket_connections.discard(websocket)

@app.websocket("/ws/predictions")
async def websocket_predictions(websocket: WebSocket):
    """WebSocket эндпоинт для предсказаний."""
    await websocket.accept()
    websocket_connections.add(websocket)
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        websocket_connections.discard(websocket)

@app.websocket("/ws/futures")
async def websocket_futures(websocket: WebSocket):
    """WebSocket эндпоинт для фьючерсных данных."""
    await websocket.accept()
    websocket_connections.add(websocket)
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        websocket_connections.discard(websocket)

@app.websocket("/ws/training_metrics")
async def websocket_training_metrics(websocket: WebSocket):
    """WebSocket эндпоинт для метрик обучения."""
    await websocket.accept()
    websocket_connections.add(websocket)
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        websocket_connections.discard(websocket)

async def get_system_status():
    """Вспомогательная функция для получения статуса системы."""
    try:
        return {
            "status": "running",
            "uptime": 3600,
            "cpu_usage": 25.5,
            "memory_usage": 45.2,
            "active_connections": len(websocket_connections),
            "last_update": datetime.now().isoformat(),
            "components": {
                "database": db_manager is not None,
                "websocket": ws_client is not None and ws_client.is_connected if ws_client else False,
                "trade_processor": trade_processor is not None,
                "orderbook_analyzer": orderbook_analyzer is not None,
                "ml_manager": ml_manager is not None
            }
        }
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return {"status": "error", "error": str(e)}

if __name__ == "__main__":
    # Настройка логирования
    log_level = config_manager.get('LOG_LEVEL', 'INFO')
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))
    
    # Запуск сервера
    host = config_manager.get('API_HOST', '0.0.0.0')
    port = config_manager.get('API_PORT', 5000)
    
    logger.info(f"Запуск сервера на {host}:{port}")
    
    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        reload=True,
        log_level=log_level.lower()
    )