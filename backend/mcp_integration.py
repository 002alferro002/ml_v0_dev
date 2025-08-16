import asyncio
import logging
from typing import Dict, List, Optional, Any
from fastapi import FastAPI
from fastapi_mcp import FastApiMCP
from pydantic import BaseModel, Field
from datetime import datetime
import json

logger = logging.getLogger(__name__)

# Модели данных для MCP инструментов
class SymbolInfo(BaseModel):
    symbol: str = Field(description="Торговая пара")
    base_coin: str = Field(description="Базовая валюта")
    quote_coin: str = Field(description="Котируемая валюта")
    status: str = Field(description="Статус торговой пары")

class OrderBookData(BaseModel):
    symbol: str = Field(description="Торговая пара")
    bids: List[List[str]] = Field(description="Заявки на покупку")
    asks: List[List[str]] = Field(description="Заявки на продажу")
    timestamp: int = Field(description="Временная метка")

class TradeData(BaseModel):
    symbol: str = Field(description="Торговая пара")
    price: float = Field(description="Цена сделки")
    quantity: float = Field(description="Объем сделки")
    side: str = Field(description="Сторона сделки (Buy/Sell)")
    timestamp: int = Field(description="Временная метка")

class WatchlistRequest(BaseModel):
    symbol: str = Field(description="Торговая пара для добавления/удаления")

class AlertRequest(BaseModel):
    symbol: str = Field(description="Торговая пара")
    condition_type: str = Field(description="Тип условия алерта")
    threshold: float = Field(description="Пороговое значение")
    message: Optional[str] = Field(default=None, description="Сообщение алерта")

class MLPredictionRequest(BaseModel):
    symbol: str = Field(description="Торговая пара")
    timeframe: str = Field(description="Таймфрейм для предсказания")
    features: Optional[Dict[str, Any]] = Field(default=None, description="Дополнительные признаки")

class MCPIntegration:
    """Интеграция MCP (Model Context Protocol) с FastAPI для расширения функциональности."""
    
    def __init__(self, app: FastAPI):
        self.app = app
        self.mcp_server = FastApiMCP(app)
        self.is_running = False
        self.setup_endpoints()
        logger.info("MCPIntegration инициализирован с FastApiMCP")
        
    def setup_endpoints(self):
        """Настройка эндпоинтов для MCP инструментов."""
        
        @self.app.get("/mcp/symbols", response_model=List[SymbolInfo])
        async def get_symbols() -> List[SymbolInfo]:
            """Получает список всех доступных торговых пар."""
            try:
                from app_fast_start import bybit_api_client
                if bybit_api_client:
                    symbols_data = await bybit_api_client.get_all_futures_symbols()
                    return [
                        SymbolInfo(
                            symbol=s.get("symbol", ""),
                            base_coin=s.get("baseCoin", ""),
                            quote_coin=s.get("quoteCoin", ""),
                            status=s.get("status", "")
                        ) for s in symbols_data
                    ]
                return []
            except Exception as e:
                logger.error(f"Ошибка получения символов через MCP: {e}")
                return []
        
        @self.app.get("/mcp/orderbook/{symbol}", response_model=Optional[OrderBookData])
        async def get_orderbook(symbol: str) -> Optional[OrderBookData]:
            """Получает стакан заявок для указанной торговой пары."""
            try:
                from app_fast_start import bybit_api_client
                if bybit_api_client:
                    orderbook = await bybit_api_client.get_orderbook(symbol)
                    if orderbook:
                        return OrderBookData(
                            symbol=symbol,
                            bids=orderbook.get("b", []),
                            asks=orderbook.get("a", []),
                            timestamp=int(orderbook.get("ts", 0))
                        )
                return None
            except Exception as e:
                logger.error(f"Ошибка получения стакана через MCP для {symbol}: {e}")
                return None
        
        @self.app.get("/mcp/trades/{symbol}", response_model=List[TradeData])
        async def get_recent_trades(symbol: str, limit: int = 50) -> List[TradeData]:
            """Получает последние сделки для указанной торговой пары."""
            try:
                from app_fast_start import bybit_api_client
                if bybit_api_client:
                    trades = await bybit_api_client.get_recent_trades(symbol, limit)
                    return [
                        TradeData(
                            symbol=symbol,
                            price=float(trade.get("p", 0)),
                            quantity=float(trade.get("v", 0)),
                            side=trade.get("S", ""),
                            timestamp=int(trade.get("T", 0))
                        ) for trade in trades
                    ]
                return []
            except Exception as e:
                logger.error(f"Ошибка получения сделок через MCP для {symbol}: {e}")
                return []
        
        @self.app.post("/mcp/watchlist/add")
        async def add_to_watchlist(request: WatchlistRequest) -> Dict[str, Any]:
            """Добавляет символ в список наблюдения."""
            try:
                from app import db_manager
                if db_manager:
                    await db_manager.add_to_watchlist(request.symbol)
                    return {"success": True, "message": f"Символ {request.symbol} добавлен в watchlist"}
                return {"success": False, "message": "DB Manager недоступен"}
            except Exception as e:
                logger.error(f"Ошибка добавления в watchlist через MCP: {e}")
                return {"success": False, "message": str(e)}
        
        @self.app.delete("/mcp/watchlist/remove")
        async def remove_from_watchlist(request: WatchlistRequest) -> Dict[str, Any]:
            """Удаляет символ из списка наблюдения."""
            try:
                from app import db_manager
                if db_manager:
                    await db_manager.remove_from_watchlist(request.symbol)
                    return {"success": True, "message": f"Символ {request.symbol} удален из watchlist"}
                return {"success": False, "message": "DB Manager недоступен"}
            except Exception as e:
                logger.error(f"Ошибка удаления из watchlist через MCP: {e}")
                return {"success": False, "message": str(e)}
        
        @self.app.get("/mcp/watchlist", response_model=List[str])
        async def get_watchlist() -> List[str]:
            """Получает текущий список наблюдения."""
            try:
                from app import db_manager
                if db_manager:
                    watchlist = await db_manager.get_watchlist()
                    return [item['symbol'] for item in watchlist]
                return []
            except Exception as e:
                logger.error(f"Ошибка получения watchlist через MCP: {e}")
                return []
        
        @self.app.post("/mcp/alerts/create")
        async def create_alert(request: AlertRequest) -> Dict[str, Any]:
            """Создает новый алерт."""
            try:
                from app import alert_manager
                if alert_manager:
                    alert_id = await alert_manager.create_alert(
                        symbol=request.symbol,
                        condition_type=request.condition_type,
                        threshold=request.threshold,
                        message=request.message
                    )
                    return {
                        "success": True,
                        "alert_id": alert_id,
                        "message": "Алерт успешно создан"
                    }
                return {"success": False, "message": "Alert Manager недоступен"}
            except Exception as e:
                logger.error(f"Ошибка создания алерта через MCP: {e}")
                return {"success": False, "message": str(e)}
        
        @self.app.post("/mcp/ml/prediction")
        async def get_ml_prediction(request: MLPredictionRequest) -> Dict[str, Any]:
            """Получает ML предсказание для символа."""
            try:
                from app import ml_manager
                if ml_manager:
                    prediction = await ml_manager.get_prediction(request.symbol)
                    return {
                        "success": True,
                        "symbol": request.symbol,
                        "prediction": prediction,
                        "timestamp": datetime.now().isoformat()
                    }
                return {"success": False, "message": "ML Manager недоступен"}
            except Exception as e:
                logger.error(f"Ошибка получения ML предсказания через MCP: {e}")
                return {"success": False, "message": str(e)}
        
        @self.app.get("/mcp/system/status")
        async def get_system_status() -> Dict[str, Any]:
            """Получает статус системы и всех компонентов."""
            try:
                from app_fast_start import (
                    db_manager, bybit_ws_client, bybit_api_client,
                    trade_processor, orderbook_analyzer, alert_manager,
                    ml_manager, anomaly_detector
                )
                
                status = {
                    "timestamp": datetime.now().isoformat(),
                    "components": {
                        "database": db_manager is not None and hasattr(db_manager, 'pool') and db_manager.pool is not None,
                        "websocket_client": bybit_ws_client is not None and bybit_ws_client.is_connected(),
                        "api_client": bybit_api_client is not None,
                        "trade_processor": trade_processor is not None,
                        "orderbook_analyzer": orderbook_analyzer is not None,
                        "alert_manager": alert_manager is not None,
                        "ml_manager": ml_manager is not None,
                        "anomaly_detector": anomaly_detector is not None
                    }
                }
                
                # Добавляем статистику WebSocket подключений
                if bybit_ws_client:
                    try:
                        subscribed_symbols = await bybit_ws_client.get_subscribed_symbols()
                        status["websocket_subscriptions"] = len(subscribed_symbols)
                        status["subscribed_symbols"] = subscribed_symbols
                    except:
                        status["websocket_subscriptions"] = 0
                        status["subscribed_symbols"] = []
                
                return status
            except Exception as e:
                logger.error(f"Ошибка получения статуса системы через MCP: {e}")
                return {"error": str(e), "timestamp": datetime.now().isoformat()}
        
        logger.info("MCP эндпоинты настроены")
        

        
        @self.mcp_server.tool("get_recent_trades")
        async def get_recent_trades(symbol: str, limit: int = 50) -> List[TradeData]:
            """Получает последние сделки для указанной торговой пары."""
            try:
                from app_fast_start import bybit_api_client
                if bybit_api_client:
                    trades = await bybit_api_client.get_recent_trades(symbol, limit)
                    return [
                        TradeData(
                            symbol=symbol,
                            price=trade.get("price", "0"),
                            size=trade.get("size", "0"),
                            side=trade.get("side", ""),
                            timestamp=int(trade.get("time", 0))
                        ) for trade in trades
                    ]
                return []
            except Exception as e:
                logger.error(f"Ошибка получения сделок через MCP для {symbol}: {e}")
                return []
        
    def integrate_with_app(self):
        """Интегрирует MCP сервер с FastAPI приложением."""
        try:
            # MCP эндпоинты уже интегрированы через setup_endpoints()
            logger.info("MCP интеграция успешно добавлена к FastAPI приложению")
        except Exception as e:
            logger.error(f"Ошибка интеграции MCP с FastAPI: {e}")
    
    async def start(self):
        """Запускает MCP сервер."""
        try:
            self.is_running = True
            logger.info("MCP сервер запущен")
        except Exception as e:
            logger.error(f"Ошибка запуска MCP сервера: {e}")
    
    async def stop(self):
        """Останавливает MCP сервер."""
        try:
            self.is_running = False
            logger.info("MCP сервер остановлен")
        except Exception as e:
            logger.error(f"Ошибка остановки MCP сервера: {e}")