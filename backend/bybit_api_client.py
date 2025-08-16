import aiohttp
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

class BybitAPIClient:
    """Клиент для работы с REST API Bybit."""
    
    def __init__(self):
        self.base_url = "https://api.bybit.com"
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def _get_session(self) -> aiohttp.ClientSession:
        """Получает или создает HTTP сессию."""
        if not self.session or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
        
    async def close(self):
        """Закрывает HTTP сессию."""
        if self.session and not self.session.closed:
            await self.session.close()
            
    async def get_all_futures_symbols(self) -> List[Dict[str, Any]]:
        """Получает список всех фьючерсных торговых пар с Bybit."""
        session = await self._get_session()
        url = f"{self.base_url}/v5/market/instruments-info"
        
        params = {
            "category": "linear",  # Фьючерсы USDT
            "status": "Trading",   # Только активные пары
            "limit": 1000
        }
        
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0:
                        instruments = data.get("result", {}).get("list", [])
                        
                        # Фильтруем и форматируем данные
                        symbols = []
                        for instrument in instruments:
                            symbol_info = {
                                "symbol": instrument.get("symbol"),
                                "baseCoin": instrument.get("baseCoin"),
                                "quoteCoin": instrument.get("quoteCoin"),
                                "status": instrument.get("status"),
                                "contractType": instrument.get("contractType"),
                                "launchTime": instrument.get("launchTime"),
                                "deliveryTime": instrument.get("deliveryTime"),
                                "minPrice": float(instrument.get("priceFilter", {}).get("minPrice", 0)),
                                "maxPrice": float(instrument.get("priceFilter", {}).get("maxPrice", 0)),
                                "tickSize": float(instrument.get("priceFilter", {}).get("tickSize", 0)),
                                "minOrderQty": float(instrument.get("lotSizeFilter", {}).get("minOrderQty", 0)),
                                "maxOrderQty": float(instrument.get("lotSizeFilter", {}).get("maxOrderQty", 0))
                            }
                            symbols.append(symbol_info)
                            
                        # logger.info(f"Получено {len(symbols)} фьючерсных торговых пар")
                        return symbols
                    else:
                        logger.error(f"Ошибка API Bybit: {data.get('retMsg')}")
                        return []
        except Exception as e:
            logger.error(f"Ошибка при получении фьючерсных символов: {e}")
            return []
            
    async def get_multi_timeframe_data(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Получает данные по множественным временным интервалам для символов.
        
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ: Ограничивает количество запросов для избежания таймаутов.
        Получает данные только для первых 50 символов и только для ключевых интервалов.
        """
        session = await self._get_session()
        result = {}
        
        # Ограничиваем количество символов для обработки (первые 50)
        limited_symbols = symbols[:50] if len(symbols) > 50 else symbols
        
        # Интервалы для получения данных о волатильности и изменениях цены
        intervals = {
            '1m': {'interval': '1', 'limit': 2},
            '5m': {'interval': '5', 'limit': 2},
            '15m': {'interval': '15', 'limit': 2},
            '1h': {'interval': '60', 'limit': 2},
            '24h': {'interval': '1440', 'limit': 2}
        }
        
        # logger.info(f"Получение данных по интервалам для {len(limited_symbols)} символов из {len(symbols)}")
        
        # Создаем задачи для параллельного выполнения
        async def fetch_symbol_data(symbol: str) -> tuple[str, Dict[str, Any]]:
            symbol_data = {}
            
            for timeframe, config in intervals.items():
                try:
                    url = f"{self.base_url}/v5/market/kline"
                    params = {
                        "category": "linear",
                        "symbol": symbol,
                        "interval": config['interval'],
                        "limit": config['limit']
                    }
                    
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data.get("retCode") == 0:
                                klines = data.get("result", {}).get("list", [])
                                
                                if len(klines) >= 2:
                                    # Берем последние две свечи для расчета изменений
                                    current = klines[0]  # Последняя свеча
                                    previous = klines[1]  # Предыдущая свеча
                                    
                                    current_close = float(current[4])
                                    previous_close = float(previous[4])
                                    current_high = float(current[2])
                                    current_low = float(current[3])
                                    current_volume = float(current[5])
                                    
                                    # Расчет изменения цены в процентах
                                    price_change_percent = 0
                                    if previous_close > 0:
                                        price_change_percent = ((current_close - previous_close) / previous_close) * 100
                                    
                                    # Расчет волатильности (разброс цены в процентах от закрытия)
                                    volatility = 0
                                    if current_close > 0:
                                        volatility = ((current_high - current_low) / current_close) * 100
                                    
                                    symbol_data[timeframe] = {
                                        'price_change_percent': price_change_percent,
                                        'volatility': volatility,
                                        'volume': current_volume,
                                        'close_price': current_close
                                    }
                                else:
                                    # Если данных недостаточно, заполняем нулями
                                    symbol_data[timeframe] = {
                                        'price_change_percent': 0,
                                        'volatility': 0,
                                        'volume': 0,
                                        'close_price': 0
                                    }
                            else:
                                symbol_data[timeframe] = {
                                    'price_change_percent': 0,
                                    'volatility': 0,
                                    'volume': 0,
                                    'close_price': 0
                                }
                        else:
                            symbol_data[timeframe] = {
                                'price_change_percent': 0,
                                'volatility': 0,
                                'volume': 0,
                                'close_price': 0
                            }
                            
                except Exception as e:
                    logger.error(f"Ошибка при получении данных {timeframe} для {symbol}: {e}")
                    symbol_data[timeframe] = {
                        'price_change_percent': 0,
                        'volatility': 0,
                        'volume': 0,
                        'close_price': 0
                    }
                    
                # Небольшая задержка между запросами для одного символа
                await asyncio.sleep(0.05)
            
            return symbol, symbol_data
        
        # Выполняем запросы для символов с ограничением параллелизма
        semaphore = asyncio.Semaphore(10)  # Максимум 10 параллельных запросов
        
        async def fetch_with_semaphore(symbol: str):
            async with semaphore:
                return await fetch_symbol_data(symbol)
        
        # Создаем задачи для всех символов
        tasks = [fetch_with_semaphore(symbol) for symbol in limited_symbols]
        
        # Выполняем все задачи параллельно
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        for result_item in results:
            if isinstance(result_item, Exception):
                logger.error(f"Ошибка при получении данных: {result_item}")
                continue
            symbol, symbol_data = result_item
            result[symbol] = symbol_data
        
        # Для остальных символов заполняем пустыми данными
        for symbol in symbols[50:]:
            result[symbol] = {
                '1h': {'price_change_percent': 0, 'volatility': 0, 'volume': 0, 'close_price': 0},
                '24h': {'price_change_percent': 0, 'volatility': 0, 'volume': 0, 'close_price': 0}
            }
        
        # logger.info(f"Получены данные по интервалам для {len(result)} символов")
        return result

    async def get_24h_ticker(self, symbols: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """Получает 24-часовую статистику для символов.
        
        Оптимизированная версия: всегда получает все тикеры одним запросом,
        затем фильтрует нужные символы для избежания множественных API вызовов.
        """
        session = await self._get_session()
        url = f"{self.base_url}/v5/market/tickers"
        
        params = {
            "category": "linear"
        }
        
        try:
            # Всегда получаем все тикеры одним запросом для оптимизации
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0:
                        tickers = data.get("result", {}).get("list", [])
                        result = {}
                        
                        # Создаем set для быстрого поиска, если нужна фильтрация
                        symbols_set = set(symbols) if symbols else None
                        
                        for ticker in tickers:
                            symbol = ticker.get("symbol")
                            
                            # Фильтруем символы, если список передан
                            if symbols_set and symbol not in symbols_set:
                                continue
                                
                            result[symbol] = {
                                "symbol": symbol,
                                "lastPrice": float(ticker.get("lastPrice", 0)),
                                "price24hPcnt": float(ticker.get("price24hPcnt", 0)),
                                "volume24h": float(ticker.get("volume24h", 0)),
                                "turnover24h": float(ticker.get("turnover24h", 0)),
                                "highPrice24h": float(ticker.get("highPrice24h", 0)),
                                "lowPrice24h": float(ticker.get("lowPrice24h", 0))
                            }
                        
                        # logger.info(f"Получено {len(result)} тикеров из {len(tickers)} доступных")
                        return result
                else:
                    logger.error(f"HTTP ошибка при получении тикеров: {response.status}")
                    return {}
        except Exception as e:
            logger.error(f"Ошибка при получении тикеров: {e}", exc_info=True)
            return {}
                
    async def get_kline_data(self, symbol: str, interval: str = "1", limit: int = 200) -> List[Dict[str, Any]]:
        """Получает данные свечей для символа."""
        session = await self._get_session()
        url = f"{self.base_url}/v5/market/kline"
        
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0:
                        klines = data.get("result", {}).get("list", [])
                        formatted_klines = []
                        
                        for kline in klines:
                            formatted_klines.append({
                                "startTime": int(kline[0]),
                                "openPrice": float(kline[1]),
                                "highPrice": float(kline[2]),
                                "lowPrice": float(kline[3]),
                                "closePrice": float(kline[4]),
                                "volume": float(kline[5]),
                                "turnover": float(kline[6])
                            })
                            
                        return formatted_klines
                    else:
                        logger.error(f"Ошибка API при получении свечей для {symbol}: {data.get('retMsg')}")
                        return []
                else:
                    logger.error(f"HTTP ошибка при получении свечей для {symbol}: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Ошибка при получении свечей для {symbol}: {e}", exc_info=True)
            return []
    
    async def get_orderbook(self, symbol: str, limit: int = 50) -> Optional[Dict[str, Any]]:
        """Получает стакан заявок для символа."""
        session = await self._get_session()
        url = f"{self.base_url}/v5/market/orderbook"
        
        params = {
            "category": "linear",
            "symbol": symbol,
            "limit": limit
        }
        
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0:
                        result = data.get("result", {})
                        return {
                            "s": symbol,
                            "b": result.get("b", []),  # bids
                            "a": result.get("a", []),  # asks
                            "ts": result.get("ts", 0)   # timestamp
                        }
                    else:
                        logger.error(f"Ошибка API при получении стакана для {symbol}: {data.get('retMsg')}")
                        return None
                else:
                    logger.error(f"HTTP ошибка при получении стакана для {symbol}: {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"Ошибка при получении стакана для {symbol}: {e}", exc_info=True)
            return None
    
    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Получает последние сделки для символа."""
        session = await self._get_session()
        url = f"{self.base_url}/v5/market/recent-trade"
        
        params = {
            "category": "linear",
            "symbol": symbol,
            "limit": limit
        }
        
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0:
                        trades = data.get("result", {}).get("list", [])
                        return [{
                            "price": trade.get("price", "0"),
                            "size": trade.get("size", "0"),
                            "side": trade.get("side", ""),
                            "time": trade.get("time", 0),
                            "p": float(trade.get("price", 0)),
                            "v": float(trade.get("size", 0)),
                            "S": trade.get("side", ""),
                            "T": int(trade.get("time", 0))
                        } for trade in trades]
                    else:
                        logger.error(f"Ошибка API при получении сделок для {symbol}: {data.get('retMsg')}")
                        return []
                else:
                    logger.error(f"HTTP ошибка при получении сделок для {symbol}: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Ошибка при получении сделок для {symbol}: {e}", exc_info=True)
            return []
    
    async def get_instruments_info(self, category: str = "linear", status: str = "Trading", limit: int = 1000) -> Dict[str, Any]:
        """Получает информацию об инструментах (совместимость с pybit)."""
        try:
            symbols_data = await self.get_all_futures_symbols()
            return {
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": symbols_data
                }
            }
        except Exception as e:
            logger.error(f"Ошибка при получении информации об инструментах: {e}")
            return {
                "retCode": -1,
                "retMsg": str(e),
                "result": {
                    "list": []
                }
            }

