#!/usr/bin/env python3
"""
Скрипт для исправления проблемы с массовой подпиской WebSocket.
Подписывается на символы батчами с задержками.
"""

import asyncio
import json
from typing import List
from bybit_websocket_client import BybitWebSocketClient
from db_manager import DBManager
from message_broker import MessageBroker
import os
import logging
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchSubscriptionManager:
    def __init__(self, ws_client: BybitWebSocketClient, batch_size: int = 50, delay_between_batches: float = 2.0):
        self.ws_client = ws_client
        self.batch_size = batch_size
        self.delay_between_batches = delay_between_batches
    
    async def subscribe_symbols_in_batches(self, symbols: List[str]):
        """Подписывается на символы батчами с задержками."""
        total_symbols = len(symbols)
        logger.info(f"Начинаем батчевую подписку на {total_symbols} символов (батчи по {self.batch_size})")
        
        successful_subscriptions = 0
        failed_subscriptions = 0
        
        # Разбиваем символы на батчи
        for i in range(0, total_symbols, self.batch_size):
            batch = symbols[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total_symbols + self.batch_size - 1) // self.batch_size
            
            logger.info(f"Обрабатываем батч {batch_num}/{total_batches} ({len(batch)} символов)")
            
            try:
                # Подписываемся на publicTrade для всего батча
                await self.ws_client.subscribe("publicTrade", batch)
                logger.info(f"✅ Подписались на publicTrade для {len(batch)} символов")
                
                # Небольшая задержка между типами подписок
                await asyncio.sleep(0.5)
                
                # Подписываемся на orderbook для всего батча
                await self.ws_client.subscribe("orderbook.1", batch)
                logger.info(f"✅ Подписались на orderbook для {len(batch)} символов")
                
                successful_subscriptions += len(batch)
                
            except Exception as e:
                logger.error(f"❌ Ошибка подписки на батч {batch_num}: {e}")
                failed_subscriptions += len(batch)
            
            # Задержка между батчами (кроме последнего)
            if i + self.batch_size < total_symbols:
                logger.info(f"Ожидание {self.delay_between_batches} секунд перед следующим батчем...")
                await asyncio.sleep(self.delay_between_batches)
        
        logger.info(f"Батчевая подписка завершена:")
        logger.info(f"  Успешно: {successful_subscriptions} символов")
        logger.info(f"  Ошибки: {failed_subscriptions} символов")
        
        return successful_subscriptions, failed_subscriptions

async def message_handler(data: dict):
    """Простой обработчик сообщений для тестирования."""
    if 'topic' in data:
        topic = data['topic']
        if 'publicTrade' in topic:
            symbol = topic.replace('publicTrade.', '')
            logger.debug(f"Получены торговые данные для {symbol}")
        elif 'orderbook' in topic:
            symbol = topic.replace('orderbook.', '')
            logger.debug(f"Получены данные стакана для {symbol}")

async def test_batch_subscription():
    """Тестирует батчевую подписку."""
    # Инициализация компонентов
    db_url = os.getenv('DATABASE_URL', 'postgresql://bybit:bybit@localhost:5432/crypto_manipulation')
    db_manager = DBManager(db_url)
    await db_manager.connect()
    
    rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')
    message_broker = MessageBroker(rabbitmq_url)
    await message_broker.connect()
    
    # Создаем WebSocket клиент
    ws_uri = "wss://stream.bybit.com/v5/public/linear"
    ws_client = BybitWebSocketClient(ws_uri, message_handler)
    
    try:
        # Подключаемся к WebSocket
        await ws_client.connect()
        logger.info("WebSocket подключен")
        
        # Получаем символы из watchlist
        watchlist_symbols = await db_manager.get_watchlist_symbols()
        logger.info(f"Получено {len(watchlist_symbols)} символов из watchlist")
        
        # Создаем менеджер батчевой подписки
        batch_manager = BatchSubscriptionManager(ws_client, batch_size=30, delay_between_batches=1.5)
        
        # Выполняем батчевую подписку
        successful, failed = await batch_manager.subscribe_symbols_in_batches(watchlist_symbols)
        
        logger.info(f"\nРезультат батчевой подписки:")
        logger.info(f"Успешно подписались на {successful} символов")
        logger.info(f"Ошибки при подписке на {failed} символов")
        
        # Ждем немного для получения данных
        logger.info("\nОжидание 30 секунд для проверки поступления данных...")
        await asyncio.sleep(30)
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await message_broker.disconnect()
        await db_manager.disconnect()
        if ws_client.websocket:
            await ws_client.disconnect()
        logger.info("Соединения закрыты")

if __name__ == "__main__":
    asyncio.run(test_batch_subscription())