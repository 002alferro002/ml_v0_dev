#!/usr/bin/env python3
"""
Утилита для подсчета количества подписок WebSocket.
"""

import asyncio
import os
import sys
from dotenv import load_dotenv

# Добавляем текущую директорию в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_manager import DBManager
from message_broker import MessageBroker
from bybit_websocket_client import BybitWebSocketClient

load_dotenv()

async def count_subscriptions():
    """Подсчитывает количество подписок WebSocket."""
    
    # Инициализация компонентов
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'crypto_manipulation')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'password')
    db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    db_manager = DBManager(db_url)
    await db_manager.connect()
    
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    rabbitmq_port = os.getenv('RABBITMQ_PORT', '5672')
    rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
    rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')
    rabbitmq_url = f'amqp://{rabbitmq_user}:{rabbitmq_password}@{rabbitmq_host}:{rabbitmq_port}/'
    
    message_broker = MessageBroker(rabbitmq_url)
    await message_broker.connect()
    
    try:
        print("=== Анализ подписок WebSocket ===")
        
        # Получаем символы из watchlist
        watchlist_symbols = await db_manager.get_watchlist_symbols()
        print(f"Символов в watchlist: {len(watchlist_symbols)}")
        
        # Получаем все символы из базы данных
        all_symbols = await db_manager.get_futures_symbols(enabled_only=False)
        enabled_symbols = await db_manager.get_futures_symbols(enabled_only=True)
        
        print(f"Всего символов в базе данных: {len(all_symbols)}")
        print(f"Включенных символов в базе данных: {len(enabled_symbols)}")
        
        # Подсчитываем количество подписок
        # Каждый символ подписывается на 2 топика: publicTrade и orderbook.1
        total_subscriptions = len(watchlist_symbols) * 2
        print(f"Общее количество подписок WebSocket: {total_subscriptions}")
        print(f"  - publicTrade подписки: {len(watchlist_symbols)}")
        print(f"  - orderbook.1 подписки: {len(watchlist_symbols)}")
        
        # Показываем первые 10 символов для примера
        if watchlist_symbols:
            print(f"\nПервые 10 символов в watchlist:")
            for i, symbol in enumerate(watchlist_symbols[:10]):
                print(f"  {i+1}. {symbol}")
            if len(watchlist_symbols) > 10:
                print(f"  ... и еще {len(watchlist_symbols) - 10} символов")
        
        print(f"\n=== Итоги ===")
        print(f"Приложение получает данные с биржи для {len(watchlist_symbols)} символов")
        print(f"Подписывается на {total_subscriptions} WebSocket потоков")
        print(f"Из {len(all_symbols)} доступных символов используется {len(enabled_symbols)} включенных")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await message_broker.disconnect()
        await db_manager.disconnect()
        print("\nСоединения закрыты")

if __name__ == "__main__":
    asyncio.run(count_subscriptions())