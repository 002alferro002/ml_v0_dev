#!/usr/bin/env python3
"""
Скрипт для отладки обработки сделок и создания алертов.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Добавляем текущую директорию в путь для импорта модулей
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_manager import DBManager
from message_broker import MessageBroker
from manipulation.trade_processor import TradeProcessor
from manipulation.orderbook_analyzer import OrderBookAnalyzer
from manipulation.alert_manager import AlertManager
from bybit_websocket_client import BybitWebSocketClient

load_dotenv()

# Настройка детального логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DebugTradeProcessor:
    def __init__(self):
        self.trade_count = 0
        self.alert_count = 0
        self.symbols_with_trades = set()
        self.start_time = datetime.now()
        
    async def debug_process_trade(self, trade_processor, trade):
        """Отладочная обработка сделки."""
        try:
            symbol = trade.get('s', 'unknown')
            price = float(trade.get('p', 0))
            size = float(trade.get('q', 0))
            side = trade.get('S', 'unknown')
            
            self.trade_count += 1
            self.symbols_with_trades.add(symbol)
            
            # Логируем каждую 100-ю сделку
            if self.trade_count % 100 == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Обработано {self.trade_count} сделок")
                print(f"  Символов с данными: {len(self.symbols_with_trades)}")
                print(f"  Последняя сделка: {symbol} {side} {price} x {size}")
                
            # Проверяем историю сделок для ETHUSDT
            if symbol == 'ETHUSDT':
                history_len = len(trade_processor.trade_history[symbol])
                if history_len % 50 == 0 and history_len > 0:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ETHUSDT: {history_len} сделок в истории")
                    
                    # Проверяем условия для wash trading
                    recent_trades = list(trade_processor.trade_history[symbol])[-10:]
                    if len(recent_trades) >= 2:
                        last_trade = recent_trades[-1]
                        second_last_trade = recent_trades[-2]
                        
                        time_diff = abs(last_trade['timestamp'] - second_last_trade['timestamp'])
                        volume_diff = abs(last_trade['volume_usdt'] - second_last_trade['volume_usdt']) / last_trade['volume_usdt'] if last_trade['volume_usdt'] > 0 else 1
                        side_different = last_trade['side'] != second_last_trade['side']
                        
                        print(f"    Wash trading check: time_diff={time_diff:.3f}s, volume_diff={volume_diff:.3f}, sides_different={side_different}")
                        
                        if side_different and time_diff < 0.5 and volume_diff < 0.1:
                            print(f"    ⚠️  Условия для wash trading выполнены!")
                            
            # Вызываем оригинальную обработку
            await trade_processor.process_trade(trade)
            
        except Exception as e:
            logger.error(f"Ошибка в отладочной обработке сделки: {e}")
            
    async def debug_create_alert(self, alert_manager, symbol, alert_type, severity, description, data, **kwargs):
        """Отладочное создание алерта."""
        self.alert_count += 1
        current_time = datetime.now()
        
        print(f"\n🚨 АЛЕРТ #{self.alert_count} [{current_time.strftime('%H:%M:%S')}]")
        print(f"   Символ: {symbol}")
        print(f"   Тип: {alert_type}")
        print(f"   Серьезность: {severity}")
        print(f"   Описание: {description}")
        print(f"   Данные: {data}")
        print("=" * 60)
        
        # Вызываем оригинальное создание алерта
        await alert_manager.create_alert(symbol, alert_type, severity, description, data, **kwargs)

async def main():
    """Основная функция отладки."""
    print("Запуск отладки обработки сделок и алертов...")
    print("Нажмите Ctrl+C для остановки\n")
    
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
    
    # Создаем компоненты
    alert_manager = AlertManager(db_manager, message_broker)
    trade_processor = TradeProcessor(alert_manager, message_broker)
    orderbook_analyzer = OrderBookAnalyzer(alert_manager, message_broker)
    
    # Создаем отладчик
    debugger = DebugTradeProcessor()
    
    # Патчим методы для отладки
    original_process_trade = trade_processor.process_trade
    original_create_alert = alert_manager.create_alert
    
    async def patched_process_trade(trade):
        await debugger.debug_process_trade(trade_processor, trade)
        
    async def patched_create_alert(symbol, alert_type, severity, description, data, **kwargs):
        await debugger.debug_create_alert(alert_manager, symbol, alert_type, severity, description, data, **kwargs)
        
    trade_processor.process_trade = patched_process_trade
    alert_manager.create_alert = patched_create_alert
    
    # Обработчик сообщений WebSocket
    async def debug_message_handler(data: dict):
        try:
            if 'topic' in data:
                topic = data['topic']
                if 'trade' in topic:
                    if 'data' in data and data['data']:
                        for trade in data['data']:
                            await trade_processor.process_trade(trade)
                elif 'orderbook' in topic:
                    if 'data' in data:
                        await orderbook_analyzer.process_orderbook_update(data['data'])
        except Exception as e:
            logger.error(f"Ошибка в обработке сообщения: {e}")
    
    # Создаем WebSocket клиент
    bybit_ws_uri = os.getenv("BYBIT_WS_URI", "wss://stream.bybit.com/v5/public/linear")
    ws_client = BybitWebSocketClient(bybit_ws_uri, debug_message_handler)
    
    try:
        # Подключаемся
        await ws_client.connect()
        
        # Подписываемся на несколько символов для тестирования
        test_symbols = ['ETHUSDT', 'BTCUSDT']
        
        print(f"Подписываемся на данные для символов: {test_symbols}")
        for symbol in test_symbols:
            await ws_client.subscribe_to_symbol(symbol)
            
        # Статистика каждые 60 секунд
        async def print_stats():
            while True:
                await asyncio.sleep(60)
                elapsed = (datetime.now() - debugger.start_time).total_seconds()
                print(f"\n📊 Статистика за {elapsed:.0f} секунд:")
                print(f"   Обработано сделок: {debugger.trade_count}")
                print(f"   Создано алертов: {debugger.alert_count}")
                print(f"   Символов с данными: {len(debugger.symbols_with_trades)}")
                print(f"   Символы: {list(debugger.symbols_with_trades)[:10]}")
                
        stats_task = asyncio.create_task(print_stats())
        
        # Ждем бесконечно
        await asyncio.Future()
        
    except KeyboardInterrupt:
        print("\nОстановка отладки...")
    except Exception as e:
        logger.error(f"Ошибка в отладке: {e}")
    finally:
        if ws_client.is_connected:
            await ws_client.disconnect()
        await db_manager.disconnect()
        await message_broker.disconnect()
        print("Отладка завершена")

if __name__ == "__main__":
    asyncio.run(main())