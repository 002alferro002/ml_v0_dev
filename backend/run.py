#!/usr/bin/env python3
"""
Скрипт для запуска ML Manipulation Bybit системы.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Добавляем текущую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('app.log')
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Главная функция запуска."""
    try:
        logger.info("Запуск ML Manipulation Bybit системы...")
        
        # Проверяем переменные окружения
        required_env_vars = [
            'DATABASE_URL',
            'RABBITMQ_URL'
        ]
        
        missing_vars = []
        for var in required_env_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"Отсутствуют обязательные переменные окружения: {missing_vars}")
            logger.info("Создайте файл .env с необходимыми переменными:")
            logger.info("DATABASE_URL=postgresql://bybit:bybit@localhost:5432/bybit_manipulation")
            logger.info("RABBITMQ_URL=amqp://guest:guest@localhost:5672/")
            sys.exit(1)
        
        # Импортируем и запускаем приложение
        import uvicorn
        from app import app
        
        # Получаем настройки из конфигурации
        host = os.getenv('API_HOST', '0.0.0.0')
        port = int(os.getenv('API_PORT', '5000'))
        log_level = os.getenv('LOG_LEVEL', 'info').lower()
        
        logger.info(f"Запуск сервера на {host}:{port}")
        
        uvicorn.run(
            app,
            host=host,
            port=port,
            log_level=log_level,
            reload=False,  # Отключаем reload в продакшене
            access_log=True
        )
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Критическая ошибка запуска: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()