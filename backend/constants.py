# Константы для бэкенда ML Manipulation Bybit

# Проблемные символы, которые нужно исключить из WebSocket подписок
EXCLUDED_SYMBOLS = {
    "BTCUSD",  # Обратный фьючерс, не поддерживается в linear категории
    "ETHUSD",  # Обратный фьючерс, не поддерживается в linear категории
    "XRPUSD",  # Обратный фьючерс, не поддерживается в linear категории
    "EOSUSD",  # Обратный фьючерс, не поддерживается в linear категории
}

# Паттерны символов для исключения (регулярные выражения)
EXCLUDED_SYMBOL_PATTERNS = [
    r".*USD$",  # Все символы, заканчивающиеся на USD (обратные фьючерсы)
    r".*-\d{2}[A-Z]{3}\d{2}$",  # Символы с датой экспирации (например, BTCUSDT-08AUG25)
]

# Разрешенные типы контрактов
ALLOWED_CONTRACT_TYPES = {
    "LinearPerpetual",  # Бессрочные линейные контракты
    "LinearFutures",   # Срочные линейные контракты
}

# Разрешенные статусы символов
ALLOWED_SYMBOL_STATUSES = {
    "Trading",  # Только торгуемые символы
}

# Настройки WebSocket подписок
WEBSOCKET_CONFIG = {
    "MAX_SUBSCRIPTIONS": 200,  # Максимальное количество подписок
    "BATCH_SIZE": 50,  # Размер пакета для пакетных операций
    "RECONNECT_DELAY": 5,  # Задержка переподключения в секундах
    "HEARTBEAT_INTERVAL": 30,  # Интервал heartbeat в секундах
}

# Настройки базы данных
DATABASE_CONFIG = {
    "CONNECTION_POOL_SIZE": 20,
    "MAX_OVERFLOW": 30,
    "POOL_TIMEOUT": 30,
    "POOL_RECYCLE": 3600,
}

# Настройки API
API_CONFIG = {
    "REQUEST_TIMEOUT": 30,
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 1,
    "RATE_LIMIT_REQUESTS": 100,
    "RATE_LIMIT_WINDOW": 60,
}

# Настройки логирования
LOGGING_CONFIG = {
    "LEVEL": "INFO",
    "FORMAT": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "MAX_FILE_SIZE": 10 * 1024 * 1024,  # 10 MB
    "BACKUP_COUNT": 5,
}

# Настройки мониторинга
MONITORING_CONFIG = {
    "METRICS_INTERVAL": 60,  # Интервал сбора метрик в секундах
    "HEALTH_CHECK_INTERVAL": 30,  # Интервал проверки здоровья в секундах
    "ALERT_THRESHOLD": 0.8,  # Порог для алертов
}

# Настройки ML
ML_CONFIG = {
    "TRAINING_BUFFER_SIZE": 10000,
    "MIN_DATA_POINTS": 100,
    "TARGET_PREDICTION_WINDOW_SEC": 300,
    "DATA_COLLECTION_WINDOW_SEC": 60,
    "MODEL_RETRAIN_INTERVAL": 3600,  # 1 час
}

# Настройки детекции аномалий
ANOMALY_CONFIG = {
    "TRAINING_BUFFER_SIZE": 5000,
    "MIN_SAMPLES_FOR_TRAINING": 100,
    "CONTAMINATION": 0.01,
    "RETRAIN_INTERVAL_MINUTES": 60,
}

# Настройки торговли
TRADING_CONFIG = {
    "KLINE_INTERVAL_MINUTES": 1,
    "TRADE_HISTORY_MAXLEN": 10000,
    "WASH_TRADE_THRESHOLD_RATIO": 0.8,
    "PING_PONG_THRESHOLD_SECONDS": 0.1,
    "RAMPING_VOLUME_MULTIPLIER": 5,
    "RAMPING_PRICE_CHANGE_PERCENT": 0.5,
    "CONSECUTIVE_LONG_THRESHOLD": 5,
    "CONSECUTIVE_LONG_VOLUME_MULTIPLIER": 2,
}