import os
import json
import logging
from dotenv import load_dotenv, find_dotenv
from typing import Dict, Any, Optional
from pathlib import Path
import hashlib
from datetime import datetime

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Управляет централизованной конфигурацией приложения.
    Поддерживает динамическую перезагрузку настроек из JSON файла и переменных окружения.
    Обеспечивает синхронизацию между фронтендом и бэкендом.
    """
    _instance = None
    _config: Dict[str, Any] = {}
    _last_loaded_env_hash: Optional[str] = None
    _last_loaded_config_hash: Optional[str] = None
    _config_file_path: str = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Инициализирует конфигурацию при первом создании экземпляра."""
        # Определяем путь к файлу конфигурации
        self._config_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'app_config.json')
        self.reload_config()
        logger.info("ConfigManager initialized and configuration loaded.")

    def _calculate_env_hash(self) -> Optional[str]:
        """Вычисляет хэш содержимого .env файла для обнаружения изменений."""
        dotenv_path = find_dotenv()
        if dotenv_path and os.path.exists(dotenv_path):
            try:
                with open(dotenv_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                return hashlib.md5(content.encode('utf-8')).hexdigest()
            except Exception as e:
                logger.warning(f"Could not read .env file for hashing: {e}")
                return None
        return None
    
    def _calculate_config_hash(self) -> Optional[str]:
        """Вычисляет хэш содержимого JSON файла конфигурации для обнаружения изменений."""
        if os.path.exists(self._config_file_path):
            try:
                with open(self._config_file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                return hashlib.md5(content.encode('utf-8')).hexdigest()
            except Exception as e:
                logger.warning(f"Could not read config file for hashing: {e}")
                return None
        return None

    def reload_config(self):
        """
        Перезагружает конфигурацию из JSON файла и переменных окружения.
        Если файлы изменились, они будут перезагружены.
        """
        # Проверяем изменения в .env файле
        current_env_hash = self._calculate_env_hash()
        if current_env_hash and current_env_hash != self._last_loaded_env_hash:
            logger.info("Detected changes in .env file. Reloading environment variables.")
            load_dotenv(override=True)
            self._last_loaded_env_hash = current_env_hash
        else:
            load_dotenv(override=False)
        
        # Проверяем изменения в JSON файле конфигурации
        current_config_hash = self._calculate_config_hash()
        if current_config_hash != self._last_loaded_config_hash:
            logger.info("Detected changes in config file. Reloading configuration.")
            self._load_json_config()
            self._last_loaded_config_hash = current_config_hash
        
        logger.info("Configuration reloaded.")
    
    def _load_json_config(self):
        """Загружает конфигурацию из JSON файла."""
        try:
            with open(self._config_file_path, 'r', encoding='utf-8') as f:
                json_config = json.load(f)
            
            # Определяем текущую среду
            environment = os.getenv('ENVIRONMENT', 'development')
            
            # Загружаем базовую конфигурацию
            self._config = self._flatten_config(json_config)
            
            # Применяем переопределения для текущей среды
            if 'environment_overrides' in json_config and environment in json_config['environment_overrides']:
                overrides = self._flatten_config(json_config['environment_overrides'][environment])
                self._config.update(overrides)
            
            # Переопределяем значениями из переменных окружения
            self._apply_env_overrides()
            
            logger.info(f"Configuration loaded for environment: {environment}")
            
        except Exception as e:
            logger.error(f"Failed to load JSON config: {e}")
            # Fallback к старой конфигурации
            self._load_fallback_config()
    
    def _flatten_config(self, config: Dict[str, Any], prefix: str = '') -> Dict[str, Any]:
        """Преобразует вложенную конфигурацию в плоскую структуру."""
        flattened = {}
        for key, value in config.items():
            if key.startswith('_'):  # Пропускаем метаданные
                continue
            new_key = f"{prefix}_{key}" if prefix else key
            if isinstance(value, dict) and not any(k in value for k in ['url_template', 'host', 'port']):
                flattened.update(self._flatten_config(value, new_key))
            else:
                flattened[new_key.upper()] = value
        return flattened
    
    def _apply_env_overrides(self):
        """Применяет переопределения из переменных окружения."""
        env_mappings = {
            'DATABASE_URL': 'DATABASE_URL',
            'RABBITMQ_URL': 'RABBITMQ_URL', 
            'REDIS_URL': 'REDIS_URL',
            'BYBIT_API_KEY': 'BYBIT_API_KEY',
            'BYBIT_API_SECRET': 'BYBIT_API_SECRET',
            'BYBIT_TESTNET': 'BYBIT_API_TESTNET',
            'BYBIT_WS_URI': 'BYBIT_API_WS_URI',
            'LOG_LEVEL': 'LOGGING_LEVEL',
            'ENVIRONMENT': 'ENVIRONMENT'
        }
        
        for env_key, config_key in env_mappings.items():
            env_value = os.getenv(env_key)
            if env_value is not None:
                # Преобразуем строковые значения в соответствующие типы
                if env_value.lower() in ('true', 'false'):
                    self._config[config_key] = env_value.lower() == 'true'
                elif env_value.isdigit():
                    self._config[config_key] = int(env_value)
                else:
                    try:
                        self._config[config_key] = float(env_value)
                    except ValueError:
                        self._config[config_key] = env_value
    
    def _load_fallback_config(self):
        """Загружает резервную конфигурацию в случае ошибки."""
        logger.warning("Loading fallback configuration")
        self._config = {
            "DATABASE_URL": os.getenv("DATABASE_URL", "postgresql://bybit:bybit@localhost:5432/bybit_manipulation"),
            "RABBITMQ_URL": os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
            "BYBIT_WS_URI": os.getenv("BYBIT_WS_URI", "wss://stream.bybit.com/v5/public/linear"),
            "ML_MODEL_PATH": os.getenv("ML_MODEL_PATH", "ml_model.joblib"),
            "ANOMALY_MODEL_PATH": os.getenv("ANOMALY_MODEL_PATH", "anomaly_model.joblib"),
            "ALERT_GROUPING_SECONDS": int(os.getenv("ALERT_GROUPING_SECONDS", "60")),
            "ALERT_COOLDOWN_SECONDS": int(os.getenv("ALERT_COOLDOWN_SECONDS", "30")),
            "KLINE_INTERVAL_MINUTES": int(os.getenv("KLINE_INTERVAL_MINUTES", "1")),
            "TRADE_HISTORY_MAXLEN": int(os.getenv("TRADE_HISTORY_MAXLEN", "10000")),
            "WASH_TRADE_THRESHOLD_RATIO": float(os.getenv("WASH_TRADE_THRESHOLD_RATIO", "0.8")),
            "PING_PONG_THRESHOLD_SECONDS": float(os.getenv("PING_PONG_THRESHOLD_SECONDS", "0.1")),
            "RAMPING_VOLUME_MULTIPLIER": float(os.getenv("RAMPING_VOLUME_MULTIPLIER", "5")),
            "RAMPING_PRICE_CHANGE_PERCENT": float(os.getenv("RAMPING_PRICE_CHANGE_PERCENT", "0.5")),
            "CONSECUTIVE_LONG_THRESHOLD": int(os.getenv("CONSECUTIVE_LONG_THRESHOLD", "5")),
            "CONSECUTIVE_LONG_VOLUME_MULTIPLIER": float(os.getenv("CONSECUTIVE_LONG_VOLUME_MULTIPLIER", "2")),
            "ML_TRAINING_BUFFER_SIZE": int(os.getenv("ML_TRAINING_BUFFER_SIZE", "10000")),
            "ML_MIN_DATA_POINTS": int(os.getenv("ML_MIN_DATA_POINTS", "100")),
            "ML_TARGET_PREDICTION_WINDOW_SEC": int(os.getenv("ML_TARGET_PREDICTION_WINDOW_SEC", "300")),
            "ML_DATA_COLLECTION_WINDOW_SEC": int(os.getenv("ML_DATA_COLLECTION_WINDOW_SEC", "60")),
            "ANOMALY_TRAINING_BUFFER_SIZE": int(os.getenv("ANOMALY_TRAINING_BUFFER_SIZE", "5000")),
            "ANOMALY_MIN_SAMPLES_FOR_TRAINING": int(os.getenv("ANOMALY_MIN_SAMPLES_FOR_TRAINING", "100")),
            "ANOMALY_CONTAMINATION": float(os.getenv("ANOMALY_CONTAMINATION", "0.01")),
            "ANOMALY_RETRAIN_INTERVAL_MINUTES": int(os.getenv("ANOMALY_RETRAIN_INTERVAL_MINUTES", "60")),
        }

    def get(self, key: str, default: Any = None) -> Any:
        """Возвращает значение конфигурации по ключу."""
        return self._config.get(key, default)

    def get_all(self) -> Dict[str, Any]:
        """Возвращает всю текущую конфигурацию."""
        return self._config.copy()

    def set(self, key: str, value: Any):
        """
        Устанавливает значение конфигурации.
        Это не сохраняет значение в .env файл, а только обновляет текущую конфигурацию в памяти.
        Для постоянного изменения нужно обновить .env файл и вызвать reload_config().
        """
        self._config[key] = value
        logger.info(f"Configuration key '{key}' updated in memory to '{value}'.")
    
    def get_url(self, service: str, endpoint: str = '') -> str:
        """
        Формирует URL для указанного сервиса.
        
        Args:
            service: Название сервиса (api, websocket, database, rabbitmq, redis, etc.)
            endpoint: Дополнительный путь к endpoint
        
        Returns:
            Полный URL для сервиса
        """
        service_key = service.upper()
        
        # Получаем параметры сервиса
        host = self.get(f'{service_key}_HOST', 'localhost')
        port = self.get(f'{service_key}_PORT', 5000)
        protocol = self.get(f'{service_key}_PROTOCOL', 'http')
        
        # Формируем базовый URL
        base_url = f"{protocol}://{host}:{port}"
        
        # Добавляем endpoint если указан
        if endpoint:
            if not endpoint.startswith('/'):
                endpoint = '/' + endpoint
            base_url += endpoint
        
        return base_url
    
    def get_database_url(self) -> str:
        """Возвращает URL для подключения к базе данных."""
        if 'DATABASE_URL' in self._config and self._config['DATABASE_URL']:
            return self._config['DATABASE_URL']
        
        # Формируем URL из компонентов
        host = self.get('DATABASE_HOST', 'localhost')
        port = self.get('DATABASE_PORT', 5432)
        name = self.get('DATABASE_NAME', 'bybit_manipulation')
        user = self.get('DATABASE_USER', 'bybit')
        password = self.get('DATABASE_PASSWORD', 'bybit')
        
        return f"postgresql://{user}:{password}@{host}:{port}/{name}"
    
    def get_rabbitmq_url(self) -> str:
        """Возвращает URL для подключения к RabbitMQ."""
        if 'RABBITMQ_URL' in self._config and self._config['RABBITMQ_URL']:
            return self._config['RABBITMQ_URL']
        
        # Формируем URL из компонентов
        host = self.get('RABBITMQ_HOST', 'localhost')
        port = self.get('RABBITMQ_PORT', 5672)
        user = self.get('RABBITMQ_USER', 'guest')
        password = self.get('RABBITMQ_PASSWORD', 'guest')
        
        return f"amqp://{user}:{password}@{host}:{port}/"
    
    def get_redis_url(self) -> str:
        """Возвращает URL для подключения к Redis."""
        if 'REDIS_URL' in self._config and self._config['REDIS_URL']:
            return self._config['REDIS_URL']
        
        # Формируем URL из компонентов
        host = self.get('REDIS_HOST', 'localhost')
        port = self.get('REDIS_PORT', 6379)
        
        return f"redis://{host}:{port}"
    
    def get_frontend_config(self) -> Dict[str, Any]:
        """Возвращает конфигурацию для фронтенда."""
        return {
            'api': {
                'host': self.get('API_HOST', 'localhost'),
                'port': self.get('API_PORT', 5000),
                'protocol': self.get('API_PROTOCOL', 'http'),
                'url': self.get_url('api')
            },
            'websocket': {
                'host': self.get('WEBSOCKET_HOST', 'localhost'),
                'port': self.get('WEBSOCKET_PORT', 5000),
                'protocol': self.get('WEBSOCKET_PROTOCOL', 'ws'),
                'secure_protocol': self.get('WEBSOCKET_SECURE_PROTOCOL', 'wss'),
                'url': self.get_url('websocket'),
                'reconnect_interval': self.get('WEBSOCKET_RECONNECT_INTERVAL', 5),
                'max_reconnect_attempts': self.get('WEBSOCKET_MAX_RECONNECT_ATTEMPTS', 10)
            },
            'frontend': {
                'host': self.get('FRONTEND_HOST', 'localhost'),
                'port': self.get('FRONTEND_PORT', 3001),
                'protocol': self.get('FRONTEND_PROTOCOL', 'http'),
                'url': self.get_url('frontend')
            },
            'monitoring': {
                'prometheus': {
                    'host': self.get('MONITORING_PROMETHEUS_HOST', 'localhost'),
                    'port': self.get('MONITORING_PROMETHEUS_PORT', 9090),
                    'protocol': self.get('MONITORING_PROMETHEUS_PROTOCOL', 'http'),
                    'url': self.get_url('monitoring_prometheus')
                },
                'grafana': {
                    'host': self.get('MONITORING_GRAFANA_HOST', 'localhost'),
                    'port': self.get('MONITORING_GRAFANA_PORT', 3000),
                    'protocol': self.get('MONITORING_GRAFANA_PROTOCOL', 'http'),
                    'url': self.get_url('monitoring_grafana')
                }
            },
            'environment': os.getenv('ENVIRONMENT', 'development'),
            'last_updated': datetime.now().isoformat()
        }
    
    def update_config_file(self, updates: Dict[str, Any]) -> bool:
        """
        Обновляет JSON файл конфигурации.
        
        Args:
            updates: Словарь с обновлениями конфигурации
        
        Returns:
            True если обновление прошло успешно, False в противном случае
        """
        try:
            # Читаем текущую конфигурацию
            with open(self._config_file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Применяем обновления
            self._deep_update(config, updates)
            
            # Обновляем метаданные
            if '_metadata' not in config:
                config['_metadata'] = {}
            config['_metadata']['last_updated'] = datetime.now().isoformat()
            
            # Сохраняем обновленную конфигурацию
            with open(self._config_file_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            # Перезагружаем конфигурацию
            self.reload_config()
            
            logger.info("Configuration file updated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update config file: {e}")
            return False
    
    def _deep_update(self, base_dict: Dict[str, Any], update_dict: Dict[str, Any]):
        """Рекурсивно обновляет словарь."""
        for key, value in update_dict.items():
            if key in base_dict and isinstance(base_dict[key], dict) and isinstance(value, dict):
                self._deep_update(base_dict[key], value)
            else:
                base_dict[key] = value

config_manager = ConfigManager()


