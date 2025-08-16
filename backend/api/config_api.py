from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any, Optional
from pydantic import BaseModel
import logging
from datetime import datetime

from config_manager import config_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/config", tags=["configuration"])

class ConfigUpdateRequest(BaseModel):
    """Модель для запроса обновления конфигурации."""
    updates: Dict[str, Any]
    force_reload: Optional[bool] = False

class ConfigResponse(BaseModel):
    """Модель ответа с конфигурацией."""
    config: Dict[str, Any]
    last_updated: str
    environment: str
    status: str = "success"

@router.get("/frontend", response_model=ConfigResponse)
async def get_frontend_config():
    """
    Получает конфигурацию для фронтенда.
    
    Returns:
        Конфигурация, адаптированная для использования во фронтенде
    """
    try:
        frontend_config = config_manager.get_frontend_config()
        
        return ConfigResponse(
            config=frontend_config,
            last_updated=datetime.now().isoformat(),
            environment=config_manager.get('ENVIRONMENT', 'development')
        )
    except Exception as e:
        logger.error(f"Failed to get frontend config: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get configuration: {str(e)}")

@router.get("/urls")
async def get_service_urls():
    """
    Получает URL всех сервисов.
    
    Returns:
        Словарь с URL всех сервисов
    """
    try:
        urls = {
            'api': config_manager.get_url('api'),
            'websocket': config_manager.get_url('websocket'),
            'frontend': config_manager.get_url('frontend'),
            'database': config_manager.get_database_url(),
            'rabbitmq': config_manager.get_rabbitmq_url(),
            'redis': config_manager.get_redis_url(),
            'prometheus': config_manager.get_url('monitoring_prometheus'),
            'grafana': config_manager.get_url('monitoring_grafana')
        }
        
        return {
            'urls': urls,
            'last_updated': datetime.now().isoformat(),
            'environment': config_manager.get('ENVIRONMENT', 'development')
        }
    except Exception as e:
        logger.error(f"Failed to get service URLs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get URLs: {str(e)}")

@router.get("/reload")
async def reload_config():
    """
    Перезагружает конфигурацию из файла.
    
    Returns:
        Статус перезагрузки
    """
    try:
        config_manager.reload_config()
        
        return {
            'status': 'success',
            'message': 'Configuration reloaded successfully',
            'last_updated': datetime.now().isoformat(),
            'environment': config_manager.get('ENVIRONMENT', 'development')
        }
    except Exception as e:
        logger.error(f"Failed to reload config: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to reload configuration: {str(e)}")

@router.post("/update")
async def update_config(request: ConfigUpdateRequest):
    """
    Обновляет конфигурацию.
    
    Args:
        request: Запрос с обновлениями конфигурации
    
    Returns:
        Статус обновления
    """
    try:
        # Обновляем файл конфигурации
        success = config_manager.update_config_file(request.updates)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to update configuration file")
        
        # Принудительная перезагрузка если запрошена
        if request.force_reload:
            config_manager.reload_config()
        
        return {
            'status': 'success',
            'message': 'Configuration updated successfully',
            'last_updated': datetime.now().isoformat(),
            'environment': config_manager.get('ENVIRONMENT', 'development'),
            'updated_keys': list(request.updates.keys())
        }
    except Exception as e:
        logger.error(f"Failed to update config: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update configuration: {str(e)}")

@router.get("/health")
async def config_health():
    """
    Проверяет состояние системы конфигурации.
    
    Returns:
        Информация о состоянии конфигурации
    """
    try:
        # Проверяем доступность основных параметров
        health_info = {
            'status': 'healthy',
            'config_file_exists': config_manager._config_file_path.exists(),
            'config_loaded': bool(config_manager._config),
            'environment': config_manager.get('ENVIRONMENT', 'development'),
            'last_updated': datetime.now().isoformat(),
            'services': {
                'api': {
                    'host': config_manager.get('API_HOST', 'localhost'),
                    'port': config_manager.get('API_PORT', 5000),
                    'url': config_manager.get_url('api')
                },
                'websocket': {
                    'host': config_manager.get('WEBSOCKET_HOST', 'localhost'),
                    'port': config_manager.get('WEBSOCKET_PORT', 5000),
                    'url': config_manager.get_url('websocket')
                },
                'frontend': {
                    'host': config_manager.get('FRONTEND_HOST', 'localhost'),
                    'port': config_manager.get('FRONTEND_PORT', 3001),
                    'url': config_manager.get_url('frontend')
                }
            }
        }
        
        return health_info
    except Exception as e:
        logger.error(f"Config health check failed: {e}")
        return {
            'status': 'unhealthy',
            'error': str(e),
            'last_updated': datetime.now().isoformat()
        }

@router.get("/schema")
async def get_config_schema():
    """
    Возвращает схему конфигурации для валидации.
    
    Returns:
        JSON схема конфигурации
    """
    schema = {
        "type": "object",
        "properties": {
            "database": {
                "type": "object",
                "properties": {
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "name": {"type": "string"},
                    "user": {"type": "string"},
                    "password": {"type": "string"}
                }
            },
            "api": {
                "type": "object",
                "properties": {
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "protocol": {"type": "string", "enum": ["http", "https"]}
                }
            },
            "websocket": {
                "type": "object",
                "properties": {
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "protocol": {"type": "string", "enum": ["ws", "wss"]},
                    "reconnect_interval": {"type": "integer"},
                    "max_reconnect_attempts": {"type": "integer"}
                }
            },
            "frontend": {
                "type": "object",
                "properties": {
                    "host": {"type": "string"},
                    "port": {"type": "integer"},
                    "protocol": {"type": "string", "enum": ["http", "https"]}
                }
            }
        }
    }
    
    return {
        'schema': schema,
        'last_updated': datetime.now().isoformat()
    }