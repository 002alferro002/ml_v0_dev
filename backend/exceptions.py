import logging

logger = logging.getLogger(__name__)

class AppError(Exception):
    """Базовый класс для всех кастомных ошибок приложения."""
    def __init__(self, message: str, status_code: int = 500, details: dict = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.details = details if details is not None else {}
        logger.error(f"AppError: {message} (Status: {status_code}, Details: {details})")

class DatabaseError(AppError):
    """Ошибка, связанная с операциями базы данных."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, status_code=500, details=details)

class WebSocketError(AppError):
    """Ошибка, связанная с WebSocket соединением."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, status_code=500, details=details)

class MLPredictionError(AppError):
    """Ошибка, связанная с прогнозированием ML модели."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, status_code=500, details=details)

class ConfigurationError(AppError):
    """Ошибка конфигурации приложения."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, status_code=500, details=details)

class DataProcessingError(AppError):
    """Ошибка при обработке рыночных данных."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, status_code=500, details=details)

class AnomalyDetectionError(AppError):
    """Ошибка при обнаружении аномалий."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, status_code=500, details=details)

class MessageBrokerError(AppError):
    """Ошибка, связанная с брокером сообщений."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, status_code=500, details=details)


