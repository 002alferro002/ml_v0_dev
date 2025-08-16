// Константы конфигурации для фронтенда

// Значения по умолчанию для разработки
export const DEFAULT_CONFIG = {
  API_BASE_URL: 'http://localhost:5000',
  WEBSOCKET_BASE_URL: 'ws://localhost:5000',
  FRONTEND_PORT: 3001,
  
  // Настройки WebSocket
  WEBSOCKET: {
    RECONNECT_INTERVAL: 5000, // 5 секунд
    MAX_RECONNECT_ATTEMPTS: 10,
    HEARTBEAT_INTERVAL: 30000, // 30 секунд
  },
  
  // Настройки кэширования
  CACHE: {
    CONFIG_DURATION: 30000, // 30 секунд
    DATA_DURATION: 60000, // 1 минута
  },
  
  // Настройки API
  API: {
    TIMEOUT: 10000, // 10 секунд
    RETRY_ATTEMPTS: 3,
    RETRY_DELAY: 1000, // 1 секунда
  },
  
  // Настройки мониторинга
  MONITORING: {
    PROMETHEUS_URL: 'http://localhost:9090',
    GRAFANA_URL: 'http://localhost:3000',
  },
  
  // Настройки базы данных
  DATABASE: {
    CONNECTION_STRING: 'postgresql://bybit:bybit@localhost:5432/bybit_manipulation',
  },
  
  // Настройки очередей
  QUEUES: {
    RABBITMQ_URL: 'amqp://guest:guest@localhost:5672/',
    REDIS_URL: 'redis://localhost:6379',
  },
} as const;

// Типы для конфигурации
export type ConfigType = typeof DEFAULT_CONFIG;

// Переменные окружения (если доступны)
export const ENV_CONFIG = {
  API_BASE_URL: import.meta.env.VITE_API_BASE_URL || DEFAULT_CONFIG.API_BASE_URL,
  WEBSOCKET_BASE_URL: import.meta.env.VITE_WEBSOCKET_BASE_URL || DEFAULT_CONFIG.WEBSOCKET_BASE_URL,
  ENVIRONMENT: import.meta.env.MODE || 'development',
} as const;

// Экспорт основной конфигурации
export const CONFIG = {
  ...DEFAULT_CONFIG,
  ...ENV_CONFIG,
} as const;

// Утилиты для работы с URL
export const getApiUrl = (endpoint: string = ''): string => {
  const baseUrl = CONFIG.API_BASE_URL;
  if (!endpoint) return baseUrl;
  
  const cleanEndpoint = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
  return `${baseUrl}${cleanEndpoint}`;
};

export const getWebSocketUrl = (endpoint: string = ''): string => {
  const baseUrl = CONFIG.WEBSOCKET_BASE_URL;
  if (!endpoint) return baseUrl;
  
  const cleanEndpoint = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
  return `${baseUrl}${cleanEndpoint}`;
};

// Проверка доступности сервиса
export const checkServiceHealth = async (url: string): Promise<boolean> => {
  try {
    const response = await fetch(`${url}/health`, {
      method: 'GET',
      timeout: CONFIG.API.TIMEOUT,
    });
    return response.ok;
  } catch {
    return false;
  }
};

export default CONFIG;