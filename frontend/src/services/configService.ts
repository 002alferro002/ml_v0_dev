// Удаляем зависимость от apiService для избежания циклической зависимости
import { CONFIG } from '../constants/config';

export interface ServiceConfig {
  host: string;
  port: number;
  protocol: string;
  url: string;
}

export interface MonitoringConfig {
  prometheus: ServiceConfig;
  grafana: ServiceConfig;
}

export interface WebSocketConfig extends ServiceConfig {
  secure_protocol: string;
  reconnect_interval: number;
  max_reconnect_attempts: number;
}

export interface AppConfig {
  api: ServiceConfig;
  websocket: WebSocketConfig;
  frontend: ServiceConfig;
  monitoring: MonitoringConfig;
  environment: string;
  last_updated: string;
}

export interface ConfigResponse {
  config: AppConfig;
  last_updated: string;
  environment: string;
  status: string;
}

export interface ServiceUrls {
  api: string;
  websocket: string;
  frontend: string;
  database: string;
  rabbitmq: string;
  redis: string;
  prometheus: string;
  grafana: string;
}

export interface UrlsResponse {
  urls: ServiceUrls;
  last_updated: string;
  environment: string;
}

class ConfigService {
  private static instance: ConfigService;
  private config: AppConfig | null = null;
  private urls: ServiceUrls | null = null;
  private lastFetch: number = 0;
  private readonly CACHE_DURATION = 30000; // 30 секунд
  private configUpdateCallbacks: Array<(config: AppConfig) => void> = [];
  private urlUpdateCallbacks: Array<(urls: ServiceUrls) => void> = [];

  private constructor() {
    // apiService уже инициализирован как экземпляр
  }

  public static getInstance(): ConfigService {
    if (!ConfigService.instance) {
      ConfigService.instance = new ConfigService();
    }
    return ConfigService.instance;
  }

  /**
   * Получает конфигурацию для фронтенда
   */
  public async getConfig(forceRefresh: boolean = false): Promise<AppConfig> {
    const now = Date.now();
    
    if (!forceRefresh && this.config && (now - this.lastFetch) < this.CACHE_DURATION) {
      return this.config;
    }

    try {
      console.log('Loading configuration from API...');
      const response = await fetch(`${CONFIG.API_BASE_URL}/api/config/frontend`, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data: ConfigResponse = await response.json();
      this.config = data.config;
      this.lastFetch = now;
      
      // Уведомляем подписчиков об обновлении конфигурации
      this.notifyConfigUpdate(this.config);
      
      console.log('Configuration loaded successfully:', this.config);
      return this.config;
    } catch (error) {
      console.error('Failed to load configuration:', error);
      
      // Возвращаем конфигурацию по умолчанию если не удалось загрузить
      if (!this.config) {
        this.config = this.getDefaultConfig();
        console.log('Using default configuration:', this.config);
      }
      
      return this.config;
    }
  }

  /**
   * Получает URL всех сервисов
   */
  public async getServiceUrls(forceRefresh: boolean = false): Promise<ServiceUrls> {
    const now = Date.now();
    
    if (!forceRefresh && this.urls && (now - this.lastFetch) < this.CACHE_DURATION) {
      return this.urls;
    }

    try {
      console.log('Loading service URLs from API...');
      const response = await fetch(`${CONFIG.API_BASE_URL}/api/config/urls`, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data: UrlsResponse = await response.json();
      this.urls = data.urls;
      this.lastFetch = now;
      
      // Уведомляем подписчиков об обновлении URL
      this.notifyUrlUpdate(this.urls);
      
      console.log('Service URLs loaded successfully:', this.urls);
      return this.urls;
    } catch (error) {
      console.error('Failed to load service URLs:', error);
      
      // Возвращаем URL по умолчанию если не удалось загрузить
      if (!this.urls) {
        this.urls = this.getDefaultUrls();
        console.log('Using default service URLs:', this.urls);
      }
      
      return this.urls;
    }
  }

  /**
   * Получает URL для конкретного сервиса
   */
  public async getServiceUrl(service: keyof ServiceUrls): Promise<string> {
    const urls = await this.getServiceUrls();
    return urls[service];
  }

  /**
   * Получает WebSocket URL
   */
  public async getWebSocketUrl(endpoint: string = ''): Promise<string> {
    const config = await this.getConfig();
    let url = config.websocket.url;
    
    // Заменяем http на ws, https на wss
    if (url.startsWith('http://')) {
      url = url.replace('http://', 'ws://');
    } else if (url.startsWith('https://')) {
      url = url.replace('https://', 'wss://');
    }
    
    if (endpoint) {
      if (!endpoint.startsWith('/')) {
        endpoint = '/' + endpoint;
      }
      url += endpoint;
    }
    
    return url;
  }

  /**
   * Получает API URL
   */
  public async getApiUrl(endpoint: string = ''): Promise<string> {
    const config = await this.getConfig();
    let url = config.api.url;
    
    if (endpoint) {
      if (!endpoint.startsWith('/')) {
        endpoint = '/' + endpoint;
      }
      url += endpoint;
    }
    
    return url;
  }

  /**
   * Перезагружает конфигурацию с сервера
   */
  public async reloadConfig(): Promise<void> {
    try {
      const response = await fetch('/api/config/reload');
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      // Принудительно обновляем локальную конфигурацию
      await this.getConfig(true);
      await this.getServiceUrls(true);
    } catch (error) {
      console.error('Failed to reload configuration:', error);
      throw error;
    }
  }

  /**
   * Обновляет конфигурацию на сервере
   */
  public async updateConfig(updates: Record<string, any>): Promise<void> {
    try {
      const response = await fetch('/api/config/update', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          updates,
          force_reload: true
        })
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      // Обновляем локальную конфигурацию
      await this.getConfig(true);
      await this.getServiceUrls(true);
    } catch (error) {
      console.error('Failed to update configuration:', error);
      throw error;
    }
  }

  /**
   * Проверяет состояние системы конфигурации
   */
  public async checkHealth(): Promise<any> {
    try {
      const response = await fetch('/api/config/health');
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      return await response.json();
    } catch (error) {
      console.error('Configuration health check failed:', error);
      throw error;
    }
  }

  /**
   * Подписывается на обновления конфигурации
   */
  public onConfigUpdate(callback: (config: AppConfig) => void): () => void {
    this.configUpdateCallbacks.push(callback);
    
    // Возвращаем функцию для отписки
    return () => {
      const index = this.configUpdateCallbacks.indexOf(callback);
      if (index > -1) {
        this.configUpdateCallbacks.splice(index, 1);
      }
    };
  }

  /**
   * Подписывается на обновления URL
   */
  public onUrlUpdate(callback: (urls: ServiceUrls) => void): () => void {
    this.urlUpdateCallbacks.push(callback);
    
    // Возвращаем функцию для отписки
    return () => {
      const index = this.urlUpdateCallbacks.indexOf(callback);
      if (index > -1) {
        this.urlUpdateCallbacks.splice(index, 1);
      }
    };
  }

  /**
   * Уведомляет подписчиков об обновлении конфигурации
   */
  private notifyConfigUpdate(config: AppConfig): void {
    this.configUpdateCallbacks.forEach(callback => {
      try {
        callback(config);
      } catch (error) {
        console.error('Error in config update callback:', error);
      }
    });
  }

  /**
   * Уведомляет подписчиков об обновлении URL
   */
  private notifyUrlUpdate(urls: ServiceUrls): void {
    this.urlUpdateCallbacks.forEach(callback => {
      try {
        callback(urls);
      } catch (error) {
        console.error('Error in URL update callback:', error);
      }
    });
  }

  /**
   * Возвращает конфигурацию по умолчанию
   */
  private getDefaultConfig(): AppConfig {
    const defaultHost = 'localhost';
    
    return {
      api: {
        host: defaultHost,
        port: 5000,
        protocol: 'http',
        url: CONFIG.API_BASE_URL
      },
      websocket: {
        host: defaultHost,
        port: 5000,
        protocol: 'ws',
        secure_protocol: 'wss',
        url: CONFIG.WEBSOCKET_BASE_URL,
        reconnect_interval: CONFIG.WEBSOCKET.RECONNECT_INTERVAL / 1000,
        max_reconnect_attempts: CONFIG.WEBSOCKET.MAX_RECONNECT_ATTEMPTS
      },
      frontend: {
        host: defaultHost,
        port: CONFIG.FRONTEND_PORT,
        protocol: 'http',
        url: `http://localhost:${CONFIG.FRONTEND_PORT}`
      },
      monitoring: {
        prometheus: {
          host: defaultHost,
          port: 9090,
          protocol: 'http',
          url: CONFIG.MONITORING.PROMETHEUS_URL
        },
        grafana: {
          host: defaultHost,
          port: 3000,
          protocol: 'http',
          url: CONFIG.MONITORING.GRAFANA_URL
        }
      },
      environment: CONFIG.ENVIRONMENT,
      last_updated: new Date().toISOString()
    };
  }

  /**
   * Возвращает URL по умолчанию
   */
  private getDefaultUrls(): ServiceUrls {
    return {
      api: CONFIG.API_BASE_URL,
      websocket: CONFIG.WEBSOCKET_BASE_URL,
      frontend: `http://localhost:${CONFIG.FRONTEND_PORT}`,
      database: CONFIG.DATABASE.CONNECTION_STRING,
      rabbitmq: CONFIG.QUEUES.RABBITMQ_URL,
      redis: CONFIG.QUEUES.REDIS_URL,
      prometheus: CONFIG.MONITORING.PROMETHEUS_URL,
      grafana: CONFIG.MONITORING.GRAFANA_URL
    };
  }

  /**
   * Очищает кэш
   */
  public clearCache(): void {
    this.config = null;
    this.urls = null;
    this.lastFetch = 0;
  }

  /**
   * Получает текущую конфигурацию из кэша (без запроса к серверу)
   */
  public getCachedConfig(): AppConfig | null {
    return this.config;
  }

  /**
   * Получает текущие URL из кэша (без запроса к серверу)
   */
  public getCachedUrls(): ServiceUrls | null {
    return this.urls;
  }
}

export const configService = ConfigService.getInstance();
export default configService;