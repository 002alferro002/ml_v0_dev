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
  private lastFetch: number = 0;
  private readonly CACHE_DURATION = 30000; // 30 секунд

  private constructor() {}

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
   * Получает WebSocket URL
   */
  public async getWebSocketUrl(endpoint: string = ''): Promise<string> {
    try {
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
    } catch (error) {
      console.error('Failed to get WebSocket URL, using default:', error);
      return `${CONFIG.WEBSOCKET_BASE_URL}${endpoint}`;
    }
  }

  /**
   * Получает API URL
   */
  public async getApiUrl(endpoint: string = ''): Promise<string> {
    try {
      const config = await this.getConfig();
      let url = config.api.url;
      
      if (endpoint) {
        if (!endpoint.startsWith('/')) {
          endpoint = '/' + endpoint;
        }
        url += endpoint;
      }
      
      return url;
    } catch (error) {
      console.error('Failed to get API URL, using default:', error);
      return `${CONFIG.API_BASE_URL}${endpoint}`;
    }
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
        reconnect_interval: 5,
        max_reconnect_attempts: 10
      },
      frontend: {
        host: defaultHost,
        port: CONFIG.FRONTEND_PORT,
        protocol: 'http',
        url: `http://localhost:${CONFIG.FRONTEND_PORT}`
      },
      environment: CONFIG.ENVIRONMENT,
      last_updated: new Date().toISOString()
    };
  }

}

export const configService = ConfigService.getInstance();
export default configService;