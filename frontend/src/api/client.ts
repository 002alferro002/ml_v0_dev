import { MLStatus, TrainingMetrics, Prediction, PredictionHistory, Alert, SystemStatus, StreamingData, DatabaseStats, MetricsData } from '@/types/ml';
import { configService } from '@/services/configService';
import { CONFIG } from '@/constants/config';

class ApiClient {
  private apiBaseUrl: string = `${CONFIG.API_BASE_URL}/api`; // Значение по умолчанию
  private configLoaded: boolean = false;

  constructor() {
    this.loadConfig();
  }

  private async loadConfig(): Promise<void> {
    try {
      console.log('Loading API configuration...');
      const apiUrl = await configService.getApiUrl('/api');
      console.log('API URL loaded:', apiUrl);
      this.apiBaseUrl = apiUrl;
      this.configLoaded = true;
    } catch (error) {
      console.error('Failed to load API configuration, using default:', error);
      console.log('Using default API URL:', this.apiBaseUrl);
      this.configLoaded = true; // Продолжаем работу с дефолтными значениями
    }
  }

  private async ensureConfigLoaded(): Promise<void> {
    if (!this.configLoaded) {
      await this.loadConfig();
    }
  }

  private async request<T>(endpoint: string, options?: RequestInit): Promise<T> {
    await this.ensureConfigLoaded();
    const url = `${this.apiBaseUrl}${endpoint}`;
    
    console.log(`Making API request to: ${url}`);
    
    try {
      const response = await fetch(url, {
        headers: {
          'Content-Type': 'application/json',
          ...options?.headers,
        },
        ...options,
      });

      console.log(`API response status: ${response.status}`);

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      console.log(`API response data:`, data);
      return data;
    } catch (error) {
      console.error(`API request failed for ${endpoint}:`, error);
      throw error;
    }
  }

  // ML Status
  async getMLStatus(): Promise<MLStatus> {
    return this.request<MLStatus>('/ml/status');
  }

  async startTraining(): Promise<{ message: string }> {
    return this.request<{ message: string }>('/ml/start_training', {
      method: 'POST',
    });
  }

  async stopTraining(): Promise<{ message: string }> {
    return this.request<{ message: string }>('/ml/stop_training', {
      method: 'POST',
    });
  }

  async startPrediction(): Promise<{ message: string }> {
    return this.request<{ message: string }>('/ml/start_prediction', {
      method: 'POST',
    });
  }

  async stopPrediction(): Promise<{ message: string }> {
    return this.request<{ message: string }>('/ml/stop_prediction', {
      method: 'POST',
    });
  }

  // Training Metrics
  async getTrainingMetrics(): Promise<TrainingMetrics> {
    return this.request<TrainingMetrics>('/ml/training_metrics');
  }

  // Predictions
  async getCurrentPredictions(): Promise<Prediction[]> {
    return this.request<Prediction[]>('/predictions/current');
  }

  async getPredictionHistory(limit?: number): Promise<PredictionHistory[]> {
    const params = limit ? `?limit=${limit}` : '';
    return this.request<PredictionHistory[]>(`/predictions/history${params}`);
  }

  // Alerts
  async getAlerts(limit?: number): Promise<Alert[]> {
    const params = limit ? `?limit=${limit}` : '';
    return this.request<Alert[]>(`/alerts${params}`);
  }

  async markAlertAsRead(alertId: number): Promise<{ message: string }> {
    return this.request<{ message: string }>(`/alerts/${alertId}/read`, {
      method: 'POST',
    });
  }

  // System Status
  async getSystemStatus(): Promise<SystemStatus> {
    return this.request<SystemStatus>('/status');
  }

  // Streaming Data
  async getStreamingData(): Promise<StreamingData[]> {
    return this.request<StreamingData[]>('/futures-symbols/market-data');
  }

  // Database
  async getDatabaseStats(): Promise<DatabaseStats> {
    return this.request<DatabaseStats>('/database/stats');
  }

  // Metrics
  async getMetrics(): Promise<MetricsData> {
    return this.request<MetricsData>('/metrics');
  }

  // WebSocket connection for real-time updates
  async createWebSocket(endpoint: string): Promise<WebSocket> {
    const wsUrl = await configService.getWebSocketUrl(endpoint);
    return new WebSocket(wsUrl);
  }

  // Методы для работы с конфигурацией
  async reloadConfig(): Promise<void> {
    this.configLoaded = false;
    await this.loadConfig();
  }

  getApiBaseUrl(): string {
    return this.apiBaseUrl;
  }
}

export const apiClient = new ApiClient();
export default apiClient;