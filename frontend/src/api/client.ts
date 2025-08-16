import { MLStatus, TrainingMetrics, Prediction, PredictionHistory, Alert, SystemStatus, StreamingData, DatabaseStats, MetricsData } from '@/types/ml';
import { CONFIG } from '@/constants/config';

class ApiClient {
  private apiBaseUrl: string = CONFIG.API_BASE_URL;

  constructor() {}

  private async request<T>(endpoint: string, options?: RequestInit): Promise<T> {
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

  async startTraining(symbol?: string, timeframe?: string): Promise<{ message: string }> {
    return this.request<{ message: string }>('/ml/training/force', {
      method: 'POST',
      body: JSON.stringify({ symbol, timeframe }),
    });
  }

  async stopTraining(): Promise<{ message: string }> {
    return this.request<{ message: string }>('/ml/mode/switch', {
      method: 'POST',
      body: JSON.stringify({ mode: 'prediction' }),
    });
  }

  async startPrediction(symbol?: string): Promise<{ message: string }> {
    return this.request<{ message: string }>('/ml/mode/switch', {
      method: 'POST',
      body: JSON.stringify({ mode: 'prediction' }),
    });
  }

  async stopPrediction(): Promise<{ message: string }> {
    return this.request<{ message: string }>('/ml/mode/switch', {
      method: 'POST',
      body: JSON.stringify({ mode: 'training' }),
    });
  }

  // Training Metrics
  async getTrainingMetrics(): Promise<TrainingMetrics> {
    return this.request<TrainingMetrics>('/ml/training/metrics');
  }

  // Predictions
  async getCurrentPredictions(): Promise<Prediction[]> {
    return this.request<Prediction[]>('/ml/predictions/active');
  }

  async getPredictionHistory(limit?: number): Promise<PredictionHistory[]> {
    const params = limit ? `?limit=${limit}` : '';
    return this.request<PredictionHistory[]>(`/ml/predictions/history${params}`);
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

}

export const apiClient = new ApiClient();
export default apiClient;