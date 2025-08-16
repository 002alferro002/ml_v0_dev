import { PredictionRequest, PredictionResponse, SystemMetrics, ModelMetrics, TradeSignal, MarketData, ApiError } from '../types/api';
import { configService } from './configService';
import { CONFIG } from '../constants/config';

class ApiService {
  private apiBaseUrl: string = CONFIG.API_BASE_URL;
  private configLoaded: boolean = false;

  constructor() {
    this.loadConfig();
  }

  private async loadConfig(): Promise<void> {
    try {
      console.log('Loading API configuration...');
      const apiUrl = await configService.getApiUrl();
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

  private async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    await this.ensureConfigLoaded();
    const url = `${this.apiBaseUrl}${endpoint}`;
    
    console.log(`Making API request to: ${url}`);
    
    try {
      const response = await fetch(url, {
        headers: {
          'Content-Type': 'application/json',
          ...options.headers,
        },
        ...options,
      });

      if (!response.ok) {
        const error: ApiError = await response.json();
        throw new Error(error.message || `HTTP error! status: ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      console.error('API request failed:', error);
      throw error;
    }
  }

  // Generic HTTP methods
  async get<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint, { method: 'GET' });
  }

  async post<T>(endpoint: string, data?: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: 'POST',
      body: data ? JSON.stringify(data) : undefined,
    });
  }

  // Prediction endpoints
  async makePrediction(request: PredictionRequest): Promise<PredictionResponse> {
    return this.request<PredictionResponse>('/api/predict', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async getPredictionHistory(symbol?: string, limit?: number): Promise<PredictionResponse[]> {
    const params = new URLSearchParams();
    if (symbol) params.append('symbol', symbol);
    if (limit) params.append('limit', limit.toString());
    
    return this.request<PredictionResponse[]>(`/api/predictions?${params}`);
  }

  // Metrics endpoints
  async getSystemMetrics(): Promise<SystemMetrics> {
    return this.request<SystemMetrics>('/api/metrics/system');
  }

  async getModelMetrics(): Promise<ModelMetrics> {
    return this.request<ModelMetrics>('/api/metrics/model');
  }

  // Trading signals
  async getTradeSignals(symbol?: string): Promise<TradeSignal[]> {
    const params = symbol ? `?symbol=${symbol}` : '';
    return this.request<TradeSignal[]>(`/api/signals${params}`);
  }

  // Market data
  async getMarketData(symbol: string): Promise<MarketData> {
    return this.request<MarketData>(`/api/market/${symbol}`);
  }

  async getAvailableSymbols(): Promise<string[]> {
    return this.request<string[]>('/api/symbols');
  }

  // Model management
  async trainModel(symbol: string, parameters?: any): Promise<{ status: string; message: string }> {
    return this.request('/api/model/train', {
      method: 'POST',
      body: JSON.stringify({ symbol, parameters }),
    });
  }

  async getModelStatus(): Promise<{ status: string; last_trained: string; accuracy: number }> {
    return this.request('/api/model/status');
  }
}

export { ApiService };
export const apiService = new ApiService();
export default apiService;