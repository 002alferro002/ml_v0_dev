export interface PredictionRequest {
  symbol: string;
  timeframe: string;
  features: string[];
  model_type: string;
}

export interface PredictionResponse {
  prediction: number;
  confidence: number;
  timestamp: string;
  features_used: string[];
  model_info: {
    type: string;
    accuracy: number;
    last_trained: string;
  };
}

export interface SystemMetrics {
  cpu_usage: number;
  memory_usage: number;
  disk_usage: number;
  network_io: {
    bytes_sent: number;
    bytes_recv: number;
  };
  active_connections: number;
  uptime: number;
}

export interface ModelMetrics {
  accuracy: number;
  precision: number;
  recall: number;
  f1_score: number;
  last_updated: string;
  training_samples: number;
}

export interface TradeSignal {
  id: string;
  symbol: string;
  signal_type: 'BUY' | 'SELL' | 'HOLD';
  confidence: number;
  price: number;
  timestamp: string;
  reasoning: string;
}

export interface MarketData {
  symbol: string;
  price: number;
  volume: number;
  change_24h: number | undefined;
  timestamp: string;
}

export interface ApiError {
  message: string;
  code: number;
  details?: any;
}

export interface WebSocketMessage {
  type: 'prediction' | 'metrics' | 'signal' | 'market_data' | 'error';
  data: any;
  timestamp: string;
}