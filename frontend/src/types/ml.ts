export interface MLStatus {
  mode: 'training' | 'prediction' | 'hybrid';
  is_running: boolean;
  training_active: boolean;
  prediction_active: boolean;
  model_status: {
    price_model_trained: boolean;
    direction_model_trained: boolean;
    online_models_trained: boolean;
    last_training: string;
    samples_processed: number;
  };
  training_stats: {
    samples_processed: number;
    last_training_time: string;
    model_version: number;
    accuracy_history: number[];
    error_history: number[];
  };
  collection_stats: {
    total_samples: number;
    successful_predictions: number;
    failed_predictions: number;
    last_collection_time: string;
    symbols_tracked: string[];
    feature_importance: string;
    buffer_size: number;
    is_running: boolean;
  };
  active_predictions_count: number;
  prediction_stats: any;
  total_prediction_history: number;
  model_info: {
    model_type: string;
    training_stats: string;
    feature_names: string[];
    buffer_size: number;
    model_path: string;
    models_trained: string;
  };
}

export interface TrainingMetrics {
  model_info: {
    model_type: string;
    training_stats: string;
    feature_names: string[];
    buffer_size: number;
    model_path: string;
    models_trained: string;
  };
  accuracy_history: number[];
  error_history: number[];
  overfitting_score: number;
  is_overfitting: boolean;
  training_recommendations: string[];
  training_duration?: number;
  accuracy?: number;
  precision?: number;
  recall?: number;
  f1_score?: number;
  loss?: number;
  val_loss?: number;
  epochs?: number;
  learning_rate?: number;
  batch_size?: number;
  samples_count?: number;
  validation_accuracy?: number;
  training_time?: string;
  model_size?: number;
  feature_count?: number;
  cross_validation_score?: number;
  confusion_matrix?: number[][];
  roc_auc?: number;
  mae?: number;
  mse?: number;
  rmse?: number;
  r2_score?: number;
}

export interface Prediction {
  symbol: string;
  predicted_price_change: number;
  predicted_direction: 'up' | 'down' | 'neutral';
  confidence: number;
  timestamp: string;
  model_status: any;
  features: Record<string, number>;
}

export interface PredictionHistory {
  id: number;
  symbol: string;
  predicted_price_change: number;
  predicted_direction: string;
  confidence: number;
  actual_price_change?: number;
  actual_direction?: string;
  accuracy?: number;
  timestamp: string;
  validated: boolean;
}

export interface Alert {
  id: number;
  symbol: string;
  alert_type: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  message: string;
  timestamp: string;
  data: Record<string, any>;
}

export interface SystemStatus {
  status: 'running' | 'stopped' | 'error';
  uptime: number;
  memory_usage: number;
  cpu_usage: number;
  active_connections: number;
  last_update: string;
}

export interface StreamingData {
  symbol: string;
  price: number;
  volume: number;
  change_24h: number | undefined;
  timestamp: string;
}

export interface DatabaseStats {
  total_records: number;
  tables: {
    name: string;
    count: number;
    size: string;
  }[];
  last_update: string;
}

export interface MetricsData {
  predictions_total: number;
  predictions_accuracy: number;
  alerts_total: number;
  data_points_collected: number;
  uptime: number;
  last_training: string;
}