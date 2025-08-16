import { apiService } from '../services/api';

// Re-export apiService as apiClient for backward compatibility
export const apiClient = apiService;

// Export individual methods for convenience
export const {
  makePrediction,
  getPredictionHistory,
  getSystemMetrics,
  getModelMetrics,
  getTradeSignals,
  getMarketData,
  getAvailableSymbols,
  trainModel,
  getModelStatus
} = apiService;

export default apiClient;