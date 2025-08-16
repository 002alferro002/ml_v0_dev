import React, { useState, useEffect } from 'react';
import { 
  ChartBarIcon,
  TrophyIcon,
  ClockIcon,
  ExclamationTriangleIcon,
  ArrowTrendingUpIcon,
  ArrowTrendingDownIcon,
  EyeIcon,
  CheckCircleIcon,
  XCircleIcon
} from '@heroicons/react/24/outline';
import { Prediction, PredictionHistory } from '@/types/ml';
import { useWebSocket } from '@/hooks/useWebSocket';
import apiClient from '@/api/client';

interface PredictionStats {
  total: number;
  correct: number;
  incorrect: number;
  accuracy: number;
  avgConfidence: number;
  profitability: number;
}

const PredictionQuality: React.FC = () => {
  const [predictions, setPredictions] = useState<Prediction[]>([]);
  const [predictionHistory, setPredictionHistory] = useState<PredictionHistory[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [timeRange, setTimeRange] = useState<'1h' | '6h' | '24h' | '7d' | '30d'>('24h');
  const [symbolFilter, setSymbolFilter] = useState<string>('all');
  
  const { isConnected, data: wsData } = useWebSocket('/ws/predictions');

  // Обновление предсказаний через WebSocket
  useEffect(() => {
    if (wsData) {
      if (wsData.type === 'new_prediction') {
        setPredictions(prev => [wsData.data, ...prev.slice(0, 99)]); // Ограничиваем до 100 записей
      } else if (wsData.type === 'prediction_result') {
        setPredictionHistory(prev => [wsData.data, ...prev.slice(0, 499)]); // Ограничиваем до 500 записей
      }
    }
  }, [wsData]);

  // Загрузка данных
  const loadPredictions = async () => {
    try {
      setLoading(true);
      const [currentPredictions, history] = await Promise.all([
        apiClient.getCurrentPredictions(),
        apiClient.getPredictionHistory()
      ]);
      
      setPredictions(currentPredictions);
      setPredictionHistory(history);
      setError(null);
    } catch (err) {
      setError('Ошибка загрузки предсказаний');
      console.error('Failed to load predictions:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadPredictions();
  }, []);

  // Автообновление
  useEffect(() => {
    const interval = setInterval(() => {
      loadPredictions();
    }, 30000); // Обновление каждые 30 секунд

    return () => clearInterval(interval);
  }, []);

  // Вычисление статистики
  const calculateStats = (): PredictionStats => {
    const now = new Date();
    const timeRangeMs = {
      '1h': 60 * 60 * 1000,
      '6h': 6 * 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000,
      '7d': 7 * 24 * 60 * 60 * 1000,
      '30d': 30 * 24 * 60 * 60 * 1000
    }[timeRange];
    
    const filteredHistory = predictionHistory.filter(p => {
      const predictionTime = new Date(p.timestamp).getTime();
      const isInTimeRange = now.getTime() - predictionTime <= timeRangeMs;
      const isSymbolMatch = symbolFilter === 'all' || p.symbol === symbolFilter;
      return isInTimeRange && isSymbolMatch && p.actual_direction !== null;
    });
    
    const total = filteredHistory.length;
    const correct = filteredHistory.filter(p => p.predicted_direction === p.actual_direction).length;
    const incorrect = total - correct;
    const accuracy = total > 0 ? correct / total : 0;
    
    const avgConfidence = total > 0 
      ? filteredHistory.reduce((sum, p) => sum + p.confidence, 0) / total 
      : 0;
    
    // Простой расчет прибыльности (предполагаем 1% прибыль за правильное предсказание)
    const profitability = total > 0 ? (correct * 0.01 - incorrect * 0.01) : 0;
    
    return {
      total,
      correct,
      incorrect,
      accuracy,
      avgConfidence,
      profitability
    };
  };

  const getUniqueSymbols = (): string[] => {
    const symbols = new Set(predictionHistory.map(p => p.symbol));
    return Array.from(symbols).sort();
  };

  const formatPercentage = (value: number): string => {
    return `${(value * 100).toFixed(1)}%`;
  };

  const formatDateTime = (dateString: string): string => {
    return new Date(dateString).toLocaleString('ru-RU');
  };

  const getDirectionIcon = (direction: 'up' | 'down') => {
    return direction === 'up' 
      ? <ArrowTrendingUpIcon className="w-4 h-4 text-green-500" />
      : <ArrowTrendingDownIcon className="w-4 h-4 text-red-500" />;
  };

  const getResultIcon = (predicted: 'up' | 'down', actual: 'up' | 'down' | null) => {
    if (actual === null) return <ClockIcon className="w-4 h-4 text-gray-400" />;
    return predicted === actual 
      ? <CheckCircleIcon className="w-4 h-4 text-green-500" />
      : <XCircleIcon className="w-4 h-4 text-red-500" />;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6">
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-center space-x-2">
            <ExclamationTriangleIcon className="w-5 h-5 text-red-600" />
            <span className="text-red-700">{error}</span>
          </div>
        </div>
      </div>
    );
  }

  const stats = calculateStats();
  const uniqueSymbols = getUniqueSymbols();

  return (
    <div className="p-6">
      {/* Заголовок */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold text-gray-900">Качество Предсказаний</h2>
          <p className="text-sm text-gray-500 mt-1">
            Анализ точности и эффективности ML предсказаний
          </p>
        </div>
        
        <div className="flex items-center space-x-4">
          {/* Статус подключения */}
          <div className={`flex items-center space-x-2 px-3 py-1 rounded-full text-sm ${
            isConnected ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
          }`}>
            <div className={`w-2 h-2 rounded-full ${
              isConnected ? 'bg-green-500 pulse-indicator' : 'bg-red-500'
            }`}></div>
            <span>{isConnected ? 'В реальном времени' : 'Отключено'}</span>
          </div>
          
          <button
            onClick={loadPredictions}
            className="flex items-center space-x-2 px-3 py-2 text-sm text-primary-600 hover:bg-primary-50 rounded-lg transition-colors"
          >
            <EyeIcon className="w-4 h-4" />
            <span>Обновить</span>
          </button>
        </div>
      </div>

      {/* Фильтры */}
      <div className="card mb-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-6">
            <div className="flex items-center space-x-2">
              <span className="text-sm font-medium text-gray-700">Период:</span>
              <div className="flex space-x-2">
                {(['1h', '6h', '24h', '7d', '30d'] as const).map((range) => (
                  <button
                    key={range}
                    onClick={() => setTimeRange(range)}
                    className={`px-3 py-1 text-sm rounded-lg transition-colors ${
                      timeRange === range
                        ? 'bg-primary-100 text-primary-700'
                        : 'text-gray-600 hover:bg-gray-100'
                    }`}
                  >
                    {range === '1h' ? '1 час' :
                     range === '6h' ? '6 часов' :
                     range === '24h' ? '24 часа' :
                     range === '7d' ? '7 дней' : '30 дней'}
                  </button>
                ))}
              </div>
            </div>
            
            <div className="flex items-center space-x-2">
              <span className="text-sm font-medium text-gray-700">Символ:</span>
              <select
                value={symbolFilter}
                onChange={(e) => setSymbolFilter(e.target.value)}
                className="border border-gray-300 rounded-lg px-3 py-1 text-sm focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              >
                <option value="all">Все символы</option>
                {uniqueSymbols.map(symbol => (
                  <option key={symbol} value={symbol}>{symbol}</option>
                ))}
              </select>
            </div>
          </div>
          
          <div className="text-sm text-gray-500">
            Активных предсказаний: {predictions.length}
          </div>
        </div>
      </div>

      {/* Основные метрики */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
        <div className="metric-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Точность</p>
              <p className="text-2xl font-bold text-gray-900">{formatPercentage(stats.accuracy)}</p>
              <p className="text-xs text-gray-500">{stats.correct} из {stats.total}</p>
            </div>
            <div className="p-3 bg-green-50 rounded-lg">
              <TrophyIcon className="w-6 h-6 text-green-600" />
            </div>
          </div>
        </div>

        <div className="metric-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Средняя Уверенность</p>
              <p className="text-2xl font-bold text-gray-900">{formatPercentage(stats.avgConfidence)}</p>
              <p className="text-xs text-gray-500">За выбранный период</p>
            </div>
            <div className="p-3 bg-blue-50 rounded-lg">
              <ChartBarIcon className="w-6 h-6 text-blue-600" />
            </div>
          </div>
        </div>

        <div className="metric-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Прибыльность</p>
              <p className={`text-2xl font-bold ${
                stats.profitability >= 0 ? 'text-green-600' : 'text-red-600'
              }`}>
                {stats.profitability >= 0 ? '+' : ''}{formatPercentage(stats.profitability)}
              </p>
              <p className="text-xs text-gray-500">Теоретическая</p>
            </div>
            <div className={`p-3 rounded-lg ${
              stats.profitability >= 0 ? 'bg-green-50' : 'bg-red-50'
            }`}>
              {stats.profitability >= 0 
                ? <ArrowTrendingUpIcon className="w-6 h-6 text-green-600" />
                : <ArrowTrendingDownIcon className="w-6 h-6 text-red-600" />
              }
            </div>
          </div>
        </div>

        <div className="metric-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Всего Предсказаний</p>
              <p className="text-2xl font-bold text-gray-900">{stats.total}</p>
              <p className="text-xs text-gray-500">За выбранный период</p>
            </div>
            <div className="p-3 bg-purple-50 rounded-lg">
              <EyeIcon className="w-6 h-6 text-purple-600" />
            </div>
          </div>
        </div>
      </div>

      {/* Детальный анализ */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        {/* Распределение результатов */}
        <div className="card">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Распределение Результатов</h3>
          
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="w-4 h-4 bg-green-500 rounded"></div>
                <span className="text-sm text-gray-600">Правильные</span>
              </div>
              <div className="flex items-center space-x-2">
                <span className="font-medium">{stats.correct}</span>
                <span className="text-sm text-gray-500">({formatPercentage(stats.total > 0 ? stats.correct / stats.total : 0)})</span>
              </div>
            </div>
            
            <div className="w-full bg-gray-200 rounded-full h-2">
              <div 
                className="bg-green-500 h-2 rounded-full transition-all duration-300"
                style={{ width: `${stats.total > 0 ? (stats.correct / stats.total) * 100 : 0}%` }}
              ></div>
            </div>
            
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="w-4 h-4 bg-red-500 rounded"></div>
                <span className="text-sm text-gray-600">Неправильные</span>
              </div>
              <div className="flex items-center space-x-2">
                <span className="font-medium">{stats.incorrect}</span>
                <span className="text-sm text-gray-500">({formatPercentage(stats.total > 0 ? stats.incorrect / stats.total : 0)})</span>
              </div>
            </div>
            
            <div className="w-full bg-gray-200 rounded-full h-2">
              <div 
                className="bg-red-500 h-2 rounded-full transition-all duration-300"
                style={{ width: `${stats.total > 0 ? (stats.incorrect / stats.total) * 100 : 0}%` }}
              ></div>
            </div>
          </div>
        </div>

        {/* Анализ по символам */}
        <div className="card">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Топ Символы по Точности</h3>
          
          <div className="space-y-3">
            {uniqueSymbols.slice(0, 5).map(symbol => {
              const symbolHistory = predictionHistory.filter(p => 
                p.symbol === symbol && p.actual_direction !== null
              );
              const symbolCorrect = symbolHistory.filter(p => 
                p.predicted_direction === p.actual_direction
              ).length;
              const symbolAccuracy = symbolHistory.length > 0 ? symbolCorrect / symbolHistory.length : 0;
              
              return (
                <div key={symbol} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <div>
                    <p className="font-medium text-gray-900">{symbol}</p>
                    <p className="text-sm text-gray-500">{symbolHistory.length} предсказаний</p>
                  </div>
                  <div className="text-right">
                    <p className="font-medium text-gray-900">{formatPercentage(symbolAccuracy)}</p>
                    <p className="text-sm text-gray-500">{symbolCorrect}/{symbolHistory.length}</p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Текущие предсказания */}
      <div className="card mb-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Текущие Предсказания</h3>
        
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Символ
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Направление
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Уверенность
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Изменение Цены
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Время
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Статус
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {predictions.slice(0, 10).map((prediction, index) => (
                <tr key={`${prediction.symbol}-${prediction.timestamp}-${index}`} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="font-medium text-gray-900">{prediction.symbol}</span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center space-x-2">
                      {getDirectionIcon(prediction.predicted_direction)}
                      <span className={`font-medium ${
                        prediction.predicted_direction === 'up' ? 'text-green-600' : 'text-red-600'
                      }`}>
                        {prediction.predicted_direction === 'up' ? 'Вверх' : 'Вниз'}
                      </span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="font-medium text-gray-900">
                      {formatPercentage(prediction.confidence)}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {prediction.predicted_price_change !== undefined ? 
                      `${prediction.predicted_price_change > 0 ? '+' : ''}${prediction.predicted_price_change.toFixed(4)}%` : 
                      'N/A'
                    }
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {formatDateTime(prediction.timestamp)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="inline-flex px-2 py-1 text-xs font-semibold rounded-full bg-blue-100 text-blue-800">
                      Активно
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* История предсказаний */}
      <div className="card">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">История Предсказаний</h3>
        
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Символ
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Предсказание
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Факт
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Результат
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Уверенность
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Время
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {predictionHistory.slice(0, 20).map((prediction) => (
                <tr key={prediction.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="font-medium text-gray-900">{prediction.symbol}</span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center space-x-2">
                      {getDirectionIcon(prediction.predicted_direction)}
                      <span className={`text-sm ${
                        prediction.predicted_direction === 'up' ? 'text-green-600' : 'text-red-600'
                      }`}>
                        {prediction.predicted_direction === 'up' ? 'Вверх' : 'Вниз'}
                      </span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    {prediction.actual_direction ? (
                      <div className="flex items-center space-x-2">
                        {getDirectionIcon(prediction.actual_direction)}
                        <span className={`text-sm ${
                          prediction.actual_direction === 'up' ? 'text-green-600' : 'text-red-600'
                        }`}>
                          {prediction.actual_direction === 'up' ? 'Вверх' : 'Вниз'}
                        </span>
                      </div>
                    ) : (
                      <span className="text-sm text-gray-400">Ожидание</span>
                    )}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center space-x-2">
                      {getResultIcon(prediction.predicted_direction, prediction.actual_direction)}
                      <span className={`text-sm font-medium ${
                        prediction.actual_direction === null ? 'text-gray-400' :
                        prediction.predicted_direction === prediction.actual_direction ? 'text-green-600' : 'text-red-600'
                      }`}>
                        {prediction.actual_direction === null ? 'Ожидание' :
                         prediction.predicted_direction === prediction.actual_direction ? 'Правильно' : 'Неправильно'}
                      </span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {formatPercentage(prediction.confidence)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {formatDateTime(prediction.timestamp)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default PredictionQuality;