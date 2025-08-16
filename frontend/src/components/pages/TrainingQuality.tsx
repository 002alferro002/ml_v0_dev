import React, { useState, useEffect } from 'react';
import { 
  ChartBarIcon,
  TrophyIcon,
  ClockIcon,
  ExclamationTriangleIcon,
  ArrowTrendingUpIcon,
  ArrowTrendingDownIcon,
  CalendarIcon,
  AcademicCapIcon
} from '@heroicons/react/24/outline';
import { TrainingMetrics } from '@/types/ml';
import { useWebSocket } from '@/hooks/useWebSocket';
import apiClient from '@/api/client';

interface QualityMetric {
  name: string;
  value: number;
  target: number;
  status: 'good' | 'warning' | 'poor';
  trend: 'up' | 'down' | 'stable';
}

const TrainingQuality: React.FC = () => {
  const [trainingMetrics, setTrainingMetrics] = useState<TrainingMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [timeRange, setTimeRange] = useState<'1h' | '6h' | '24h' | '7d'>('24h');
  
  const { isConnected, data: wsData } = useWebSocket('/ws/training_metrics');

  // Обновление метрик через WebSocket
  useEffect(() => {
    if (wsData && wsData.type === 'training_metrics_update') {
      setTrainingMetrics(wsData.data);
    }
  }, [wsData]);

  // Загрузка данных
  const loadTrainingMetrics = async () => {
    try {
      setLoading(true);
      const data = await apiClient.getTrainingMetrics();
      setTrainingMetrics(data);
      setError(null);
    } catch (err) {
      setError('Ошибка загрузки метрик обучения');
      console.error('Failed to load training metrics:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadTrainingMetrics();
  }, []);

  // Автообновление
  useEffect(() => {
    const interval = setInterval(() => {
      loadTrainingMetrics();
    }, 30000); // Обновление каждые 30 секунд

    return () => clearInterval(interval);
  }, []);

  const getQualityMetrics = (): QualityMetric[] => {
    if (!trainingMetrics) return [];
    
    return [
      {
        name: 'Точность',
        value: trainingMetrics.accuracy,
        target: 0.85,
        status: trainingMetrics.accuracy >= 0.85 ? 'good' : trainingMetrics.accuracy >= 0.7 ? 'warning' : 'poor',
        trend: trainingMetrics.accuracy > 0.8 ? 'up' : trainingMetrics.accuracy < 0.6 ? 'down' : 'stable'
      },
      {
        name: 'Потери',
        value: trainingMetrics.loss,
        target: 0.1,
        status: trainingMetrics.loss <= 0.1 ? 'good' : trainingMetrics.loss <= 0.3 ? 'warning' : 'poor',
        trend: trainingMetrics.loss < 0.2 ? 'up' : trainingMetrics.loss > 0.4 ? 'down' : 'stable'
      },
      {
        name: 'F1-Score',
        value: trainingMetrics.f1_score || 0,
        target: 0.8,
        status: (trainingMetrics.f1_score || 0) >= 0.8 ? 'good' : (trainingMetrics.f1_score || 0) >= 0.6 ? 'warning' : 'poor',
        trend: (trainingMetrics.f1_score || 0) > 0.75 ? 'up' : (trainingMetrics.f1_score || 0) < 0.5 ? 'down' : 'stable'
      },
      {
        name: 'Precision',
        value: trainingMetrics.precision || 0,
        target: 0.8,
        status: (trainingMetrics.precision || 0) >= 0.8 ? 'good' : (trainingMetrics.precision || 0) >= 0.6 ? 'warning' : 'poor',
        trend: (trainingMetrics.precision || 0) > 0.75 ? 'up' : (trainingMetrics.precision || 0) < 0.5 ? 'down' : 'stable'
      },
      {
        name: 'Recall',
        value: trainingMetrics.recall || 0,
        target: 0.8,
        status: (trainingMetrics.recall || 0) >= 0.8 ? 'good' : (trainingMetrics.recall || 0) >= 0.6 ? 'warning' : 'poor',
        trend: (trainingMetrics.recall || 0) > 0.75 ? 'up' : (trainingMetrics.recall || 0) < 0.5 ? 'down' : 'stable'
      }
    ];
  };

  const formatPercentage = (value: number): string => {
    return `${(value * 100).toFixed(1)}%`;
  };

  const formatDuration = (seconds: number): string => {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    if (hours > 0) {
      return `${hours}ч ${minutes}м ${secs}с`;
    } else if (minutes > 0) {
      return `${minutes}м ${secs}с`;
    } else {
      return `${secs}с`;
    }
  };

  const getStatusColor = (status: 'good' | 'warning' | 'poor'): string => {
    switch (status) {
      case 'good':
        return 'text-green-600 bg-green-50';
      case 'warning':
        return 'text-yellow-600 bg-yellow-50';
      case 'poor':
        return 'text-red-600 bg-red-50';
      default:
        return 'text-gray-600 bg-gray-50';
    }
  };

  const getTrendIcon = (trend: 'up' | 'down' | 'stable') => {
    switch (trend) {
      case 'up':
        return <ArrowTrendingUpIcon className="w-4 h-4 text-green-500" />;
      case 'down':
        return <ArrowTrendingDownIcon className="w-4 h-4 text-red-500" />;
      default:
        return null;
    }
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

  const qualityMetrics = getQualityMetrics();

  return (
    <div className="p-6">
      {/* Заголовок */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold text-gray-900">Качество Обучения</h2>
          <p className="text-sm text-gray-500 mt-1">
            Метрики и анализ качества обучения ML моделей
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
            onClick={loadTrainingMetrics}
            className="flex items-center space-x-2 px-3 py-2 text-sm text-primary-600 hover:bg-primary-50 rounded-lg transition-colors"
          >
            <AcademicCapIcon className="w-4 h-4" />
            <span>Обновить</span>
          </button>
        </div>
      </div>

      {/* Фильтры */}
      <div className="card mb-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <span className="text-sm font-medium text-gray-700">Период:</span>
            <div className="flex space-x-2">
              {(['1h', '6h', '24h', '7d'] as const).map((range) => (
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
                   range === '24h' ? '24 часа' : '7 дней'}
                </button>
              ))}
            </div>
          </div>
          
          <div className="text-sm text-gray-500">
            Последнее обновление: {trainingMetrics ? new Date(trainingMetrics.timestamp).toLocaleString('ru-RU') : 'Н/Д'}
          </div>
        </div>
      </div>

      {/* Основные метрики */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-6">
        {qualityMetrics.map((metric, index) => (
          <div key={index} className="metric-card">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-gray-600">{metric.name}</h3>
              <div className={`px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(metric.status)}`}>
                {metric.status === 'good' ? 'Отлично' :
                 metric.status === 'warning' ? 'Удовлетворительно' : 'Плохо'}
              </div>
            </div>
            
            <div className="flex items-center justify-between">
              <div>
                <p className="text-2xl font-bold text-gray-900">
                  {metric.name === 'Потери' ? metric.value.toFixed(3) : formatPercentage(metric.value)}
                </p>
                <p className="text-sm text-gray-500">
                  Цель: {metric.name === 'Потери' ? `≤${metric.target}` : formatPercentage(metric.target)}
                </p>
              </div>
              <div className="flex items-center space-x-1">
                {getTrendIcon(metric.trend)}
              </div>
            </div>
            
            {/* Прогресс бар */}
            <div className="mt-3">
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className={`h-2 rounded-full transition-all duration-300 ${
                    metric.status === 'good' ? 'bg-green-500' :
                    metric.status === 'warning' ? 'bg-yellow-500' : 'bg-red-500'
                  }`}
                  style={{ 
                    width: metric.name === 'Потери' 
                      ? `${Math.max(0, Math.min(100, (1 - metric.value) * 100))}%`
                      : `${Math.min(100, metric.value * 100)}%` 
                  }}
                ></div>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Детальная информация */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Информация о последнем обучении */}
        <div className="card">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Последнее Обучение</h3>
          
          <div className="space-y-4">
            <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
              <span className="text-sm text-gray-600">Время начала</span>
              <span className="font-medium text-gray-900">
                {trainingMetrics ? new Date(trainingMetrics.timestamp).toLocaleString('ru-RU') : 'Н/Д'}
              </span>
            </div>
            
            <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
              <span className="text-sm text-gray-600">Продолжительность</span>
              <span className="font-medium text-gray-900">
                {trainingMetrics?.training_duration ? formatDuration(trainingMetrics.training_duration) : 'Н/Д'}
              </span>
            </div>
            
            <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
              <span className="text-sm text-gray-600">Количество эпох</span>
              <span className="font-medium text-gray-900">
                {trainingMetrics?.epochs || 'Н/Д'}
              </span>
            </div>
            
            <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
              <span className="text-sm text-gray-600">Размер обучающей выборки</span>
              <span className="font-medium text-gray-900">
                {trainingMetrics?.training_samples ? trainingMetrics.training_samples.toLocaleString('ru-RU') : 'Н/Д'}
              </span>
            </div>
            
            <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
              <span className="text-sm text-gray-600">Размер валидационной выборки</span>
              <span className="font-medium text-gray-900">
                {trainingMetrics?.validation_samples ? trainingMetrics.validation_samples.toLocaleString('ru-RU') : 'Н/Д'}
              </span>
            </div>
          </div>
        </div>

        {/* Анализ качества */}
        <div className="card">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Анализ Качества</h3>
          
          <div className="space-y-4">
            {/* Общая оценка */}
            <div className="p-4 bg-gradient-to-r from-blue-50 to-purple-50 rounded-lg">
              <div className="flex items-center space-x-3">
                <TrophyIcon className="w-8 h-8 text-blue-600" />
                <div>
                  <p className="font-semibold text-gray-900">Общая Оценка</p>
                  <p className="text-sm text-gray-600">
                    {qualityMetrics.filter(m => m.status === 'good').length >= 3 ? 'Отличное качество модели' :
                     qualityMetrics.filter(m => m.status === 'good').length >= 2 ? 'Хорошее качество модели' :
                     qualityMetrics.filter(m => m.status === 'poor').length >= 2 ? 'Требуется улучшение' :
                     'Удовлетворительное качество'}
                  </p>
                </div>
              </div>
            </div>
            
            {/* Рекомендации */}
            <div className="space-y-3">
              <h4 className="font-medium text-gray-900">Рекомендации:</h4>
              
              {qualityMetrics.some(m => m.status === 'poor') && (
                <div className="flex items-start space-x-3 p-3 bg-red-50 rounded-lg">
                  <ExclamationTriangleIcon className="w-5 h-5 text-red-600 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-red-900">Критические проблемы</p>
                    <p className="text-xs text-red-700">
                      Обнаружены метрики с низким качеством. Рекомендуется пересмотреть архитектуру модели.
                    </p>
                  </div>
                </div>
              )}
              
              {trainingMetrics && trainingMetrics.loss > 0.3 && (
                <div className="flex items-start space-x-3 p-3 bg-yellow-50 rounded-lg">
                  <ClockIcon className="w-5 h-5 text-yellow-600 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-yellow-900">Высокие потери</p>
                    <p className="text-xs text-yellow-700">
                      Увеличьте количество эпох обучения или настройте гиперпараметры.
                    </p>
                  </div>
                </div>
              )}
              
              {trainingMetrics && trainingMetrics.accuracy > 0.9 && (
                <div className="flex items-start space-x-3 p-3 bg-green-50 rounded-lg">
                  <ChartBarIcon className="w-5 h-5 text-green-600 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-green-900">Отличные результаты</p>
                    <p className="text-xs text-green-700">
                      Модель показывает высокую точность. Можно переходить к продакшену.
                    </p>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* История обучения */}
      <div className="card mt-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">История Обучения</h3>
        
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Дата
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Точность
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Потери
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  F1-Score
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Продолжительность
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Статус
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {trainingMetrics && (
                <tr className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {new Date(trainingMetrics.timestamp).toLocaleString('ru-RU')}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {formatPercentage(trainingMetrics.accuracy)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {trainingMetrics.loss.toFixed(3)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {trainingMetrics.f1_score ? formatPercentage(trainingMetrics.f1_score) : 'Н/Д'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {trainingMetrics.training_duration ? formatDuration(trainingMetrics.training_duration) : 'Н/Д'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="inline-flex px-2 py-1 text-xs font-semibold rounded-full bg-green-100 text-green-800">
                      Завершено
                    </span>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default TrainingQuality;