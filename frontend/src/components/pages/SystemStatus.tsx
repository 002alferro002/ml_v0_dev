import React, { useState, useEffect } from 'react';
import { 
  CpuChipIcon, 
  ServerIcon, 
  ClockIcon, 
  SignalIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon
} from '@heroicons/react/24/outline';
import { MLStatus, SystemStatus as SystemStatusType } from '@/types/ml';
import { useWebSocket } from '@/hooks/useWebSocket';
import apiClient from '@/api/client';

const SystemStatus: React.FC = () => {
  const [mlStatus, setMlStatus] = useState<MLStatus | null>(null);
  const [systemStatus, setSystemStatus] = useState<SystemStatusType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // WebSocket для обновлений в реальном времени
  const { isConnected } = useWebSocket('/ws/streaming', {
    onMessage: (data) => {
      if (data.type === 'ml_status') {
        setMlStatus(data.data);
      } else if (data.type === 'system_status') {
        setSystemStatus(data.data);
      }
    },
    onError: (error) => {
      console.error('WebSocket error:', error);
      setError('Ошибка подключения к WebSocket');
    }
  });

  // Загрузка данных
  useEffect(() => {
    const loadData = async () => {
      try {
        setLoading(true);
        const [mlData, sysData] = await Promise.all([
          apiClient.getMLStatus(),
          apiClient.getSystemStatus()
        ]);
        setMlStatus(mlData);
        setSystemStatus(sysData);
        setError(null);
      } catch (err) {
        setError('Ошибка загрузки данных');
        console.error('Failed to load system status:', err);
      } finally {
        setLoading(false);
      }
    };

    loadData();
    const interval = setInterval(loadData, 30000); // Обновление каждые 30 секунд

    return () => clearInterval(interval);
  }, []);

  const formatUptime = (seconds: number): string => {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    
    if (days > 0) {
      return `${days}д ${hours}ч ${minutes}м`;
    }
    return `${hours}ч ${minutes}м`;
  };

  const getStatusIcon = (status: boolean | string) => {
    if (typeof status === 'boolean') {
      return status ? (
        <CheckCircleIcon className="w-5 h-5 text-green-500" />
      ) : (
        <ExclamationTriangleIcon className="w-5 h-5 text-red-500" />
      );
    }
    
    switch (status) {
      case 'running':
        return <CheckCircleIcon className="w-5 h-5 text-green-500" />;
      case 'error':
        return <ExclamationTriangleIcon className="w-5 h-5 text-red-500" />;
      default:
        return <ClockIcon className="w-5 h-5 text-yellow-500" />;
    }
  };

  const getStatusText = (status: boolean | string) => {
    if (typeof status === 'boolean') {
      return status ? 'Активно' : 'Неактивно';
    }
    
    switch (status) {
      case 'running':
        return 'Работает';
      case 'error':
        return 'Ошибка';
      case 'stopped':
        return 'Остановлено';
      default:
        return 'Неизвестно';
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

  return (
    <div className="p-6 space-y-6">
      {/* Общий статус */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="metric-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Статус Системы</p>
              <p className="text-2xl font-bold text-gray-900">
                {getStatusText(systemStatus?.status || 'unknown')}
              </p>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <ServerIcon className="w-6 h-6 text-gray-600" />
            </div>
          </div>
        </div>

        <div className="metric-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Время Работы</p>
              <p className="text-2xl font-bold text-gray-900">
                {systemStatus ? formatUptime(systemStatus.uptime) : '—'}
              </p>
            </div>
            <div className="p-3 bg-blue-50 rounded-lg">
              <ClockIcon className="w-6 h-6 text-blue-600" />
            </div>
          </div>
        </div>

        <div className="metric-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Использование CPU</p>
              <p className="text-2xl font-bold text-gray-900">
                {systemStatus && systemStatus.cpu_usage !== undefined ? `${systemStatus.cpu_usage.toFixed(1)}%` : '—'}
              </p>
            </div>
            <div className="p-3 bg-green-50 rounded-lg">
              <CpuChipIcon className="w-6 h-6 text-green-600" />
            </div>
          </div>
        </div>

        <div className="metric-card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Использование RAM</p>
              <p className="text-2xl font-bold text-gray-900">
                {systemStatus && systemStatus.memory_usage !== undefined ? `${systemStatus.memory_usage.toFixed(1)}%` : '—'}
              </p>
            </div>
            <div className="p-3 bg-purple-50 rounded-lg">
              <ServerIcon className="w-6 h-6 text-purple-600" />
            </div>
          </div>
        </div>
      </div>

      {/* ML Статус */}
      {mlStatus && (
        <div className="card">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Статус Машинного Обучения</h3>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
              <div>
                <p className="text-sm font-medium text-gray-600">Режим Работы</p>
                <p className="text-lg font-semibold text-gray-900 capitalize">
                  {mlStatus.mode === 'training' ? 'Обучение' : 
                   mlStatus.mode === 'prediction' ? 'Предсказание' : 'Гибридный'}
                </p>
              </div>
              {getStatusIcon(mlStatus.is_running)}
            </div>

            <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
              <div>
                <p className="text-sm font-medium text-gray-600">Обучение</p>
                <p className="text-lg font-semibold text-gray-900">
                  {getStatusText(mlStatus.training_active)}
                </p>
              </div>
              {getStatusIcon(mlStatus.training_active)}
            </div>

            <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
              <div>
                <p className="text-sm font-medium text-gray-600">Предсказания</p>
                <p className="text-lg font-semibold text-gray-900">
                  {getStatusText(mlStatus.prediction_active)}
                </p>
              </div>
              {getStatusIcon(mlStatus.prediction_active)}
            </div>
          </div>

          {/* Статус моделей */}
          <div className="mt-6">
            <h4 className="text-md font-medium text-gray-900 mb-3">Статус Моделей</h4>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="flex items-center justify-between p-3 border border-gray-200 rounded-lg">
                <span className="text-sm text-gray-600">Модель Цены</span>
                {getStatusIcon(mlStatus.model_status.price_model_trained)}
              </div>
              <div className="flex items-center justify-between p-3 border border-gray-200 rounded-lg">
                <span className="text-sm text-gray-600">Модель Направления</span>
                {getStatusIcon(mlStatus.model_status.direction_model_trained)}
              </div>
              <div className="flex items-center justify-between p-3 border border-gray-200 rounded-lg">
                <span className="text-sm text-gray-600">Онлайн Модели</span>
                {getStatusIcon(mlStatus.model_status.online_models_trained)}
              </div>
              <div className="flex items-center justify-between p-3 border border-gray-200 rounded-lg">
                <span className="text-sm text-gray-600">Сбор Данных</span>
                {getStatusIcon(mlStatus.collection_stats.is_running)}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Статус подключения */}
      <div className="card">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Статус Подключений</h3>
        
        <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
          <div className="flex items-center space-x-3">
            <SignalIcon className={`w-5 h-5 ${isConnected ? 'text-green-500' : 'text-red-500'}`} />
            <div>
              <p className="text-sm font-medium text-gray-900">WebSocket</p>
              <p className="text-xs text-gray-500">
                {isConnected ? 'Подключено к серверу' : 'Отключено от сервера'}
              </p>
            </div>
          </div>
          <span className={`px-2 py-1 rounded-full text-xs font-medium ${
            isConnected ? 'bg-green-100 text-green-600' : 'bg-red-100 text-red-600'
          }`}>
            {isConnected ? 'Активно' : 'Неактивно'}
          </span>
        </div>

        {systemStatus && (
          <div className="mt-4 p-4 bg-gray-50 rounded-lg">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-900">Активные Подключения</p>
                <p className="text-xs text-gray-500">Количество активных соединений</p>
              </div>
              <span className="text-lg font-semibold text-gray-900">
                {systemStatus.active_connections}
              </span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default SystemStatus;