import React, { useState, useEffect } from 'react';
import { 
  BellIcon, 
  Cog6ToothIcon, 
  SignalIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import { useWebSocket } from '@/hooks/useWebSocket';
import { SystemStatus } from '@/types/ml';
import apiClient from '@/api/client';

interface HeaderProps {
  title: string;
  subtitle?: string;
}

const Header: React.FC<HeaderProps> = ({ title, subtitle }) => {
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(null);
  const [alertsCount, setAlertsCount] = useState(0);
  const [currentTime, setCurrentTime] = useState(new Date());

  // WebSocket для обновлений в реальном времени
  const { isConnected } = useWebSocket('/ws/streaming', {
    onMessage: (data) => {
      if (data.type === 'system_status') {
        setSystemStatus(data.data);
      } else if (data.type === 'alerts_count') {
        setAlertsCount(data.count);
      }
    },
    onError: (error) => {
      console.error('WebSocket error:', error);
    }
  });

  // Обновление времени каждую секунду
  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentTime(new Date());
    }, 1000);

    return () => clearInterval(timer);
  }, []);

  // Загрузка начальных данных
  useEffect(() => {
    const loadInitialData = async () => {
      try {
        const [status, alerts] = await Promise.all([
          apiClient.getSystemStatus(),
          apiClient.getAlerts()
        ]);
        setSystemStatus(status);
        // Проверяем, что alerts является массивом перед использованием filter
        if (Array.isArray(alerts)) {
          setAlertsCount(alerts.filter(alert => alert.severity === 'high' || alert.severity === 'critical').length);
        } else {
          setAlertsCount(0);
        }
      } catch (error) {
        console.error('Failed to load initial data:', error);
      }
    };

    loadInitialData();
  }, []);

  const formatUptime = (seconds: number): string => {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return `${hours}ч ${minutes}м`;
  };

  const getStatusColor = (status?: string) => {
    switch (status) {
      case 'running':
        return 'text-green-600 bg-green-100';
      case 'error':
        return 'text-red-600 bg-red-100';
      case 'stopped':
        return 'text-yellow-600 bg-yellow-100';
      default:
        return 'text-gray-600 bg-gray-100';
    }
  };

  const getConnectionStatusColor = (connected: boolean) => {
    return connected ? 'text-green-600' : 'text-red-600';
  };

  return (
    <header className="bg-white border-b border-gray-200 px-6 py-4">
      <div className="flex items-center justify-between">
        {/* Заголовок */}
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{title}</h1>
          {subtitle && (
            <p className="text-sm text-gray-500 mt-1">{subtitle}</p>
          )}
        </div>

        {/* Статус и индикаторы */}
        <div className="flex items-center space-x-6">
          {/* Время */}
          <div className="text-sm text-gray-500">
            <div className="font-medium">
              {currentTime.toLocaleTimeString('ru-RU')}
            </div>
            <div className="text-xs">
              {currentTime.toLocaleDateString('ru-RU')}
            </div>
          </div>

          {/* Статус соединения */}
          <div className="flex items-center space-x-2">
            <SignalIcon 
              className={`w-5 h-5 ${getConnectionStatusColor(isConnected)}`} 
            />
            <span className={`text-sm font-medium ${getConnectionStatusColor(isConnected)}`}>
              {isConnected ? 'Подключено' : 'Отключено'}
            </span>
          </div>

          {/* Статус системы */}
          {systemStatus && (
            <div className="flex items-center space-x-2">
              <div className={`px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(systemStatus.status)}`}>
                {systemStatus.status === 'running' ? 'Работает' : 
                 systemStatus.status === 'error' ? 'Ошибка' : 'Остановлено'}
              </div>
              <div className="text-xs text-gray-500">
                Время работы: {formatUptime(systemStatus.uptime)}
              </div>
            </div>
          )}

          {/* Системные ресурсы */}
          {systemStatus && (
            <div className="flex items-center space-x-4 text-xs text-gray-500">
              <div>
                CPU: {systemStatus.cpu_usage?.toFixed(1) || '0.0'}%
              </div>
              <div>
                RAM: {systemStatus.memory_usage?.toFixed(1) || '0.0'}%
              </div>
            </div>
          )}

          {/* Уведомления */}
          <div className="relative">
            <button className="p-2 text-gray-400 hover:text-gray-600 transition-colors duration-200">
              <BellIcon className="w-5 h-5" />
              {alertsCount > 0 && (
                <span className="absolute -top-1 -right-1 bg-red-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center">
                  {alertsCount > 9 ? '9+' : alertsCount}
                </span>
              )}
            </button>
          </div>

          {/* Настройки */}
          <button className="p-2 text-gray-400 hover:text-gray-600 transition-colors duration-200">
            <Cog6ToothIcon className="w-5 h-5" />
          </button>
        </div>
      </div>

      {/* Дополнительная информация */}
      {systemStatus && systemStatus.status === 'error' && (
        <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg flex items-center space-x-2">
          <ExclamationTriangleIcon className="w-5 h-5 text-red-600" />
          <span className="text-sm text-red-700">
            Обнаружены проблемы в работе системы. Проверьте логи для получения подробной информации.
          </span>
        </div>
      )}
    </header>
  );
};

export default Header;