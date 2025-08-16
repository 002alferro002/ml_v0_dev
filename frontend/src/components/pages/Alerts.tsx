import React, { useState, useEffect } from 'react';
import { 
  ExclamationTriangleIcon,
  InformationCircleIcon,
  CheckCircleIcon,
  XCircleIcon,
  BellIcon,
  FunnelIcon,
  TrashIcon
} from '@heroicons/react/24/outline';
import { Alert } from '@/types/ml';
import { useWebSocket } from '@/hooks/useWebSocket';
import apiClient from '@/api/client';

type SeverityFilter = 'all' | 'low' | 'medium' | 'high' | 'critical';
type StatusFilter = 'all' | 'unread' | 'read';

const Alerts: React.FC = () => {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [filteredAlerts, setFilteredAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [severityFilter, setSeverityFilter] = useState<SeverityFilter>('all');
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');
  const [readAlerts, setReadAlerts] = useState<Set<number>>(new Set());

  // WebSocket для новых уведомлений
  const { isConnected } = useWebSocket('/ws/alerts', {
    onMessage: (data) => {
      if (data.type === 'new_alert') {
        setAlerts(prevAlerts => [data.data, ...prevAlerts]);
      }
    },
    onError: (error) => {
      console.error('WebSocket error:', error);
    }
  });

  // Загрузка уведомлений
  useEffect(() => {
    const loadAlerts = async () => {
      try {
        setLoading(true);
        const alertsData = await apiClient.getAlerts(100);
        setAlerts(alertsData);
        setError(null);
      } catch (err) {
        setError('Ошибка загрузки уведомлений');
        console.error('Failed to load alerts:', err);
      } finally {
        setLoading(false);
      }
    };

    loadAlerts();
  }, []);

  // Фильтрация уведомлений
  useEffect(() => {
    let filtered = alerts;

    if (severityFilter !== 'all') {
      filtered = filtered.filter(alert => alert.severity === severityFilter);
    }

    if (statusFilter === 'read') {
      filtered = filtered.filter(alert => readAlerts.has(alert.id));
    } else if (statusFilter === 'unread') {
      filtered = filtered.filter(alert => !readAlerts.has(alert.id));
    }

    setFilteredAlerts(filtered);
  }, [alerts, severityFilter, statusFilter, readAlerts]);

  const markAsRead = async (alertId: number) => {
    try {
      await apiClient.markAlertAsRead(alertId);
      setReadAlerts(prev => new Set([...prev, alertId]));
    } catch (err) {
      console.error('Failed to mark alert as read:', err);
    }
  };

  const markAllAsRead = () => {
    const unreadIds = filteredAlerts
      .filter(alert => !readAlerts.has(alert.id))
      .map(alert => alert.id);
    
    setReadAlerts(prev => new Set([...prev, ...unreadIds]));
  };

  const getSeverityIcon = (severity: string) => {
    switch (severity) {
      case 'critical':
        return <XCircleIcon className="w-5 h-5 text-red-600" />;
      case 'high':
        return <ExclamationTriangleIcon className="w-5 h-5 text-orange-600" />;
      case 'medium':
        return <InformationCircleIcon className="w-5 h-5 text-yellow-600" />;
      case 'low':
        return <CheckCircleIcon className="w-5 h-5 text-blue-600" />;
      default:
        return <BellIcon className="w-5 h-5 text-gray-600" />;
    }
  };

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case 'critical':
        return 'bg-red-50 border-red-200 text-red-800';
      case 'high':
        return 'bg-orange-50 border-orange-200 text-orange-800';
      case 'medium':
        return 'bg-yellow-50 border-yellow-200 text-yellow-800';
      case 'low':
        return 'bg-blue-50 border-blue-200 text-blue-800';
      default:
        return 'bg-gray-50 border-gray-200 text-gray-800';
    }
  };

  const getSeverityBadge = (severity: string) => {
    const labels = {
      critical: 'Критический',
      high: 'Высокий',
      medium: 'Средний',
      low: 'Низкий'
    };
    
    return (
      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
        severity === 'critical' ? 'bg-red-100 text-red-800' :
        severity === 'high' ? 'bg-orange-100 text-orange-800' :
        severity === 'medium' ? 'bg-yellow-100 text-yellow-800' :
        'bg-blue-100 text-blue-800'
      }`}>
        {labels[severity as keyof typeof labels] || severity}
      </span>
    );
  };

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffMins < 1) {
      return 'Только что';
    } else if (diffMins < 60) {
      return `${diffMins} мин назад`;
    } else if (diffHours < 24) {
      return `${diffHours} ч назад`;
    } else if (diffDays < 7) {
      return `${diffDays} дн назад`;
    } else {
      return date.toLocaleDateString('ru-RU');
    }
  };

  const unreadCount = filteredAlerts.filter(alert => !readAlerts.has(alert.id)).length;

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
          <span className="text-red-700">{error}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      {/* Заголовок и фильтры */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold text-gray-900">Уведомления</h2>
          <p className="text-sm text-gray-500 mt-1">
            {unreadCount > 0 ? `${unreadCount} непрочитанных из ${filteredAlerts.length}` : `Все ${filteredAlerts.length} уведомлений прочитаны`}
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
          
          {unreadCount > 0 && (
            <button
              onClick={markAllAsRead}
              className="btn-secondary text-sm"
            >
              Отметить все как прочитанные
            </button>
          )}
        </div>
      </div>

      {/* Фильтры */}
      <div className="card mb-6">
        <div className="flex items-center space-x-6">
          <div className="flex items-center space-x-2">
            <FunnelIcon className="w-5 h-5 text-gray-400" />
            <span className="text-sm font-medium text-gray-700">Фильтры:</span>
          </div>
          
          <div className="flex items-center space-x-2">
            <label className="text-sm text-gray-600">Важность:</label>
            <select
              value={severityFilter}
              onChange={(e) => setSeverityFilter(e.target.value as SeverityFilter)}
              className="border border-gray-300 rounded-lg px-3 py-1 text-sm focus:ring-2 focus:ring-primary-500 focus:border-transparent"
            >
              <option value="all">Все</option>
              <option value="critical">Критический</option>
              <option value="high">Высокий</option>
              <option value="medium">Средний</option>
              <option value="low">Низкий</option>
            </select>
          </div>
          
          <div className="flex items-center space-x-2">
            <label className="text-sm text-gray-600">Статус:</label>
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value as StatusFilter)}
              className="border border-gray-300 rounded-lg px-3 py-1 text-sm focus:ring-2 focus:ring-primary-500 focus:border-transparent"
            >
              <option value="all">Все</option>
              <option value="unread">Непрочитанные</option>
              <option value="read">Прочитанные</option>
            </select>
          </div>
        </div>
      </div>

      {/* Список уведомлений */}
      <div className="space-y-4">
        {filteredAlerts.map((alert) => {
          const isRead = readAlerts.has(alert.id);
          
          return (
            <div
              key={alert.id}
              className={`border rounded-lg p-4 transition-all duration-200 hover:shadow-md ${
                isRead ? 'bg-white border-gray-200' : 'bg-blue-50 border-blue-200'
              } ${getSeverityColor(alert.severity)}`}
            >
              <div className="flex items-start justify-between">
                <div className="flex items-start space-x-3 flex-1">
                  {getSeverityIcon(alert.severity)}
                  
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center space-x-2 mb-2">
                      <h3 className="text-sm font-medium text-gray-900">
                        {alert.alert_type}
                      </h3>
                      {getSeverityBadge(alert.severity)}
                      {!isRead && (
                        <span className="w-2 h-2 bg-blue-500 rounded-full"></span>
                      )}
                    </div>
                    
                    <p className="text-sm text-gray-700 mb-2">
                      {alert.message}
                    </p>
                    
                    <div className="flex items-center justify-between text-xs text-gray-500">
                      <div className="flex items-center space-x-4">
                        <span>Символ: {alert.symbol}</span>
                        <span>{formatTimestamp(alert.timestamp)}</span>
                      </div>
                    </div>
                    
                    {/* Дополнительные данные */}
                    {alert.data && Object.keys(alert.data).length > 0 && (
                      <div className="mt-3 p-3 bg-gray-50 rounded-lg">
                        <h4 className="text-xs font-medium text-gray-700 mb-2">Дополнительная информация:</h4>
                        <div className="grid grid-cols-2 gap-2 text-xs">
                          {Object.entries(alert.data).map(([key, value]) => (
                            <div key={key} className="flex justify-between">
                              <span className="text-gray-600">{key}:</span>
                              <span className="text-gray-900 font-medium">
                                {typeof value === 'number' && !isNaN(value) ? value.toFixed(4) : String(value)}
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
                
                <div className="flex items-center space-x-2 ml-4">
                  {!isRead && (
                    <button
                      onClick={() => markAsRead(alert.id)}
                      className="p-1 text-blue-600 hover:bg-blue-100 rounded-lg transition-colors duration-200"
                      title="Отметить как прочитанное"
                    >
                      <CheckCircleIcon className="w-4 h-4" />
                    </button>
                  )}
                  
                  <button
                    className="p-1 text-gray-400 hover:bg-gray-100 rounded-lg transition-colors duration-200"
                    title="Удалить уведомление"
                  >
                    <TrashIcon className="w-4 h-4" />
                  </button>
                </div>
              </div>
            </div>
          );
        })}
        
        {filteredAlerts.length === 0 && (
          <div className="text-center py-12">
            <BellIcon className="w-12 h-12 text-gray-400 mx-auto mb-4" />
            <p className="text-gray-500">Нет уведомлений для отображения</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default Alerts;