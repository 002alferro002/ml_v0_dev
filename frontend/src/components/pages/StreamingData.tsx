import React, { useState, useEffect } from 'react';
import { 
  SignalIcon,
  ArrowPathIcon,
  PauseIcon,
  PlayIcon,
  ChartBarIcon,
  ClockIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import { StreamingData as StreamingDataType } from '@/types/ml';
import { useWebSocket } from '@/hooks/useWebSocket';
import apiClient from '@/api/client';

interface StreamingDataPoint extends StreamingDataType {
  id: string;
  status: 'active' | 'paused' | 'error';
  lastUpdate: Date;
  updateCount: number;
  priceHistory: number[];
}

const StreamingData: React.FC = () => {
  const [dataPoints, setDataPoints] = useState<StreamingDataPoint[]>([]);
  const [filteredData, setFilteredData] = useState<StreamingDataPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | 'active' | 'paused' | 'error'>('all');
  const [isStreamingPaused, setIsStreamingPaused] = useState(false);
  const [totalUpdates, setTotalUpdates] = useState(0);
  const [updateRate, setUpdateRate] = useState(0);
  
  const { isConnected, data: wsData } = useWebSocket('/ws/streaming');

  // Подсчет скорости обновлений
  useEffect(() => {
    const interval = setInterval(() => {
      setUpdateRate(prev => {
        const newRate = totalUpdates;
        setTotalUpdates(0);
        return newRate;
      });
    }, 1000);

    return () => clearInterval(interval);
  }, [totalUpdates]);

  // Обновление данных через WebSocket
  useEffect(() => {
    if (wsData && wsData.type === 'streaming_update' && !isStreamingPaused) {
      const updateData = wsData.data;
      
      setDataPoints(prevData => {
        const existingIndex = prevData.findIndex(item => item.symbol === updateData.symbol);
        
        if (existingIndex >= 0) {
          const updated = [...prevData];
          const existing = updated[existingIndex];
          
          updated[existingIndex] = {
            ...existing,
            ...updateData,
            lastUpdate: new Date(),
            updateCount: existing.updateCount + 1,
            priceHistory: [...existing.priceHistory.slice(-19), updateData.price], // Последние 20 значений
            status: 'active'
          };
          
          return updated;
        } else {
          const newDataPoint: StreamingDataPoint = {
            ...updateData,
            id: `${updateData.symbol}_${Date.now()}`,
            status: 'active',
            lastUpdate: new Date(),
            updateCount: 1,
            priceHistory: [updateData.price]
          };
          
          return [...prevData, newDataPoint];
        }
      });
      
      setTotalUpdates(prev => prev + 1);
    }
  }, [wsData, isStreamingPaused]);

  // Загрузка начальных данных
  useEffect(() => {
    const loadData = async () => {
      try {
        setLoading(true);
        const streamingData = await apiClient.getStreamingData();
        
        const initialData: StreamingDataPoint[] = streamingData.map(item => ({
          ...item,
          id: `${item.symbol}_${Date.now()}`,
          status: 'active' as const,
          lastUpdate: new Date(item.timestamp),
          updateCount: 1,
          priceHistory: [item.price]
        }));
        
        setDataPoints(initialData);
        setError(null);
      } catch (err) {
        setError('Ошибка загрузки потоковых данных');
        console.error('Failed to load streaming data:', err);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, []);

  // Фильтрация данных
  useEffect(() => {
    let filtered = dataPoints.filter(item => {
      const matchesSearch = item.symbol.toLowerCase().includes(searchTerm.toLowerCase());
      const matchesStatus = statusFilter === 'all' || item.status === statusFilter;
      return matchesSearch && matchesStatus;
    });

    // Сортировка по последнему обновлению
    filtered.sort((a, b) => b.lastUpdate.getTime() - a.lastUpdate.getTime());

    setFilteredData(filtered);
  }, [dataPoints, searchTerm, statusFilter]);

  // Проверка устаревших данных
  useEffect(() => {
    const interval = setInterval(() => {
      const now = new Date();
      setDataPoints(prevData => 
        prevData.map(item => {
          const timeDiff = now.getTime() - item.lastUpdate.getTime();
          const newStatus = timeDiff > 30000 ? 'error' : timeDiff > 10000 ? 'paused' : item.status;
          
          return { ...item, status: newStatus };
        })
      );
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  const toggleStreaming = () => {
    setIsStreamingPaused(!isStreamingPaused);
  };

  const clearData = () => {
    setDataPoints([]);
    setTotalUpdates(0);
  };

  const formatPrice = (price: number | undefined | null): string => {
    if (price === undefined || price === null || isNaN(price)) {
      return '0.00';
    }
    return price.toLocaleString('ru-RU', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 8
    });
  };

  const formatVolume = (volume: number | undefined | null): string => {
    if (volume === undefined || volume === null || isNaN(volume)) {
      return '0.00';
    }
    if (volume >= 1e9) {
      return `${(volume / 1e9).toFixed(2)}B`;
    } else if (volume >= 1e6) {
      return `${(volume / 1e6).toFixed(2)}M`;
    } else if (volume >= 1e3) {
      return `${(volume / 1e3).toFixed(2)}K`;
    }
    return volume.toFixed(2);
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'active': return 'text-green-600 bg-green-100';
      case 'paused': return 'text-yellow-600 bg-yellow-100';
      case 'error': return 'text-red-600 bg-red-100';
      default: return 'text-gray-600 bg-gray-100';
    }
  };

  const getStatusText = (status: string) => {
    switch (status) {
      case 'active': return 'Активно';
      case 'paused': return 'Пауза';
      case 'error': return 'Ошибка';
      default: return 'Неизвестно';
    }
  };

  const getTrendColor = (change: number | undefined | null) => {
    if (!change || isNaN(change)) return 'text-gray-600';
    if (change > 0) return 'text-green-600';
    if (change < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const renderMiniChart = (priceHistory: number[]) => {
    if (priceHistory.length < 2) return null;
    
    const min = Math.min(...priceHistory);
    const max = Math.max(...priceHistory);
    const range = max - min;
    
    if (range === 0) return <div className="w-16 h-8 bg-gray-100 rounded"></div>;
    
    const points = priceHistory.map((price, index) => {
      const x = (index / (priceHistory.length - 1)) * 60;
      const y = 30 - ((price - min) / range) * 28;
      return `${x},${y}`;
    }).join(' ');
    
    const isUpTrend = priceHistory[priceHistory.length - 1] > priceHistory[0];
    
    return (
      <svg width="64" height="32" className="inline-block">
        <polyline
          points={points}
          fill="none"
          stroke={isUpTrend ? '#10b981' : '#ef4444'}
          strokeWidth="1.5"
        />
      </svg>
    );
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
    <div className="p-6">
      {/* Заголовок и статистика */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold text-gray-900">Потоковые Данные</h2>
          <p className="text-sm text-gray-500 mt-1">
            Данные рынка в реальном времени ({filteredData.length} активных потоков)
          </p>
        </div>
        
        <div className="flex items-center space-x-4">
          {/* Статистика обновлений */}
          <div className="text-sm text-gray-600">
            <span className="font-medium">{updateRate}</span> обновлений/сек
          </div>
          
          {/* Статус подключения */}
          <div className={`flex items-center space-x-2 px-3 py-1 rounded-full text-sm ${
            isConnected ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
          }`}>
            <div className={`w-2 h-2 rounded-full ${
              isConnected ? 'bg-green-500 pulse-indicator' : 'bg-red-500'
            }`}></div>
            <span>{isConnected ? 'Подключено' : 'Отключено'}</span>
          </div>
          
          {/* Управление потоком */}
          <button
            onClick={toggleStreaming}
            className={`flex items-center space-x-2 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
              isStreamingPaused 
                ? 'bg-green-100 text-green-700 hover:bg-green-200' 
                : 'bg-yellow-100 text-yellow-700 hover:bg-yellow-200'
            }`}
          >
            {isStreamingPaused ? <PlayIcon className="w-4 h-4" /> : <PauseIcon className="w-4 h-4" />}
            <span>{isStreamingPaused ? 'Возобновить' : 'Приостановить'}</span>
          </button>
        </div>
      </div>

      {/* Фильтры и управление */}
      <div className="card mb-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            {/* Поиск */}
            <div className="relative">
              <SignalIcon className="w-5 h-5 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Поиск символов..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              />
            </div>
            
            {/* Фильтр по статусу */}
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value as any)}
              className="border border-gray-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
            >
              <option value="all">Все статусы</option>
              <option value="active">Активные</option>
              <option value="paused">На паузе</option>
              <option value="error">Ошибки</option>
            </select>
          </div>
          
          <button
            onClick={clearData}
            className="flex items-center space-x-2 px-3 py-2 text-sm text-red-600 hover:bg-red-50 rounded-lg transition-colors"
          >
            <ArrowPathIcon className="w-4 h-4" />
            <span>Очистить</span>
          </button>
        </div>
      </div>

      {/* Таблица данных */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Статус
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Символ
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Цена
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Изменение 24ч
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Объем
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Мини График
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Обновления
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Последнее Обновление
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Действия
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredData.map((item) => (
                <tr key={item.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
                      getStatusColor(item.status)
                    }`}>
                      {getStatusText(item.status)}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm font-medium text-gray-900">{item.symbol}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm font-medium text-gray-900">
                      ${formatPrice(item.price)}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className={`text-sm font-medium ${getTrendColor(item.change_24h || 0)}`}>
                      {(item.change_24h || 0) > 0 ? '+' : ''}{(item.change_24h || 0).toFixed(2)}%
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {formatVolume(item.volume)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    {renderMiniChart(item.priceHistory)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm text-gray-900">
                      <span className="font-medium">{item.updateCount}</span>
                      <div className="text-xs text-gray-500">обновлений</div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm text-gray-900">
                      {item.lastUpdate.toLocaleTimeString('ru-RU')}
                    </div>
                    <div className="text-xs text-gray-500">
                      {Math.floor((Date.now() - item.lastUpdate.getTime()) / 1000)}с назад
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center space-x-2">
                      <button
                        className="p-1 rounded-lg text-gray-400 hover:bg-gray-50 hover:text-gray-600 transition-colors duration-200"
                        title="Просмотр графика"
                      >
                        <ChartBarIcon className="w-5 h-5" />
                      </button>
                      <button
                        className="p-1 rounded-lg text-gray-400 hover:bg-gray-50 hover:text-gray-600 transition-colors duration-200"
                        title="История"
                      >
                        <ClockIcon className="w-5 h-5" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        
        {filteredData.length === 0 && (
          <div className="text-center py-12">
            <SignalIcon className="w-12 h-12 text-gray-400 mx-auto mb-4" />
            <p className="text-gray-500">Нет активных потоков данных</p>
            <p className="text-sm text-gray-400 mt-1">
              {isStreamingPaused ? 'Потоковая передача приостановлена' : 'Ожидание данных...'}
            </p>
          </div>
        )}
      </div>
    </div>
  );
};

export default StreamingData;