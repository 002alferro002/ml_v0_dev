import React, { useState, useEffect } from 'react';
import { 
  EyeIcon, 
  EyeSlashIcon,
  ChartBarIcon,
  ArrowUpIcon,
  ArrowDownIcon,
  MagnifyingGlassIcon
} from '@heroicons/react/24/outline';
import { StreamingData } from '@/types/ml';
import { useWebSocket } from '@/hooks/useWebSocket';
import apiClient from '@/api/client';

interface PairData extends StreamingData {
  isWatched: boolean;
  trend: 'up' | 'down' | 'neutral';
}

const PairsWatchlist: React.FC = () => {
  const [pairs, setPairs] = useState<PairData[]>([]);
  const [filteredPairs, setFilteredPairs] = useState<PairData[]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<'symbol' | 'price' | 'change_24h' | 'volume'>('symbol');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('asc');

  // WebSocket для обновлений в реальном времени
  const { isConnected } = useWebSocket('/ws/streaming', {
    onMessage: (data) => {
      if (data.type === 'price_update') {
        setPairs(prevPairs => 
          prevPairs.map(pair => 
            pair.symbol === data.data.symbol 
              ? { 
                  ...pair, 
                  ...data.data,
                  trend: (data.data.change_24h || 0) > 0 ? 'up' : (data.data.change_24h || 0) < 0 ? 'down' : 'neutral'
                }
              : pair
          )
        );
      }
    },
    onError: (error) => {
      console.error('WebSocket error:', error);
      setError('Ошибка подключения к потоку данных');
    }
  });

  // Загрузка данных
  useEffect(() => {
    const loadData = async () => {
      try {
        setLoading(true);
        const streamingData = await apiClient.getStreamingData();
        
        const pairsData: PairData[] = streamingData.map(item => ({
          ...item,
          isWatched: true, // По умолчанию все пары отслеживаются
          trend: (item.change_24h || 0) > 0 ? 'up' : (item.change_24h || 0) < 0 ? 'down' : 'neutral'
        }));
        
        setPairs(pairsData);
        setError(null);
      } catch (err) {
        setError('Ошибка загрузки данных');
        console.error('Failed to load pairs data:', err);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, []);

  // Фильтрация и поиск
  useEffect(() => {
    let filtered = pairs.filter(pair => 
      pair.symbol.toLowerCase().includes(searchTerm.toLowerCase())
    );

    // Сортировка
    filtered.sort((a, b) => {
      let aValue = a[sortBy];
      let bValue = b[sortBy];
      
      if (typeof aValue === 'string') {
        aValue = aValue.toLowerCase();
        bValue = (bValue as string).toLowerCase();
      }
      
      if (sortOrder === 'asc') {
        return aValue < bValue ? -1 : aValue > bValue ? 1 : 0;
      } else {
        return aValue > bValue ? -1 : aValue < bValue ? 1 : 0;
      }
    });

    setFilteredPairs(filtered);
  }, [pairs, searchTerm, sortBy, sortOrder]);

  const toggleWatchStatus = (symbol: string) => {
    setPairs(prevPairs => 
      prevPairs.map(pair => 
        pair.symbol === symbol 
          ? { ...pair, isWatched: !pair.isWatched }
          : pair
      )
    );
  };

  const handleSort = (field: typeof sortBy) => {
    if (sortBy === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(field);
      setSortOrder('asc');
    }
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

  const getTrendIcon = (trend: string) => {
    switch (trend) {
      case 'up':
        return <ArrowUpIcon className="w-4 h-4 text-green-500" />;
      case 'down':
        return <ArrowDownIcon className="w-4 h-4 text-red-500" />;
      default:
        return <div className="w-4 h-4" />;
    }
  };

  const getTrendColor = (change: number | undefined | null) => {
    if (!change || isNaN(change)) return 'text-gray-600';
    if (change > 0) return 'text-green-600';
    if (change < 0) return 'text-red-600';
    return 'text-gray-600';
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
          <span className="text-red-700">{error}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      {/* Заголовок и поиск */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold text-gray-900">Список Торговых Пар</h2>
          <p className="text-sm text-gray-500 mt-1">
            Отслеживаемые криптовалютные пары ({filteredPairs.length} из {pairs.length})
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
          
          {/* Поиск */}
          <div className="relative">
            <MagnifyingGlassIcon className="w-5 h-5 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
            <input
              type="text"
              placeholder="Поиск пар..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
            />
          </div>
        </div>
      </div>

      {/* Таблица */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Статус
                </th>
                <th 
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('symbol')}
                >
                  <div className="flex items-center space-x-1">
                    <span>Символ</span>
                    {sortBy === 'symbol' && (
                      sortOrder === 'asc' ? <ArrowUpIcon className="w-4 h-4" /> : <ArrowDownIcon className="w-4 h-4" />
                    )}
                  </div>
                </th>
                <th 
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('price')}
                >
                  <div className="flex items-center space-x-1">
                    <span>Цена</span>
                    {sortBy === 'price' && (
                      sortOrder === 'asc' ? <ArrowUpIcon className="w-4 h-4" /> : <ArrowDownIcon className="w-4 h-4" />
                    )}
                  </div>
                </th>
                <th 
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('change_24h')}
                >
                  <div className="flex items-center space-x-1">
                    <span>Изменение 24ч</span>
                    {sortBy === 'change_24h' && (
                      sortOrder === 'asc' ? <ArrowUpIcon className="w-4 h-4" /> : <ArrowDownIcon className="w-4 h-4" />
                    )}
                  </div>
                </th>
                <th 
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('volume')}
                >
                  <div className="flex items-center space-x-1">
                    <span>Объем</span>
                    {sortBy === 'volume' && (
                      sortOrder === 'asc' ? <ArrowUpIcon className="w-4 h-4" /> : <ArrowDownIcon className="w-4 h-4" />
                    )}
                  </div>
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
              {filteredPairs.map((pair) => (
                <tr key={pair.symbol} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      {pair.isWatched ? (
                        <div className="flex items-center space-x-2">
                          <div className="w-2 h-2 bg-green-500 rounded-full pulse-indicator"></div>
                          <span className="text-sm text-green-600 font-medium">Активно</span>
                        </div>
                      ) : (
                        <div className="flex items-center space-x-2">
                          <div className="w-2 h-2 bg-gray-400 rounded-full"></div>
                          <span className="text-sm text-gray-500">Пауза</span>
                        </div>
                      )}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center space-x-2">
                      {getTrendIcon(pair.trend)}
                      <span className="text-sm font-medium text-gray-900">{pair.symbol}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="text-sm text-gray-900">${formatPrice(pair.price)}</span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`text-sm font-medium ${getTrendColor(pair.change_24h || 0)}`}>
                      {(pair.change_24h || 0) > 0 ? '+' : ''}{(pair.change_24h || 0).toFixed(2)}%
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="text-sm text-gray-900">{formatVolume(pair.volume)}</span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="text-sm text-gray-500">
                      {new Date(pair.timestamp).toLocaleTimeString('ru-RU')}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center space-x-2">
                      <button
                        onClick={() => toggleWatchStatus(pair.symbol)}
                        className={`p-1 rounded-lg transition-colors duration-200 ${
                          pair.isWatched 
                            ? 'text-green-600 hover:bg-green-50' 
                            : 'text-gray-400 hover:bg-gray-50'
                        }`}
                        title={pair.isWatched ? 'Остановить отслеживание' : 'Начать отслеживание'}
                      >
                        {pair.isWatched ? (
                          <EyeIcon className="w-5 h-5" />
                        ) : (
                          <EyeSlashIcon className="w-5 h-5" />
                        )}
                      </button>
                      <button
                        className="p-1 rounded-lg text-gray-400 hover:bg-gray-50 hover:text-gray-600 transition-colors duration-200"
                        title="Просмотр графика"
                      >
                        <ChartBarIcon className="w-5 h-5" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        
        {filteredPairs.length === 0 && (
          <div className="text-center py-12">
            <p className="text-gray-500">Нет данных для отображения</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default PairsWatchlist;