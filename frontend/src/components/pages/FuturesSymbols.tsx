import React, { useState, useEffect } from 'react';
import { 
  CurrencyDollarIcon,
  ArrowUpIcon,
  ArrowDownIcon,
  MagnifyingGlassIcon,
  ChartBarIcon,
  ClockIcon
} from '@heroicons/react/24/outline';
import { StreamingData } from '@/types/ml';
import { useWebSocket } from '@/hooks/useWebSocket';
import apiClient from '@/api/client';

interface FuturesContract extends StreamingData {
  contractType: 'perpetual' | 'quarterly' | 'bi-quarterly';
  expiryDate?: string;
  fundingRate?: number;
  openInterest?: number;
  markPrice?: number;
  indexPrice?: number;
}

type SortField = 'symbol' | 'price' | 'change_24h' | 'volume' | 'fundingRate' | 'openInterest';
type SortOrder = 'asc' | 'desc';

const FuturesSymbols: React.FC = () => {
  const [contracts, setContracts] = useState<FuturesContract[]>([]);
  const [filteredContracts, setFilteredContracts] = useState<FuturesContract[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState<SortField>('volume');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');
  const [contractTypeFilter, setContractTypeFilter] = useState<'all' | 'perpetual' | 'quarterly' | 'bi-quarterly'>('all');
  
  const { isConnected, data: wsData } = useWebSocket('/ws/futures');

  // Обновление данных через WebSocket
  useEffect(() => {
    if (wsData && wsData.type === 'futures_update') {
      setContracts(prevContracts => 
        prevContracts.map(contract => 
          contract.symbol === wsData.data.symbol 
            ? { ...contract, ...wsData.data }
            : contract
        )
      );
    }
  }, [wsData]);

  // Загрузка начальных данных
  useEffect(() => {
    const loadData = async () => {
      try {
        setLoading(true);
        const streamingData = await apiClient.getStreamingData();
        
        // Преобразуем данные в фьючерсные контракты
        const futuresData: FuturesContract[] = streamingData
          .filter(item => item.symbol.includes('USDT')) // Фильтруем только USDT пары
          .map(item => ({
            ...item,
            contractType: item.symbol.endsWith('PERP') ? 'perpetual' : 'quarterly',
            fundingRate: Math.random() * 0.01 - 0.005, // Мок данные для демонстрации
            openInterest: Math.random() * 1000000000,
            markPrice: item.price * (1 + (Math.random() * 0.002 - 0.001)),
            indexPrice: item.price * (1 + (Math.random() * 0.001 - 0.0005))
          }));
        
        setContracts(futuresData);
        setError(null);
      } catch (err) {
        setError('Ошибка загрузки данных фьючерсных контрактов');
        console.error('Failed to load futures data:', err);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, []);

  // Фильтрация и сортировка
  useEffect(() => {
    let filtered = contracts.filter(contract => {
      const matchesSearch = contract.symbol.toLowerCase().includes(searchTerm.toLowerCase());
      const matchesType = contractTypeFilter === 'all' || contract.contractType === contractTypeFilter;
      return matchesSearch && matchesType;
    });

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

    setFilteredContracts(filtered);
  }, [contracts, searchTerm, sortBy, sortOrder, contractTypeFilter]);

  const handleSort = (field: SortField) => {
    if (sortBy === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(field);
      setSortOrder('desc');
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

  const formatFundingRate = (rate: number): string => {
    return `${(rate * 100).toFixed(4)}%`;
  };

  const getTrendColor = (change: number | undefined | null) => {
    if (!change || isNaN(change)) return 'text-gray-600';
    if (change > 0) return 'text-green-600';
    if (change < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const getContractTypeColor = (type: string) => {
    switch (type) {
      case 'perpetual': return 'bg-blue-100 text-blue-800';
      case 'quarterly': return 'bg-green-100 text-green-800';
      case 'bi-quarterly': return 'bg-purple-100 text-purple-800';
      default: return 'bg-gray-100 text-gray-800';
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
          <h2 className="text-xl font-semibold text-gray-900">Фьючерсные Контракты</h2>
          <p className="text-sm text-gray-500 mt-1">
            Мониторинг фьючерсных инструментов ({filteredContracts.length} из {contracts.length})
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
        </div>
      </div>

      {/* Фильтры */}
      <div className="card mb-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            {/* Поиск */}
            <div className="relative">
              <MagnifyingGlassIcon className="w-5 h-5 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Поиск контрактов..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              />
            </div>
            
            {/* Фильтр по типу контракта */}
            <select
              value={contractTypeFilter}
              onChange={(e) => setContractTypeFilter(e.target.value as any)}
              className="border border-gray-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
            >
              <option value="all">Все типы</option>
              <option value="perpetual">Бессрочные</option>
              <option value="quarterly">Квартальные</option>
              <option value="bi-quarterly">Полугодовые</option>
            </select>
          </div>
        </div>
      </div>

      {/* Таблица */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th 
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('symbol')}
                >
                  <div className="flex items-center space-x-1">
                    <span>Контракт</span>
                    {sortBy === 'symbol' && (
                      sortOrder === 'asc' ? <ArrowUpIcon className="w-4 h-4" /> : <ArrowDownIcon className="w-4 h-4" />
                    )}
                  </div>
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Тип
                </th>
                <th 
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('price')}
                >
                  <div className="flex items-center space-x-1">
                    <span>Марк Цена</span>
                    {sortBy === 'price' && (
                      sortOrder === 'asc' ? <ArrowUpIcon className="w-4 h-4" /> : <ArrowDownIcon className="w-4 h-4" />
                    )}
                  </div>
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Индекс Цена
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
                <th 
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('fundingRate')}
                >
                  <div className="flex items-center space-x-1">
                    <span>Ставка Финансирования</span>
                    {sortBy === 'fundingRate' && (
                      sortOrder === 'asc' ? <ArrowUpIcon className="w-4 h-4" /> : <ArrowDownIcon className="w-4 h-4" />
                    )}
                  </div>
                </th>
                <th 
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort('openInterest')}
                >
                  <div className="flex items-center space-x-1">
                    <span>Открытый Интерес</span>
                    {sortBy === 'openInterest' && (
                      sortOrder === 'asc' ? <ArrowUpIcon className="w-4 h-4" /> : <ArrowDownIcon className="w-4 h-4" />
                    )}
                  </div>
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Действия
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredContracts.map((contract) => (
                <tr key={contract.symbol} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <CurrencyDollarIcon className="w-5 h-5 text-gray-400 mr-2" />
                      <div>
                        <div className="text-sm font-medium text-gray-900">{contract.symbol}</div>
                        <div className="text-sm text-gray-500">
                          {new Date(contract.timestamp).toLocaleTimeString('ru-RU')}
                        </div>
                      </div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
                      getContractTypeColor(contract.contractType)
                    }`}>
                      {contract.contractType === 'perpetual' ? 'Бессрочный' : 
                       contract.contractType === 'quarterly' ? 'Квартальный' : 'Полугодовой'}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    ${formatPrice(contract.markPrice || contract.price)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    ${formatPrice(contract.indexPrice || contract.price)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className={`text-sm font-medium ${getTrendColor(contract.change_24h || 0)}`}>
                      {(contract.change_24h || 0) > 0 ? '+' : ''}{(contract.change_24h || 0).toFixed(2)}%
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {formatVolume(contract.volume)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className={`text-sm font-medium ${
                      (contract.fundingRate || 0) > 0 ? 'text-red-600' : 'text-green-600'
                    }`}>
                      {formatFundingRate(contract.fundingRate || 0)}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {formatVolume(contract.openInterest || 0)}
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
        
        {filteredContracts.length === 0 && (
          <div className="text-center py-12">
            <p className="text-gray-500">Нет данных для отображения</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default FuturesSymbols;