import React, { useState, useEffect } from 'react';
import { 
  CircleStackIcon,
  MagnifyingGlassIcon,
  ArrowPathIcon,
  DocumentTextIcon,
  ChartBarIcon,
  ClockIcon
} from '@heroicons/react/24/outline';
import { DatabaseStats } from '@/types/ml';
import apiClient from '@/api/client';

interface TableInfo {
  name: string;
  count: number;
  size: string;
  lastUpdate?: string;
  columns?: string[];
}

const DatabaseView: React.FC = () => {
  const [dbStats, setDbStats] = useState<DatabaseStats | null>(null);
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const loadData = async () => {
    try {
      setLoading(true);
      const stats = await apiClient.getDatabaseStats();
      setDbStats(stats);
      setTables(stats.tables);
      setError(null);
    } catch (err) {
      setError('Ошибка загрузки статистики базы данных');
      console.error('Failed to load database stats:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadData();
  }, []);

  const handleRefresh = async () => {
    setRefreshing(true);
    await loadData();
    setRefreshing(false);
  };

  const filteredTables = tables.filter(table => 
    table.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const formatSize = (size: string): string => {
    return size;
  };

  const formatNumber = (num: number): string => {
    return num.toLocaleString('ru-RU');
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
      {/* Заголовок */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold text-gray-900">База Данных</h2>
          <p className="text-sm text-gray-500 mt-1">
            Просмотр и анализ структуры базы данных
          </p>
        </div>
        
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="flex items-center space-x-2 px-4 py-2 bg-primary-600 text-white rounded-lg hover:bg-primary-700 disabled:opacity-50 transition-colors"
        >
          <ArrowPathIcon className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
          <span>Обновить</span>
        </button>
      </div>

      {/* Общая статистика */}
      {dbStats && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
          <div className="metric-card">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-600">Всего Записей</p>
                <p className="text-2xl font-bold text-gray-900">
                  {formatNumber(dbStats.total_records)}
                </p>
              </div>
              <div className="p-3 bg-blue-50 rounded-lg">
                <DocumentTextIcon className="w-6 h-6 text-blue-600" />
              </div>
            </div>
          </div>

          <div className="metric-card">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-600">Количество Таблиц</p>
                <p className="text-2xl font-bold text-gray-900">
                  {tables.length}
                </p>
              </div>
              <div className="p-3 bg-green-50 rounded-lg">
                <CircleStackIcon className="w-6 h-6 text-green-600" />
              </div>
            </div>
          </div>

          <div className="metric-card">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-600">Последнее Обновление</p>
                <p className="text-lg font-semibold text-gray-900">
                  {new Date(dbStats.last_update).toLocaleString('ru-RU')}
                </p>
              </div>
              <div className="p-3 bg-purple-50 rounded-lg">
                <ClockIcon className="w-6 h-6 text-purple-600" />
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Поиск */}
      <div className="card mb-6">
        <div className="relative">
          <MagnifyingGlassIcon className="w-5 h-5 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Поиск таблиц..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
          />
        </div>
      </div>

      {/* Таблицы */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Название Таблицы
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Количество Записей
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Размер
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Процент от Общего
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Действия
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredTables.map((table) => {
                const percentage = dbStats ? ((table.count / dbStats.total_records) * 100).toFixed(1) : '0';
                
                return (
                  <tr 
                    key={table.name} 
                    className={`hover:bg-gray-50 cursor-pointer ${
                      selectedTable === table.name ? 'bg-blue-50' : ''
                    }`}
                    onClick={() => setSelectedTable(selectedTable === table.name ? null : table.name)}
                  >
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center">
                        <CircleStackIcon className="w-5 h-5 text-gray-400 mr-3" />
                        <div>
                          <div className="text-sm font-medium text-gray-900">{table.name}</div>
                          {selectedTable === table.name && (
                            <div className="text-xs text-gray-500 mt-1">
                              Нажмите для скрытия деталей
                            </div>
                          )}
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm text-gray-900">{formatNumber(table.count)}</div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm text-gray-900">{formatSize(table.size)}</div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center">
                        <div className="text-sm text-gray-900 mr-2">{percentage}%</div>
                        <div className="w-16 bg-gray-200 rounded-full h-2">
                          <div 
                            className="bg-blue-600 h-2 rounded-full" 
                            style={{ width: `${Math.min(parseFloat(percentage), 100)}%` }}
                          ></div>
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center space-x-2">
                        <button
                          className="p-1 rounded-lg text-gray-400 hover:bg-gray-50 hover:text-gray-600 transition-colors duration-200"
                          title="Просмотр данных"
                        >
                          <DocumentTextIcon className="w-5 h-5" />
                        </button>
                        <button
                          className="p-1 rounded-lg text-gray-400 hover:bg-gray-50 hover:text-gray-600 transition-colors duration-200"
                          title="Статистика"
                        >
                          <ChartBarIcon className="w-5 h-5" />
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        
        {filteredTables.length === 0 && (
          <div className="text-center py-12">
            <CircleStackIcon className="w-12 h-12 text-gray-400 mx-auto mb-4" />
            <p className="text-gray-500">Таблицы не найдены</p>
            <p className="text-sm text-gray-400 mt-1">
              {searchTerm ? 'Попробуйте изменить поисковый запрос' : 'База данных пуста'}
            </p>
          </div>
        )}
      </div>

      {/* Детали выбранной таблицы */}
      {selectedTable && (
        <div className="card mt-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">
            Детали таблицы: {selectedTable}
          </h3>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h4 className="text-sm font-medium text-gray-700 mb-2">Основная информация</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-600">Записей:</span>
                  <span className="font-medium">
                    {formatNumber(tables.find(t => t.name === selectedTable)?.count || 0)}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Размер:</span>
                  <span className="font-medium">
                    {tables.find(t => t.name === selectedTable)?.size || 'Неизвестно'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Тип:</span>
                  <span className="font-medium">
                    {selectedTable.includes('prediction') ? 'Предсказания' :
                     selectedTable.includes('training') ? 'Обучение' :
                     selectedTable.includes('market') ? 'Рыночные данные' : 'Системная'}
                  </span>
                </div>
              </div>
            </div>
            
            <div>
              <h4 className="text-sm font-medium text-gray-700 mb-2">Операции</h4>
              <div className="space-y-2">
                <button className="w-full text-left px-3 py-2 text-sm bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors">
                  Просмотреть последние записи
                </button>
                <button className="w-full text-left px-3 py-2 text-sm bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors">
                  Экспортировать данные
                </button>
                <button className="w-full text-left px-3 py-2 text-sm bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors">
                  Анализ структуры
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default DatabaseView;