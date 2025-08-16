import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Separator } from '@/components/ui/separator';
import { 
  ChartBarIcon, 
  CpuChipIcon, 
  ServerIcon, 
  ClockIcon,
  ArrowPathIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import { useWebSocket } from '@/hooks/useWebSocket';
import { apiClient } from '@/lib/client';

interface SystemMetrics {
  cpu_usage: number;
  memory_usage: number;
  disk_usage: number;
  network_io: {
    bytes_sent: number;
    bytes_recv: number;
  };
  uptime: number;
  active_connections: number;
  ml_processes: {
    training: boolean;
    prediction: boolean;
    data_collection: boolean;
  };
  database: {
    size: string;
    connections: number;
    queries_per_second: number;
  };
  api: {
    requests_per_minute: number;
    avg_response_time: number;
    error_rate: number;
  };
}

const Metrics: React.FC = () => {
  const [metrics, setMetrics] = useState<SystemMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);

  // WebSocket для обновления метрик в реальном времени
  const { isConnected } = useWebSocket('/ws/streaming', {
    onMessage: (data) => {
      if (data.type === 'system_metrics') {
        setMetrics(data.data);
        setLastUpdate(new Date());
      }
    },
    onError: (error) => {
      console.error('WebSocket error:', error);
    }
  });

  useEffect(() => {
    loadMetrics();

    // Обновление каждые 30 секунд
    const interval = setInterval(loadMetrics, 30000);

    return () => {
      clearInterval(interval);
    };
  }, []);

  const loadMetrics = async () => {
    try {
      const data = await apiClient.getMetrics();
      setMetrics(data);
      setLastUpdate(new Date());
      setError(null);
    } catch (error) {
      console.error('Ошибка загрузки метрик:', error);
      setError('Ошибка загрузки метрик системы');
    } finally {
      setLoading(false);
    }
  };

  const formatUptime = (seconds: number): string => {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return `${days}д ${hours}ч ${minutes}м`;
  };

  const formatBytes = (bytes: number): string => {
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    if (bytes === 0) return '0 B';
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
  };

  const getUsageColor = (usage: number): string => {
    if (usage < 50) return 'text-green-600';
    if (usage < 80) return 'text-yellow-600';
    return 'text-red-600';
  };

  const getUsageVariant = (usage: number): 'default' | 'secondary' | 'destructive' => {
    if (usage < 50) return 'default';
    if (usage < 80) return 'secondary';
    return 'destructive';
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <ArrowPathIcon className="h-8 w-8 animate-spin" />
        <span className="ml-2">Загрузка метрик...</span>
      </div>
    );
  }

  if (error || !metrics) {
    return (
      <div className="flex items-center justify-center h-64 text-red-600">
        <ExclamationTriangleIcon className="h-8 w-8" />
        <span className="ml-2">{error || 'Нет данных'}</span>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-2">
          <ChartBarIcon className="h-6 w-6" />
          <h1 className="text-2xl font-bold">Системные метрики</h1>
        </div>
        {lastUpdate && (
          <div className="flex items-center space-x-2 text-sm text-muted-foreground">
            <ClockIcon className="h-4 w-4" />
            <span>Обновлено: {lastUpdate.toLocaleTimeString()}</span>
          </div>
        )}
      </div>

      {/* Основные метрики системы */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Использование CPU</CardTitle>
            <CpuChipIcon className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className={`text-2xl font-bold ${getUsageColor(metrics.cpu_usage)}`}>
              {metrics.cpu_usage !== undefined ? metrics.cpu_usage.toFixed(1) : '0.0'}%
            </div>
            <Progress value={metrics.cpu_usage} className="mt-2" />
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Использование RAM</CardTitle>
            <ServerIcon className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className={`text-2xl font-bold ${getUsageColor(metrics.memory_usage)}`}>
              {metrics.memory_usage !== undefined ? metrics.memory_usage.toFixed(1) : '0.0'}%
            </div>
            <Progress value={metrics.memory_usage} className="mt-2" />
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Использование диска</CardTitle>
            <ServerIcon className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className={`text-2xl font-bold ${getUsageColor(metrics.disk_usage)}`}>
              {metrics.disk_usage !== undefined ? metrics.disk_usage.toFixed(1) : '0.0'}%
            </div>
            <Progress value={metrics.disk_usage} className="mt-2" />
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Время работы</CardTitle>
            <ClockIcon className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-lg font-bold">
              {formatUptime(metrics.uptime)}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Статус ML процессов */}
      <Card>
        <CardHeader>
          <CardTitle>Статус ML процессов</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="flex items-center justify-between">
              <span>Обучение модели</span>
              <Badge variant={metrics.ml_processes.training ? 'default' : 'secondary'}>
                {metrics.ml_processes.training ? 'Активно' : 'Остановлено'}
              </Badge>
            </div>
            <div className="flex items-center justify-between">
              <span>Предсказания</span>
              <Badge variant={metrics.ml_processes.prediction ? 'default' : 'secondary'}>
                {metrics.ml_processes.prediction ? 'Активно' : 'Остановлено'}
              </Badge>
            </div>
            <div className="flex items-center justify-between">
              <span>Сбор данных</span>
              <Badge variant={metrics.ml_processes.data_collection ? 'default' : 'secondary'}>
                {metrics.ml_processes.data_collection ? 'Активно' : 'Остановлено'}
              </Badge>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Сетевая активность и подключения */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Сетевая активность</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex justify-between">
              <span>Отправлено:</span>
              <span className="font-mono">{formatBytes(metrics.network_io.bytes_sent)}</span>
            </div>
            <div className="flex justify-between">
              <span>Получено:</span>
              <span className="font-mono">{formatBytes(metrics.network_io.bytes_recv)}</span>
            </div>
            <Separator />
            <div className="flex justify-between">
              <span>Активные подключения:</span>
              <span className="font-bold">{metrics.active_connections}</span>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>База данных</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex justify-between">
              <span>Размер БД:</span>
              <span className="font-mono">{metrics.database.size}</span>
            </div>
            <div className="flex justify-between">
              <span>Подключения:</span>
              <span className="font-bold">{metrics.database.connections}</span>
            </div>
            <div className="flex justify-between">
              <span>Запросов/сек:</span>
              <span className="font-bold">{metrics.database.queries_per_second}</span>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* API метрики */}
      <Card>
        <CardHeader>
          <CardTitle>API метрики</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="text-center">
              <div className="text-2xl font-bold">{metrics.api.requests_per_minute}</div>
              <div className="text-sm text-muted-foreground">Запросов/мин</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold">{metrics.api.avg_response_time}ms</div>
              <div className="text-sm text-muted-foreground">Среднее время ответа</div>
            </div>
            <div className="text-center">
              <div className={`text-2xl font-bold ${getUsageColor(metrics.api.error_rate * 100)}`}>
                {metrics.api.error_rate !== undefined ? (metrics.api.error_rate * 100).toFixed(2) : '0.00'}%
              </div>
              <div className="text-sm text-muted-foreground">Процент ошибок</div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default Metrics;