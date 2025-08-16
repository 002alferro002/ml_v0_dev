import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Separator } from '@/components/ui/separator';
import { Play, Square, Settings, Activity, TrendingUp } from 'lucide-react';
import { useWebSocket } from '@/hooks/useWebSocket';
import apiClient from '@/api/client';

interface MLStatus {
  training_active: boolean;
  prediction_active: boolean;
  model_accuracy?: number;
  last_training?: string;
  current_symbol?: string;
}

interface ModelConfig {
  symbol: string;
  timeframe: string;
  lookback_period: number;
  prediction_horizon: number;
}

const MLControl: React.FC = () => {
  const [status, setStatus] = useState<MLStatus>({
    training_active: false,
    prediction_active: false
  });
  const [config, setConfig] = useState<ModelConfig>({
    symbol: 'BTCUSDT',
    timeframe: '1h',
    lookback_period: 100,
    prediction_horizon: 24
  });
  const [loading, setLoading] = useState(false);

  // WebSocket для обновления статуса в реальном времени
  const { isConnected } = useWebSocket('/ws/streaming', {
    onMessage: (data) => {
      if (data.type === 'ml_status') {
        setStatus(data.data);
      }
    },
    onError: (error) => {
      console.error('WebSocket error:', error);
    }
  });

  useEffect(() => {
    loadStatus();
  }, []);

  const loadStatus = async () => {
    try {
      const data = await apiClient.getMLStatus();
      setStatus(data);
    } catch (error) {
      console.error('Ошибка загрузки статуса ML:', error);
    }
  };

  const handleStartTraining = async () => {
    setLoading(true);
    try {
      await apiClient.startTraining(config.symbol, config.timeframe);
      await loadStatus();
    } catch (error) {
      console.error('Ошибка запуска обучения:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleStopTraining = async () => {
    setLoading(true);
    try {
      await apiClient.stopTraining();
      await loadStatus();
    } catch (error) {
      console.error('Ошибка остановки обучения:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleStartPrediction = async () => {
    setLoading(true);
    try {
      await apiClient.startPrediction(config.symbol);
      await loadStatus();
    } catch (error) {
      console.error('Ошибка запуска предсказаний:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleStopPrediction = async () => {
    setLoading(true);
    try {
      await apiClient.stopPrediction();
      await loadStatus();
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center space-x-2">
        <Settings className="h-6 w-6" />
        <h1 className="text-2xl font-bold">Управление ML</h1>
      </div>

      {/* Статус системы */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Статус обучения</CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <Badge variant={status.training_active ? 'default' : 'secondary'}>
              {status.training_active ? 'Активно' : 'Остановлено'}
            </Badge>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Статус предсказаний</CardTitle>
            <TrendingUp className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <Badge variant={status.prediction_active ? 'default' : 'secondary'}>
              {status.prediction_active ? 'Активно' : 'Остановлено'}
            </Badge>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Точность модели</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {status.model_accuracy && !isNaN(status.model_accuracy) ? `${(status.model_accuracy * 100).toFixed(1)}%` : 'N/A'}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Последнее обучение</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-sm text-muted-foreground">
              {status.last_training ? new Date(status.last_training).toLocaleString() : 'Никогда'}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Управление процессами */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Управление обучением</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex space-x-2">
              <Button
                onClick={handleStartTraining}
                disabled={loading || status.training_active}
                className="flex-1"
              >
                <Play className="h-4 w-4 mr-2" />
                Запустить обучение
              </Button>
              <Button
                onClick={handleStopTraining}
                disabled={loading || !status.training_active}
                variant="destructive"
                className="flex-1"
              >
                <Square className="h-4 w-4 mr-2" />
                Остановить
              </Button>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Управление предсказаниями</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex space-x-2">
              <Button
                onClick={handleStartPrediction}
                disabled={loading || status.prediction_active}
                className="flex-1"
              >
                <Play className="h-4 w-4 mr-2" />
                Запустить предсказания
              </Button>
              <Button
                onClick={handleStopPrediction}
                disabled={loading || !status.prediction_active}
                variant="destructive"
                className="flex-1"
              >
                <Square className="h-4 w-4 mr-2" />
                Остановить
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      <Separator />

      {/* Конфигурация модели */}
      <Card>
        <CardHeader>
          <CardTitle>Конфигурация модели</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="space-y-2">
              <Label htmlFor="symbol">Символ</Label>
              <Input
                id="symbol"
                value={config.symbol}
                onChange={(e) => setConfig({ ...config, symbol: e.target.value })}
                placeholder="BTCUSDT"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="timeframe">Таймфрейм</Label>
              <Input
                id="timeframe"
                value={config.timeframe}
                onChange={(e) => setConfig({ ...config, timeframe: e.target.value })}
                placeholder="1h"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="lookback">Период анализа</Label>
              <Input
                id="lookback"
                type="number"
                value={config.lookback_period}
                onChange={(e) => setConfig({ ...config, lookback_period: parseInt(e.target.value) })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="horizon">Горизонт предсказания</Label>
              <Input
                id="horizon"
                type="number"
                value={config.prediction_horizon}
                onChange={(e) => setConfig({ ...config, prediction_horizon: parseInt(e.target.value) })}
              />
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default MLControl;