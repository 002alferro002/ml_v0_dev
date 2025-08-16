import React, { useState } from 'react';
import Sidebar from './components/Sidebar';
import Header from './components/Header';
import SystemStatus from './components/pages/SystemStatus';
import PairsWatchlist from './components/pages/PairsWatchlist';
import Alerts from './components/pages/Alerts';
import FuturesSymbols from './components/pages/FuturesSymbols';
import StreamingData from './components/pages/StreamingData';
import DatabaseView from './components/pages/DatabaseView';
import Metrics from './components/pages/Metrics';
import MLControl from './components/pages/MLControl';
import TrainingQuality from './components/pages/TrainingQuality';
import PredictionQuality from './components/pages/PredictionQuality';

interface PageConfig {
  title: string;
  subtitle?: string;
  component: React.ComponentType;
}

const pageConfigs: Record<string, PageConfig> = {
  'system-status': {
    title: 'Статус Системы',
    subtitle: 'Общий мониторинг состояния системы и ML процессов',
    component: SystemStatus
  },
  'pairs-watchlist': {
    title: 'Список Торговых Пар',
    subtitle: 'Отслеживаемые криптовалютные пары в реальном времени',
    component: PairsWatchlist
  },
  'futures-symbols': {
    title: 'Фьючерсные Контракты',
    subtitle: 'Мониторинг фьючерсных инструментов',
    component: FuturesSymbols
  },
  'streaming-data': {
    title: 'Потоковые Данные',
    subtitle: 'Данные рынка в реальном времени',
    component: StreamingData
  },
  'alerts': {
    title: 'Уведомления',
    subtitle: 'Системные уведомления и алерты',
    component: Alerts
  },
  'database-view': {
    title: 'База Данных',
    subtitle: 'Просмотр и анализ данных базы',
    component: DatabaseView
  },
  'metrics': {
    title: 'Метрики и Аналитика',
    subtitle: 'Статистика работы системы и ML моделей',
    component: Metrics
  },
  'ml-control': {
    title: 'Управление ML',
    subtitle: 'Контроль процессов машинного обучения',
    component: MLControl
  },
  'training-quality': {
    title: 'Качество Обучения',
    subtitle: 'Метрики и анализ качества обучения моделей',
    component: TrainingQuality
  },
  'prediction-quality': {
    title: 'Качество Предсказаний',
    subtitle: 'Анализ точности и эффективности предсказаний',
    component: PredictionQuality
  }
};

const App: React.FC = () => {
  const [activeItem, setActiveItem] = useState('system-status');
  
  const currentPage = pageConfigs[activeItem];
  const CurrentComponent = currentPage?.component || (() => (
    <div className="p-6">
      <div className="card">
        <p className="text-gray-500">Страница не найдена</p>
      </div>
    </div>
  ));

  const handleItemClick = (item: string) => {
    setActiveItem(item);
  };

  return (
    <div className="h-screen flex bg-gray-50">
      {/* Боковая панель */}
      <Sidebar 
        activeItem={activeItem} 
        onItemClick={handleItemClick} 
      />
      
      {/* Основной контент */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Заголовок */}
        <Header 
          title={currentPage?.title || 'Неизвестная страница'}
          subtitle={currentPage?.subtitle}
        />
        
        {/* Контент страницы */}
        <main className="flex-1 overflow-y-auto">
          <CurrentComponent />
        </main>
      </div>
    </div>
  );
};

export default App;