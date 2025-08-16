import React from 'react';
import { 
  ChartBarIcon, 
  CpuChipIcon, 
  ExclamationTriangleIcon, 
  EyeIcon, 
  CircleStackIcon, 
  SignalIcon,
  ClockIcon,
  AcademicCapIcon
} from '@heroicons/react/24/outline';

interface SidebarProps {
  activeItem: string;
  onItemClick: (item: string) => void;
}

interface MenuItem {
  id: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  description: string;
}

const menuItems: MenuItem[] = [
  {
    id: 'system-status',
    label: 'Статус Системы',
    icon: CpuChipIcon,
    description: 'Общий статус системы и мониторинг'
  },
  {
    id: 'pairs-watchlist',
    label: 'Список Пар',
    icon: EyeIcon,
    description: 'Отслеживаемые торговые пары'
  },
  {
    id: 'futures-symbols',
    label: 'Фьючерсы',
    icon: ChartBarIcon,
    description: 'Фьючерсные контракты'
  },
  {
    id: 'streaming-data',
    label: 'Потоковые Данные',
    icon: SignalIcon,
    description: 'Данные в реальном времени'
  },
  {
    id: 'alerts',
    label: 'Уведомления',
    icon: ExclamationTriangleIcon,
    description: 'Системные уведомления и алерты'
  },
  {
    id: 'database-view',
    label: 'База Данных',
    icon: CircleStackIcon,
    description: 'Просмотр данных базы'
  },
  {
    id: 'metrics',
    label: 'Метрики',
    icon: ChartBarIcon,
    description: 'Аналитика и статистика'
  },
  {
    id: 'ml-control',
    label: 'ML Управление',
    icon: AcademicCapIcon,
    description: 'Управление машинным обучением'
  },
  {
    id: 'training-quality',
    label: 'Качество Обучения',
    icon: ClockIcon,
    description: 'Метрики качества обучения'
  },
  {
    id: 'prediction-quality',
    label: 'Качество Предсказаний',
    icon: ChartBarIcon,
    description: 'Анализ точности предсказаний'
  }
];

const Sidebar: React.FC<SidebarProps> = ({ activeItem, onItemClick }) => {
  return (
    <div className="w-64 bg-white border-r border-gray-200 h-full flex flex-col">
      {/* Header */}
      <div className="p-6 border-b border-gray-200">
        <h1 className="text-xl font-bold text-gray-900">
          Crypto Manipulation Detector
        </h1>
        <p className="text-sm text-gray-500 mt-1">
          ML Predictions Dashboard
        </p>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto py-4">
        <div className="space-y-1 px-3">
          {menuItems.map((item) => {
            const Icon = item.icon;
            const isActive = activeItem === item.id;
            
            return (
              <div
                key={item.id}
                onClick={() => onItemClick(item.id)}
                className={`sidebar-item group relative ${
                  isActive ? 'active' : ''
                }`}
                title={item.description}
              >
                <Icon className="w-5 h-5 mr-3 flex-shrink-0" />
                <span className="text-sm font-medium truncate">
                  {item.label}
                </span>
                
                {/* Tooltip */}
                <div className="absolute left-full ml-2 px-2 py-1 bg-gray-900 text-white text-xs rounded opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-50">
                  {item.description}
                </div>
              </div>
            );
          })}
        </div>
      </nav>

      {/* Footer */}
      <div className="p-4 border-t border-gray-200">
        <div className="flex items-center space-x-3">
          <div className="w-2 h-2 bg-green-500 rounded-full pulse-indicator"></div>
          <span className="text-xs text-gray-500">Система активна</span>
        </div>
        <div className="mt-2 text-xs text-gray-400">
          Версия 1.0.0
        </div>
      </div>
    </div>
  );
};

export default Sidebar;