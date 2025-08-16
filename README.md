# ML Manipulation Bybit - Система Обнаружения Манипуляций

Система машинного обучения для обнаружения манипуляций на криптовалютной бирже Bybit.

## 🚀 Быстрый Запуск

### Предварительные Требования

1. **Python 3.9+**
2. **Node.js 18+**
3. **PostgreSQL 14+ с TimescaleDB**
4. **RabbitMQ**
5. **Redis** (опционально)

### Установка

1. **Клонирование репозитория**
```bash
git clone <repository-url>
cd ml-manipulation-bybit
```

2. **Настройка Backend**
```bash
cd backend
pip install -r requirements.txt
cp ../.env.example .env
# Отредактируйте .env файл с вашими настройками
```

3. **Настройка Frontend**
```bash
cd frontend
npm install
```

4. **Настройка Базы Данных**
```bash
# Создайте базу данных PostgreSQL
createdb bybit_manipulation

# Установите TimescaleDB расширение
psql -d bybit_manipulation -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
```

5. **Запуск Сервисов**

**RabbitMQ:**
```bash
# Ubuntu/Debian
sudo systemctl start rabbitmq-server

# macOS
brew services start rabbitmq

# Docker
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

**Redis (опционально):**
```bash
# Ubuntu/Debian
sudo systemctl start redis

# macOS
brew services start redis

# Docker
docker run -d --name redis -p 6379:6379 redis:alpine
```

### Запуск Приложения

1. **Запуск Backend**
```bash
cd backend
python run.py
```

2. **Запуск Frontend**
```bash
cd frontend
npm run dev
```

3. **Доступ к Приложению**
- Frontend: http://localhost:5174
- API: http://localhost:5000
- API Docs: http://localhost:5000/docs

## 📁 Структура Проекта

```
ml-manipulation-bybit/
├── backend/                    # Python FastAPI backend
│   ├── api/                   # API роутеры
│   ├── manipulation/          # Модули обнаружения манипуляций
│   │   ├── ml/               # ML компоненты
│   │   └── ...
│   ├── utils/                # Утилиты
│   ├── app.py                # Главный файл приложения
│   ├── config_manager.py     # Управление конфигурацией
│   ├── db_manager.py         # Управление базой данных
│   └── requirements.txt      # Python зависимости
├── frontend/                  # React TypeScript frontend
│   ├── src/
│   │   ├── components/       # React компоненты
│   │   ├── hooks/           # React хуки
│   │   ├── services/        # API сервисы
│   │   ├── types/           # TypeScript типы
│   │   └── ...
│   └── package.json         # Node.js зависимости
├── config/                   # Конфигурационные файлы
└── .env.example             # Пример переменных окружения
```

## 🔧 Конфигурация

### Переменные Окружения

Основные переменные окружения (см. `.env.example`):

- `DATABASE_URL` - URL подключения к PostgreSQL
- `RABBITMQ_URL` - URL подключения к RabbitMQ
- `BYBIT_WS_URI` - WebSocket URI для Bybit API
- `API_HOST`, `API_PORT` - Настройки API сервера
- `LOG_LEVEL` - Уровень логирования

### Конфигурационный Файл

Дополнительные настройки в `config/app_config.json`:
- Настройки ML моделей
- Параметры обнаружения манипуляций
- Настройки мониторинга

## 🤖 ML Компоненты

### Обнаружение Манипуляций

1. **Wash Trading** - Обнаружение фиктивных сделок
2. **Ping-Pong Trading** - Обнаружение быстрых покупок/продаж
3. **Ramping** - Обнаружение искусственного завышения цены
4. **Layering/Spoofing** - Обнаружение фиктивных ордеров
5. **Iceberg Orders** - Обнаружение скрытых крупных ордеров

### ML Модели

1. **Price Prediction** - Предсказание изменения цены
2. **Direction Prediction** - Предсказание направления движения
3. **Anomaly Detection** - Обнаружение аномалий в торговых паттернах

## 📊 Мониторинг

### Метрики

- Количество обработанных сделок
- Количество обновлений стакана ордеров
- Количество сгенерированных алертов
- Точность ML предсказаний
- Производительность системы

### Логирование

Система использует структурированное логирование с различными уровнями:
- `DEBUG` - Детальная отладочная информация
- `INFO` - Общая информация о работе
- `WARNING` - Предупреждения
- `ERROR` - Ошибки
- `CRITICAL` - Критические ошибки

## 🔍 Отладка

### Проверка Компонентов

1. **Проверка подключения к базе данных:**
```bash
cd backend
python -c "from db_manager import DBManager; import asyncio; asyncio.run(DBManager('your_db_url').connect())"
```

2. **Проверка WebSocket подключения:**
```bash
cd backend
python debug_trade_processing.py
```

3. **Проверка API:**
```bash
curl http://localhost:5000/health
```

### Логи

Логи сохраняются в:
- `backend/app.log` - Основные логи приложения
- Консоль - Вывод в реальном времени

## 🛠 Разработка

### Добавление Новых Детекторов

1. Создайте новый класс в `backend/manipulation/`
2. Наследуйте от базового класса детектора
3. Реализуйте методы обнаружения
4. Зарегистрируйте в `app.py`

### Добавление ML Признаков

1. Обновите `feature_engineering.py`
2. Добавьте новые признаки в `extract_features()`
3. Обновите нормализацию в `utils/feature_normalizer.py`

## 📈 Производительность

### Оптимизация

- Используйте пакетную обработку для больших объемов данных
- Настройте размеры буферов в зависимости от нагрузки
- Мониторьте использование памяти и CPU

### Масштабирование

- Горизонтальное масштабирование через Docker
- Использование нескольких WebSocket соединений
- Распределение нагрузки между инстансами

## 🔒 Безопасность

### Рекомендации

1. Используйте HTTPS в продакшене
2. Настройте аутентификацию для API
3. Ограничьте CORS политики
4. Регулярно обновляйте зависимости

## 📞 Поддержка

При возникновении проблем:

1. Проверьте логи в `backend/app.log`
2. Убедитесь, что все сервисы запущены
3. Проверьте переменные окружения
4. Обратитесь к разделу "Отладка"

## 📄 Лицензия

MIT License