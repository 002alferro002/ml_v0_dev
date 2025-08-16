import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './index.css';

// Проверяем наличие элемента root
const rootElement = document.getElementById('root');
if (!rootElement) {
  throw new Error('Root element not found');
}

// Создаем корневой элемент React
const root = ReactDOM.createRoot(rootElement);

// Рендерим приложение
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);