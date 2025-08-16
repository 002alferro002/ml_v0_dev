import { useEffect, useRef, useState, useCallback } from 'react';
import { configService } from '../services/configService';
import { CONFIG } from '@/constants/config';

interface UseWebSocketOptions {
  onMessage?: (data: any) => void;
  onError?: (error: Event) => void;
  onOpen?: () => void;
  onClose?: () => void;
  reconnectInterval?: number;
  maxReconnectAttempts?: number;
  endpoint?: string; // Новый параметр для endpoint
}

export const useWebSocket = (urlOrEndpoint: string, options: UseWebSocketOptions = {}) => {
  const {
    onMessage,
    onError,
    onOpen,
    onClose,
    reconnectInterval,
    maxReconnectAttempts,
    endpoint
  } = options;
  
  const [actualUrl, setActualUrl] = useState<string>('');
  const [configLoaded, setConfigLoaded] = useState(false);
  const [reconnectIntervalValue, setReconnectIntervalValue] = useState(reconnectInterval || 3000);
  const [maxReconnectAttemptsValue, setMaxReconnectAttemptsValue] = useState(maxReconnectAttempts || 5);

  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reconnectAttempts, setReconnectAttempts] = useState(0);
  
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);
  const shouldReconnectRef = useRef(true);

  // Загрузка конфигурации и определение URL
  useEffect(() => {
    const loadConfig = async () => {
      try {
        // Если передан полный URL, используем его
        if (urlOrEndpoint.startsWith('ws://') || urlOrEndpoint.startsWith('wss://')) {
          setActualUrl(urlOrEndpoint);
          setConfigLoaded(true);
          return;
        }
        
        // Иначе получаем URL из конфигурации
        const config = await configService.getConfig();
        
        // Обновляем параметры переподключения из конфигурации
        if (!reconnectInterval) {
          setReconnectIntervalValue(config.websocket.reconnect_interval * 1000);
        }
        if (!maxReconnectAttempts) {
          setMaxReconnectAttemptsValue(config.websocket.max_reconnect_attempts);
        }
        
        // Определяем endpoint
        const wsEndpoint = endpoint || urlOrEndpoint;
        const wsUrl = await configService.getWebSocketUrl(wsEndpoint);
        
        setActualUrl(wsUrl);
        setConfigLoaded(true);
      } catch (error) {
        console.error('Failed to load WebSocket configuration:', error);
        // Fallback к переданному URL или создаем URL по умолчанию
        const fallbackUrl = urlOrEndpoint.startsWith('ws') 
          ? urlOrEndpoint 
          : `${CONFIG.WEBSOCKET_BASE_URL}${urlOrEndpoint.startsWith('/') ? urlOrEndpoint : '/' + urlOrEndpoint}`;
        setActualUrl(fallbackUrl);
        setConfigLoaded(true);
      }
    };
    
    loadConfig();
  }, [urlOrEndpoint, endpoint, reconnectInterval, maxReconnectAttempts]);

  const connect = useCallback(() => {
    try {
      if (!actualUrl || !configLoaded) {
        return;
      }
      
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        return;
      }

      const ws = new WebSocket(actualUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        setIsConnected(true);
        setError(null);
        setReconnectAttempts(0);
        onOpen?.();
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          onMessage?.(data);
        } catch (err) {
          console.error('Failed to parse WebSocket message:', err);
        }
      };

      ws.onerror = (event) => {
        setError('WebSocket connection error');
        onError?.(event);
      };

      ws.onclose = () => {
        setIsConnected(false);
        onClose?.();
        
        if (shouldReconnectRef.current && reconnectAttempts < maxReconnectAttemptsValue) {
          reconnectTimeoutRef.current = setTimeout(() => {
            setReconnectAttempts(prev => prev + 1);
            connect();
          }, reconnectIntervalValue);
        }
      };
    } catch (err) {
      setError('Failed to create WebSocket connection');
      console.error('WebSocket connection error:', err);
    }
  }, [actualUrl, reconnectIntervalValue, maxReconnectAttemptsValue, reconnectAttempts, configLoaded]);

  const disconnect = useCallback(() => {
    shouldReconnectRef.current = false;
    
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
    
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    
    setIsConnected(false);
  }, []);

  const sendMessage = useCallback((message: any) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(message));
      return true;
    }
    return false;
  }, []);

  useEffect(() => {
    if (configLoaded && actualUrl) {
      shouldReconnectRef.current = true;
      connect();
    }

    return () => {
      disconnect();
    };
  }, [actualUrl, configLoaded, connect]);

  return {
    isConnected,
    error,
    reconnectAttempts,
    sendMessage,
    disconnect,
    reconnect: connect
  };
};