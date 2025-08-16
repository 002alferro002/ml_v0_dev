"""Утилиты для работы со стаканом ордеров и определения оптимальной глубины на основе tick size."""

import math
from typing import List, Tuple, Dict, Any, Optional
from decimal import Decimal, ROUND_HALF_UP
import logging

logger = logging.getLogger(__name__)

class OrderBookDepthCalculator:
    """Класс для расчета оптимальной глубины стакана ордеров на основе tick size."""
    
    def __init__(self):
        # Кэш для хранения информации о символах
        self.symbol_info_cache: Dict[str, Dict[str, Any]] = {}
        
        # Предустановленные уровни агрегации для разных диапазонов цен
        self.price_aggregation_levels = {
            # Для цен < 1: используем tick size как базу
            "low": [1, 2, 5, 10, 20, 50],
            # Для цен 1-100: используем кратные tick size
            "medium": [1, 2, 5, 10, 25, 50, 100],
            # Для цен > 100: используем более крупные шаги
            "high": [1, 5, 10, 25, 50, 100, 250, 500]
        }
    
    def update_symbol_info(self, symbol: str, symbol_info: Dict[str, Any]):
        """Обновляет информацию о символе, включая tick size."""
        self.symbol_info_cache[symbol] = symbol_info
        logger.debug(f"Updated symbol info for {symbol}: tick_size={symbol_info.get('tickSize', 'unknown')}")
    
    def get_tick_size(self, symbol: str) -> float:
        """Получает tick size для символа."""
        symbol_info = self.symbol_info_cache.get(symbol, {})
        tick_size = symbol_info.get('tickSize', 0.01)  # Значение по умолчанию
        return float(tick_size) if tick_size > 0 else 0.01
    
    def calculate_optimal_aggregation_levels(self, symbol: str, current_price: float) -> List[float]:
        """Рассчитывает оптимальные уровни агрегации для символа на основе tick size и текущей цены.
        
        Args:
            symbol: Торговый символ
            current_price: Текущая цена символа
            
        Returns:
            Список оптимальных уровней агрегации
        """
        tick_size = self.get_tick_size(symbol)
        
        # Определяем категорию цены
        if current_price < 1:
            multipliers = self.price_aggregation_levels["low"]
        elif current_price <= 100:
            multipliers = self.price_aggregation_levels["medium"]
        else:
            multipliers = self.price_aggregation_levels["high"]
        
        # Рассчитываем уровни агрегации
        aggregation_levels = []
        for multiplier in multipliers:
            level = tick_size * multiplier
            # Округляем до разумного количества знаков после запятой
            decimal_places = max(0, -int(math.floor(math.log10(tick_size))) + 2)
            level = round(level, decimal_places)
            if level not in aggregation_levels:  # Избегаем дубликатов
                aggregation_levels.append(level)
        
        return sorted(aggregation_levels)
    
    def round_price_to_level(self, price: float, aggregation_level: float) -> float:
        """Округляет цену до ближайшего уровня агрегации.
        
        Args:
            price: Исходная цена
            aggregation_level: Уровень агрегации
            
        Returns:
            Округленная цена
        """
        if aggregation_level <= 0:
            return price
        
        # Используем Decimal для точных вычислений
        price_decimal = Decimal(str(price))
        level_decimal = Decimal(str(aggregation_level))
        
        # Округляем до ближайшего уровня
        rounded = (price_decimal / level_decimal).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * level_decimal
        
        return float(rounded)
    
    def aggregate_orderbook_levels(self, 
                                 orders: List[Tuple[float, float]], 
                                 aggregation_level: float, 
                                 is_ask: bool = False) -> List[Tuple[float, float]]:
        """Агрегирует уровни стакана ордеров по заданному уровню агрегации.
        
        Args:
            orders: Список ордеров [(price, quantity), ...]
            aggregation_level: Уровень агрегации цен
            is_ask: True для asks, False для bids
            
        Returns:
            Агрегированный список ордеров
        """
        if not orders or aggregation_level <= 0:
            return orders
        
        # Группируем ордера по округленным ценам
        aggregated_orders: Dict[float, float] = {}
        
        for price, quantity in orders:
            rounded_price = self.round_price_to_level(price, aggregation_level)
            
            if rounded_price in aggregated_orders:
                aggregated_orders[rounded_price] += quantity
            else:
                aggregated_orders[rounded_price] = quantity
        
        # Преобразуем обратно в список и сортируем
        result = [(price, qty) for price, qty in aggregated_orders.items() if qty > 0]
        result.sort(key=lambda x: x[0], reverse=is_ask)
        
        return result
    
    def calculate_optimal_depth(self, 
                              symbol: str, 
                              current_price: float, 
                              total_orders: int) -> int:
        """Рассчитывает оптимальную глубину стакана на основе символа и количества ордеров.
        
        Args:
            symbol: Торговый символ
            current_price: Текущая цена
            total_orders: Общее количество ордеров в стакане
            
        Returns:
            Оптимальная глубина стакана
        """
        tick_size = self.get_tick_size(symbol)
        
        # Базовая глубина в зависимости от tick size и цены
        if current_price < 1:
            base_depth = 50  # Для низких цен показываем больше уровней
        elif current_price <= 100:
            base_depth = 50  # Средняя глубина
        else:
            base_depth = 15  # Для высоких цен меньше уровней
        
        # Корректируем на основе количества доступных ордеров
        if total_orders < 10:
            return min(base_depth, total_orders)
        elif total_orders < 50:
            return min(base_depth, int(total_orders * 0.8))
        else:
            return base_depth
    
    def get_price_precision(self, symbol: str) -> int:
        """Определяет количество знаков после запятой для отображения цены.
        
        Args:
            symbol: Торговый символ
            
        Returns:
            Количество знаков после запятой
        """
        tick_size = self.get_tick_size(symbol)
        
        if tick_size >= 1:
            return 0
        elif tick_size >= 0.1:
            return 1
        elif tick_size >= 0.01:
            return 2
        elif tick_size >= 0.001:
            return 3
        elif tick_size >= 0.0001:
            return 4
        else:
            return 5
    
    def validate_aggregation_level(self, symbol: str, aggregation_level: float) -> bool:
        """Проверяет, является ли уровень агрегации валидным для символа.
        
        Args:
            symbol: Торговый символ
            aggregation_level: Уровень агрегации для проверки
            
        Returns:
            True если уровень валиден, False иначе
        """
        tick_size = self.get_tick_size(symbol)
        
        # Уровень агрегации должен быть кратен tick size
        if aggregation_level < tick_size:
            return False
        
        # Проверяем, что уровень агрегации кратен tick size (с учетом погрешности)
        ratio = aggregation_level / tick_size
        return abs(ratio - round(ratio)) < 1e-10
    
    def get_spread_in_ticks(self, symbol: str, bid_price: float, ask_price: float) -> float:
        """Рассчитывает спред в тиках.
        
        Args:
            symbol: Торговый символ
            bid_price: Цена лучшего бида
            ask_price: Цена лучшего аска
            
        Returns:
            Спред в тиках
        """
        tick_size = self.get_tick_size(symbol)
        spread = ask_price - bid_price
        return spread / tick_size if tick_size > 0 else 0


# Глобальный экземпляр калькулятора
depth_calculator = OrderBookDepthCalculator()


def get_optimal_orderbook_config(symbol: str, 
                                current_price: float, 
                                total_orders: int) -> Dict[str, Any]:
    """Получает оптимальную конфигурацию для отображения стакана ордеров.
    
    Args:
        symbol: Торговый символ
        current_price: Текущая цена
        total_orders: Общее количество ордеров
        
    Returns:
        Словарь с конфигурацией стакана
    """
    config = {
        'optimal_depth': depth_calculator.calculate_optimal_depth(symbol, current_price, total_orders),
        'aggregation_levels': depth_calculator.calculate_optimal_aggregation_levels(symbol, current_price),
        'price_precision': depth_calculator.get_price_precision(symbol),
        'tick_size': depth_calculator.get_tick_size(symbol)
    }
    
    return config

