import re
from typing import List, Set
from constants import EXCLUDED_SYMBOLS, EXCLUDED_SYMBOL_PATTERNS, ALLOWED_CONTRACT_TYPES, ALLOWED_SYMBOL_STATUSES
import logging

logger = logging.getLogger(__name__)

def is_symbol_allowed(symbol: str, contract_type: str = None, status: str = None) -> bool:
    """
    Проверяет, разрешен ли символ для WebSocket подписки.
    
    Args:
        symbol: Символ для проверки
        contract_type: Тип контракта (опционально)
        status: Статус символа (опционально)
        
    Returns:
        True если символ разрешен, False если нужно исключить
    """
    # Проверяем прямое исключение
    if symbol in EXCLUDED_SYMBOLS:
        logger.debug(f"Symbol {symbol} excluded by direct match")
        return False
    
    # Проверяем паттерны исключения
    for pattern in EXCLUDED_SYMBOL_PATTERNS:
        if re.match(pattern, symbol):
            logger.debug(f"Symbol {symbol} excluded by pattern {pattern}")
            return False
    
    # Проверяем тип контракта
    if contract_type and contract_type not in ALLOWED_CONTRACT_TYPES:
        logger.debug(f"Symbol {symbol} excluded by contract type {contract_type}")
        return False
    
    # Проверяем статус
    if status and status not in ALLOWED_SYMBOL_STATUSES:
        logger.debug(f"Symbol {symbol} excluded by status {status}")
        return False
    
    return True

def filter_symbols(symbols: List[str], symbols_data: List[dict] = None) -> List[str]:
    """
    Фильтрует список символов, исключая проблемные.
    
    Args:
        symbols: Список символов для фильтрации
        symbols_data: Дополнительные данные о символах (опционально)
        
    Returns:
        Отфильтрованный список символов
    """
    filtered_symbols = []
    symbols_data_dict = {}
    
    # Создаем словарь для быстрого поиска данных символов
    if symbols_data:
        symbols_data_dict = {item.get('symbol'): item for item in symbols_data}
    
    for symbol in symbols:
        symbol_info = symbols_data_dict.get(symbol, {})
        contract_type = symbol_info.get('contract_type') or symbol_info.get('contractType')
        status = symbol_info.get('status')
        
        if is_symbol_allowed(symbol, contract_type, status):
            filtered_symbols.append(symbol)
        else:
            logger.info(f"Excluding symbol {symbol} from WebSocket subscription")
    
    logger.info(f"Filtered {len(symbols)} symbols to {len(filtered_symbols)} allowed symbols")
    return filtered_symbols

def filter_symbols_data(symbols_data: List[dict]) -> List[dict]:
    """
    Фильтрует список данных символов, исключая проблемные.
    
    Args:
        symbols_data: Список данных символов
        
    Returns:
        Отфильтрованный список данных символов
    """
    filtered_data = []
    
    for symbol_info in symbols_data:
        symbol = symbol_info.get('symbol')
        contract_type = symbol_info.get('contract_type') or symbol_info.get('contractType')
        status = symbol_info.get('status')
        
        if symbol and is_symbol_allowed(symbol, contract_type, status):
            filtered_data.append(symbol_info)
        else:
            logger.info(f"Excluding symbol data for {symbol} from processing")
    
    logger.info(f"Filtered {len(symbols_data)} symbol data entries to {len(filtered_data)} allowed entries")
    return filtered_data

def get_excluded_symbols_summary() -> dict:
    """
    Возвращает сводку по исключенным символам.
    
    Returns:
        Словарь с информацией об исключениях
    """
    return {
        'excluded_symbols': list(EXCLUDED_SYMBOLS),
        'excluded_patterns': EXCLUDED_SYMBOL_PATTERNS,
        'allowed_contract_types': list(ALLOWED_CONTRACT_TYPES),
        'allowed_statuses': list(ALLOWED_SYMBOL_STATUSES)
    }