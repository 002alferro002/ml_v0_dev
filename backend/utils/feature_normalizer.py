import numpy as np
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def normalize_features(features: Dict[str, float], symbol: str = "unknown") -> Dict[str, float]:
    """
    Нормализует признаки для работы с данными разного масштаба.
    Применяет различные методы нормализации в зависимости от типа признака.
    """
    normalized = {}
    if not features:
        logger.warning(f"Empty features provided for normalization for symbol {symbol}")
        return {}

    for key, value in features.items():
        if not isinstance(value, (int, float)) or np.isnan(value) or np.isinf(value):
            logger.warning(f"Invalid feature value for {key} in {symbol}: {value}, setting to 0.0")
            normalized[key] = 0.0
            continue

        if 'price' in key.lower() or 'volume' in key.lower() or 'liquidity' in key.lower() or 'depth' in key.lower():
            # Для цен, объемов, ликвидности и глубины используем логарифмическое масштабирование
            # Добавляем 1, чтобы избежать log(0) и сохранить положительные значения
            if value > 0:
                normalized[key] = np.log1p(value)
            else:
                normalized[key] = 0.0
        elif 'spread' in key.lower() or 'ratio' in key.lower():
            # Для спредов и отношений ограничиваем диапазон
            normalized[key] = np.clip(value, -10.0, 10.0)
        elif 'imbalance' in key.lower():
            # Для дисбалансов ограничиваем диапазон [-1, 1]
            normalized[key] = np.clip(value, -1.0, 1.0)
        elif 'volatility' in key.lower():
            # Для волатильности используем квадратный корень
            if value >= 0:
                normalized[key] = np.sqrt(value)
            else:
                normalized[key] = 0.0
        elif 'count' in key.lower():
            # Для счетчиков используем квадратный корень
            if value >= 0:
                normalized[key] = np.sqrt(value)
            else:
                normalized[key] = 0.0
        elif 'signal' in key.lower():
            # Сигналы уже бинарные, оставляем как есть
            normalized[key] = float(value)
        else:
            # Для остальных признаков, если они не попадают ни в одну категорию,
            # можно применить общую нормализацию или оставить как есть,
            # в зависимости от их природы. Здесь просто ограничиваем.
            normalized[key] = np.clip(value, -100.0, 100.0)

    return normalized


