from fastapi import APIRouter, Depends, HTTPException, status, Request
from typing import List, Dict, Any, Optional
from enum import Enum
import logging

from manipulation.ml.ml_manager import MLManager, MLMode
from exceptions import AppError, MLPredictionError, AnomalyDetectionError

logger = logging.getLogger(__name__)

# Dependency to get MLManager instance
def get_ml_manager(request: Request) -> MLManager:
    logger.info("Getting ML Manager from app state")
    try:
        ml_manager = request.app.state.ml_manager
        if ml_manager is None:
            logger.error("ML Manager is None in app state")
            raise HTTPException(status_code=500, detail="ML Manager not initialized")
        logger.info("ML Manager retrieved successfully")
        return ml_manager
    except AttributeError as e:
        logger.error(f"ML Manager attribute not found in app state: {e}")
        raise HTTPException(status_code=500, detail="ML Manager not found in app state")
    except Exception as e:
        logger.error(f"Error getting ML Manager: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting ML Manager: {e}")

# Dependency to get AnomalyDetector instance
def get_anomaly_detector(request: Request) -> Any: # Use Any for now, or define a proper type hint for AnomalyDetector
    return request.app.state.anomaly_detector

router = APIRouter(prefix="/ml", tags=["Machine Learning"])

@router.get("/status", summary="Get ML System Status")
async def get_ml_status(ml_manager: MLManager = Depends(get_ml_manager)):
    """
    Возвращает текущий статус ML системы, включая режим работы,
    статус обучения моделей и статистику сбора данных.
    """
    try:
        status = ml_manager.get_system_status()
        return status
    except Exception as e:
        logger.error(f"Error getting ML status: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get ML status: {e}")

@router.post("/mode/switch", summary="Switch ML Mode")
async def switch_ml_mode(mode: MLMode, ml_manager: MLManager = Depends(get_ml_manager)):
    """
    Переключает режим работы ML системы (training, prediction, hybrid).
    """
    try:
        await ml_manager.switch_mode(mode)
        return {"message": f"ML system switched to {mode.value} mode successfully."}
    except AppError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error switching ML mode to {mode.value}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to switch ML mode: {e}")

@router.post("/training/force", summary="Force ML Model Retraining")
async def force_ml_training(ml_manager: MLManager = Depends(get_ml_manager)):
    """
    Принудительно запускает полное переобучение ML модели на всех доступных данных.
    """
    try:
        success = await ml_manager.force_training()
        if success:
            return {"message": "ML model full training initiated successfully."}
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Failed to initiate full training. Check logs for details (e.g., insufficient data).")
    except AppError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error forcing ML training: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to force ML training: {e}")

@router.post("/models/reset", summary="Reset ML Models")
async def reset_ml_models(ml_manager: MLManager = Depends(get_ml_manager)):
    """
    Сбрасывает все ML модели и их состояние к начальному.
    """
    try:
        await ml_manager.reset_models()
        return {"message": "ML models reset successfully."}
    except AppError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error resetting ML models: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to reset ML models: {e}")

@router.get("/predictions/active", summary="Get Active ML Predictions")
async def get_active_ml_predictions(ml_manager: MLManager = Depends(get_ml_manager)) -> List[Dict[str, Any]]:
    """
    Возвращает список активных (невалидированных) прогнозов ML модели.
    """
    try:
        predictions = ml_manager.get_active_predictions()
        return predictions
    except Exception as e:
        logger.error(f"Error getting active ML predictions: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get active ML predictions: {e}")

@router.get("/predictions/history", summary="Get ML Prediction History")
async def get_ml_prediction_history(limit: int = 100, ml_manager: MLManager = Depends(get_ml_manager)) -> List[Dict[str, Any]]:
    """
    Возвращает историю прогнозов ML модели.
    """
    try:
        history = ml_manager.get_prediction_history(limit)
        return history
    except Exception as e:
        logger.error(f"Error getting ML prediction history: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get ML prediction history: {e}")

@router.get("/predictions/{symbol}", summary="Get Real-time ML Prediction for Symbol")
async def get_realtime_ml_prediction(symbol: str, ml_manager: MLManager = Depends(get_ml_manager)) -> Dict[str, Any]:
    """
    Возвращает текущий прогноз ML модели для указанного символа.
    """
    try:
        prediction = await ml_manager.price_predictor.get_prediction_for_current_state(symbol)
        if 'error' in prediction:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=prediction['error'])
        return prediction
    except MLPredictionError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error getting real-time ML prediction for {symbol}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get real-time ML prediction: {e}")

@router.get("/training/metrics", summary="Get ML Training Metrics")
async def get_ml_training_metrics(ml_manager: MLManager = Depends(get_ml_manager)) -> Dict[str, Any]:
    """
    Возвращает метрики обучения ML модели, включая историю точности и ошибок,
    а также рекомендации по улучшению обучения.
    """
    try:
        metrics = ml_manager.get_training_metrics()
        return metrics
    except Exception as e:
        logger.error(f"Error getting ML training metrics: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get ML training metrics: {e}")

@router.get("/statistics/accuracy", summary="Get ML Prediction Accuracy Statistics")
async def get_ml_accuracy_statistics(ml_manager: MLManager = Depends(get_ml_manager)) -> Dict[str, Any]:
    """
    Возвращает статистику точности прогнозов ML модели по символам.
    """
    try:
        return ml_manager.prediction_stats
    except Exception as e:
        logger.error(f"Error getting ML accuracy statistics: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get ML accuracy statistics: {e}")

@router.get("/explain/{symbol}", summary="Get SHAP Explanation for ML Prediction")
async def get_shap_explanation(symbol: str, ml_manager: MLManager = Depends(get_ml_manager)) -> Dict[str, float]:
    """
    Возвращает SHAP-значения для последнего прогноза ML модели по указанному символу,
    объясняющие вклад каждого признака в прогноз.
    """
    try:
        shap_explanation = await ml_manager.get_shap_explanation_for_symbol(symbol)
        if shap_explanation is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No SHAP explanation available for {symbol}. Ensure a prediction has been made and model is trained.")
        return shap_explanation
    except MLPredictionError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error getting SHAP explanation for {symbol}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get SHAP explanation: {e}")

# Anomaly Detection Endpoints
anomaly_router = APIRouter(prefix="/anomaly", tags=["Anomaly Detection"])

@anomaly_router.get("/status", summary="Get Anomaly Detector Status")
async def get_anomaly_detector_status(anomaly_detector: Any = Depends(get_anomaly_detector)):
    """
    Возвращает текущий статус детектора аномалий.
    """
    try:
        status = anomaly_detector.get_status()
        return status
    except Exception as e:
        logger.error(f"Error getting Anomaly Detector status: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get Anomaly Detector status: {e}")




