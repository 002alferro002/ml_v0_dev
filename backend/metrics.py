from prometheus_client import Counter, Gauge, Histogram, generate_latest

# Define Counters
TRADES_PROCESSED_TOTAL = Counter(
    'trades_processed_total', 'Total number of trades processed'
)
ORDERBOOK_UPDATES_TOTAL = Counter(
    'orderbook_updates_total', 'Total number of order book updates processed'
)
ALERTS_GENERATED_TOTAL = Counter(
    'alerts_generated_total', 'Total number of alerts generated', ['symbol', 'alert_type', 'severity']
)
ML_PREDICTIONS_TOTAL = Counter(
    'ml_predictions_total', 'Total number of ML predictions made', ['symbol', 'predicted_direction']
)
ML_PREDICTIONS_VALIDATED_TOTAL = Counter(
    'ml_predictions_validated_total', 'Total number of ML predictions validated', ['symbol', 'is_correct']
)
DB_OPERATIONS_TOTAL = Counter(
    'db_operations_total', 'Total number of database operations', ['operation', 'status']
)
WS_MESSAGES_RECEIVED_TOTAL = Counter(
    'ws_messages_received_total', 'Total number of WebSocket messages received', ['topic']
)
MESSAGE_BROKER_PUBLISH_TOTAL = Counter(
    'message_broker_publish_total', 'Total messages published to broker', ['queue_name']
)
MESSAGE_BROKER_CONSUME_TOTAL = Counter(
    'message_broker_consume_total', 'Total messages consumed from broker', ['queue_name']
)
ERRORS_TOTAL = Counter(
    'errors_total', 'Total number of errors encountered', ['component', 'error_type']
)
ANOMALIES_DETECTED_TOTAL = Counter(
    'anomalies_detected_total', 'Total number of anomalies detected', ['symbol', 'anomaly_type']
)
ANOMALY_DETECTOR_TRAINING_TOTAL = Counter(
    'anomaly_detector_training_total', 'Total number of anomaly detector training sessions'
)


# Define Gauges
ACTIVE_WS_CONNECTIONS = Gauge(
    'active_ws_connections', 'Number of active WebSocket connections'
)
ML_TRAINING_BUFFER_SIZE = Gauge(
    'ml_training_buffer_size', 'Current size of the ML training data buffer'
)
ML_MODEL_VERSION = Gauge(
    'ml_model_version', 'Current version of the ML model'
)
ML_MODEL_ACCURACY = Gauge(
    'ml_model_accuracy', 'Latest accuracy of the ML direction model', ['symbol']
)
ML_MODEL_LOSS = Gauge(
    'ml_model_loss', 'Latest loss (MSE) of the ML price model', ['symbol']
)
ACTIVE_WATCHLIST_SYMBOLS = Gauge(
    'active_watchlist_symbols', 'Number of symbols in the active watchlist'
)
PENDING_ALERTS_QUEUE_SIZE = Gauge(
    'pending_alerts_queue_size', 'Number of alerts currently in the pending queue for ML processing'
)
ANOMALY_DETECTOR_MODEL_STATUS = Gauge(
    'anomaly_detector_model_status', 'Status of the anomaly detection model (1=trained, 0=untrained)', ['symbol']
)


# Define Histograms
TRADE_PROCESSING_LATENCY = Histogram(
    'trade_processing_latency_seconds', 'Latency of processing a single trade message'
)
ORDERBOOK_PROCESSING_LATENCY = Histogram(
    'orderbook_processing_latency_seconds', 'Latency of processing a single order book update'
)
ML_PREDICTION_LATENCY = Histogram(
    'ml_prediction_latency_seconds', 'Latency of making an ML prediction'
)
DB_OPERATION_LATENCY = Histogram(
    'db_operation_latency_seconds', 'Latency of database operations', ['operation']
)
ANOMALY_DETECTION_LATENCY = Histogram(
    'anomaly_detection_latency_seconds', 'Latency of running anomaly detection'
)

def get_metrics_text():
    """Returns the current Prometheus metrics in text format."""
    return generate_latest().decode('utf-8')


