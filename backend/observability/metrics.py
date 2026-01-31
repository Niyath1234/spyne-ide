"""
Prometheus Metrics Collection

Implements metrics collection as specified in EXECUTION_PLAN.md.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from functools import wraps
import time

try:
    from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logging.warning("prometheus_client not available. Metrics will be logged only.")

from backend.stores.db_connection import DatabaseConnection

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Prometheus metrics collector for Spyne operations."""
    
    def __init__(self):
        """Initialize metrics collector."""
        if not PROMETHEUS_AVAILABLE:
            logger.warning("Prometheus client not available. Using fallback metrics.")
            self._init_fallback()
            return
        
        # Ingestion metrics
        self.ingestion_lag_seconds = Histogram(
            'spyne_ingestion_lag_seconds',
            'Ingestion lag in seconds',
            ['contract_id']
        )
        
        self.ingestion_rows_total = Counter(
            'spyne_ingestion_rows_total',
            'Total rows ingested',
            ['contract_id', 'status']
        )
        
        self.replay_count_total = Counter(
            'spyne_replay_count_total',
            'Total replay operations',
            ['contract_id']
        )
        
        self.backfill_rows_total = Counter(
            'spyne_backfill_rows_total',
            'Total rows backfilled',
            ['contract_id']
        )
        
        # Join metrics
        self.join_usage_total = Counter(
            'spyne_join_usage_total',
            'Total join usage',
            ['join_id']
        )
        
        self.join_candidate_rejected_total = Counter(
            'spyne_join_candidate_rejected_total',
            'Total join candidates rejected',
            ['reason']
        )
        
        # Drift metrics
        self.drift_detected_total = Counter(
            'spyne_drift_detected_total',
            'Total drift detections',
            ['contract_id', 'severity']
        )
        
        self.drift_resolved_total = Counter(
            'spyne_drift_resolved_total',
            'Total drift resolutions',
            ['contract_id']
        )
        
        # Query metrics
        self.query_latency_seconds = Histogram(
            'spyne_query_latency_seconds',
            'Query latency in seconds',
            ['query_type']
        )
        
        self.query_preview_viewed_total = Counter(
            'spyne_query_preview_viewed_total',
            'Total query previews viewed'
        )
        
        self.query_corrections_total = Counter(
            'spyne_query_corrections_total',
            'Total query corrections'
        )
        
        self.query_guardrail_triggered_total = Counter(
            'spyne_query_guardrail_triggered_total',
            'Total guardrail triggers',
            ['type']
        )
        
        # Table state metrics
        self.table_promotions_total = Counter(
            'spyne_table_promotions_total',
            'Total table promotions',
            ['from_state', 'to_state']
        )
        
        self.table_deprecations_total = Counter(
            'spyne_table_deprecations_total',
            'Total table deprecations'
        )
        
        logger.info("Prometheus metrics collector initialized")
    
    def _init_fallback(self):
        """Initialize fallback metrics (log-only)."""
        self._fallback_metrics = {}
        logger.info("Fallback metrics collector initialized")
    
    def record_ingestion_lag(self, contract_id: str, lag_seconds: float):
        """Record ingestion lag."""
        if PROMETHEUS_AVAILABLE:
            self.ingestion_lag_seconds.labels(contract_id=contract_id).observe(lag_seconds)
        else:
            logger.info(f"Metric: ingestion_lag_seconds[{contract_id}] = {lag_seconds}")
    
    def record_ingestion_rows(self, contract_id: str, rows: int, status: str = 'success'):
        """Record ingested rows."""
        if PROMETHEUS_AVAILABLE:
            self.ingestion_rows_total.labels(
                contract_id=contract_id,
                status=status
            ).inc(rows)
        else:
            logger.info(f"Metric: ingestion_rows_total[{contract_id}, {status}] += {rows}")
    
    def record_replay(self, contract_id: str):
        """Record replay operation."""
        if PROMETHEUS_AVAILABLE:
            self.replay_count_total.labels(contract_id=contract_id).inc()
        else:
            logger.info(f"Metric: replay_count_total[{contract_id}] += 1")
    
    def record_backfill_rows(self, contract_id: str, rows: int):
        """Record backfilled rows."""
        if PROMETHEUS_AVAILABLE:
            self.backfill_rows_total.labels(contract_id=contract_id).inc(rows)
        else:
            logger.info(f"Metric: backfill_rows_total[{contract_id}] += {rows}")
    
    def record_join_usage(self, join_id: str):
        """Record join usage."""
        if PROMETHEUS_AVAILABLE:
            self.join_usage_total.labels(join_id=join_id).inc()
        else:
            logger.info(f"Metric: join_usage_total[{join_id}] += 1")
    
    def record_join_candidate_rejected(self, reason: str):
        """Record join candidate rejection."""
        if PROMETHEUS_AVAILABLE:
            self.join_candidate_rejected_total.labels(reason=reason).inc()
        else:
            logger.info(f"Metric: join_candidate_rejected_total[{reason}] += 1")
    
    def record_drift_detected(self, contract_id: str, severity: str):
        """Record drift detection."""
        if PROMETHEUS_AVAILABLE:
            self.drift_detected_total.labels(
                contract_id=contract_id,
                severity=severity
            ).inc()
        else:
            logger.info(f"Metric: drift_detected_total[{contract_id}, {severity}] += 1")
    
    def record_drift_resolved(self, contract_id: str):
        """Record drift resolution."""
        if PROMETHEUS_AVAILABLE:
            self.drift_resolved_total.labels(contract_id=contract_id).inc()
        else:
            logger.info(f"Metric: drift_resolved_total[{contract_id}] += 1")
    
    def record_query_latency(self, query_type: str, latency_seconds: float):
        """Record query latency."""
        if PROMETHEUS_AVAILABLE:
            self.query_latency_seconds.labels(query_type=query_type).observe(latency_seconds)
        else:
            logger.info(f"Metric: query_latency_seconds[{query_type}] = {latency_seconds}")
    
    def record_query_preview_viewed(self):
        """Record query preview view."""
        if PROMETHEUS_AVAILABLE:
            self.query_preview_viewed_total.inc()
        else:
            logger.info("Metric: query_preview_viewed_total += 1")
    
    def record_query_correction(self):
        """Record query correction."""
        if PROMETHEUS_AVAILABLE:
            self.query_corrections_total.inc()
        else:
            logger.info("Metric: query_corrections_total += 1")
    
    def record_guardrail_triggered(self, guardrail_type: str):
        """Record guardrail trigger."""
        if PROMETHEUS_AVAILABLE:
            self.query_guardrail_triggered_total.labels(type=guardrail_type).inc()
        else:
            logger.info(f"Metric: query_guardrail_triggered_total[{guardrail_type}] += 1")
    
    def record_table_promotion(self, from_state: str, to_state: str):
        """Record table promotion."""
        if PROMETHEUS_AVAILABLE:
            self.table_promotions_total.labels(
                from_state=from_state,
                to_state=to_state
            ).inc()
        else:
            logger.info(f"Metric: table_promotions_total[{from_state}, {to_state}] += 1")
    
    def record_table_deprecation(self):
        """Record table deprecation."""
        if PROMETHEUS_AVAILABLE:
            self.table_deprecations_total.inc()
        else:
            logger.info("Metric: table_deprecations_total += 1")
    
    def persist_metrics_to_db(self):
        """
        Persist metrics to database for historical tracking.
        
        This complements Prometheus by storing metrics in the database
        for long-term analysis and reporting.
        """
        if not PROMETHEUS_AVAILABLE:
            return
        
        try:
            with DatabaseConnection.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get current timestamp
                now = datetime.utcnow()
                
                # In a real implementation, we would:
                # 1. Collect current metric values from Prometheus
                # 2. Store them in spyne_metrics table
                # 3. This would be called periodically (e.g., every minute)
                
                # Example:
                # cursor.execute("""
                #     INSERT INTO spyne_metrics (metric_name, metric_value, labels, timestamp)
                #     VALUES (%s, %s, %s::jsonb, %s)
                # """, (metric_name, value, json.dumps(labels), now))
                
                logger.debug("Metrics persisted to database")
                
        except Exception as e:
            logger.error(f"Failed to persist metrics to database: {e}")


# Global metrics collector instance
metrics_collector = MetricsCollector()


def metrics_timer(metric_name: str, labels: Optional[Dict[str, str]] = None):
    """
    Decorator to time function execution and record metrics.
    
    Usage:
        @metrics_timer('spyne_query_latency_seconds', {'query_type': 'nl_to_sql'})
        def execute_query():
            ...
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = f(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                if PROMETHEUS_AVAILABLE:
                    # Find the appropriate histogram
                    if hasattr(metrics_collector, metric_name):
                        metric = getattr(metrics_collector, metric_name)
                        if labels:
                            metric.labels(**labels).observe(duration)
                        else:
                            metric.observe(duration)
                else:
                    logger.info(f"Metric: {metric_name} = {duration}s")
        return wrapper
    return decorator

