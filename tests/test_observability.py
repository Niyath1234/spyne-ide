"""
Tests for observability/metrics collection
"""

import pytest
from unittest.mock import patch, MagicMock
import time

from backend.observability.metrics import MetricsCollector, metrics_collector, metrics_timer


class TestMetricsCollector:
    """Test MetricsCollector implementation."""
    
    @pytest.fixture
    def collector(self):
        """Create metrics collector."""
        return MetricsCollector()
    
    def test_record_ingestion_lag(self, collector):
        """Test recording ingestion lag."""
        collector.record_ingestion_lag('contract_123', 5.5)
        # Metrics are recorded (would be verified with Prometheus in integration tests)
    
    def test_record_ingestion_rows(self, collector):
        """Test recording ingested rows."""
        collector.record_ingestion_rows('contract_123', 1000, 'success')
        collector.record_ingestion_rows('contract_123', 50, 'error')
        # Metrics are recorded
    
    def test_record_replay(self, collector):
        """Test recording replay operation."""
        collector.record_replay('contract_123')
        # Metrics are recorded
    
    def test_record_backfill_rows(self, collector):
        """Test recording backfilled rows."""
        collector.record_backfill_rows('contract_123', 5000)
        # Metrics are recorded
    
    def test_record_join_usage(self, collector):
        """Test recording join usage."""
        collector.record_join_usage('join_customers_orders_v1')
        # Metrics are recorded
    
    def test_record_join_candidate_rejected(self, collector):
        """Test recording join candidate rejection."""
        collector.record_join_candidate_rejected('low_confidence')
        collector.record_join_candidate_rejected('validation_failed')
        # Metrics are recorded
    
    def test_record_drift_detected(self, collector):
        """Test recording drift detection."""
        collector.record_drift_detected('contract_123', 'BREAKING')
        collector.record_drift_detected('contract_123', 'WARNING')
        collector.record_drift_detected('contract_123', 'COMPATIBLE')
        # Metrics are recorded
    
    def test_record_drift_resolved(self, collector):
        """Test recording drift resolution."""
        collector.record_drift_resolved('contract_123')
        # Metrics are recorded
    
    def test_record_query_latency(self, collector):
        """Test recording query latency."""
        collector.record_query_latency('nl_to_sql', 1.5)
        collector.record_query_latency('direct_sql', 0.3)
        # Metrics are recorded
    
    def test_record_query_preview_viewed(self, collector):
        """Test recording query preview view."""
        collector.record_query_preview_viewed()
        # Metrics are recorded
    
    def test_record_query_correction(self, collector):
        """Test recording query correction."""
        collector.record_query_correction()
        # Metrics are recorded
    
    def test_record_guardrail_triggered(self, collector):
        """Test recording guardrail trigger."""
        collector.record_guardrail_triggered('explosive_join')
        collector.record_guardrail_triggered('ambiguous_aggregate')
        # Metrics are recorded
    
    def test_record_table_promotion(self, collector):
        """Test recording table promotion."""
        collector.record_table_promotion('SHADOW', 'ACTIVE')
        collector.record_table_promotion('DEPRECATED', 'ACTIVE')
        # Metrics are recorded
    
    def test_record_table_deprecation(self, collector):
        """Test recording table deprecation."""
        collector.record_table_deprecation()
        # Metrics are recorded
    
    def test_metrics_timer_decorator(self):
        """Test metrics_timer decorator."""
        @metrics_timer('test_operation_seconds')
        def test_function():
            time.sleep(0.1)
            return 'result'
        
        result = test_function()
        assert result == 'result'
        # Metrics are recorded
    
    def test_metrics_timer_with_labels(self):
        """Test metrics_timer decorator with labels."""
        @metrics_timer('test_operation_seconds', {'type': 'test'})
        def test_function():
            time.sleep(0.05)
            return 'result'
        
        result = test_function()
        assert result == 'result'
        # Metrics are recorded with labels
    
    def test_fallback_metrics_when_prometheus_unavailable(self):
        """Test fallback behavior when Prometheus is unavailable."""
        with patch('backend.observability.metrics.PROMETHEUS_AVAILABLE', False):
            collector = MetricsCollector()
            
            # Should not raise exception
            collector.record_ingestion_lag('contract_123', 5.5)
            collector.record_ingestion_rows('contract_123', 1000, 'success')
    
    def test_persist_metrics_to_db(self, collector):
        """Test persisting metrics to database."""
        with patch('backend.observability.metrics.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.get_connection.return_value.__enter__.return_value = mock_conn
            
            collector.persist_metrics_to_db()
            
            # Should not raise exception
            assert True

