"""
Metric Resolver

Resolves metrics from user intent and schema.
"""

from typing import Dict, Any, List, Optional
import json
import logging
from pathlib import Path
import re

logger = logging.getLogger(__name__)


class MetricResolver:
    """Resolves metrics from intent and schema."""
    
    def __init__(self, metadata_path: Optional[str] = None):
        """
        Initialize metric resolver.
        
        Args:
            metadata_path: Path to metadata directory
        """
        self.metadata_path = metadata_path or Path(__file__).parent.parent.parent / 'metadata'
        self.semantic_registry = self._load_semantic_registry()
        self.known_metrics = self._extract_known_metrics()
    
    def _load_semantic_registry(self) -> Dict[str, Any]:
        """Load semantic registry."""
        try:
            # Try to use MetadataProvider first
            from backend.metadata_provider import MetadataProvider
            metadata = MetadataProvider.load()
            return metadata.get('semantic_registry', {})
        except Exception:
            # Fallback to direct file loading
            try:
                for filename in ['semantic_registry.json', 'ad_hoc_semantic_registry.json']:
                    registry_file = self.metadata_path / filename
                    if registry_file.exists():
                        with open(registry_file, 'r') as f:
                            return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load semantic registry: {e}")
        return {}
    
    def _extract_known_metrics(self) -> List[str]:
        """Extract list of known metrics."""
        metrics = self.semantic_registry.get('metrics', [])
        return [m.get('name') for m in metrics if m.get('name')]
    
    def resolve(self, intent: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
        """
        Resolve metrics from intent and schema.
        
        Args:
            intent: Intent dictionary
            schema: Schema dictionary with tables
        
        Returns:
            List of resolved metric names
        """
        query = intent.get('query', '').lower()
        metric = intent.get('metric')
        resolved_metrics = []
        
        # If metric is explicitly specified in intent
        if metric:
            if isinstance(metric, str):
                resolved_metrics.append(metric)
            elif isinstance(metric, list):
                resolved_metrics.extend(metric)
        
        # Try to extract metrics from query
        if not resolved_metrics:
            resolved_metrics = self._extract_metrics_from_query(query)
        
        # Validate metrics against known metrics
        validated_metrics = []
        for metric_name in resolved_metrics:
            if self._is_valid_metric(metric_name):
                validated_metrics.append(metric_name)
            else:
                # Try fuzzy matching
                fuzzy_match = self._fuzzy_match_metric(metric_name)
                if fuzzy_match:
                    validated_metrics.append(fuzzy_match)
                    logger.info(f"Fuzzy matched '{metric_name}' to '{fuzzy_match}'")
        
        # If still no metrics, try to infer from schema
        if not validated_metrics:
            validated_metrics = self._infer_metrics_from_schema(schema, query)
        
        logger.info(f"Resolved metrics: {validated_metrics}")
        return validated_metrics
    
    def _extract_metrics_from_query(self, query: str) -> List[str]:
        """Extract metric names from query text."""
        metrics = []
        query_lower = query.lower()
        
        # Check for common metric keywords
        metric_keywords = {
            'current_pos': ['current position', 'current_pos', 'outstanding', 'balance'],
            'revenue': ['revenue', 'sales', 'income'],
            'count': ['count', 'number of', 'total'],
            'sum': ['sum', 'total', 'aggregate'],
        }
        
        for metric_name, keywords in metric_keywords.items():
            if any(kw in query_lower for kw in keywords):
                metrics.append(metric_name)
        
        # Check against known metrics
        for known_metric in self.known_metrics:
            if known_metric.lower() in query_lower:
                metrics.append(known_metric)
        
        return list(set(metrics))  # Remove duplicates
    
    def _is_valid_metric(self, metric_name: str) -> bool:
        """Check if metric is valid (exists in semantic registry)."""
        return metric_name in self.known_metrics
    
    def _fuzzy_match_metric(self, metric_name: str) -> Optional[str]:
        """Fuzzy match metric name to known metrics."""
        metric_lower = metric_name.lower()
        
        # Try exact substring match
        for known_metric in self.known_metrics:
            if metric_lower in known_metric.lower() or known_metric.lower() in metric_lower:
                return known_metric
        
        # Try word-based matching
        metric_words = set(metric_lower.split('_'))
        best_match = None
        best_score = 0
        
        for known_metric in self.known_metrics:
            known_words = set(known_metric.lower().split('_'))
            common_words = metric_words & known_words
            score = len(common_words) / max(len(metric_words), len(known_words))
            
            if score > best_score and score > 0.3:  # Threshold for fuzzy match
                best_score = score
                best_match = known_metric
        
        return best_match
    
    def _infer_metrics_from_schema(self, schema: Dict[str, Any], query: str) -> List[str]:
        """Infer metrics from schema and query."""
        tables = schema.get('tables', [])
        inferred = []
        
        # Check table columns for metric-like names
        for table in tables:
            columns = table.get('columns', [])
            for column in columns:
                col_name = column.get('name', '').lower()
                col_type = column.get('type', '').lower()
                
                # If column is numeric and query mentions it, might be a metric
                if col_type in ['double', 'int', 'integer', 'bigint', 'decimal', 'numeric']:
                    if col_name in query.lower():
                        # Check if it matches a known metric pattern
                        for metric in self.known_metrics:
                            if col_name in metric.lower() or metric.lower() in col_name:
                                inferred.append(metric)
                                break
        
        # Default to first known metric if available
        if not inferred and self.known_metrics:
            inferred.append(self.known_metrics[0])
        
        return list(set(inferred))  # Remove duplicates
    
    def get_metric_info(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """Get full metric information from semantic registry."""
        metrics = self.semantic_registry.get('metrics', [])
        for metric in metrics:
            if metric.get('name') == metric_name:
                return metric
        return None

