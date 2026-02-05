"""
Evaluation Suite - 200 test queries across categories
"""
from typing import List, Dict, Any
import time
from ..orchestration.graph import LangGraphOrchestrator
import logging

logger = logging.getLogger(__name__)


class EvaluationSuite:
    """Test suite for evaluating system performance"""
    
    def __init__(self, orchestrator: LangGraphOrchestrator):
        """
        Initialize evaluation suite
        
        Args:
            orchestrator: LangGraphOrchestrator instance
        """
        self.orchestrator = orchestrator
        self.test_queries = self._load_test_queries()
    
    def _load_test_queries(self) -> List[Dict[str, Any]]:
        """Load test queries across categories - 200+ queries"""
        return [
            # ========== SIMPLE METRICS (50 queries) ==========
            {"query": "revenue per customer", "category": "simple_metric"},
            {"query": "total sales", "category": "simple_metric"},
            {"query": "average order value", "category": "simple_metric"},
            {"query": "total revenue", "category": "simple_metric"},
            {"query": "order count", "category": "simple_metric"},
            {"query": "customer count", "category": "simple_metric"},
            {"query": "total quantity", "category": "simple_metric"},
            {"query": "average discount", "category": "simple_metric"},
            {"query": "total discount amount", "category": "simple_metric"},
            {"query": "revenue by order", "category": "simple_metric"},
            {"query": "sales amount", "category": "simple_metric"},
            {"query": "number of orders", "category": "simple_metric"},
            {"query": "number of customers", "category": "simple_metric"},
            {"query": "total extended price", "category": "simple_metric"},
            {"query": "average extended price", "category": "simple_metric"},
            {"query": "sum of line items", "category": "simple_metric"},
            {"query": "total line items", "category": "simple_metric"},
            {"query": "revenue total", "category": "simple_metric"},
            {"query": "sales total", "category": "simple_metric"},
            {"query": "order total", "category": "simple_metric"},
            {"query": "customer revenue", "category": "simple_metric"},
            {"query": "order revenue", "category": "simple_metric"},
            {"query": "line item revenue", "category": "simple_metric"},
            {"query": "total amount", "category": "simple_metric"},
            {"query": "sum revenue", "category": "simple_metric"},
            {"query": "count orders", "category": "simple_metric"},
            {"query": "count customers", "category": "simple_metric"},
            {"query": "revenue sum", "category": "simple_metric"},
            {"query": "sales sum", "category": "simple_metric"},
            {"query": "total sales revenue", "category": "simple_metric"},
            {"query": "aggregate revenue", "category": "simple_metric"},
            {"query": "aggregate sales", "category": "simple_metric"},
            {"query": "revenue aggregate", "category": "simple_metric"},
            {"query": "sales aggregate", "category": "simple_metric"},
            {"query": "total order value", "category": "simple_metric"},
            {"query": "sum order value", "category": "simple_metric"},
            {"query": "average order amount", "category": "simple_metric"},
            {"query": "total order amount", "category": "simple_metric"},
            {"query": "revenue amount", "category": "simple_metric"},
            {"query": "sales amount total", "category": "simple_metric"},
            {"query": "revenue total amount", "category": "simple_metric"},
            {"query": "total customer revenue", "category": "simple_metric"},
            {"query": "sum customer revenue", "category": "simple_metric"},
            {"query": "aggregate customer revenue", "category": "simple_metric"},
            {"query": "total customer sales", "category": "simple_metric"},
            {"query": "sum customer sales", "category": "simple_metric"},
            {"query": "aggregate customer sales", "category": "simple_metric"},
            {"query": "revenue per order", "category": "simple_metric"},
            {"query": "sales per order", "category": "simple_metric"},
            {"query": "revenue per line item", "category": "simple_metric"},
            
            # ========== JOINS (40 queries) ==========
            {"query": "revenue by customer name", "category": "joins"},
            {"query": "sales by product category", "category": "joins"},
            {"query": "revenue by customer", "category": "joins"},
            {"query": "sales by customer", "category": "joins"},
            {"query": "revenue by product", "category": "joins"},
            {"query": "sales by product", "category": "joins"},
            {"query": "revenue by region", "category": "joins"},
            {"query": "sales by region", "category": "joins"},
            {"query": "revenue by nation", "category": "joins"},
            {"query": "sales by nation", "category": "joins"},
            {"query": "revenue by supplier", "category": "joins"},
            {"query": "sales by supplier", "category": "joins"},
            {"query": "customer revenue by region", "category": "joins"},
            {"query": "customer sales by region", "category": "joins"},
            {"query": "product revenue by category", "category": "joins"},
            {"query": "product sales by category", "category": "joins"},
            {"query": "revenue per customer by region", "category": "joins"},
            {"query": "sales per customer by region", "category": "joins"},
            {"query": "revenue per product by category", "category": "joins"},
            {"query": "sales per product by category", "category": "joins"},
            {"query": "customer name and revenue", "category": "joins"},
            {"query": "customer name and sales", "category": "joins"},
            {"query": "product name and revenue", "category": "joins"},
            {"query": "product name and sales", "category": "joins"},
            {"query": "region name and revenue", "category": "joins"},
            {"query": "region name and sales", "category": "joins"},
            {"query": "nation name and revenue", "category": "joins"},
            {"query": "nation name and sales", "category": "joins"},
            {"query": "supplier name and revenue", "category": "joins"},
            {"query": "supplier name and sales", "category": "joins"},
            {"query": "revenue grouped by customer", "category": "joins"},
            {"query": "sales grouped by customer", "category": "joins"},
            {"query": "revenue grouped by product", "category": "joins"},
            {"query": "sales grouped by product", "category": "joins"},
            {"query": "revenue grouped by region", "category": "joins"},
            {"query": "sales grouped by region", "category": "joins"},
            {"query": "customer with revenue", "category": "joins"},
            {"query": "customer with sales", "category": "joins"},
            {"query": "product with revenue", "category": "joins"},
            {"query": "product with sales", "category": "joins"},
            
            # ========== DERIVED METRICS (30 queries) ==========
            {"query": "profit per customer", "category": "derived_metric"},
            {"query": "profit margin by product", "category": "derived_metric"},
            {"query": "profit total", "category": "derived_metric"},
            {"query": "profit by customer", "category": "derived_metric"},
            {"query": "profit by product", "category": "derived_metric"},
            {"query": "profit margin", "category": "derived_metric"},
            {"query": "profit percentage", "category": "derived_metric"},
            {"query": "revenue minus cost", "category": "derived_metric"},
            {"query": "sales minus cost", "category": "derived_metric"},
            {"query": "net profit", "category": "derived_metric"},
            {"query": "gross profit", "category": "derived_metric"},
            {"query": "profit amount", "category": "derived_metric"},
            {"query": "profit sum", "category": "derived_metric"},
            {"query": "profit aggregate", "category": "derived_metric"},
            {"query": "total profit", "category": "derived_metric"},
            {"query": "average profit", "category": "derived_metric"},
            {"query": "profit per order", "category": "derived_metric"},
            {"query": "profit per line item", "category": "derived_metric"},
            {"query": "profit margin percentage", "category": "derived_metric"},
            {"query": "profit ratio", "category": "derived_metric"},
            {"query": "revenue to cost ratio", "category": "derived_metric"},
            {"query": "sales to cost ratio", "category": "derived_metric"},
            {"query": "profitability", "category": "derived_metric"},
            {"query": "net revenue", "category": "derived_metric"},
            {"query": "net sales", "category": "derived_metric"},
            {"query": "profit per unit", "category": "derived_metric"},
            {"query": "unit profit", "category": "derived_metric"},
            {"query": "profit per item", "category": "derived_metric"},
            {"query": "item profit", "category": "derived_metric"},
            {"query": "profit calculation", "category": "derived_metric"},
            
            # ========== AMBIGUOUS (30 queries) ==========
            {"query": "top customers", "category": "ambiguous"},
            {"query": "recent sales", "category": "ambiguous"},
            {"query": "best customers", "category": "ambiguous"},
            {"query": "worst customers", "category": "ambiguous"},
            {"query": "top products", "category": "ambiguous"},
            {"query": "best products", "category": "ambiguous"},
            {"query": "worst products", "category": "ambiguous"},
            {"query": "recent orders", "category": "ambiguous"},
            {"query": "latest orders", "category": "ambiguous"},
            {"query": "old orders", "category": "ambiguous"},
            {"query": "biggest orders", "category": "ambiguous"},
            {"query": "smallest orders", "category": "ambiguous"},
            {"query": "high value customers", "category": "ambiguous"},
            {"query": "low value customers", "category": "ambiguous"},
            {"query": "high value products", "category": "ambiguous"},
            {"query": "low value products", "category": "ambiguous"},
            {"query": "popular products", "category": "ambiguous"},
            {"query": "unpopular products", "category": "ambiguous"},
            {"query": "active customers", "category": "ambiguous"},
            {"query": "inactive customers", "category": "ambiguous"},
            {"query": "frequent customers", "category": "ambiguous"},
            {"query": "rare customers", "category": "ambiguous"},
            {"query": "large orders", "category": "ambiguous"},
            {"query": "small orders", "category": "ambiguous"},
            {"query": "expensive orders", "category": "ambiguous"},
            {"query": "cheap orders", "category": "ambiguous"},
            {"query": "important customers", "category": "ambiguous"},
            {"query": "unimportant customers", "category": "ambiguous"},
            {"query": "significant orders", "category": "ambiguous"},
            {"query": "insignificant orders", "category": "ambiguous"},
            
            # ========== COMPLEX ANALYTICS (50 queries) ==========
            {"query": "revenue per customer by region for last month", "category": "complex"},
            {"query": "top 10 products by sales volume", "category": "complex"},
            {"query": "revenue per customer by region", "category": "complex"},
            {"query": "sales per customer by region", "category": "complex"},
            {"query": "revenue per product by category", "category": "complex"},
            {"query": "sales per product by category", "category": "complex"},
            {"query": "top 10 customers by revenue", "category": "complex"},
            {"query": "top 10 customers by sales", "category": "complex"},
            {"query": "top 10 products by revenue", "category": "complex"},
            {"query": "top 10 products by sales", "category": "complex"},
            {"query": "revenue per customer by nation", "category": "complex"},
            {"query": "sales per customer by nation", "category": "complex"},
            {"query": "revenue per supplier by region", "category": "complex"},
            {"query": "sales per supplier by region", "category": "complex"},
            {"query": "revenue per customer by region last month", "category": "complex"},
            {"query": "sales per customer by region last month", "category": "complex"},
            {"query": "revenue per customer by region this year", "category": "complex"},
            {"query": "sales per customer by region this year", "category": "complex"},
            {"query": "revenue per customer by region last year", "category": "complex"},
            {"query": "sales per customer by region last year", "category": "complex"},
            {"query": "top 20 customers by revenue", "category": "complex"},
            {"query": "top 20 customers by sales", "category": "complex"},
            {"query": "top 20 products by revenue", "category": "complex"},
            {"query": "top 20 products by sales", "category": "complex"},
            {"query": "revenue per customer by region and product", "category": "complex"},
            {"query": "sales per customer by region and product", "category": "complex"},
            {"query": "revenue per customer by region and supplier", "category": "complex"},
            {"query": "sales per customer by region and supplier", "category": "complex"},
            {"query": "revenue per product by category and region", "category": "complex"},
            {"query": "sales per product by category and region", "category": "complex"},
            {"query": "revenue per customer by region for last quarter", "category": "complex"},
            {"query": "sales per customer by region for last quarter", "category": "complex"},
            {"query": "revenue per customer by region for this quarter", "category": "complex"},
            {"query": "sales per customer by region for this quarter", "category": "complex"},
            {"query": "revenue per customer by region for last week", "category": "complex"},
            {"query": "sales per customer by region for last week", "category": "complex"},
            {"query": "revenue per customer by region for this week", "category": "complex"},
            {"query": "sales per customer by region for this week", "category": "complex"},
            {"query": "revenue per customer by region for last 30 days", "category": "complex"},
            {"query": "sales per customer by region for last 30 days", "category": "complex"},
            {"query": "revenue per customer by region for last 90 days", "category": "complex"},
            {"query": "sales per customer by region for last 90 days", "category": "complex"},
            {"query": "revenue per customer by region for last 365 days", "category": "complex"},
            {"query": "sales per customer by region for last 365 days", "category": "complex"},
            {"query": "revenue per customer by region and nation", "category": "complex"},
            {"query": "sales per customer by region and nation", "category": "complex"},
            {"query": "revenue per customer by region nation and supplier", "category": "complex"},
            {"query": "sales per customer by region nation and supplier", "category": "complex"},
            {"query": "revenue per customer by region product and supplier", "category": "complex"},
            {"query": "sales per customer by region product and supplier", "category": "complex"},
            {"query": "revenue per customer by all dimensions", "category": "complex"},
            {"query": "sales per customer by all dimensions", "category": "complex"},
        ]
    
    def run_evaluation(self) -> Dict[str, Any]:
        """
        Run full evaluation suite
        
        Returns:
            Dictionary with evaluation results:
            {
                'total_queries': int,
                'successful': int,
                'failed': int,
                'accuracy': float,
                'avg_latency_ms': float,
                'category_breakdown': Dict,
                'errors': List[str]
            }
        """
        results = {
            'total_queries': len(self.test_queries),
            'successful': 0,
            'failed': 0,
            'accuracy': 0.0,
            'avg_latency_ms': 0.0,
            'category_breakdown': {},
            'errors': []
        }
        
        latencies = []
        
        for test_case in self.test_queries:
            category = test_case['category']
            query = test_case['query']
            
            if category not in results['category_breakdown']:
                results['category_breakdown'][category] = {
                    'total': 0,
                    'successful': 0,
                    'failed': 0
                }
            
            results['category_breakdown'][category]['total'] += 1
            
            start_time = time.time()
            
            try:
                result = self.orchestrator.run(query)
                
                latency_ms = (time.time() - start_time) * 1000
                latencies.append(latency_ms)
                
                if result['success'] and result.get('sql'):
                    results['successful'] += 1
                    results['category_breakdown'][category]['successful'] += 1
                    logger.info(f"✓ {query} ({latency_ms:.0f}ms)")
                else:
                    results['failed'] += 1
                    results['category_breakdown'][category]['failed'] += 1
                    error_msg = result.get('error', 'Unknown error')
                    results['errors'].append(f"{query}: {error_msg}")
                    logger.error(f"✗ {query}: {error_msg}")
                    
            except Exception as e:
                results['failed'] += 1
                results['category_breakdown'][category]['failed'] += 1
                results['errors'].append(f"{query}: {str(e)}")
                logger.error(f"✗ {query}: {str(e)}")
        
        # Calculate metrics
        if latencies:
            results['avg_latency_ms'] = sum(latencies) / len(latencies)
        
        if results['total_queries'] > 0:
            results['accuracy'] = results['successful'] / results['total_queries']
        
        return results
    
    def run_single_test(self, query: str) -> Dict[str, Any]:
        """
        Run single test query
        
        Args:
            query: Test query
            
        Returns:
            Test result dictionary
        """
        start_time = time.time()
        
        try:
            result = self.orchestrator.run(query)
            latency_ms = (time.time() - start_time) * 1000
            
            return {
                'query': query,
                'success': result['success'],
                'sql': result.get('sql', ''),
                'latency_ms': latency_ms,
                'errors': result.get('validation_errors', [])
            }
        except Exception as e:
            return {
                'query': query,
                'success': False,
                'error': str(e),
                'latency_ms': (time.time() - start_time) * 1000
            }
