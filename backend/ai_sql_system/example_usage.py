"""
Example Usage - How to use the AI SQL System
"""
import os
from orchestration.graph import LangGraphOrchestrator
from trino.client import TrinoClient
from trino.validator import TrinoValidator
from metadata.semantic_registry import SemanticRegistry
from planning.join_graph import JoinGraph

# Example 1: Basic usage
def example_basic():
    """Basic query example"""
    orchestrator = LangGraphOrchestrator()
    
    result = orchestrator.run("revenue per customer")
    
    print(f"Success: {result['success']}")
    print(f"SQL: {result['sql']}")
    print(f"Intent: {result.get('intent')}")


# Example 2: With Trino validation
def example_with_trino():
    """Example with Trino validation enabled"""
    trino_client = TrinoClient(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080"))
    )
    trino_validator = TrinoValidator(trino_client)
    
    orchestrator = LangGraphOrchestrator(trino_validator=trino_validator)
    
    result = orchestrator.run("total sales by product category")
    
    if result['success']:
        print(f"Generated SQL: {result['sql']}")
        
        # Validate with Trino
        validation = trino_validator.validate(result['sql'])
        print(f"Trino validation: {validation['valid']}")


# Example 3: Run evaluation suite
def example_evaluation():
    """Run evaluation suite"""
    from evaluation.test_suite import EvaluationSuite
    
    orchestrator = LangGraphOrchestrator()
    suite = EvaluationSuite(orchestrator)
    
    results = suite.run_evaluation()
    
    print(f"Total Queries: {results['total_queries']}")
    print(f"Successful: {results['successful']}")
    print(f"Failed: {results['failed']}")
    print(f"Accuracy: {results['accuracy']:.2%}")
    print(f"Avg Latency: {results['avg_latency_ms']:.0f}ms")


if __name__ == "__main__":
    # Set environment variables
    os.environ.setdefault("OPENAI_API_KEY", "your-key-here")
    
    # Run example
    example_basic()
