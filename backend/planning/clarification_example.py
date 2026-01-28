"""
Example: Using Proactive Clarification Agent

This shows how to integrate proactive clarification into your planning pipeline.
"""

from backend.planning.clarification_agent import ClarificationAgent
from backend.invariants.fail_open import FailOpenEnforcer
from backend.metadata_provider import MetadataProvider
from backend.interfaces import LLMProvider  # Your LLM provider interface


def example_clarification_flow():
    """Example of how clarification works."""
    
    # 1. Initialize components
    metadata = MetadataProvider.load()
    llm_provider = LLMProvider()  # Your LLM provider
    
    # 2. Create clarification agent
    clarification_agent = ClarificationAgent(
        llm_provider=llm_provider,
        metadata=metadata
    )
    
    # 3. Example ambiguous queries
    ambiguous_queries = [
        "show me customers",  # Missing: what about customers?
        "total revenue",  # Missing: time range, grouping
        "orders by region",  # Missing: metric, time range
        "recent sales",  # Missing: time definition, grouping
    ]
    
    for query in ambiguous_queries:
        print(f"\n{'='*80}")
        print(f"Query: {query}")
        print('='*80)
        
        # Analyze query
        result = clarification_agent.analyze_query(query, metadata=metadata)
        
        if result.needs_clarification:
            print(f"️  Needs Clarification (Confidence: {result.confidence:.1%})")
            print(f"\nQuestions to ask:")
            for i, q in enumerate(result.questions, 1):
                print(f"\n{i}. {q.question}")
                print(f"   Context: {q.context}")
                if q.options:
                    print(f"   Options: {', '.join(q.options[:5])}")
        else:
            print(f" No clarification needed (Confidence: {result.confidence:.1%})")
            if result.suggested_intent:
                print(f"   Suggested intent: {result.suggested_intent.get('query_type', 'unknown')}")


def example_integration_with_planning():
    """Example of integrating clarification into planning plane."""
    
    from backend.planes.planning import PlanningPlane, PlanningResult
    
    metadata = MetadataProvider.load()
    llm_provider = LLMProvider()
    
    # Create clarification agent
    clarification_agent = ClarificationAgent(
        llm_provider=llm_provider,
        metadata=metadata
    )
    
    # Create enforcer with clarification mode
    enforcer = FailOpenEnforcer(
        clarification_mode=True,
        clarification_agent=clarification_agent
    )
    
    # Modified planning plane
    class PlanningPlaneWithClarification(PlanningPlane):
        def plan_query(self, user_query: str, context: dict):
            planning_id = self._generate_planning_id()
            
            # Step 1: Extract intent (or get partial intent)
            # ... intent extraction ...
            intent = {}  # Placeholder
            
            # Step 2: Check if clarification is needed
            clarification_questions = enforcer.get_clarification_questions(
                user_query, intent, metadata
            )
            
            if clarification_questions:
                # Return clarification response
                return PlanningResult(
                    success=False,
                    planning_id=planning_id,
                    error="Query needs clarification",
                    steps=[{
                        'step': 'clarification_needed',
                        'questions': clarification_questions
                    }]
                )
            
            # Step 3: Continue with normal planning
            # ... rest of planning logic ...
            
            return PlanningResult(
                success=True,
                planning_id=planning_id,
                # ... other fields ...
            )
    
    return PlanningPlaneWithClarification()


def example_api_response():
    """Example API response format for clarification."""
    
    clarification_agent = ClarificationAgent()
    
    query = "show me customers"
    response = clarification_agent.generate_clarification_response(query)
    
    # Response format:
    """
    {
        "success": false,
        "needs_clarification": true,
        "confidence": 0.6,
        "query": "show me customers",
        "clarification": {
            "message": "I need a bit more information to understand your query. Please answer these 2 question(s):",
            "questions": [
                {
                    "question": "What would you like to see about customers?",
                    "context": "Query is vague - need to know what metric or information",
                    "field": "metric",
                    "options": ["total_customers", "customer_count", "revenue_by_customer"],
                    "required": true
                },
                {
                    "question": "What time period are you interested in?",
                    "context": "No time range specified",
                    "field": "time_range",
                    "options": ["last 7 days", "last 30 days", "last 90 days", "all time"],
                    "required": false
                }
            ]
        },
        "suggested_intent": {
            "query_type": "relational",
            "base_table": "customers",
            ...
        }
    }
    """
    
    return response


if __name__ == "__main__":
    example_clarification_flow()

