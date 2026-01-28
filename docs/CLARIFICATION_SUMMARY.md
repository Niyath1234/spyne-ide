# Proactive Clarification: Answer to "Can it proactively ask doubts?"

## ✅ YES! The system can now proactively ask clarifying questions

## What Was Added

### 1. **ClarificationAgent** (`backend/planning/clarification_agent.py`)
   - Detects ambiguities in queries BEFORE processing
   - Generates intelligent, context-aware questions
   - Uses LLM for natural question phrasing (when available)
   - Falls back to rule-based questions if LLM unavailable

### 2. **Enhanced FailOpenEnforcer** (`backend/invariants/fail_open.py`)
   - New `clarification_mode` parameter
   - When enabled, asks questions instead of making assumptions
   - Returns clarification questions in validation result

### 3. **LLM Prompt Enhancement** (`backend/llm_query_generator.py`)
   - Added section: "HANDLING VAGUE/AMBIGUOUS QUERIES"
   - Encourages interpretation but also documents assumptions

## How It Works

### Flow Diagram

```
User Query: "show me customers"
    ↓
ClarificationAgent.analyze_query()
    ↓
Detects Ambiguities:
  - Missing metric
  - Missing time range
  - Unclear intent
    ↓
Generates Questions:
  1. "What would you like to see about customers?"
  2. "What time period are you interested in?"
    ↓
Returns Clarification Response
    ↓
User Answers Questions
    ↓
Proceed with Complete Intent
```

## Example Usage

```python
from backend.planning.clarification_agent import ClarificationAgent
from backend.invariants.fail_open import FailOpenEnforcer
from backend.metadata_provider import MetadataProvider

# 1. Initialize
metadata = MetadataProvider.load()
clarification_agent = ClarificationAgent(
    llm_provider=llm_provider,  # Optional but recommended
    metadata=metadata
)

# 2. Analyze query
query = "show me customers"
result = clarification_agent.analyze_query(query, metadata=metadata)

# 3. Check if clarification needed
if result.needs_clarification:
    print("Questions to ask:")
    for q in result.questions:
        print(f"- {q.question}")
        if q.options:
            print(f"  Options: {', '.join(q.options)}")
```

## API Response Format

```json
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
    "base_table": "customers"
  }
}
```

## What Ambiguities Are Detected

1. **Missing Metric** - Query mentions aggregation but no specific metric
2. **Ambiguous Table** - Multiple tables match query keywords
3. **Missing Table** - No table identified from query
4. **Missing Time Range** - Metric query but no time period specified
5. **Missing Dimensions** - Query mentions grouping but no dimensions specified
6. **Ambiguous Filters** - Query mentions filtering but no filters extracted

## Three Modes Comparison

| Mode | Behavior | Example Response |
|------|----------|------------------|
| **Fail-Closed** | ❌ Rejects | "Error: Ambiguous intent: metric not specified" |
| **Fail-Open (Assumption)** | ⚠️ Assumes + Warns | "Warning: Assuming relational query" |
| **Fail-Open (Clarification)** | ❓ **Asks Questions** | **"What would you like to see about customers?"** |

## Integration Points

### 1. Planning Plane Integration

```python
class PlanningPlane:
    def __init__(self):
        self.clarification_agent = ClarificationAgent(...)
        self.enforcer = FailOpenEnforcer(
            clarification_mode=True,
            clarification_agent=self.clarification_agent
        )
    
    def plan_query(self, query: str, context: dict):
        # Extract intent
        intent = self.extract_intent(query)
        
        # Check for clarification
        questions = self.enforcer.get_clarification_questions(query, intent)
        if questions:
            return {"needs_clarification": True, "questions": questions}
        
        # Continue planning...
```

### 2. API Endpoint Integration

```python
@app.route('/api/query', methods=['POST'])
def handle_query():
    query = request.json.get('query')
    
    # Check for clarification
    clarification_agent = ClarificationAgent(...)
    result = clarification_agent.analyze_query(query)
    
    if result.needs_clarification:
        return jsonify({
            "needs_clarification": True,
            "questions": [q.to_dict() for q in result.questions]
        })
    
    # Process query...
```

## Benefits

✅ **Better UX** - Users get helpful questions instead of errors  
✅ **More Accurate** - No guessing, get exact requirements  
✅ **Context-Aware** - Questions use available tables/metrics  
✅ **Natural Language** - LLM generates conversational questions  
✅ **Flexible** - Can proceed with suggested intent if user prefers  

## Next Steps

1. **Enable clarification mode** in your planning plane
2. **Integrate with frontend** to display questions
3. **Handle user responses** to complete the query
4. **Learn from corrections** to improve future questions

See `PERMISSIVE_MODE.md` for complete documentation and `backend/planning/clarification_example.py` for code examples.

