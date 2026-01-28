# Permissive Mode: Cursor-like Vague Query Handling

## Overview

By default, Spyne IDE uses **fail-closed** validation that rejects ambiguous queries. However, you can enable **permissive mode** (fail-open) that handles vague queries like Cursor - making reasonable assumptions and warning users instead of failing.

## Current Behavior (Fail-Closed)

**Default:** `FailClosedEnforcer` rejects ambiguous queries:
- ❌ "Show me customers" → Rejected (no metric specified)
- ❌ "Total revenue" → Rejected (no time range)
- ❌ "Orders by region" → Rejected (ambiguous metric)

## Permissive Mode (Fail-Open)

**New:** `FailOpenEnforcer` interprets vague queries:
- ✅ "Show me customers" → Assumes relational query, shows customer records
- ✅ "Total revenue" → Assumes metric query, uses default time range (last 30 days)
- ✅ "Orders by region" → Infers metric from context, groups by region

## How It Works

### 1. Intent Inference

Instead of rejecting ambiguous queries, the system:
- **Infers metrics** from query keywords ("total", "sum", "revenue")
- **Assumes query type** (relational vs metric) from context
- **Applies defaults** (e.g., "last 30 days" for time ranges)
- **Chooses most relevant tables** based on keywords

### 2. Warnings Instead of Errors

Permissive mode returns **warnings** instead of errors:
```json
{
  "valid": true,
  "warnings": [
    "No metric specified - assuming relational query",
    "No time range specified - using default: 'last 30 days'"
  ],
  "assumptions": [
    "Query type inferred as 'relational'",
    "Applied default time range: last 30 days"
  ]
}
```

### 3. LLM Prompt Enhancement

The LLM prompt now includes:
```
HANDLING VAGUE/AMBIGUOUS QUERIES (Cursor-like behavior):
- When a query is vague or ambiguous, MAKE REASONABLE ASSUMPTIONS rather than failing
- Use context clues from the query to infer intent
- Document your assumptions in the "reasoning" field
- Prefer to generate something reasonable with warnings rather than rejecting
```

## Usage

### Enable Permissive Mode

```python
from backend.invariants.fail_open import FailOpenEnforcer

# Replace FailClosedEnforcer with FailOpenEnforcer
enforcer = FailOpenEnforcer(
    known_metrics=["revenue", "sales", "orders"],
    default_time_range="last 30 days"
)

# Validate intent (always returns valid=True with warnings)
result = enforcer.validate_query_intent(intent, query_text="show me customers")
if result.valid:
    print("Warnings:", result.warnings)
    print("Assumptions:", result.assumptions)
```

### Integration Example

```python
# In your planning plane
from backend.invariants.fail_open import FailOpenEnforcer

class PlanningPlane:
    def __init__(self, permissive_mode: bool = False):
        if permissive_mode:
            self.enforcer = FailOpenEnforcer()
        else:
            from backend.invariants.fail_closed import FailClosedEnforcer
            self.enforcer = FailClosedEnforcer()
    
    def plan_query(self, user_query: str, context: Dict[str, Any]):
        # ... generate intent ...
        
        # Validate with permissive mode
        validation = self.enforcer.validate_query_intent(intent, query_text=user_query)
        
        if not validation.valid:
            return PlanningResult(success=False, error=validation.error)
        
        # Include warnings in response
        if validation.warnings:
            # Add warnings to result
            pass
```

## Examples

### Example 1: Vague Query
**Query:** "show me customers"

**Fail-Closed:** ❌ Rejected - "Ambiguous intent: metric not specified"

**Permissive Mode:** ✅ Accepted
- Assumes relational query
- Shows customer records
- Warning: "No metric specified - assuming relational query"

### Example 2: Missing Time Range
**Query:** "total revenue"

**Fail-Closed:** ❌ Rejected - "Ambiguous intent: time range or aggregation required"

**Permissive Mode:** ✅ Accepted
- Infers metric query from "total"
- Uses default time range: "last 30 days"
- Warning: "No time range specified - using default: 'last 30 days'"

### Example 3: Ambiguous Table
**Query:** "orders by region"

**Fail-Closed:** ❌ Rejected - "Ambiguous intent: metric not specified"

**Permissive Mode:** ✅ Accepted
- Infers metric query from "by region" (grouping)
- Chooses most relevant metric (e.g., "total_orders")
- Groups by region dimension
- Warning: "Metric inferred from context - verify correctness"

## Safety Considerations

Permissive mode still blocks:
- ❌ **Dangerous operations**: DROP TABLE, TRUNCATE, DELETE without WHERE
- ❌ **Unsafe SQL**: ALTER TABLE, unbounded updates

Permissive mode warns but allows:
- ⚠️ **Complex queries**: Many joins (>10), many subqueries (>5)
- ⚠️ **Unknown metrics**: Proceeds but warns
- ⚠️ **Access issues**: Warns about potentially inaccessible tables/columns

## Configuration

### Environment Variable

```bash
# Enable permissive mode globally
export SPYNE_PERMISSIVE_MODE=true
```

### Code Configuration

```python
# In your application initialization
import os
from backend.invariants.fail_open import FailOpenEnforcer
from backend.invariants.fail_closed import FailClosedEnforcer

permissive = os.getenv("SPYNE_PERMISSIVE_MODE", "false").lower() == "true"
enforcer = FailOpenEnforcer() if permissive else FailClosedEnforcer()
```

## Comparison: Fail-Closed vs Fail-Open

| Aspect | Fail-Closed | Fail-Open (Permissive) |
|--------|-------------|------------------------|
| **Ambiguous queries** | ❌ Rejects | ✅ Interprets with warnings |
| **Missing metrics** | ❌ Error | ⚠️ Infers from context |
| **Missing time range** | ❌ Error | ⚠️ Uses default |
| **Unknown metrics** | ❌ Error | ⚠️ Warns but allows |
| **Complex queries** | ❌ Blocks (>4 joins) | ⚠️ Warns but allows (>10 joins) |
| **Dangerous SQL** | ❌ Blocks | ❌ Still blocks |
| **User experience** | Strict, safe | Flexible, Cursor-like |

## Best Practices

1. **Use permissive mode** for:
   - Interactive/exploratory queries
   - User-facing applications
   - When you want Cursor-like behavior

2. **Use fail-closed mode** for:
   - Production critical queries
   - Automated systems
   - When strict validation is required

3. **Always check warnings** in permissive mode to understand assumptions made

4. **Monitor assumptions** - log them for analysis and improvement

## Proactive Clarification Mode (Best of Both Worlds)

**NEW:** Instead of making assumptions OR rejecting, you can **ask clarifying questions** proactively!

### How It Works

The `ClarificationAgent` detects ambiguities and asks intelligent questions BEFORE proceeding:

```python
from backend.planning.clarification_agent import ClarificationAgent
from backend.invariants.fail_open import FailOpenEnforcer

# Create clarification agent
clarification_agent = ClarificationAgent(
    llm_provider=llm_provider,
    metadata=metadata
)

# Use with fail-open enforcer in clarification mode
enforcer = FailOpenEnforcer(
    clarification_mode=True,  # Enable clarification mode
    clarification_agent=clarification_agent
)
```

### Example: Proactive Questions

**Query:** "show me customers"

**Response:**
```json
{
  "success": false,
  "needs_clarification": true,
  "confidence": 0.6,
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

### What Gets Detected

The clarification agent detects:
1. **Missing metrics** - "What metric do you want?"
2. **Ambiguous tables** - "Which table: customers or customer_orders?"
3. **Missing time ranges** - "What time period?"
4. **Missing dimensions** - "How should results be grouped?"
5. **Ambiguous filters** - "What filters do you want?"

### LLM-Powered Questions

When an LLM provider is available, questions are:
- **Natural and conversational** (not technical)
- **Context-aware** (uses available tables/metrics)
- **Specific** (asks exactly what's missing)
- **Helpful** (offers options when available)

### Integration Example

```python
from backend.planning.clarification_agent import ClarificationAgent
from backend.planes.planning import PlanningPlane

class PlanningPlaneWithClarification(PlanningPlane):
    def __init__(self):
        super().__init__()
        self.clarification_agent = ClarificationAgent(
            llm_provider=self.llm_provider,
            metadata=self.metadata
        )
    
    def plan_query(self, user_query: str, context: dict):
        # Extract intent
        intent = self.extract_intent(user_query)
        
        # Check for clarification needs
        clarification_result = self.clarification_agent.analyze_query(
            user_query, intent, self.metadata
        )
        
        if clarification_result.needs_clarification:
            # Return clarification response
            return {
                "success": False,
                "needs_clarification": True,
                "questions": [q.to_dict() for q in clarification_result.questions],
                "suggested_intent": clarification_result.suggested_intent
            }
        
        # Continue with normal planning...
```

### Comparison: Three Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| **Fail-Closed** | ❌ Rejects ambiguous queries | Production, strict validation |
| **Fail-Open (Assumption)** | ⚠️ Makes assumptions, warns | Quick exploration, Cursor-like |
| **Fail-Open (Clarification)** | ❓ Asks questions proactively | **Best UX - interactive, accurate** |

### When to Use Each Mode

1. **Fail-Closed**: Production systems, automated queries, strict compliance
2. **Fail-Open (Assumption)**: Quick exploration, prototyping, when speed > accuracy
3. **Fail-Open (Clarification)**: **User-facing applications, when accuracy matters, best UX**

## Future Enhancements

- [x] Proactive clarification questions ✅
- [ ] Learn from user corrections to improve inference
- [ ] Confidence scoring for assumptions
- [ ] User preference learning (what defaults they prefer)
- [ ] Multi-turn clarification (follow-up questions)

