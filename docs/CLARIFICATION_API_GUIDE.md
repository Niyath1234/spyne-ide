# Clarification API Quick Reference

## Overview

The clarification system provides endpoints to handle ambiguous queries by asking clarifying questions and resolving them with user answers.

## Endpoints

### 1. Analyze Query for Clarification

**Endpoint:** `POST /api/clarification/analyze`

**Request:**
```json
{
  "query": "show me customers",
  "use_llm": true
}
```

**Response (Needs Clarification):**
```json
{
  "success": true,
  "needs_clarification": true,
  "confidence": 0.6,
  "query": "show me customers",
  "questions": [
    {
      "question": "What would you like to see about customers?",
      "context": "Query is vague - need to know what metric or information",
      "field": "metric",
      "options": ["revenue", "total_customers", "customer_count"],
      "required": true
    },
    {
      "question": "What time period are you interested in?",
      "context": "No time range specified",
      "field": "time_range",
      "options": ["last 7 days", "last 30 days", "last 90 days", "all time"],
      "required": false
    }
  ],
  "suggested_intent": {
    "query_type": "relational",
    "base_table": "customers"
  }
}
```

**Response (No Clarification Needed):**
```json
{
  "success": true,
  "needs_clarification": false,
  "confidence": 1.0,
  "query": "show me total revenue by region for last 30 days",
  "message": "Query is clear, no clarification needed",
  "suggested_intent": {...}
}
```

### 2. Resolve Clarified Query

**Endpoint:** `POST /api/clarification/resolve`

**Request:**
```json
{
  "query": "show me customers",
  "original_intent": {...},  // Optional, from previous clarification
  "answers": {
    "metric": "revenue",
    "time_range": "last 30 days",
    "dimensions": ["region"]
  },
  "use_llm": true
}
```

**Response (Success):**
```json
{
  "success": true,
  "resolved_intent": {
    "query_type": "metric",
    "metric": {
      "name": "revenue",
      "base_table": "orders",
      "sql_expression": "SUM(orders.amount)"
    },
    "time_range": "last 30 days",
    "time_context": {
      "type": "relative",
      "value": 30,
      "unit": "day"
    },
    "dimensions": [{"name": "region", "base_table": "customers"}],
    "group_by": ["region"]
  },
  "clarified_query": "show me customers | metric: revenue | time range: last 30 days",
  "answers": {
    "metric": "revenue",
    "time_range": "last 30 days",
    "dimensions": ["region"]
  },
  "sql": "SELECT region, SUM(orders.amount) AS revenue FROM customers...",
  "intent": {...},
  "reasoning_steps": [...]
}
```

**Response (Error):**
```json
{
  "success": false,
  "error": "Could not generate SQL",
  "warnings": [...]
}
```

### 3. Get Metrics

**Endpoint:** `GET /api/clarification/metrics`

**Response:**
```json
{
  "total_queries": 100,
  "clarification_needed": 25,
  "clarification_resolved": 20,
  "clarification_rate": 0.25,
  "resolution_rate": 0.8,
  "average_questions_per_query": 2.1,
  "average_confidence": 0.65,
  "average_clarification_time_ms": 150.5
}
```

### 4. Health Check

**Endpoint:** `GET /api/clarification/health`

**Response:**
```json
{
  "status": "healthy",
  "metadata_loaded": true,
  "tables_count": 15,
  "metrics": {
    "total_queries": 100,
    "clarification_needed": 25,
    ...
  }
}
```

## Complete Flow Example

### Step 1: User submits query
```bash
curl -X POST http://localhost:5000/api/agent/run \
  -H "Content-Type: application/json" \
  -d '{"query": "show me customers"}'
```

### Step 2: System returns clarification questions
```json
{
  "status": "needs_clarification",
  "clarification": {
    "questions": [...]
  }
}
```

### Step 3: Frontend displays questions, user answers

### Step 4: Frontend calls resolve endpoint
```bash
curl -X POST http://localhost:5000/api/clarification/resolve \
  -H "Content-Type: application/json" \
  -d '{
    "query": "show me customers",
    "answers": {
      "metric": "revenue",
      "time_range": "last 30 days"
    }
  }'
```

### Step 5: System returns SQL
```json
{
  "success": true,
  "sql": "SELECT ..."
}
```

## Answer Format

Answers should be provided as a dictionary with field names matching the clarification questions:

```json
{
  "metric": "revenue",                    // String: metric name
  "time_range": "last 30 days",           // String: time range
  "base_table": "customers",              // String: table name
  "dimensions": ["region", "segment"],    // Array: dimension names
  "columns": ["id", "name", "email"],     // Array: column names
  "filters": [                            // Array: filter objects
    {
      "column": "status",
      "operator": "=",
      "value": "active"
    }
  ]
}
```

## Supported Time Range Formats

- `"last 7 days"`
- `"last 30 days"`
- `"last 90 days"`
- `"2024-01-01 to 2024-01-31"`
- `"all time"`

## Error Handling

All endpoints return standard error responses:

```json
{
  "success": false,
  "error": "Error message here"
}
```

HTTP status codes:
- `200` - Success
- `400` - Bad request (missing required fields)
- `500` - Server error

## Integration with Main Query Flow

The clarification system is also integrated into the main query flow:

```bash
# This will automatically check for clarification
curl -X POST http://localhost:5000/api/agent/run \
  -H "Content-Type: application/json" \
  -d '{
    "query": "show me customers",
    "clarification_mode": true
  }'
```

If clarification is needed, it returns the same format as `/api/clarification/analyze`.

## Best Practices

1. **Always check `needs_clarification`** before proceeding
2. **Store `suggested_intent`** - useful if user wants to proceed without answering
3. **Validate answers** - ensure required fields are provided
4. **Handle errors gracefully** - show user-friendly error messages
5. **Use metrics endpoint** - monitor clarification usage and success rates

