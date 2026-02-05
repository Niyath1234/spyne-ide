# API Key Verification

## Your API Key (from .env)
```
sk-proj-...YOUR_API_KEY_HERE
```

## ✅ Code Updates Applied

1. **.env Loading** - Loads at startup, reloads in endpoint handler
2. **API Key Setting** - Explicitly sets `os.environ['OPENAI_API_KEY']`
3. **Model Support** - Uses `OPENAI_MODEL=gpt-5.2` from your .env
4. **All Modules** - Every LLM module checks `os.getenv('OPENAI_API_KEY')`

## Where API Key is Used

### ✅ IntentEngine
- File: `planning/intent_engine.py`
- Line 23: `api_key = os.getenv('OPENAI_API_KEY')`
- Uses: Intent extraction

### ✅ ResolutionEngine  
- File: `planning/resolution_engine.py`
- Line 31: `api_key = os.getenv('OPENAI_API_KEY')`
- Uses: Query classification

### ✅ QueryPlanner
- File: `planning/query_planner.py`
- Line 23: `api_key = os.getenv('OPENAI_API_KEY')`
- Uses: Query plan building

### ✅ SQLGenerator
- File: `sql/generator.py`
- Line 22: `api_key = os.getenv('OPENAI_API_KEY')`
- Uses: SQL generation

### ✅ SQLCritic
- File: `sql/critic.py`
- Line 22: `api_key = os.getenv('OPENAI_API_KEY')`
- Uses: SQL self-correction

### ✅ SemanticRetriever (Optional)
- File: `retrieval/semantic_search.py`
- Line 36: `openai_key = os.getenv('OPENAI_API_KEY')`
- Uses: Embeddings (falls back to sentence-transformers)

## Model Configuration

Your `.env` has:
```
OPENAI_MODEL=gpt-5.2
```

All modules check in this order:
1. `OPENAI_MODEL` ✅ (your .env)
2. `LLM_MODEL`
3. `RCA_LLM_MODEL`
4. Default: `gpt-4`

## Restart Command

```bash
cd /Users/niyathnair/Desktop/RCA-ENGINE/RCA-Engine
docker compose -f docker/docker-compose.yml up --build -d
```

## Verify After Restart

```bash
# Check logs
docker compose -f docker/docker-compose.yml logs backend | grep -i "api key\|loaded .env"

# Should see:
# ✓ Loaded .env file from /app/.env
#   - OPENAI_API_KEY: ********************... (length: XXX)
#   - OPENAI_MODEL: gpt-5.2
```

## Test Query

```bash
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
  }'
```

**Expected**: Should generate SQL successfully with gpt-5.2 model!
