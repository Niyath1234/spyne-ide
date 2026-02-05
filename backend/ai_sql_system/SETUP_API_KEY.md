# Setting Up OpenAI API Key

## Issue
The system is working but needs an OpenAI API key to generate SQL queries.

## Error Message
```
Error code: 401 - You didn't provide an API key
```

## Solution

### Option 1: Set Environment Variable (Recommended)

```bash
export OPENAI_API_KEY=sk-your-api-key-here
```

Then restart your server:
```bash
docker-compose restart backend
```

### Option 2: Add to Docker Compose

Edit `docker/docker-compose.yml` and add:

```yaml
services:
  backend:
    environment:
      - OPENAI_API_KEY=sk-your-api-key-here
```

Then restart:
```bash
docker-compose restart
```

### Option 3: Add to .env File

Create or edit `.env` file in project root:

```bash
OPENAI_API_KEY=sk-your-api-key-here
```

Then restart the server.

## Get API Key

1. Go to https://platform.openai.com/account/api-keys
2. Sign in or create an account
3. Click "Create new secret key"
4. Copy the key (starts with `sk-`)

## Verify It's Set

```bash
# Check if environment variable is set
echo $OPENAI_API_KEY

# Or test the API
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{"query": "test query"}'
```

If the API key is set correctly, you should see SQL generation working.

## Current Status

✅ Postgres fallback working (uses JSON files)
✅ System running without Postgres
❌ Need OpenAI API key for LLM features

Once API key is set, the system will:
- Extract intent from queries
- Generate SQL using LangGraph pipeline
- Self-correct SQL with critic loop
- Validate with Trino
