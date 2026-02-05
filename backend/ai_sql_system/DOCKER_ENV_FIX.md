# Docker Environment Variable Fix

## Issue Found
Docker container shows `OPENAI_API_KEY=` (empty), meaning the `.env` file isn't being loaded correctly by Docker Compose.

## Fixes Applied

### 1. Updated docker-compose.yml
- Added explicit `OPENAI_API_KEY=${OPENAI_API_KEY}` in environment section
- Added `OPENAI_MODEL=${OPENAI_MODEL:-gpt-4}` to pass model from .env
- Added volume mount: `../.env:/app/.env:ro` so .env file is accessible in container

### 2. Updated app_production.py
- Enhanced .env loading to check multiple paths:
  - `/app/.env` (Docker container path)
  - Project root `.env` (local development)
  - Current directory `.env`
- Added better error logging to show what's happening

## Restart Command

```bash
cd /Users/niyathnair/Desktop/RCA-ENGINE/RCA-Engine
docker compose -f docker/docker-compose.yml down
docker compose -f docker/docker-compose.yml up --build -d
```

## Verify After Restart

### Check Environment Variable in Container
```bash
docker compose -f docker/docker-compose.yml exec backend env | grep OPENAI_API_KEY
```

Should show:
```
OPENAI_API_KEY=sk-proj-...YOUR_API_KEY_HERE
```

### Check .env File in Container
```bash
docker compose -f docker/docker-compose.yml exec backend cat /app/.env | grep OPENAI_API_KEY
```

### Check Logs
```bash
docker compose -f docker/docker-compose.yml logs backend | grep -i "api key\|loaded .env"
```

## Test Query

```bash
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
  }'
```

## What Changed

1. **docker-compose.yml**:
   - Added volume mount for .env file
   - Explicitly pass OPENAI_API_KEY from host environment
   - Added OPENAI_MODEL environment variable

2. **app_production.py**:
   - Enhanced .env loading with multiple fallback paths
   - Better error messages showing what was checked
   - Explicitly sets environment variables

**After restart, the API key should be available!**
