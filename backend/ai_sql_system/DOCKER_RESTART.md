# Docker Restart Guide

## Command to Run

From the project root (`/Users/niyathnair/Desktop/RCA-ENGINE/RCA-Engine`):

```bash
docker compose -f docker/docker-compose.yml up --build -d
```

**Note**: Add `-d` flag to run in detached mode (background).

## What This Does

1. **`-f docker/docker-compose.yml`** - Specifies the compose file location
2. **`--build`** - Rebuilds Docker images with latest code changes
3. **`-d`** - Runs in detached mode (optional, but recommended)

## Full Command Options

### Option 1: Rebuild and Start (Recommended)
```bash
cd /Users/niyathnair/Desktop/RCA-ENGINE/RCA-Engine
docker compose -f docker/docker-compose.yml up --build -d
```

### Option 2: Just Restart (Faster, if no code changes)
```bash
docker compose -f docker/docker-compose.yml restart backend
```

### Option 3: Stop, Rebuild, Start (Clean restart)
```bash
docker compose -f docker/docker-compose.yml down
docker compose -f docker/docker-compose.yml up --build -d
```

## Verify It's Working

### Check Logs
```bash
docker compose -f docker/docker-compose.yml logs backend | tail -20
```

Look for:
- `✓ Loaded .env file from /app/.env`
- `✓ OPENAI_API_KEY loaded`
- `Starting RCA Engine on 0.0.0.0:8080`

### Check Health
```bash
curl http://localhost:8080/api/v1/health
```

### Test Query
```bash
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
  }'
```

## Expected After Restart

✅ `.env` file loaded automatically
✅ `OPENAI_API_KEY` available to all modules
✅ `OPENAI_MODEL=gpt-5.2` used
✅ Intent extraction works
✅ SQL generation works

## Troubleshooting

### If containers don't start:
```bash
# Check what's wrong
docker compose -f docker/docker-compose.yml up

# Check logs
docker compose -f docker/docker-compose.yml logs
```

### If API key still not working:
```bash
# Verify .env is being loaded in container
docker compose -f docker/docker-compose.yml exec backend env | grep OPENAI_API_KEY
```

### If port conflicts:
```bash
# Stop existing containers
docker compose -f docker/docker-compose.yml down

# Then start fresh
docker compose -f docker/docker-compose.yml up --build -d
```

## Important Notes

1. **Must be in project root** - The docker-compose.yml uses `../` paths
2. **.env file location** - Should be at project root (same level as docker/ directory)
3. **Build context** - Dockerfile context is set to `..` (project root)
4. **Volume mounts** - Metadata, data, config are mounted from project root

## Your Command

Yes, `docker compose -f docker/docker-compose.yml up --build` will work!

Just make sure you're in the project root directory:
```bash
cd /Users/niyathnair/Desktop/RCA-ENGINE/RCA-Engine
docker compose -f docker/docker-compose.yml up --build -d
```
