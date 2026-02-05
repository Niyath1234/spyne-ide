# Debugging API Key Issue

## Current Status
- ✅ .env file exists with `OPENAI_API_KEY`
- ✅ Added dotenv loading to app_production.py
- ❌ API key still not detected ("OpenAI API key not configured")

## Why This Is Happening

The server **must be restarted** for the changes to take effect. The current running server:
1. Was started before the dotenv loading code was added
2. Doesn't have the .env file loaded in memory
3. Needs to be restarted to load the new code

## Solution: Restart the Server

### Option 1: Docker Compose (Recommended)
```bash
cd /Users/niyathnair/Desktop/RCA-ENGINE/RCA-Engine
docker-compose restart backend
```

### Option 2: Full Docker Restart
```bash
docker-compose down
docker-compose up -d
```

### Option 3: If Running Flask Directly
```bash
# Stop the server (Ctrl+C)
# Then restart:
python backend/app_production.py
```

## Verify .env is Loaded

After restart, check the logs for:
```
✓ Loaded .env file from /path/to/.env (API key length: XXX)
```

Or check startup logs:
```bash
docker-compose logs backend | grep -i "loaded .env\|api key"
```

## Test After Restart

```bash
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
  }'
```

Expected after restart:
- ✅ Intent extraction should work (metric: "discount", grain: "customer")
- ✅ SQL should be generated
- ✅ No "API key not configured" error

## If Still Not Working After Restart

1. **Check Docker Environment Variables**
   - Docker might be overriding .env
   - Check `docker-compose.yml` for env_file or environment sections

2. **Verify .env File Location**
   - Should be at: `/Users/niyathnair/Desktop/RCA-ENGINE/RCA-Engine/.env`
   - Check if file is readable

3. **Check Logs**
   ```bash
   docker-compose logs backend | tail -50
   ```
   Look for "Loaded .env file" message

4. **Manual Test**
   ```bash
   python3 -c "from dotenv import load_dotenv; import os; load_dotenv('.env'); print('API Key:', 'SET' if os.getenv('OPENAI_API_KEY') else 'NOT SET')"
   ```

## Current Code Changes

The code now:
1. Loads .env at the very top of app_production.py
2. Reloads .env in the endpoint handler (to be safe)
3. Verifies API key is loaded before proceeding
4. Logs API key status for debugging

**The server just needs to be restarted to pick up these changes.**
