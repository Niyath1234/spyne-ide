# .env File Loading Fix

## Issue
The `.env` file exists with `OPENAI_API_KEY` but the Flask app wasn't loading it.

## Fix Applied
Added `python-dotenv` loading at the start of `app_production.py`:

```python
from dotenv import load_dotenv
# Load .env from project root
backend_dir = Path(__file__).parent
project_root = backend_dir.parent
env_file = project_root / '.env'
if env_file.exists():
    load_dotenv(env_file)
```

## Your .env File
Located at: `/Users/niyathnair/Desktop/RCA-ENGINE/RCA-Engine/.env`

Contains:
- `OPENAI_API_KEY=sk-proj-...`
- `OPENAI_MODEL=gpt-5.2`
- Database config
- Other settings

## Next Step: Restart Server

**The server must be restarted for the .env file to be loaded:**

```bash
# If using Docker
docker-compose restart backend

# Or restart entire stack
docker-compose restart

# If running Flask directly
# Stop (Ctrl+C) and restart:
python backend/app_production.py
```

## After Restart

The system should:
1. ✅ Load `.env` file automatically
2. ✅ Read `OPENAI_API_KEY` from `.env`
3. ✅ Initialize OpenAI clients with the API key
4. ✅ Generate SQL queries successfully

## Test After Restart

```bash
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
  }'
```

Expected: Should now generate SQL with proper intent extraction and query planning.

## Verification

To verify .env is loaded, check logs on startup:
- Should see: "Loaded .env file from /path/to/.env"
- Or check: `os.getenv('OPENAI_API_KEY')` should return your key
