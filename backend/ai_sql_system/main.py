"""
Main Entry Point - Run the AI SQL System
"""
import uvicorn
import os
from .api.routes import app

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
