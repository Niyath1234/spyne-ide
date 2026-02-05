"""
Configuration - Environment variables and settings
"""
import os
from typing import Optional


class Config:
    """Configuration class for AI SQL System"""
    
    # Trino Configuration
    TRINO_HOST: str = os.getenv("TRINO_HOST", "localhost")
    TRINO_PORT: int = int(os.getenv("TRINO_PORT", "8080"))
    TRINO_USER: str = os.getenv("TRINO_USER", "admin")
    TRINO_CATALOG: str = os.getenv("TRINO_CATALOG", "tpch")
    TRINO_SCHEMA: str = os.getenv("TRINO_SCHEMA", "tiny")
    
    # Postgres Configuration
    POSTGRES_CONNECTION_STRING: str = os.getenv(
        "POSTGRES_CONNECTION_STRING",
        "postgresql://postgres:postgres@localhost:5432/rca_engine"
    )
    
    # LLM Configuration
    OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4")
    
    # Redis Configuration (optional)
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    
    # API Configuration
    API_PORT: int = int(os.getenv("PORT", "8000"))
    API_HOST: str = os.getenv("HOST", "0.0.0.0")
    
    # Performance Targets
    TARGET_LATENCY_MS: int = 3500
    TARGET_ACCURACY: float = 0.90
    TARGET_HALLUCINATION_RATE: float = 0.03
