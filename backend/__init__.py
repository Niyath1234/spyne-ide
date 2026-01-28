"""
Spyne IDE Backend Module

A production-grade SQL query generation system using LLM and metadata.
"""

__version__ = "1.0.0"

# Core exports
from backend.metadata_provider import MetadataProvider
from backend.llm_query_generator import LLMQueryGenerator, ContextBundle

__all__ = [
    "MetadataProvider",
    "LLMQueryGenerator",
    "ContextBundle",
]

