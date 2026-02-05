"""
API Routes - FastAPI endpoints for the AI SQL system
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import logging
import os

from ..orchestration.graph import LangGraphOrchestrator
from ..trino.client import TrinoClient
from ..trino.validator import TrinoValidator
from ..metadata.semantic_registry import SemanticRegistry
from ..planning.join_graph import JoinGraph

logger = logging.getLogger(__name__)

app = FastAPI(title="AI SQL System", version="1.0.0")

# Configure logging
logging.basicConfig(level=logging.INFO)


class QueryRequest(BaseModel):
    """Request model for query endpoint"""
    query: str
    trino_host: Optional[str] = None
    trino_port: Optional[int] = None


class QueryResponse(BaseModel):
    """Response model for query endpoint"""
    success: bool
    sql: str
    intent: Optional[Dict[str, Any]] = None
    resolution: Optional[Dict[str, Any]] = None
    query_plan: Optional[Dict[str, Any]] = None
    validation_errors: Optional[list] = None
    error: Optional[str] = None


# Initialize orchestrator (lazy initialization)
_orchestrator: Optional[LangGraphOrchestrator] = None


def get_orchestrator() -> LangGraphOrchestrator:
    """Get or create orchestrator instance"""
    global _orchestrator
    
    if _orchestrator is None:
        # Initialize Trino client and validator
        trino_client = TrinoClient()
        trino_validator = TrinoValidator(trino_client)
        
        # Initialize semantic registry and join graph
        semantic_registry = SemanticRegistry()
        join_graph = JoinGraph()
        
        # Build join graph from metadata
        try:
            from pathlib import Path
            import json
            
            # Load lineage from metadata
            metadata_dir = Path(__file__).parent.parent.parent.parent / "metadata"
            lineage_file = metadata_dir / "lineage.json"
            
            if lineage_file.exists():
                with open(lineage_file, 'r') as f:
                    lineage_data = json.load(f)
                
                # Build join graph from lineage edges
                for edge in lineage_data.get('edges', []):
                    from_table = edge.get('from', '').split('.')[-1]  # Extract table name
                    to_table = edge.get('to', '').split('.')[-1]
                    condition = edge.get('on', '')
                    join_type = 'LEFT'  # Default
                    
                    if from_table and to_table and condition:
                        join_graph.add_join(from_table, to_table, condition, join_type)
                
                logger.info(f"Loaded {len(lineage_data.get('edges', []))} join relationships")
        except Exception as e:
            logger.warning(f"Could not load join graph from metadata: {e}")
        
        _orchestrator = LangGraphOrchestrator(
            trino_validator=trino_validator,
            semantic_registry=semantic_registry,
            join_graph=join_graph
        )
    
    return _orchestrator


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "ai_sql_system"}


@app.post("/api/query", response_model=QueryResponse)
async def generate_sql(request: QueryRequest):
    """
    Generate SQL from natural language query
    
    This is the main endpoint that runs the complete pipeline.
    """
    try:
        orchestrator = get_orchestrator()
        
        # Run pipeline
        result = orchestrator.run(request.query)
        
        if result["success"]:
            return QueryResponse(
                success=True,
                sql=result["sql"],
                intent=result.get("intent"),
                resolution=result.get("resolution"),
                query_plan=result.get("query_plan"),
                validation_errors=result.get("validation_errors")
            )
        else:
            return QueryResponse(
                success=False,
                sql="",
                error=result.get("error", "Unknown error")
            )
            
    except Exception as e:
        logger.error(f"Error in /api/query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/metrics")
async def get_metrics():
    """Get available metrics"""
    try:
        orchestrator = get_orchestrator()
        metrics = orchestrator.semantic_registry.get_metrics()
        return {"metrics": metrics}
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tables")
async def get_tables():
    """Get available tables"""
    try:
        orchestrator = get_orchestrator()
        tables = orchestrator.semantic_registry.get_tables()
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error getting tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))
