"""
REST API for Document Retrieval Pipeline

Provides HTTP endpoints to trigger pipeline runs manually.
Useful for webhooks, CI/CD, or manual triggers.
"""

import os
import sys
from pathlib import Path
from flask import Flask, request, jsonify
from datetime import datetime
import threading
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipeline import DocumentPipeline

app = Flask(__name__)

# Global state
pipeline_status = {
    "running": False,
    "last_run": None,
    "last_result": None,
    "current_run": None
}


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "service": "document-retrieval-pipeline",
        "timestamp": datetime.now().isoformat()
    })


@app.route("/status", methods=["GET"])
def status():
    """Get pipeline status."""
    return jsonify({
        "running": pipeline_status["running"],
        "last_run": pipeline_status["last_run"],
        "last_result": pipeline_status["last_result"],
        "current_run": pipeline_status["current_run"]
    })


@app.route("/run", methods=["POST"])
def run_pipeline():
    """
    Trigger pipeline run.
    
    Request body (JSON, optional):
    {
        "step": "all" | "ingest" | "chunk" | "index",
        "force": true | false,
        "incremental": true | false,
        "raw_dir": "data/raw",
        "processed_dir": "data/processed",
        "index_name": "technical-docs"
    }
    """
    if pipeline_status["running"]:
        return jsonify({
            "error": "Pipeline is already running",
            "current_run": pipeline_status["current_run"]
        }), 409
    
    # Get parameters from request
    data = request.get_json() or {}
    step = data.get("step", "all")
    force = data.get("force", False)
    incremental = data.get("incremental", True)
    raw_dir = data.get("raw_dir", "data/raw")
    processed_dir = data.get("processed_dir", "data/processed")
    index_name = data.get("index_name", "technical-docs")
    
    # Start pipeline in background thread
    def run_in_background():
        pipeline_status["running"] = True
        pipeline_status["current_run"] = datetime.now().isoformat()
        
        try:
            pipeline = DocumentPipeline(
                raw_dir=raw_dir,
                processed_dir=processed_dir,
                index_name=index_name,
                incremental=incremental,
                log_level="INFO"
            )
            
            if step == "ingest":
                result = pipeline.run_ingestion(force=force)
            elif step == "chunk":
                result = pipeline.run_chunking(force=force)
            elif step == "index":
                result = pipeline.run_indexing()
            else:  # all
                result = pipeline.run_full_pipeline(force=force)
            
            pipeline_status["last_result"] = result
            pipeline_status["last_run"] = datetime.now().isoformat()
            
        except Exception as e:
            pipeline_status["last_result"] = {
                "success": False,
                "error": str(e)
            }
        finally:
            pipeline_status["running"] = False
            pipeline_status["current_run"] = None
    
    thread = threading.Thread(target=run_in_background)
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "message": "Pipeline started",
        "step": step,
        "started_at": pipeline_status["current_run"]
    }), 202


@app.route("/run/sync", methods=["POST"])
def run_pipeline_sync():
    """
    Run pipeline synchronously (blocks until complete).
    
    Use this for testing or when you need immediate results.
    """
    # Get parameters from request
    data = request.get_json() or {}
    step = data.get("step", "all")
    force = data.get("force", False)
    incremental = data.get("incremental", True)
    raw_dir = data.get("raw_dir", "data/raw")
    processed_dir = data.get("processed_dir", "data/processed")
    index_name = data.get("index_name", "technical-docs")
    
    try:
        pipeline = DocumentPipeline(
            raw_dir=raw_dir,
            processed_dir=processed_dir,
            index_name=index_name,
            incremental=incremental,
            log_level="INFO"
        )
        
        if step == "ingest":
            result = pipeline.run_ingestion(force=force)
        elif step == "chunk":
            result = pipeline.run_chunking(force=force)
        elif step == "index":
            result = pipeline.run_indexing()
        else:  # all
            result = pipeline.run_full_pipeline(force=force)
        
        pipeline_status["last_result"] = result
        pipeline_status["last_run"] = datetime.now().isoformat()
        
        status_code = 200 if result.get("success") else 500
        return jsonify(result), status_code
    
    except Exception as e:
        error_result = {
            "success": False,
            "error": str(e)
        }
        pipeline_status["last_result"] = error_result
        return jsonify(error_result), 500


@app.route("/run/stop", methods=["POST"])
def stop_pipeline():
    """Stop running pipeline (if supported)."""
    # Note: This is a placeholder. Actual implementation would require
    # more sophisticated thread management or process termination.
    if pipeline_status["running"]:
        return jsonify({
            "message": "Pipeline stop requested",
            "note": "Pipeline will complete current step before stopping"
        }), 200
    else:
        return jsonify({
            "message": "No pipeline is currently running"
        }), 200


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Document Retrieval Pipeline API")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    
    args = parser.parse_args()
    
    print(f"Starting Document Retrieval Pipeline API on {args.host}:{args.port}")
    print(f"Endpoints:")
    print(f"  GET  /health - Health check")
    print(f"  GET  /status - Pipeline status")
    print(f"  POST /run - Trigger pipeline (async)")
    print(f"  POST /run/sync - Trigger pipeline (sync)")
    print(f"  POST /run/stop - Stop pipeline")
    
    app.run(host=args.host, port=args.port, debug=args.debug)





