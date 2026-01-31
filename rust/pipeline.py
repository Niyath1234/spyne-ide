"""
Document Retrieval Pipeline Orchestrator

Runs the complete pipeline: Ingest -> Chunk -> Index
Can be run manually, via cron, or via Airflow.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingest import DocumentIngester
from src.chunking import HierarchicalChunker
from src.vector_db import HybridVectorDB


class DocumentPipeline:
    """
    Orchestrates the complete document processing pipeline.
    
    Supports:
    - Manual execution
    - Cron job scheduling
    - Airflow DAG integration
    - Incremental updates (only process new/changed files)
    """
    
    def __init__(
        self,
        raw_dir: str = "data/raw",
        processed_dir: str = "data/processed",
        index_name: str = "technical-docs",
        incremental: bool = True,
        log_level: str = "INFO"
    ):
        """
        Initialize the pipeline.
        
        Args:
            raw_dir: Directory containing raw documents
            processed_dir: Directory for processed files
            index_name: Pinecone index name
            incremental: Only process new/changed files
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.index_name = index_name
        self.incremental = incremental
        
        # Setup logging
        self._setup_logging(log_level)
        
        # State file for tracking processed files
        self.state_file = self.processed_dir / ".pipeline_state.json"
        self.state = self._load_state()
    
    def _setup_logging(self, log_level: str):
        """Setup logging configuration."""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Pipeline initialized. Logging to {log_file}")
    
    def _load_state(self) -> Dict:
        """Load pipeline state (tracked files)."""
        if self.state_file.exists():
            try:
                return json.loads(self.state_file.read_text(encoding='utf-8'))
            except Exception as e:
                self.logger.warning(f"Could not load state file: {e}")
        return {
            "processed_files": {},
            "last_run": None,
            "total_runs": 0
        }
    
    def _save_state(self):
        """Save pipeline state."""
        self.state["last_run"] = datetime.now().isoformat()
        self.state["total_runs"] = self.state.get("total_runs", 0) + 1
        
        try:
            self.state_file.write_text(
                json.dumps(self.state, indent=2),
                encoding='utf-8'
            )
        except Exception as e:
            self.logger.error(f"Could not save state file: {e}")
    
    def _should_process_file(self, file_path: Path) -> bool:
        """
        Check if file should be processed (incremental mode).
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file should be processed
        """
        if not self.incremental:
            return True
        
        file_stat = file_path.stat()
        file_key = str(file_path)
        
        if file_key not in self.state["processed_files"]:
            return True
        
        # Check if file was modified
        last_processed = self.state["processed_files"][file_key].get("last_modified")
        current_modified = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
        
        return last_processed != current_modified
    
    def _mark_file_processed(self, file_path: Path, metadata: Dict):
        """Mark file as processed in state."""
        file_key = str(file_path)
        self.state["processed_files"][file_key] = {
            "last_modified": metadata.get("last_modified"),
            "processed_at": datetime.now().isoformat(),
            "document_type": metadata.get("document_type"),
            "file_size": metadata.get("file_size")
        }
    
    def run_ingestion(self, force: bool = False) -> Dict:
        """
        Run document ingestion step.
        
        Args:
            force: Force reprocessing of all files
            
        Returns:
            Dictionary with ingestion results
        """
        self.logger.info("="*60)
        self.logger.info("STEP 1: Document Ingestion")
        self.logger.info("="*60)
        
        try:
            ingester = DocumentIngester(
                raw_dir=str(self.raw_dir),
                processed_dir=str(self.processed_dir)
            )
            
            # Find files to process
            pdf_files = list(self.raw_dir.glob("*.pdf"))
            docx_files = list(self.raw_dir.glob("*.docx"))
            all_files = pdf_files + docx_files
            
            if not all_files:
                self.logger.warning(f"No PDF/Docx files found in {self.raw_dir}")
                return {"success": True, "processed": 0, "skipped": 0}
            
            # Filter files if incremental
            if not force:
                files_to_process = [
                    f for f in all_files
                    if self._should_process_file(f)
                ]
                skipped = len(all_files) - len(files_to_process)
            else:
                files_to_process = all_files
                skipped = 0
            
            self.logger.info(f"Found {len(all_files)} files total")
            self.logger.info(f"Processing {len(files_to_process)} files, skipping {skipped}")
            
            # Process files
            processed_count = 0
            errors = []
            
            for file_path in files_to_process:
                try:
                    self.logger.info(f"Processing {file_path.name}...")
                    result = ingester.process_file(file_path)
                    
                    if result.get("success"):
                        processed_count += 1
                        self._mark_file_processed(file_path, result.get("metadata", {}))
                        self.logger.info(f"✓ Processed {file_path.name}")
                    else:
                        errors.append(f"{file_path.name}: {result.get('error', 'Unknown error')}")
                
                except Exception as e:
                    error_msg = f"{file_path.name}: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error(f"✗ Error processing {file_path.name}: {e}")
            
            result = {
                "success": len(errors) == 0,
                "processed": processed_count,
                "skipped": skipped,
                "errors": errors
            }
            
            self.logger.info(f"Ingestion complete: {processed_count} processed, {skipped} skipped")
            if errors:
                self.logger.warning(f"Errors: {len(errors)}")
            
            return result
        
        except Exception as e:
            self.logger.error(f"Ingestion failed: {e}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    def run_chunking(self, force: bool = False) -> Dict:
        """
        Run document chunking step.
        
        Args:
            force: Force rechunking of all files
            
        Returns:
            Dictionary with chunking results
        """
        self.logger.info("="*60)
        self.logger.info("STEP 2: Hierarchical Chunking")
        self.logger.info("="*60)
        
        try:
            chunker = HierarchicalChunker(processed_dir=str(self.processed_dir))
            
            # Find markdown files
            md_files = list(self.processed_dir.glob("*.md"))
            
            if not md_files:
                self.logger.warning(f"No markdown files found in {self.processed_dir}")
                return {"success": True, "chunks": 0}
            
            self.logger.info(f"Chunking {len(md_files)} markdown files...")
            
            # Chunk all files
            all_nodes = []
            for file_path in md_files:
                try:
                    nodes = chunker.chunk_file(file_path)
                    all_nodes.extend(nodes)
                except Exception as e:
                    self.logger.error(f"Error chunking {file_path.name}: {e}")
            
            # Save chunks
            chunks_file = self.processed_dir / "chunks.json"
            chunker.save_chunks(all_nodes, str(chunks_file))
            
            # Get summary
            summary = chunker.get_chunk_summary(all_nodes)
            
            self.logger.info(f"Chunking complete: {len(all_nodes)} chunks created")
            self.logger.info(f"Summary: {summary}")
            
            return {
                "success": True,
                "chunks": len(all_nodes),
                "summary": summary,
                "chunks_file": str(chunks_file)
            }
        
        except Exception as e:
            self.logger.error(f"Chunking failed: {e}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    def run_indexing(self, chunks_file: Optional[str] = None) -> Dict:
        """
        Run document indexing step.
        
        Args:
            chunks_file: Path to chunks JSON file (auto-detect if None)
            
        Returns:
            Dictionary with indexing results
        """
        self.logger.info("="*60)
        self.logger.info("STEP 3: Vector Indexing")
        self.logger.info("="*60)
        
        try:
            # Find chunks file
            if chunks_file is None:
                chunks_file = self.processed_dir / "chunks.json"
            else:
                chunks_file = Path(chunks_file)
            
            if not chunks_file.exists():
                self.logger.error(f"Chunks file not found: {chunks_file}")
                return {"success": False, "error": "Chunks file not found"}
            
            # Load chunks
            self.logger.info(f"Loading chunks from {chunks_file}...")
            chunks_data = json.loads(chunks_file.read_text(encoding='utf-8'))
            
            # Convert to nodes
            from llama_index.core.schema import TextNode
            nodes = []
            for chunk_data in chunks_data:
                node = TextNode(
                    text=chunk_data.get("text", ""),
                    metadata=chunk_data.get("metadata", {})
                )
                nodes.append(node)
            
            self.logger.info(f"Loaded {len(nodes)} chunks")
            
            # Initialize vector DB
            vector_db = HybridVectorDB(index_name=self.index_name)
            
            # Upsert chunks
            self.logger.info(f"Upserting to index '{self.index_name}'...")
            vector_db.upsert_chunks(nodes)
            
            self.logger.info(f"Indexing complete: {len(nodes)} chunks indexed")
            
            return {
                "success": True,
                "indexed": len(nodes),
                "index_name": self.index_name
            }
        
        except Exception as e:
            self.logger.error(f"Indexing failed: {e}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    def run_full_pipeline(self, force: bool = False) -> Dict:
        """
        Run the complete pipeline: Ingest -> Chunk -> Index.
        
        Args:
            force: Force reprocessing of all files
            
        Returns:
            Dictionary with pipeline results
        """
        start_time = datetime.now()
        self.logger.info("="*60)
        self.logger.info("STARTING DOCUMENT RETRIEVAL PIPELINE")
        self.logger.info("="*60)
        self.logger.info(f"Start time: {start_time.isoformat()}")
        self.logger.info(f"Incremental mode: {self.incremental}")
        self.logger.info(f"Force reprocess: {force}")
        
        results = {
            "start_time": start_time.isoformat(),
            "steps": {}
        }
        
        # Step 1: Ingestion
        ingestion_result = self.run_ingestion(force=force)
        results["steps"]["ingestion"] = ingestion_result
        
        if not ingestion_result.get("success") and ingestion_result.get("processed", 0) == 0:
            self.logger.error("Ingestion failed or no files processed. Stopping pipeline.")
            results["success"] = False
            return results
        
        # Step 2: Chunking
        chunking_result = self.run_chunking(force=force)
        results["steps"]["chunking"] = chunking_result
        
        if not chunking_result.get("success"):
            self.logger.error("Chunking failed. Stopping pipeline.")
            results["success"] = False
            return results
        
        # Step 3: Indexing
        chunks_file = chunking_result.get("chunks_file")
        indexing_result = self.run_indexing(chunks_file=chunks_file)
        results["steps"]["indexing"] = indexing_result
        
        if not indexing_result.get("success"):
            self.logger.error("Indexing failed.")
            results["success"] = False
            return results
        
        # Save state
        self._save_state()
        
        # Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        results["success"] = True
        results["end_time"] = end_time.isoformat()
        results["duration_seconds"] = duration
        
        self.logger.info("="*60)
        self.logger.info("PIPELINE COMPLETE")
        self.logger.info("="*60)
        self.logger.info(f"Duration: {duration:.2f} seconds")
        self.logger.info(f"Processed: {ingestion_result.get('processed', 0)} files")
        self.logger.info(f"Chunks: {chunking_result.get('chunks', 0)}")
        self.logger.info(f"Indexed: {indexing_result.get('indexed', 0)} chunks")
        
        return results


def main():
    """Main entry point for pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Document Retrieval Pipeline")
    parser.add_argument("--step", choices=["ingest", "chunk", "index", "all"], default="all",
                       help="Which step to run")
    parser.add_argument("--force", action="store_true",
                       help="Force reprocessing of all files")
    parser.add_argument("--incremental", action="store_true", default=True,
                       help="Only process new/changed files")
    parser.add_argument("--raw-dir", type=str, default="data/raw",
                       help="Raw documents directory")
    parser.add_argument("--processed-dir", type=str, default="data/processed",
                       help="Processed documents directory")
    parser.add_argument("--index-name", type=str, default="technical-docs",
                       help="Pinecone index name")
    parser.add_argument("--log-level", type=str, default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    
    args = parser.parse_args()
    
    pipeline = DocumentPipeline(
        raw_dir=args.raw_dir,
        processed_dir=args.processed_dir,
        index_name=args.index_name,
        incremental=args.incremental,
        log_level=args.log_level
    )
    
    if args.step == "ingest":
        result = pipeline.run_ingestion(force=args.force)
    elif args.step == "chunk":
        result = pipeline.run_chunking(force=args.force)
    elif args.step == "index":
        result = pipeline.run_indexing()
    else:  # all
        result = pipeline.run_full_pipeline(force=args.force)
    
    # Exit with error code if failed
    if not result.get("success", False):
        sys.exit(1)


if __name__ == "__main__":
    main()





