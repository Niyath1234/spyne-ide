"""
Structured Document Ingestion for Technical Documents (ARD/PRD/TRD)

Uses LlamaParse to convert PDF/Docx files into Markdown while preserving
tables, structure, and hierarchical relationships.
"""

import os
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime
import json

try:
    from llama_parse import LlamaParse
    from llama_index.core import Document
except ImportError:
    print("Warning: llama-parse not installed. Install with: pip install llama-parse")
    LlamaParse = None

# Import document mapper if available
try:
    from .document_mapper import DocumentMapper
except ImportError:
    try:
        from document_mapper import DocumentMapper
    except ImportError:
        DocumentMapper = None


class DocumentIngester:
    """Handles conversion of technical documents to structured Markdown."""
    
    def __init__(self, api_key: Optional[str] = None, raw_dir: str = "data/raw", processed_dir: str = "data/processed", use_mapping: bool = True):
        """
        Initialize the document ingester.
        
        Args:
            api_key: LlamaParse API key (or set LLAMA_CLOUD_API_KEY env var)
            raw_dir: Directory containing raw PDF/Docx files
            processed_dir: Directory to save processed Markdown files
            use_mapping: Whether to use reference ID mapping
        """
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize document mapper if available
        self.mapper = None
        if use_mapping and DocumentMapper:
            try:
                self.mapper = DocumentMapper()
            except Exception as e:
                print(f"Warning: Could not initialize document mapper: {e}")
        
        # Initialize LlamaParse
        if LlamaParse is None:
            raise ImportError(
                "llama-parse is required. Install with: pip install llama-parse"
            )
        
        self.parser = LlamaParse(
            api_key=api_key or os.getenv("LLAMA_CLOUD_API_KEY"),
            result_type="markdown",  # Preserves structure
            parsing_instruction="Extract all tables, headers, and hierarchical structure accurately. "
                               "Preserve requirement IDs, version numbers, and parent-child relationships.",
            num_workers=4,  # Parallel processing
            verbose=True
        )
    
    def detect_document_type(self, file_path: Path) -> str:
        """
        Detect document type from filename or content.
        
        Args:
            file_path: Path to the document
            
        Returns:
            Document type: 'ARD', 'PRD', 'TRD', or 'UNKNOWN'
        """
        filename_lower = file_path.name.lower()
        
        if 'ard' in filename_lower or 'architecture' in filename_lower:
            return 'ARD'
        elif 'prd' in filename_lower or 'product' in filename_lower:
            return 'PRD'
        elif 'trd' in filename_lower or 'technical' in filename_lower:
            return 'TRD'
        else:
            return 'UNKNOWN'
    
    def extract_metadata(self, file_path: Path, document_type: str) -> Dict:
        """
        Extract metadata from the document file.
        
        Args:
            file_path: Path to the document
            document_type: Type of document (ARD/PRD/TRD)
            
        Returns:
            Dictionary with metadata
        """
        stat = file_path.stat()
        return {
            "file_name": file_path.name,
            "file_path": str(file_path),
            "document_type": document_type,
            "file_size": stat.st_size,
            "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "ingested_at": datetime.now().isoformat()
        }
    
    def process_file(self, file_path: Path) -> Dict:
        """
        Process a single document file.
        
        Args:
            file_path: Path to the PDF/Docx file
            
        Returns:
            Dictionary with processing results and metadata
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Detect document type
        doc_type = self.detect_document_type(file_path)
        
        print(f"Processing {file_path.name} as {doc_type}...")
        
        # Parse document
        documents = self.parser.load_data(str(file_path))
        
        # Combine all pages into single markdown
        markdown_content = "\n\n".join([doc.text for doc in documents])
        
        # Save markdown to processed directory
        output_path = self.processed_dir / f"{file_path.stem}.md"
        output_path.write_text(markdown_content, encoding='utf-8')
        
        # Extract metadata
        metadata = self.extract_metadata(file_path, doc_type)
        
        # Enhance metadata with reference ID mapping if available
        if self.mapper:
            metadata = self.mapper.enhance_metadata(file_path.name, metadata)
        
        # Save metadata as JSON
        metadata_path = self.processed_dir / f"{file_path.stem}_metadata.json"
        metadata_path.write_text(json.dumps(metadata, indent=2), encoding='utf-8')
        
        print(f" Processed {file_path.name} -> {output_path}")
        
        return {
            "success": True,
            "input_file": str(file_path),
            "output_file": str(output_path),
            "metadata_file": str(metadata_path),
            "metadata": metadata,
            "content_length": len(markdown_content)
        }
    
    def process_directory(self, pattern: str = "*.pdf") -> List[Dict]:
        """
        Process all matching files in the raw directory.
        
        Args:
            pattern: File pattern to match (e.g., "*.pdf", "*.docx")
            
        Returns:
            List of processing results
        """
        results = []
        files = list(self.raw_dir.glob(pattern))
        
        if not files:
            print(f"No {pattern} files found in {self.raw_dir}")
            return results
        
        print(f"Found {len(files)} files to process...")
        
        for file_path in files:
            try:
                result = self.process_file(file_path)
                results.append(result)
            except Exception as e:
                print(f" Error processing {file_path.name}: {e}")
                results.append({
                    "success": False,
                    "input_file": str(file_path),
                    "error": str(e)
                })
        
        return results
    
    def process_all(self) -> List[Dict]:
        """Process all PDF and Docx files in the raw directory."""
        results = []
        results.extend(self.process_directory("*.pdf"))
        results.extend(self.process_directory("*.docx"))
        return results


def main():
    """Main entry point for ingestion."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest technical documents (ARD/PRD/TRD)")
    parser.add_argument("--file", type=str, help="Process a specific file")
    parser.add_argument("--raw-dir", type=str, default="data/raw", help="Raw documents directory")
    parser.add_argument("--processed-dir", type=str, default="data/processed", help="Processed output directory")
    parser.add_argument("--api-key", type=str, help="LlamaParse API key (or set LLAMA_CLOUD_API_KEY)")
    
    args = parser.parse_args()
    
    ingester = DocumentIngester(
        api_key=args.api_key,
        raw_dir=args.raw_dir,
        processed_dir=args.processed_dir
    )
    
    if args.file:
        # Process single file
        file_path = Path(args.file)
        result = ingester.process_file(file_path)
        print(f"\n Processing complete: {result['output_file']}")
    else:
        # Process all files in directory
        results = ingester.process_all()
        successful = sum(1 for r in results if r.get("success", False))
        print(f"\n Processed {successful}/{len(results)} files successfully")


if __name__ == "__main__":
    main()

