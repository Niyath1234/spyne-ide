"""
Airflow DAG for Document Retrieval Pipeline

Schedules the document ingestion, chunking, and indexing pipeline.
Supports both scheduled runs and manual triggers.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import sys
from pathlib import Path

# Add src to path
dag_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(dag_dir))

from src.pipeline import DocumentPipeline


# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['admin@example.com'],  # Update with your email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# DAG definition
dag = DAG(
    'document_retrieval_pipeline',
    default_args=default_args,
    description='Process technical documents (ARD/PRD/TRD) and update vector index',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['document-retrieval', 'nlp', 'vector-db'],
    max_active_runs=1,  # Only one run at a time
)


def run_ingestion(**context):
    """Run document ingestion step."""
    pipeline = DocumentPipeline(
        raw_dir=os.getenv('RAW_DOCS_DIR', 'data/raw'),
        processed_dir=os.getenv('PROCESSED_DOCS_DIR', 'data/processed'),
        index_name=os.getenv('PINECONE_INDEX_NAME', 'technical-docs'),
        incremental=True,
        log_level='INFO'
    )
    
    result = pipeline.run_ingestion(force=False)
    
    if not result.get('success'):
        raise Exception(f"Ingestion failed: {result.get('error', 'Unknown error')}")
    
    return result


def run_chunking(**context):
    """Run document chunking step."""
    pipeline = DocumentPipeline(
        raw_dir=os.getenv('RAW_DOCS_DIR', 'data/raw'),
        processed_dir=os.getenv('PROCESSED_DOCS_DIR', 'data/processed'),
        index_name=os.getenv('PINECONE_INDEX_NAME', 'technical-docs'),
        incremental=True,
        log_level='INFO'
    )
    
    result = pipeline.run_chunking(force=False)
    
    if not result.get('success'):
        raise Exception(f"Chunking failed: {result.get('error', 'Unknown error')}")
    
    return result


def run_indexing(**context):
    """Run document indexing step."""
    # Get chunks file from previous step
    ti = context['ti']
    chunking_result = ti.xcom_pull(task_ids='chunk_documents')
    chunks_file = chunking_result.get('chunks_file')
    
    pipeline = DocumentPipeline(
        raw_dir=os.getenv('RAW_DOCS_DIR', 'data/raw'),
        processed_dir=os.getenv('PROCESSED_DOCS_DIR', 'data/processed'),
        index_name=os.getenv('PINECONE_INDEX_NAME', 'technical-docs'),
        incremental=True,
        log_level='INFO'
    )
    
    result = pipeline.run_indexing(chunks_file=chunks_file)
    
    if not result.get('success'):
        raise Exception(f"Indexing failed: {result.get('error', 'Unknown error')}")
    
    return result


def send_success_notification(**context):
    """Send success notification (optional)."""
    # Implement your notification logic here
    # e.g., Slack, email, etc.
    print("Pipeline completed successfully!")
    return True


# Tasks
ingest_task = PythonOperator(
    task_id='ingest_documents',
    python_callable=run_ingestion,
    dag=dag,
)

chunk_task = PythonOperator(
    task_id='chunk_documents',
    python_callable=run_chunking,
    dag=dag,
)

index_task = PythonOperator(
    task_id='index_documents',
    python_callable=run_indexing,
    dag=dag,
)

success_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# Task dependencies
ingest_task >> chunk_task >> index_task >> success_task


# Alternative: Single task that runs full pipeline
def run_full_pipeline(**context):
    """Run complete pipeline in one task."""
    pipeline = DocumentPipeline(
        raw_dir=os.getenv('RAW_DOCS_DIR', 'data/raw'),
        processed_dir=os.getenv('PROCESSED_DOCS_DIR', 'data/processed'),
        index_name=os.getenv('PINECONE_INDEX_NAME', 'technical-docs'),
        incremental=True,
        log_level='INFO'
    )
    
    result = pipeline.run_full_pipeline(force=False)
    
    if not result.get('success'):
        raise Exception(f"Pipeline failed: {result}")
    
    return result


# Uncomment to use single-task approach instead
# full_pipeline_task = PythonOperator(
#     task_id='run_full_pipeline',
#     python_callable=run_full_pipeline,
#     dag=dag,
# )





