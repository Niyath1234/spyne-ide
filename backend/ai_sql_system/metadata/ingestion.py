"""
Metadata Ingestion - Store tables, columns, metrics in Postgres
"""
import os
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class MetadataIngestion:
    """Ingests and stores metadata in Postgres"""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize metadata ingestion
        
        Args:
            connection_string: Postgres connection string (defaults to env var)
        """
        self.conn_string = connection_string or os.getenv(
            'POSTGRES_CONNECTION_STRING',
            'postgresql://postgres:postgres@localhost:5432/rca_engine'
        )
        try:
            self._ensure_tables()
        except Exception as e:
            logger.warning(f"Could not initialize Postgres tables: {e}. System will use JSON fallback.")
            # Don't raise - allow system to work without Postgres
    
    def _get_connection(self):
        """Get Postgres connection"""
        return psycopg2.connect(self.conn_string)
    
    def _ensure_tables(self):
        """Create tables if they don't exist"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
        except Exception as e:
            logger.warning(f"Could not connect to Postgres: {e}")
            raise
        
        try:
            # Enable pgvector extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            
            # Tables metadata
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tables_metadata (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(255) NOT NULL,
                    schema VARCHAR(255),
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(table_name, schema)
                )
            """)
            
            # Columns metadata
            cur.execute("""
                CREATE TABLE IF NOT EXISTS columns_metadata (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(255) NOT NULL,
                    column_name VARCHAR(255) NOT NULL,
                    datatype VARCHAR(100),
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(table_name, column_name)
                )
            """)
            
            # Metrics registry
            cur.execute("""
                CREATE TABLE IF NOT EXISTS metrics_registry (
                    id SERIAL PRIMARY KEY,
                    metric_name VARCHAR(255) NOT NULL UNIQUE,
                    sql_formula TEXT NOT NULL,
                    base_table VARCHAR(255),
                    grain VARCHAR(255),
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Query memory
            cur.execute("""
                CREATE TABLE IF NOT EXISTS query_memory (
                    id SERIAL PRIMARY KEY,
                    user_query TEXT NOT NULL,
                    final_sql TEXT NOT NULL,
                    success BOOLEAN DEFAULT TRUE,
                    execution_time_ms INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Embeddings table (for pgvector)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS metadata_embeddings (
                    id SERIAL PRIMARY KEY,
                    entity_type VARCHAR(50) NOT NULL,  -- 'column', 'metric', 'table', 'query'
                    entity_id VARCHAR(255) NOT NULL,
                    entity_name VARCHAR(255),
                    embedding vector(1024),
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(entity_type, entity_id)
                )
            """)
            
            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_entity ON metadata_embeddings(entity_type, entity_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_query_memory_query ON query_memory USING gin(to_tsvector('english', user_query))")
            
            conn.commit()
            logger.info("Metadata tables ensured")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating tables: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def ingest_table(self, table_name: str, schema: Optional[str] = None, description: Optional[str] = None):
        """Ingest table metadata"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO tables_metadata (table_name, schema, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (table_name, schema) 
                DO UPDATE SET description = EXCLUDED.description
            """, (table_name, schema, description))
            conn.commit()
            logger.info(f"Ingested table: {table_name}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error ingesting table {table_name}: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def ingest_column(self, table_name: str, column_name: str, datatype: Optional[str] = None, description: Optional[str] = None):
        """Ingest column metadata"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO columns_metadata (table_name, column_name, datatype, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (table_name, column_name)
                DO UPDATE SET datatype = EXCLUDED.datatype, description = EXCLUDED.description
            """, (table_name, column_name, datatype, description))
            conn.commit()
            logger.info(f"Ingested column: {table_name}.{column_name}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error ingesting column {table_name}.{column_name}: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def ingest_metric(self, metric_name: str, sql_formula: str, base_table: Optional[str] = None, 
                     grain: Optional[str] = None, description: Optional[str] = None):
        """Ingest metric definition"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO metrics_registry (metric_name, sql_formula, base_table, grain, description)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (metric_name)
                DO UPDATE SET sql_formula = EXCLUDED.sql_formula, 
                             base_table = EXCLUDED.base_table,
                             grain = EXCLUDED.grain,
                             description = EXCLUDED.description
            """, (metric_name, sql_formula, base_table, grain, description))
            conn.commit()
            logger.info(f"Ingested metric: {metric_name}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error ingesting metric {metric_name}: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def store_query_memory(self, user_query: str, final_sql: str, success: bool = True, execution_time_ms: Optional[int] = None):
        """Store successful query for learning"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO query_memory (user_query, final_sql, success, execution_time_ms)
                VALUES (%s, %s, %s, %s)
            """, (user_query, final_sql, success, execution_time_ms))
            conn.commit()
            logger.info(f"Stored query memory: {success}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error storing query memory: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def store_embedding(self, entity_type: str, entity_id: str, entity_name: str, 
                       embedding: List[float], metadata: Optional[Dict] = None):
        """Store embedding in pgvector"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            # Convert embedding list to pgvector format
            embedding_str = '[' + ','.join(map(str, embedding)) + ']'
            
            cur.execute("""
                INSERT INTO metadata_embeddings (entity_type, entity_id, entity_name, embedding, metadata)
                VALUES (%s, %s, %s, %s::vector, %s)
                ON CONFLICT (entity_type, entity_id)
                DO UPDATE SET embedding = EXCLUDED.embedding, metadata = EXCLUDED.metadata
            """, (entity_type, entity_id, entity_name, embedding_str, metadata))
            conn.commit()
            logger.info(f"Stored embedding: {entity_type}:{entity_id}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error storing embedding: {e}")
            raise
        finally:
            cur.close()
            conn.close()
