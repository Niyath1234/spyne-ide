"""
Database Connection Utility

Provides database connection management for stores.
"""

import os
import logging
from typing import Optional
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool, extras
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection manager for stores."""
    
    _pool: Optional[pool.SimpleConnectionPool] = None
    
    @classmethod
    def initialize_pool(cls, config: Optional[dict] = None):
        """
        Initialize connection pool.
        
        Args:
            config: Database configuration dict. If None, reads from environment.
        """
        if cls._pool is not None:
            return
        
        if config is None:
            config = {
                'host': os.getenv('RCA_DB_HOST', 'localhost'),
                'port': int(os.getenv('RCA_DB_PORT', 5432)),
                'database': os.getenv('RCA_DB_NAME', 'rca_engine'),
                'user': os.getenv('RCA_DB_USER', 'rca_user'),
                'password': os.getenv('RCA_DB_PASSWORD', ''),
            }
        
        try:
            cls._pool = psycopg2.pool.SimpleConnectionPool(
                1,
                10,  # max connections
                host=config['host'],
                port=config.get('port', 5432),
                database=config['database'],
                user=config['user'],
                password=config.get('password', ''),
                connect_timeout=config.get('timeout', 30),
            )
            
            if not cls._pool:
                raise Exception("Failed to create connection pool")
            
            logger.info("Database connection pool initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    @classmethod
    @contextmanager
    def get_connection(cls):
        """
        Get database connection from pool.
        
        Yields:
            psycopg2 connection with RealDictCursor
        """
        if cls._pool is None:
            cls.initialize_pool()
        
        conn = None
        try:
            conn = cls._pool.getconn()
            if conn is None:
                raise Exception("Failed to get connection from pool")
            
            # Use RealDictCursor for dict-like results
            conn.cursor_factory = RealDictCursor
            
            yield conn
            
            conn.commit()
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                cls._pool.putconn(conn)
    
    @classmethod
    def close_pool(cls):
        """Close connection pool."""
        if cls._pool:
            cls._pool.closeall()
            cls._pool = None
            logger.info("Database connection pool closed")

