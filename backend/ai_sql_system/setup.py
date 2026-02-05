"""
Setup Script - Initialize database and metadata
"""
import os
from metadata.ingestion import MetadataIngestion
from trino.client import TrinoClient
from trino.schema_loader import SchemaLoader

def setup_database():
    """Initialize database tables"""
    print("Setting up database tables...")
    ingestion = MetadataIngestion()
    print("✓ Database tables created")
    return ingestion

def load_trino_schema(trino_host="localhost", trino_port=8080, catalog="tpch", schema="tiny"):
    """Load schema from Trino into metadata store"""
    print(f"Loading schema from Trino ({trino_host}:{trino_port})...")
    
    ingestion = MetadataIngestion()
    client = TrinoClient(host=trino_host, port=trino_port, catalog=catalog, schema=schema)
    loader = SchemaLoader(client)
    
    try:
        client.connect()
        
        # Get all tables
        tables = loader.get_tables(catalog, schema)
        print(f"Found {len(tables)} tables")
        
        # Ingest each table
        for table_info in tables:
            table_name = table_info['table_name']
            ingestion.ingest_table(
                table_name=table_name,
                schema=schema,
                description=f"Table from {catalog}.{schema}"
            )
            
            # Get and ingest columns
            columns = loader.get_columns(table_name, catalog, schema)
            for col in columns:
                ingestion.ingest_column(
                    table_name=table_name,
                    column_name=col['column_name'],
                    datatype=col['data_type'],
                    description=None
                )
            
            print(f"  ✓ Ingested {table_name} ({len(columns)} columns)")
        
        print("✓ Schema loaded successfully")
        
    except Exception as e:
        print(f"✗ Error loading schema: {e}")
    finally:
        client.close()

def setup_sample_metrics():
    """Setup sample metrics"""
    print("Setting up sample metrics...")
    ingestion = MetadataIngestion()
    
    # Example metrics
    metrics = [
        {
            "metric_name": "revenue",
            "sql_formula": "SUM(order_amount)",
            "base_table": "orders",
            "grain": "order",
            "description": "Total revenue"
        },
        {
            "metric_name": "profit",
            "sql_formula": "SUM(order_amount - cost)",
            "base_table": "orders",
            "grain": "order",
            "description": "Total profit"
        }
    ]
    
    for metric in metrics:
        ingestion.ingest_metric(**metric)
        print(f"  ✓ Ingested metric: {metric['metric_name']}")
    
    print("✓ Sample metrics loaded")

if __name__ == "__main__":
    print("=" * 50)
    print("AI SQL System Setup")
    print("=" * 50)
    
    # Setup database
    setup_database()
    
    # Load Trino schema (if Trino is available)
    trino_host = os.getenv("TRINO_HOST", "localhost")
    if trino_host:
        try:
            load_trino_schema(
                trino_host=trino_host,
                trino_port=int(os.getenv("TRINO_PORT", "8080")),
                catalog=os.getenv("TRINO_CATALOG", "tpch"),
                schema=os.getenv("TRINO_SCHEMA", "tiny")
            )
        except Exception as e:
            print(f"⚠ Could not load Trino schema: {e}")
            print("  You can load schema later or manually ingest metadata")
    
    # Setup sample metrics
    setup_sample_metrics()
    
    print("=" * 50)
    print("Setup complete!")
    print("=" * 50)
