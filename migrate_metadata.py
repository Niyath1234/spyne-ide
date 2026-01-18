#!/usr/bin/env python3
"""
Migration script to load JSON metadata into PostgreSQL
Uses psycopg2 which can be installed via: brew install psycopg2
Or: pip3 install psycopg2-binary
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import psycopg2
from psycopg2.extras import execute_values, Json
from psycopg2 import sql

def load_json_file(filepath: Path) -> Any:
    """Load JSON file"""
    if not filepath.exists():
        return None
    with open(filepath, 'r') as f:
        return json.load(f)

def get_db_connection():
    """Get PostgreSQL connection from environment"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        # Parse DATABASE_URL or use defaults
        user = os.getenv('PGUSER', 'niyathnair')
        password = os.getenv('PGPASSWORD', '')
        host = os.getenv('PGHOST', 'localhost')
        port = os.getenv('PGPORT', '5432')
        database = os.getenv('PGDATABASE', 'rca_engine')
        
        if password:
            conn_string = f"host={host} port={port} dbname={database} user={user} password={password}"
        else:
            conn_string = f"host={host} port={port} dbname={database} user={user}"
    else:
        # Parse postgresql://user@host:port/dbname format
        conn_string = database_url.replace('postgresql://', '')
        if '@' in conn_string:
            user_part, rest = conn_string.split('@', 1)
            if ':' in rest:
                host_port, dbname = rest.split('/', 1)
                if ':' in host_port:
                    host, port = host_port.split(':')
                else:
                    host, port = host_port, '5432'
            else:
                host, port, dbname = rest.split('/')[0], '5432', rest.split('/')[1] if '/' in rest else 'rca_engine'
            
            conn_string = f"host={host} port={port} dbname={dbname} user={user_part}"
    
    return psycopg2.connect(conn_string)

def migrate_entities(conn, metadata_dir: Path):
    """Migrate entities"""
    entities_file = metadata_dir / 'entities.json'
    entities = load_json_file(entities_file)
    
    if not entities:
        print("⚠️  entities.json not found, skipping...")
        return 0
    
    if isinstance(entities, dict) and 'entities' in entities:
        entities = entities['entities']
    
    cur = conn.cursor()
    count = 0
    
    for entity in entities:
        cur.execute("""
            INSERT INTO entities (id, name, description, grain, attributes)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                grain = EXCLUDED.grain,
                attributes = EXCLUDED.attributes,
                updated_at = NOW()
        """, (
            entity['id'],
            entity['name'],
            entity.get('description', ''),
            Json(entity.get('grain', [])),
            Json(entity.get('attributes', []))
        ))
        count += 1
    
    conn.commit()
    cur.close()
    print(f"✅ Migrated {count} entities")
    return count

def migrate_tables(conn, metadata_dir: Path):
    """Migrate tables"""
    tables_file = metadata_dir / 'tables.json'
    tables_data = load_json_file(tables_file)
    
    if not tables_data:
        print("⚠️  tables.json not found, skipping...")
        return 0
    
    if isinstance(tables_data, dict) and 'tables' in tables_data:
        tables = tables_data['tables']
    else:
        tables = tables_data
    
    cur = conn.cursor()
    count = 0
    
    for table in tables:
        cur.execute("""
            INSERT INTO tables (name, entity_id, primary_key, time_column, system, path, columns, labels)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                entity_id = EXCLUDED.entity_id,
                primary_key = EXCLUDED.primary_key,
                time_column = EXCLUDED.time_column,
                system = EXCLUDED.system,
                path = EXCLUDED.path,
                columns = EXCLUDED.columns,
                labels = EXCLUDED.labels,
                updated_at = NOW()
        """, (
            table['name'],
            table.get('entity', ''),
            Json(table.get('primary_key', [])),
            table.get('time_column', ''),
            table['system'],
            table.get('path', ''),
            Json(table.get('columns')) if table.get('columns') else None,
            Json(table.get('labels')) if table.get('labels') else None
        ))
        count += 1
    
    conn.commit()
    cur.close()
    print(f"✅ Migrated {count} tables")
    return count

def migrate_metrics(conn, metadata_dir: Path):
    """Migrate metrics"""
    metrics_file = metadata_dir / 'metrics.json'
    metrics = load_json_file(metrics_file)
    
    if not metrics:
        print("⚠️  metrics.json not found, skipping...")
        return 0
    
    if isinstance(metrics, dict) and 'metrics' in metrics:
        metrics = metrics['metrics']
    
    cur = conn.cursor()
    count = 0
    
    for metric in metrics:
        cur.execute("""
            INSERT INTO metrics (id, name, description, grain, precision, null_policy, unit, versions)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                grain = EXCLUDED.grain,
                precision = EXCLUDED.precision,
                null_policy = EXCLUDED.null_policy,
                unit = EXCLUDED.unit,
                versions = EXCLUDED.versions,
                updated_at = NOW()
        """, (
            metric['id'],
            metric['name'],
            metric.get('description', ''),
            Json(metric.get('grain', [])),
            metric.get('precision', 2),
            metric.get('null_policy', 'zero'),
            metric.get('unit', 'currency'),
            Json(metric.get('versions', []))
        ))
        count += 1
    
    conn.commit()
    cur.close()
    print(f"✅ Migrated {count} metrics")
    return count

def migrate_rules(conn, metadata_dir: Path):
    """Migrate rules"""
    rules_file = metadata_dir / 'rules.json'
    rules = load_json_file(rules_file)
    
    if not rules:
        print("⚠️  rules.json not found, skipping...")
        return 0
    
    if isinstance(rules, dict) and 'rules' in rules:
        rules = rules['rules']
    
    cur = conn.cursor()
    count = 0
    
    for rule in rules:
        computation = rule.get('computation', {})
        cur.execute("""
            INSERT INTO rules (id, system, metric_id, target_entity_id, target_grain,
                             description, formula, source_entities, aggregation_grain,
                             filter_conditions, source_table, note, labels)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                system = EXCLUDED.system,
                metric_id = EXCLUDED.metric_id,
                target_entity_id = EXCLUDED.target_entity_id,
                target_grain = EXCLUDED.target_grain,
                description = EXCLUDED.description,
                formula = EXCLUDED.formula,
                source_entities = EXCLUDED.source_entities,
                aggregation_grain = EXCLUDED.aggregation_grain,
                filter_conditions = EXCLUDED.filter_conditions,
                source_table = EXCLUDED.source_table,
                note = EXCLUDED.note,
                labels = EXCLUDED.labels,
                updated_at = NOW()
        """, (
            rule['id'],
            rule['system'],
            rule.get('metric', ''),
            rule.get('target_entity', ''),
            Json(rule.get('target_grain', [])),
            computation.get('description', ''),
            computation.get('formula', ''),
            Json(computation.get('source_entities', [])),
            Json(computation.get('aggregation_grain', [])),
            Json(computation.get('filter_conditions')) if computation.get('filter_conditions') else None,
            computation.get('source_table'),
            computation.get('note'),
            Json(rule.get('labels')) if rule.get('labels') else None
        ))
        count += 1
    
    conn.commit()
    cur.close()
    print(f"✅ Migrated {count} rules")
    return count

def migrate_lineage(conn, metadata_dir: Path):
    """Migrate lineage edges"""
    lineage_file = metadata_dir / 'lineage.json'
    lineage_data = load_json_file(lineage_file)
    
    if not lineage_data:
        print("⚠️  lineage.json not found, skipping...")
        return 0
    
    # Handle both array and object formats
    if isinstance(lineage_data, dict):
        edges = lineage_data.get('edges', [])
    else:
        edges = [item for item in lineage_data if isinstance(item, dict) and item.get('type') == 'edge']
        edges = [item.get('edge', item) for item in edges if 'edge' in item or 'from' in item]
    
    cur = conn.cursor()
    
    # Clear existing edges
    cur.execute("DELETE FROM lineage_edges")
    
    count = 0
    for edge in edges:
        # Handle both formats
        if 'from' in edge and 'to' in edge:
            cur.execute("""
                INSERT INTO lineage_edges (from_table, to_table, keys, relationship)
                VALUES (%s, %s, %s, %s)
            """, (
                edge['from'],
                edge['to'],
                Json(edge.get('keys', {})),
                edge.get('relationship', 'join')
            ))
            count += 1
    
    conn.commit()
    cur.close()
    print(f"✅ Migrated {count} lineage edges")
    return count

def migrate_business_labels(conn, metadata_dir: Path):
    """Migrate business labels"""
    labels_file = metadata_dir / 'business_labels.json'
    labels_data = load_json_file(labels_file)
    
    if not labels_data:
        print("⚠️  business_labels.json not found, skipping...")
        return 0
    
    # Handle both array and object formats
    if isinstance(labels_data, dict):
        systems = labels_data.get('systems', [])
        metrics = labels_data.get('metrics', [])
        reconciliation_types = labels_data.get('reconciliation_types', [])
    else:
        # Array format
        systems = [item for item in labels_data if isinstance(item, dict) and item.get('system_id')]
        metrics = [item for item in labels_data if isinstance(item, dict) and item.get('metric_id')]
        reconciliation_types = [item for item in labels_data if isinstance(item, dict) and not item.get('system_id') and not item.get('metric_id')]
    
    cur = conn.cursor()
    
    # Clear existing labels
    cur.execute("DELETE FROM business_labels")
    
    count = 0
    
    for system in systems:
        cur.execute("""
            INSERT INTO business_labels (label_type, label, aliases, system_id)
            VALUES ('system', %s, %s, %s)
        """, (
            system.get('label', ''),
            Json(system.get('aliases', [])),
            system.get('system_id', '')
        ))
        count += 1
    
    for metric in metrics:
        cur.execute("""
            INSERT INTO business_labels (label_type, label, aliases, metric_id)
            VALUES ('metric', %s, %s, %s)
        """, (
            metric.get('label', ''),
            Json(metric.get('aliases', [])),
            metric.get('metric_id', '')
        ))
        count += 1
    
    for recon_type in reconciliation_types:
        cur.execute("""
            INSERT INTO business_labels (label_type, label, aliases)
            VALUES ('reconciliation_type', %s, %s)
        """, (
            recon_type.get('label', ''),
            Json(recon_type.get('aliases', []))
        ))
        count += 1
    
    conn.commit()
    cur.close()
    print(f"✅ Migrated {count} business labels")
    return count

def migrate_time_rules(conn, metadata_dir: Path):
    """Migrate time rules"""
    time_file = metadata_dir / 'time.json'
    time_data = load_json_file(time_file)
    
    if not time_data:
        print("⚠️  time.json not found, skipping...")
        return 0
    
    cur = conn.cursor()
    
    # Clear existing time rules
    cur.execute("DELETE FROM time_rules")
    
    count = 0
    
    # As-of rules
    as_of_rules = time_data.get('as_of_rules', [])
    for rule in as_of_rules:
        cur.execute("""
            INSERT INTO time_rules (table_name, rule_type, as_of_column, default_value)
            VALUES (%s, 'as_of', %s, %s)
        """, (
            rule.get('table', ''),
            rule.get('as_of_column', ''),
            rule.get('default', '')
        ))
        count += 1
    
    # Lateness rules
    lateness_rules = time_data.get('lateness_rules', [])
    for rule in lateness_rules:
        cur.execute("""
            INSERT INTO time_rules (table_name, rule_type, max_lateness_days, action)
            VALUES (%s, 'lateness', %s, %s)
        """, (
            rule.get('table', ''),
            rule.get('max_lateness_days', 0),
            rule.get('action', '')
        ))
        count += 1
    
    conn.commit()
    cur.close()
    print(f"✅ Migrated {count} time rules")
    return count

def migrate_exceptions(conn, metadata_dir: Path):
    """Migrate exceptions"""
    exceptions_file = metadata_dir / 'exceptions.json'
    exceptions_data = load_json_file(exceptions_file)
    
    if not exceptions_data:
        print("⚠️  exceptions.json not found, skipping...")
        return 0
    
    # Handle both array and object formats
    if isinstance(exceptions_data, dict):
        exceptions = exceptions_data.get('exceptions', [])
    else:
        exceptions = exceptions_data
    
    cur = conn.cursor()
    count = 0
    
    for exception in exceptions:
        condition = exception.get('condition', {})
        cur.execute("""
            INSERT INTO exceptions (id, description, table_name, filter_condition, applies_to, override_fields)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                description = EXCLUDED.description,
                table_name = EXCLUDED.table_name,
                filter_condition = EXCLUDED.filter_condition,
                applies_to = EXCLUDED.applies_to,
                override_fields = EXCLUDED.override_fields
        """, (
            exception['id'],
            exception.get('description', ''),
            condition.get('table', ''),
            condition.get('filter', ''),
            Json(exception.get('applies_to', [])),
            Json(exception.get('override_field')) if exception.get('override_field') else None
        ))
        count += 1
    
    conn.commit()
    cur.close()
    print(f"✅ Migrated {count} exceptions")
    return count

def verify_migration(conn):
    """Verify the migration"""
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM entities")
    entities_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM tables")
    tables_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM metrics")
    metrics_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM rules")
    rules_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM lineage_edges")
    lineage_count = cur.fetchone()[0]
    
    cur.close()
    
    print("\n📊 Migration Verification:")
    print(f"   - Entities: {entities_count}")
    print(f"   - Tables: {tables_count}")
    print(f"   - Metrics: {metrics_count}")
    print(f"   - Rules: {rules_count}")
    print(f"   - Lineage Edges: {lineage_count}")
    
    return {
        'entities': entities_count,
        'tables': tables_count,
        'metrics': metrics_count,
        'rules': rules_count,
        'lineage': lineage_count
    }

def main():
    """Main migration function"""
    print("🚀 RCA Engine - Metadata Migration to PostgreSQL (Python)")
    print("=" * 70)
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Get metadata directory
    metadata_dir = Path('metadata')
    if not metadata_dir.exists():
        print(f"❌ Metadata directory not found: {metadata_dir}")
        sys.exit(1)
    
    print(f"📂 Loading metadata from: {metadata_dir.absolute()}\n")
    
    # Connect to database
    try:
        print("📡 Connecting to database...")
        conn = get_db_connection()
        print("✅ Connected successfully!\n")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        print("\n💡 Make sure:")
        print("   1. PostgreSQL is running: brew services start postgresql@14")
        print("   2. DATABASE_URL is set in .env or use environment variables")
        sys.exit(1)
    
    try:
        # Migrate all metadata
        print("📥 Starting migration...\n")
        print("-" * 70)
        
        entities_count = migrate_entities(conn, metadata_dir)
        tables_count = migrate_tables(conn, metadata_dir)
        metrics_count = migrate_metrics(conn, metadata_dir)
        rules_count = migrate_rules(conn, metadata_dir)
        lineage_count = migrate_lineage(conn, metadata_dir)
        labels_count = migrate_business_labels(conn, metadata_dir)
        time_count = migrate_time_rules(conn, metadata_dir)
        exceptions_count = migrate_exceptions(conn, metadata_dir)
        
        print("\n" + "=" * 70)
        print("✅ Migration completed successfully!")
        print("\n📊 Summary:")
        print(f"   ✓ {entities_count} entities")
        print(f"   ✓ {tables_count} tables")
        print(f"   ✓ {metrics_count} metrics")
        print(f"   ✓ {rules_count} rules")
        print(f"   ✓ {lineage_count} lineage edges")
        print(f"   ✓ {labels_count} business labels")
        print(f"   ✓ {time_count} time rules")
        print(f"   ✓ {exceptions_count} exceptions")
        
        # Verify
        print("\n🔍 Verifying migration...")
        verify_migration(conn)
        
        print("\n🎉 All done! Metadata is now in PostgreSQL.")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == '__main__':
    main()

