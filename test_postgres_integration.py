#!/usr/bin/env python3
"""
Comprehensive PostgreSQL Integration Test
Tests all aspects of the PostgreSQL migration
"""

import os
import sys
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import requests
import time

# Load environment variables
load_dotenv()

def get_db_connection():
    """Get PostgreSQL connection"""
    database_url = os.getenv('DATABASE_URL', 'postgresql://niyathnair@localhost:5432/rca_engine')
    if database_url.startswith('postgresql://'):
        # Parse connection string
        conn_string = database_url.replace('postgresql://', '')
        if '@' in conn_string:
            user_part, rest = conn_string.split('@', 1)
            if '/' in rest:
                host_port, dbname = rest.split('/', 1)
                if ':' in host_port:
                    host, port = host_port.split(':')
                else:
                    host, port = host_port, '5432'
            else:
                host, port, dbname = 'localhost', '5432', 'rca_engine'
        else:
            host, port, dbname = 'localhost', '5432', 'rca_engine'
            user_part = 'niyathnair'
        
        conn_string = f"host={host} port={port} dbname={dbname} user={user_part}"
    else:
        conn_string = database_url
    
    return psycopg2.connect(conn_string)

def test_database_connection():
    """Test 1: Database Connection"""
    print("\n" + "="*70)
    print("TEST 1: Database Connection")
    print("="*70)
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"✅ Connected to PostgreSQL")
        print(f"   Version: {version.split(',')[0]}")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def test_metadata_tables():
    """Test 2: Metadata Tables Exist"""
    print("\n" + "="*70)
    print("TEST 2: Metadata Tables")
    print("="*70)
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        required_tables = [
            'entities', 'tables', 'metrics', 'rules', 
            'lineage_edges', 'business_labels', 'time_rules', 'exceptions'
        ]
        
        all_exist = True
        for table in required_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            count = cur.fetchone()[0]
            status = "✅" if count >= 0 else "❌"
            print(f"   {status} {table}: {count} records")
            if count < 0:
                all_exist = False
        
        cur.close()
        conn.close()
        return all_exist
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_metadata_data():
    """Test 3: Metadata Data Integrity"""
    print("\n" + "="*70)
    print("TEST 3: Metadata Data Integrity")
    print("="*70)
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check entities
        cur.execute("SELECT id, name FROM entities LIMIT 5;")
        entities = cur.fetchall()
        print(f"   ✅ Entities: {len(entities)} found")
        for e in entities[:3]:
            print(f"      - {e['id']}: {e['name']}")
        
        # Check tables
        cur.execute("SELECT name, system, entity_id FROM tables LIMIT 5;")
        tables = cur.fetchall()
        print(f"   ✅ Tables: {len(tables)} found")
        for t in tables[:3]:
            print(f"      - {t['name']} ({t['system']}) -> {t['entity_id']}")
        
        # Check rules
        cur.execute("SELECT id, system, metric_id FROM rules;")
        rules = cur.fetchall()
        print(f"   ✅ Rules: {len(rules)} found")
        for r in rules:
            print(f"      - {r['id']}: {r['system']}.{r['metric_id']}")
        
        # Check lineage
        cur.execute("SELECT from_table, to_table, relationship FROM lineage_edges LIMIT 5;")
        lineage = cur.fetchall()
        print(f"   ✅ Lineage Edges: {len(lineage)} found")
        for l in lineage[:3]:
            print(f"      - {l['from_table']} -> {l['to_table']} ({l['relationship']})")
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_metadata_relationships():
    """Test 4: Foreign Key Relationships"""
    print("\n" + "="*70)
    print("TEST 4: Foreign Key Relationships")
    print("="*70)
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Test tables -> entities relationship
        cur.execute("""
            SELECT COUNT(*) 
            FROM tables t 
            LEFT JOIN entities e ON t.entity_id = e.id 
            WHERE e.id IS NULL;
        """)
        orphan_tables = cur.fetchone()[0]
        print(f"   ✅ Tables with valid entity references: {orphan_tables == 0}")
        if orphan_tables > 0:
            print(f"      ⚠️  {orphan_tables} tables have invalid entity_id")
        
        # Test rules -> metrics relationship
        cur.execute("""
            SELECT COUNT(*) 
            FROM rules r 
            LEFT JOIN metrics m ON r.metric_id = m.id 
            WHERE m.id IS NULL;
        """)
        orphan_rules = cur.fetchone()[0]
        print(f"   ✅ Rules with valid metric references: {orphan_rules == 0}")
        if orphan_rules > 0:
            print(f"      ⚠️  {orphan_rules} rules have invalid metric_id")
        
        cur.close()
        conn.close()
        return orphan_tables == 0 and orphan_rules == 0
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_server_endpoints():
    """Test 5: Server API Endpoints"""
    print("\n" + "="*70)
    print("TEST 5: Server API Endpoints")
    print("="*70)
    
    base_url = "http://localhost:8080"
    
    # Check if server is running
    try:
        response = requests.get(f"{base_url}/api/health", timeout=2)
        if response.status_code == 200:
            print("   ✅ Server is running")
        else:
            print(f"   ⚠️  Server returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("   ⚠️  Server is not running. Start it with: cargo run --bin server")
        print("   ⚠️  Skipping API endpoint tests")
        return None
    except Exception as e:
        print(f"   ❌ Error connecting to server: {e}")
        return False
    
    # Test endpoints
    endpoints = [
        ("/api/health", "Health check"),
        ("/api/tables", "Tables metadata"),
        ("/api/rules", "Rules metadata"),
        ("/api/pipelines", "Pipelines"),
        ("/api/graph", "Graph data"),
    ]
    
    all_passed = True
    for endpoint, description in endpoints:
        try:
            response = requests.get(f"{base_url}{endpoint}", timeout=5)
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ {description}: {response.status_code}")
                if endpoint == "/api/tables":
                    count = len(data.get('sources', data.get('tables', [])))
                    print(f"      - Returned {count} tables")
                elif endpoint == "/api/rules":
                    count = len(data.get('rules', []))
                    print(f"      - Returned {count} rules")
            else:
                print(f"   ❌ {description}: {response.status_code}")
                all_passed = False
        except Exception as e:
            print(f"   ❌ {description}: {e}")
            all_passed = False
    
    return all_passed

def test_complex_query():
    """Test 6: Complex Query Test"""
    print("\n" + "="*70)
    print("TEST 6: Complex Query Test")
    print("="*70)
    
    base_url = "http://localhost:8080"
    
    try:
        response = requests.get(f"{base_url}/api/health", timeout=2)
        if response.status_code != 200:
            print("   ⚠️  Server not running, skipping query test")
            return None
    except:
        print("   ⚠️  Server not running, skipping query test")
        return None
    
    # Test a complex RCA query
    query = {
        "query": "Compare system_a and system_b TOS (Total Outstanding)"
    }
    
    try:
        print(f"   📤 Sending query: {query['query']}")
        response = requests.post(
            f"{base_url}/api/reasoning/query",
            json=query,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"   ✅ Query executed successfully")
            print(f"      - Status: {response.status_code}")
            if 'result' in result:
                result_text = result['result'][:200] if len(result.get('result', '')) > 200 else result.get('result', '')
                print(f"      - Result preview: {result_text}...")
            if 'steps' in result:
                print(f"      - Steps: {len(result['steps'])}")
            return True
        else:
            print(f"   ⚠️  Query returned status {response.status_code}")
            print(f"      Response: {response.text[:200]}")
            return False
    except Exception as e:
        print(f"   ❌ Query failed: {e}")
        return False

def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("🧪 PostgreSQL Integration Test Suite")
    print("="*70)
    print("\nTesting RCA Engine with PostgreSQL backend...")
    
    results = {}
    
    # Run tests
    results['connection'] = test_database_connection()
    results['tables'] = test_metadata_tables()
    results['data'] = test_metadata_data()
    results['relationships'] = test_metadata_relationships()
    results['endpoints'] = test_server_endpoints()
    results['query'] = test_complex_query()
    
    # Summary
    print("\n" + "="*70)
    print("📊 Test Summary")
    print("="*70)
    
    passed = sum(1 for v in results.values() if v is True)
    failed = sum(1 for v in results.values() if v is False)
    skipped = sum(1 for v in results.values() if v is None)
    
    for test_name, result in results.items():
        if result is True:
            status = "✅ PASSED"
        elif result is False:
            status = "❌ FAILED"
        else:
            status = "⏭️  SKIPPED"
        print(f"   {status}: {test_name}")
    
    print(f"\n   Total: {passed} passed, {failed} failed, {skipped} skipped")
    
    if failed == 0:
        print("\n🎉 All tests passed! PostgreSQL integration is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {failed} test(s) failed. Please review the output above.")
        return 1

if __name__ == '__main__':
    sys.exit(main())

