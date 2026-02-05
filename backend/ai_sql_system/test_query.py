#!/usr/bin/env python3
"""
Test script to verify the new AI SQL System generates correct query
"""
import os
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))
sys.path.insert(0, str(backend_dir.parent))

from backend.ai_sql_system.orchestration.graph import LangGraphOrchestrator
from backend.ai_sql_system.trino.client import TrinoClient
from backend.ai_sql_system.trino.validator import TrinoValidator
from backend.ai_sql_system.metadata.semantic_registry import SemanticRegistry
from backend.ai_sql_system.planning.join_graph import JoinGraph
import json

def load_join_graph():
    """Load join graph from metadata"""
    join_graph = JoinGraph()
    
    try:
        metadata_dir = Path(__file__).parent.parent.parent / "metadata"
        lineage_file = metadata_dir / "lineage.json"
        
        if lineage_file.exists():
            with open(lineage_file, 'r') as f:
                lineage_data = json.load(f)
            
            for edge in lineage_data.get('edges', []):
                from_table = edge.get('from', '').split('.')[-1]
                to_table = edge.get('to', '').split('.')[-1]
                condition = edge.get('on', '')
                join_type = 'LEFT'
                
                if from_table and to_table and condition:
                    join_graph.add_join(from_table, to_table, condition, join_type)
            
            print(f"✓ Loaded {len(lineage_data.get('edges', []))} join relationships")
    except Exception as e:
        print(f"⚠ Could not load join graph: {e}")
    
    return join_graph

def test_query():
    """Test the discount query"""
    query = "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
    
    print("=" * 80)
    print("Testing AI SQL System")
    print("=" * 80)
    print(f"\nQuery: {query}\n")
    
    # Initialize components
    print("Initializing components...")
    
    try:
        trino_client = TrinoClient(
            host=os.getenv('TRINO_HOST', 'localhost'),
            port=int(os.getenv('TRINO_PORT', '8080')),
            catalog=os.getenv('TRINO_CATALOG', 'tpch'),
            schema=os.getenv('TRINO_SCHEMA', 'tiny')
        )
        trino_validator = TrinoValidator(trino_client)
        semantic_registry = SemanticRegistry()
        join_graph = load_join_graph()
        
        orchestrator = LangGraphOrchestrator(
            trino_validator=trino_validator,
            semantic_registry=semantic_registry,
            join_graph=join_graph
        )
        
        print("✓ Components initialized\n")
        
        # Run pipeline
        print("Running LangGraph pipeline...")
        result = orchestrator.run(query)
        
        print("\n" + "=" * 80)
        print("RESULTS")
        print("=" * 80)
        
        if result.get('success'):
            print("\n✅ SUCCESS")
            print(f"\nGenerated SQL:\n{result.get('sql', 'N/A')}")
            
            if result.get('intent'):
                print(f"\nIntent: {json.dumps(result['intent'], indent=2)}")
            
            if result.get('resolution'):
                print(f"\nResolution: {json.dumps(result['resolution'], indent=2)}")
            
            if result.get('query_plan'):
                print(f"\nQuery Plan: {json.dumps(result['query_plan'], indent=2)}")
            
            if result.get('validation_errors'):
                print(f"\n⚠ Validation Errors: {result['validation_errors']}")
            
            # Expected SQL structure
            print("\n" + "=" * 80)
            print("EXPECTED SQL STRUCTURE")
            print("=" * 80)
            print("""
Expected SQL should:
1. SELECT customer key (c_custkey) and discount calculation
2. FROM lineitem table
3. JOIN orders ON lineitem.l_orderkey = orders.o_orderkey
4. JOIN customer ON orders.o_custkey = customer.c_custkey
5. GROUP BY customer.c_custkey
6. Use formula: SUM(l_extendedprice * (1 - l_discount)) AS discount
            """)
            
            # Validate SQL
            sql = result.get('sql', '')
            checks = {
                'Has SUM': 'SUM' in sql.upper(),
                'Has extendedprice': 'extendedprice' in sql.lower() or 'l_extendedprice' in sql.lower(),
                'Has discount': 'discount' in sql.lower() or 'l_discount' in sql.lower(),
                'Has customer': 'customer' in sql.lower() or 'c_custkey' in sql.lower(),
                'Has GROUP BY': 'GROUP BY' in sql.upper(),
                'Has JOIN': 'JOIN' in sql.upper(),
            }
            
            print("\nSQL Validation Checks:")
            for check, passed in checks.items():
                status = "✅" if passed else "❌"
                print(f"  {status} {check}")
            
        else:
            print("\n❌ FAILED")
            print(f"\nError: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Set environment variables if not set
    if not os.getenv('OPENAI_API_KEY'):
        print("⚠ Warning: OPENAI_API_KEY not set. LLM calls will fail.")
        print("Set it with: export OPENAI_API_KEY=your_key")
    
    test_query()
