#!/usr/bin/env python3
"""
Quick validation script to check if natural language query setup is ready
"""

import requests
import sys

def check_services():
    """Check if required services are running"""
    print("=" * 70)
    print("CHECKING SERVICES")
    print("=" * 70)
    
    services_ok = True
    
    # Check Pipeline API
    print("\n1. Pipeline API (port 8082)...")
    try:
        response = requests.get("http://localhost:8082/api/pipeline/health", timeout=2)
        if response.status_code == 200:
            print("   ✅ Pipeline API is running")
        else:
            print(f"   ⚠️  Pipeline API returned status {response.status_code}")
            services_ok = False
    except requests.exceptions.RequestException:
        print("   ❌ Pipeline API is NOT running")
        print("      Start with: python pipeline_api_server.py")
        services_ok = False
    
    # Check RCA API
    print("\n2. RCA API (port 8080)...")
    try:
        response = requests.get("http://localhost:8080/api/health", timeout=2)
        if response.status_code == 200:
            print("   ✅ RCA API is running")
        else:
            print(f"   ⚠️  RCA API returned status {response.status_code}")
            services_ok = False
    except requests.exceptions.RequestException:
        print("   ❌ RCA API is NOT running")
        print("      Start with: cargo run --bin server")
        print("      Or: cd src && cargo run --bin server")
        services_ok = False
    
    return services_ok

def test_simple_query():
    """Test a simple natural language query"""
    print("\n" + "=" * 70)
    print("TESTING SIMPLE NATURAL LANGUAGE QUERY")
    print("=" * 70)
    
    question = "What tables are available in the system?"
    print(f"\nQuestion: {question}")
    
    try:
        payload = {"question": question}
        response = requests.post(
            "http://localhost:8080/api/assistant/ask",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ Query successful!")
            print(f"Response: {json.dumps(result, indent=2)[:500]}...")
            return True
        else:
            print(f"\n❌ Query failed: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

if __name__ == '__main__':
    import json
    
    if check_services():
        print("\n" + "=" * 70)
        print("ALL SERVICES READY")
        print("=" * 70)
        print("\n✅ You can now run the full test:")
        print("   python test_natural_language_queries.py")
        
        # Optionally test a simple query
        if len(sys.argv) > 1 and sys.argv[1] == "--test":
            test_simple_query()
    else:
        print("\n" + "=" * 70)
        print("SERVICES NOT READY")
        print("=" * 70)
        print("\n⚠️  Please start the required services before running tests")
        sys.exit(1)

