#!/usr/bin/env python3
"""
Detailed End-to-End Test for Advanced RCA Engine
Tests the graph traversal-based RCA system with detailed output
"""

import os
import sys
import json
import time
import requests
import subprocess
from pathlib import Path

def check_env():
    """Check if .env file exists and has API key"""
    env_path = Path(".env")
    if not env_path.exists():
        print("❌ .env file not found")
        print("   Creating template...")
        with open(".env", "w") as f:
            f.write("# OpenAI Configuration\n")
            f.write("OPENAI_API_KEY=your_api_key_here\n")
            f.write("OPENAI_MODEL=gpt-4\n")
            f.write("OPENAI_BASE_URL=https://api.openai.com/v1\n")
        print("   Please add your OPENAI_API_KEY to .env file")
        return False
    
    # Load .env manually
    env_vars = {}
    with open(".env", "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip()
    
    if "OPENAI_API_KEY" not in env_vars or env_vars["OPENAI_API_KEY"] == "your_api_key_here":
        print("❌ OPENAI_API_KEY not set in .env file")
        return False
    
    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value
    
    print("✅ Environment variables loaded")
    print(f"   Model: {env_vars.get('OPENAI_MODEL', 'gpt-4')}")
    return True

def build_server():
    """Build the server"""
    print("\n🔨 Building server...")
    result = subprocess.run(
        ["cargo", "build", "--bin", "server", "--release"],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print("❌ Build failed")
        print(result.stderr)
        return False
    
    print("✅ Build successful")
    return True

def start_server():
    """Start the server in background"""
    print("\n🚀 Starting server...")
    log_file = open("/tmp/rca_server.log", "w")
    process = subprocess.Popen(
        ["./target/release/server"],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=Path.cwd()
    )
    
    # Wait for server to start
    print("⏳ Waiting for server to start...")
    for i in range(10):
        time.sleep(1)
        try:
            response = requests.get("http://localhost:8080/api/health", timeout=1)
            if response.status_code == 200:
                print("✅ Server started (PID: {})".format(process.pid))
                return process
        except:
            continue
    
    print("❌ Server failed to start")
    process.terminate()
    return None

def test_health():
    """Test health endpoint"""
    print("\n🏥 Testing health endpoint...")
    try:
        response = requests.get("http://localhost:8080/api/health", timeout=5)
        if response.status_code == 200:
            print("✅ Health check passed")
            print(f"   Response: {response.json()}")
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False

def test_graph_traversal(query):
    """Test the graph traversal RCA endpoint"""
    print(f"\n🔍 Testing Graph Traversal RCA")
    print(f"   Query: {query}")
    print()
    
    url = "http://localhost:8080/api/graph/traverse"
    payload = {
        "query": query,
        "metadata_dir": "metadata",
        "data_dir": "data"
    }
    
    print("📡 Sending request...")
    try:
        start_time = time.time()
        response = requests.post(url, json=payload, timeout=300)  # 5 minute timeout
        elapsed = time.time() - start_time
        
        if response.status_code != 200:
            print(f"❌ Request failed: {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            return None
        
        print(f"✅ Request successful (took {elapsed:.2f}s)")
        print()
        
        result = response.json()
        
        # Display results
        print("📊 RCA Results:")
        print("=" * 80)
        
        if "result" in result:
            res = result["result"]
            
            print(f"\n🎯 Root Cause Found: {res.get('root_cause_found', 'Unknown')}")
            print(f"📏 Current Depth: {res.get('current_depth', 0)}/{res.get('max_depth', 10)}")
            
            if res.get("current_hypothesis"):
                print(f"\n💡 Current Hypothesis:")
                print(f"   {res['current_hypothesis']}")
            
            if res.get("findings"):
                print(f"\n🔍 Findings ({len(res['findings'])}):")
                for i, finding in enumerate(res['findings'][:10], 1):
                    print(f"   {i}. {finding}")
                if len(res['findings']) > 10:
                    print(f"   ... and {len(res['findings']) - 10} more")
            
            if res.get("visited_path"):
                print(f"\n🛤️  Visited Path ({len(res['visited_path'])} nodes):")
                for i, node in enumerate(res['visited_path'][:15], 1):
                    print(f"   {i}. {node}")
                if len(res['visited_path']) > 15:
                    print(f"   ... and {len(res['visited_path']) - 15} more")
            
            if res.get("hints"):
                print(f"\n💭 Hints Used ({len(res['hints'])}):")
                for i, hint in enumerate(res['hints'][:5], 1):
                    print(f"   {i}. {hint}")
                if len(res['hints']) > 5:
                    print(f"   ... and {len(res['hints']) - 5} more")
        
        print("\n" + "=" * 80)
        
        return result
        
    except requests.exceptions.Timeout:
        print("❌ Request timed out (this may take a while with LLM calls)")
        return None
    except Exception as e:
        print(f"❌ Request failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_regular_rca(query):
    """Test the regular RCA endpoint"""
    print(f"\n🔍 Testing Regular RCA Endpoint")
    print(f"   Query: {query}")
    print()
    
    url = "http://localhost:8080/api/reasoning/query"
    payload = {"query": query}
    
    try:
        start_time = time.time()
        response = requests.post(url, json=payload, timeout=300)
        elapsed = time.time() - start_time
        
        if response.status_code != 200:
            print(f"⚠️  Request failed: {response.status_code}")
            return None
        
        print(f"✅ Request successful (took {elapsed:.2f}s)")
        
        result = response.json()
        if "result" in result:
            print("\n📊 Result Preview:")
            print(result["result"][:500] + "..." if len(result["result"]) > 500 else result["result"])
        
        return result
        
    except Exception as e:
        print(f"⚠️  Request failed: {e}")
        return None

def main():
    """Main test function"""
    print("🧪 RCA Engine End-to-End Test (Detailed)")
    print("=" * 80)
    print()
    
    # Check environment
    if not check_env():
        sys.exit(1)
    
    # Build server
    if not build_server():
        sys.exit(1)
    
    # Start server
    server_process = start_server()
    if not server_process:
        sys.exit(1)
    
    try:
        # Test health
        if not test_health():
            return
        
        # Test queries
        test_queries = [
            "Why is the outstanding balance different between khatabook and tb for loan L001?",
            "What is the difference in TOS between collections_mis and outstanding_daily?",
            "Why is there a mismatch in total outstanding between system A and system B?",
        ]
        
        # Use first query for graph traversal (the advanced one)
        print("\n" + "=" * 80)
        print("TESTING ADVANCED GRAPH TRAVERSAL RCA")
        print("=" * 80)
        graph_result = test_graph_traversal(test_queries[0])
        
        # Also test regular RCA
        print("\n" + "=" * 80)
        print("TESTING REGULAR RCA ENDPOINT")
        print("=" * 80)
        regular_result = test_regular_rca(test_queries[0])
        
        print("\n" + "=" * 80)
        print("✅ End-to-End Test Complete!")
        print("=" * 80)
        print("\n📋 Server logs available at: /tmp/rca_server.log")
        
        if graph_result:
            print("\n✅ Graph traversal RCA test PASSED")
        else:
            print("\n❌ Graph traversal RCA test FAILED")
        
    finally:
        # Stop server
        print("\n🛑 Stopping server...")
        server_process.terminate()
        try:
            server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_process.kill()
        print("✅ Server stopped")

if __name__ == "__main__":
    main()

