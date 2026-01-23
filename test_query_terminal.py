#!/usr/bin/env python3
"""
Terminal Query Tester
Test queries in terminal and see the generated SQL

Usage:
    python3 test_query_terminal.py "your natural language query here"
    
Or run interactively:
    python3 test_query_terminal.py
"""

import sys
import json
import requests
from typing import Optional

RCA_API_URL = "http://localhost:8080/api"

def print_header(text: str):
    """Print a formatted header"""
    print("\n" + "="*80)
    print(f"  {text}")
    print("="*80)

def print_section(text: str):
    """Print a section header"""
    print(f"\n📋 {text}")
    print("-" * 80)

def test_query(question: str, show_intent: bool = True) -> Optional[dict]:
    """
    Test a query and display the generated SQL
    
    Args:
        question: Natural language query
        show_intent: Whether to show the intent JSON
    """
    print_header("QUERY TEST")
    print(f"\n📝 Your Query:")
    print(f"   {question}")
    
    try:
        print(f"\n🔄 Sending request to API...")
        response = requests.post(
            f"{RCA_API_URL}/assistant/ask",
            json={"question": question},
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            
            if not result:
                print(f"\n❌ Error: Empty response from server")
                return {'success': False, 'error': 'Empty response'}
            
            # Extract information - handle different response structures
            answer = result.get('answer', 'N/A')
            
            # Try different possible structures for SQL
            sql = 'N/A'
            query_result = result.get('query_result')
            
            if query_result:
                if isinstance(query_result, dict):
                    sql = query_result.get('sql', query_result.get('query', 'N/A'))
                elif isinstance(query_result, str):
                    sql = query_result
            else:
                # Check if SQL is directly in result
                sql = result.get('sql', result.get('query', 'N/A'))
            
            intent = result.get('intent', {})
            
            # Check response type
            response_type = result.get('response_type', 'Success')
            
            # Show confidence if available
            confidence = result.get('confidence')
            if confidence is not None:
                print_section("Confidence")
                print(f"   {confidence * 100:.1f}%")
            
            if response_type == 'NeedsClarification':
                clarification = result.get('clarification', {})
                if clarification:
                    print_section("⚠️  Clarification Needed")
                    question = clarification.get('question', 'Need more information')
                    print(f"   {question}")
                    
                    missing = clarification.get('missing_pieces', [])
                    if missing:
                        print(f"\n   Missing information:")
                        for piece in missing[:5]:  # Show first 5
                            if isinstance(piece, dict):
                                field = piece.get('field', 'unknown')
                                desc = piece.get('description', '')
                                print(f"   - {field}: {desc}")
                            else:
                                print(f"   - {piece}")
                    
                    # Show partial understanding if available
                    partial = clarification.get('partial_understanding', {})
                    if partial:
                        print(f"\n   Partial understanding:")
                        if partial.get('metrics'):
                            print(f"   - Metrics: {partial.get('metrics')}")
                        if partial.get('dimensions'):
                            print(f"   - Dimensions: {partial.get('dimensions')}")
                        if partial.get('tables'):
                            print(f"   - Tables: {partial.get('tables')}")
            
            # Show intent if requested
            if show_intent and intent:
                print_section("Generated Intent (from LLM)")
                print(json.dumps(intent, indent=2))
                
                # Highlight dimension usage
                dimension_intents = intent.get('dimension_intents', [])
                if dimension_intents:
                    print(f"\n🎯 Dimension Usage:")
                    for dim in dimension_intents:
                        usage = dim.get('usage', 'unknown')
                        name = dim.get('name', 'unknown')
                        print(f"   - {name}: {usage}")
            
            # Show generated SQL
            print_section("Generated SQL Query")
            if sql and sql != 'N/A':
                print(sql)
                
                # Analyze joins
                import re
                joins = re.findall(r'(LEFT|INNER|RIGHT|FULL)\s+JOIN\s+(\w+(?:\.\w+)?)', sql, re.IGNORECASE)
                if joins:
                    print(f"\n🔍 Join Analysis:")
                    for join_type, table in joins:
                        print(f"   {join_type.upper()} JOIN {table}")
            else:
                print("   No SQL generated")
            
            # Show answer
            print_section("Answer")
            if answer and answer != 'N/A':
                if isinstance(answer, str):
                    # Try to parse JSON answer
                    try:
                        answer_json = json.loads(answer)
                        print(json.dumps(answer_json, indent=2))
                    except:
                        # Truncate long answers
                        print(answer[:1000] if len(answer) > 1000 else answer)
                else:
                    print(answer)
            else:
                print("   No answer provided")
            
            # Show reasoning steps if available
            reasoning_steps = result.get('reasoning_steps', [])
            if reasoning_steps:
                print_section("Reasoning Steps")
                for i, step in enumerate(reasoning_steps[:5], 1):  # Show first 5
                    print(f"   {i}. {step}")
            
            # Show full response for debugging if needed (only if SQL not found)
            if sql == 'N/A' and result.get('response_type') != 'NeedsClarification':
                print_section("Debug: Response Structure")
                print(f"   Response type: {result.get('response_type', 'Unknown')}")
                print(f"   Available keys: {list(result.keys())}")
            
            print_header("TEST COMPLETE")
            
            return {
                'success': True,
                'sql': sql,
                'intent': intent,
                'answer': answer
            }
        else:
            print(f"\n❌ Error: HTTP {response.status_code}")
            print(f"   {response.text[:500]}")
            return {
                'success': False,
                'error': f"HTTP {response.status_code}",
                'response': response.text
            }
            
    except requests.exceptions.ConnectionError:
        print(f"\n❌ Error: Cannot connect to API server at {RCA_API_URL}")
        print(f"   Make sure the server is running:")
        print(f"   cargo run --bin server")
        return None
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def interactive_mode():
    """Run in interactive mode"""
    print_header("INTERACTIVE QUERY TESTER")
    print("\nEnter your queries (type 'exit' or 'quit' to stop)")
    print("Type 'show-intent' to toggle showing intent JSON")
    
    show_intent = True
    
    while True:
        try:
            print("\n" + "-"*80)
            query = input("\n💬 Query: ").strip()
            
            if not query:
                continue
            
            if query.lower() in ['exit', 'quit', 'q']:
                print("\n👋 Goodbye!")
                break
            
            if query.lower() == 'show-intent':
                show_intent = not show_intent
                print(f"   {'Showing' if show_intent else 'Hiding'} intent JSON")
                continue
            
            test_query(query, show_intent=show_intent)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except EOFError:
            print("\n\n👋 Goodbye!")
            break


def main():
    """Main entry point"""
    if len(sys.argv) > 1:
        # Command line mode
        query = " ".join(sys.argv[1:])
        result = test_query(query)
        
        if result and result.get('success'):
            sys.exit(0)
        else:
            sys.exit(1)
    else:
        # Interactive mode
        interactive_mode()


if __name__ == '__main__':
    main()

