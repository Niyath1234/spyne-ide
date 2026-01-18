#!/usr/bin/env python3
"""
Test Upload Pipeline - CSV and Excel
Creates test files, uploads them, and verifies processing
"""

import os
import sys
import pandas as pd
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def create_test_csv():
    """Create a test CSV file"""
    test_data = {
        'customer_id': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Adams'],
        'balance': [1500.50, 2300.75, 1800.00, 3500.25, 1200.00],
        'account_type': ['savings', 'checking', 'savings', 'investment', 'checking'],
        'status': ['active', 'active', 'inactive', 'active', 'active'],
        'signup_date': ['2024-01-15', '2024-02-20', '2024-03-10', '2024-04-05', '2024-05-12']
    }
    
    df = pd.DataFrame(test_data)
    filepath = Path('test_customers.csv')
    df.to_csv(filepath, index=False)
    
    print(f"✅ Created test CSV: {filepath}")
    print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
    return filepath

def create_test_excel():
    """Create a test Excel file with multiple sheets"""
    # Sheet 1: Products
    products_data = {
        'product_id': ['P001', 'P002', 'P003', 'P004'],
        'product_name': ['Widget A', 'Widget B', 'Gadget X', 'Gadget Y'],
        'price': [99.99, 149.99, 199.99, 249.99],
        'category': ['widgets', 'widgets', 'gadgets', 'gadgets'],
        'in_stock': [True, True, False, True]
    }
    
    # Sheet 2: Sales
    sales_data = {
        'sale_id': ['S001', 'S002', 'S003', 'S004', 'S005'],
        'product_id': ['P001', 'P002', 'P001', 'P003', 'P004'],
        'customer_id': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'quantity': [2, 1, 3, 1, 2],
        'total_amount': [199.98, 149.99, 299.97, 199.99, 499.98],
        'sale_date': ['2025-01-10', '2025-01-11', '2025-01-12', '2025-01-13', '2025-01-14']
    }
    
    filepath = Path('test_data.xlsx')
    
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        pd.DataFrame(products_data).to_excel(writer, sheet_name='Products', index=False)
        pd.DataFrame(sales_data).to_excel(writer, sheet_name='Sales', index=False)
    
    print(f"✅ Created test Excel: {filepath}")
    print(f"   Sheets: Products (4 rows), Sales (5 rows)")
    return filepath

def test_csv_upload(api_url: str, filepath: Path):
    """Test CSV upload"""
    print(f"\n📤 Testing CSV upload: {filepath.name}")
    
    with open(filepath, 'rb') as f:
        files = {'file': (filepath.name, f, 'text/csv')}
        data = {'uploaded_by': 'test_user'}
        
        response = requests.post(f"{api_url}/api/upload/csv", files=files, data=data)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ CSV upload successful!")
        print(f"   Job ID: {result['job_id']}")
        print(f"   Table Name: {result['table_name']}")
        print(f"   Rows: {result['rows']}, Columns: {result['columns']}")
        print(f"   Status: {result['status']}")
        print(f"   Quality Checks: {len(result['quality_checks'])} checks performed")
        return result
    else:
        print(f"❌ CSV upload failed: {response.status_code}")
        print(f"   Error: {response.text}")
        return None

def test_excel_upload(api_url: str, filepath: Path, sheet_name: str):
    """Test Excel upload"""
    print(f"\n📤 Testing Excel upload: {filepath.name}, sheet: {sheet_name}")
    
    with open(filepath, 'rb') as f:
        files = {'file': (filepath.name, f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
        data = {'uploaded_by': 'test_user', 'sheet_name': sheet_name}
        
        response = requests.post(f"{api_url}/api/upload/excel", files=files, data=data)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ Excel upload successful!")
        print(f"   Job ID: {result['job_id']}")
        print(f"   Table Name: {result['table_name']}")
        print(f"   Sheet: {result['sheet_name']}")
        print(f"   Rows: {result['rows']}, Columns: {result['columns']}")
        print(f"   Status: {result['status']}")
        print(f"   Quality Checks: {len(result['quality_checks'])} checks performed")
        return result
    else:
        print(f"❌ Excel upload failed: {response.status_code}")
        print(f"   Error: {response.text}")
        return None

def test_get_uploads(api_url: str):
    """Test getting upload history"""
    print(f"\n📋 Testing upload history endpoint...")
    
    response = requests.get(f"{api_url}/api/uploads?limit=10")
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ Retrieved upload history!")
        print(f"   Total uploads: {result['count']}")
        for upload in result['uploads'][:5]:
            print(f"   - {upload['file_name']} ({upload['file_type']}): {upload['status']}")
        return result
    else:
        print(f"❌ Failed to get upload history: {response.status_code}")
        return None

def test_get_datasets(api_url: str):
    """Test getting datasets list"""
    print(f"\n📊 Testing datasets endpoint...")
    
    response = requests.get(f"{api_url}/api/datasets")
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ Retrieved datasets list!")
        print(f"   Total datasets: {result['count']}")
        for dataset in result['datasets']:
            print(f"   - {dataset['name']}: v{dataset['current_version']}, {dataset['row_count']} rows")
        return result
    else:
        print(f"❌ Failed to get datasets: {response.status_code}")
        return None

def test_get_versions(api_url: str, table_name: str):
    """Test getting dataset versions"""
    print(f"\n🔄 Testing versions endpoint for table: {table_name}...")
    
    response = requests.get(f"{api_url}/api/datasets/{table_name}/versions")
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ Retrieved version history!")
        print(f"   Total versions: {result['count']}")
        for version in result['versions']:
            print(f"   - v{version['version_number']}: {version['row_count']} rows, active: {version['is_active']}")
        return result
    else:
        print(f"❌ Failed to get versions: {response.status_code}")
        return None

def main():
    """Run all tests"""
    print("=" * 70)
    print("🧪 Upload Pipeline Test Suite")
    print("=" * 70)
    
    # Check if upload API is running
    api_url = "http://localhost:8081"
    
    try:
        response = requests.get(f"{api_url}/api/health", timeout=2)
        if response.status_code == 200:
            print(f"✅ Upload API is running at {api_url}")
        else:
            print(f"⚠️  Upload API returned unexpected status: {response.status_code}")
            return 1
    except requests.exceptions.ConnectionError:
        print(f"❌ Upload API is not running at {api_url}")
        print(f"   Start it with: python3 upload_api_server.py")
        return 1
    
    # Create test files
    print("\n📝 Creating test files...")
    csv_file = create_test_csv()
    excel_file = create_test_excel()
    
    # Test CSV upload
    csv_result = test_csv_upload(api_url, csv_file)
    
    # Test Excel uploads (both sheets)
    excel_result1 = test_excel_upload(api_url, excel_file, 'Products')
    excel_result2 = test_excel_upload(api_url, excel_file, 'Sales')
    
    # Test history endpoints
    test_get_uploads(api_url)
    test_get_datasets(api_url)
    
    # Test version history if uploads succeeded
    if csv_result:
        test_get_versions(api_url, csv_result['table_name'])
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 Test Summary")
    print("=" * 70)
    
    tests_passed = 0
    tests_total = 5
    
    if csv_result:
        print("✅ CSV upload test: PASSED")
        tests_passed += 1
    else:
        print("❌ CSV upload test: FAILED")
    
    if excel_result1:
        print("✅ Excel upload test (Products): PASSED")
        tests_passed += 1
    else:
        print("❌ Excel upload test (Products): FAILED")
    
    if excel_result2:
        print("✅ Excel upload test (Sales): PASSED")
        tests_passed += 1
    else:
        print("❌ Excel upload test (Sales): FAILED")
    
    print(f"\n✅ {tests_passed}/{tests_total} tests passed")
    
    # Cleanup
    print("\n🧹 Cleaning up test files...")
    csv_file.unlink()
    excel_file.unlink()
    print("✅ Test files removed")
    
    return 0 if tests_passed == tests_total else 1

if __name__ == '__main__':
    sys.exit(main())

