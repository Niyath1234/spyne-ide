# Metadata-Only Architecture - Implementation Plan

## Problem
Currently storing actual data (544 rows × 28 columns) in our system.
This violates the principle: **We only store metadata, not proprietary data.**

## Solution: Metadata-Only Storage

### What Changes

#### 1. Upload Handler (`upload_handler.py`)
**REMOVE**:
- `sample_values` from column metadata
- `min_value`, `max_value` actual values
- Converted CSV storage (or mark as temporary cache)

**KEEP**:
- Column names, data types
- Null counts (numbers), unique counts (numbers)
- Row count
- File path reference (to original file)

#### 2. Database Schema
**REMOVE from `dataset_columns`**:
```sql
ALTER TABLE dataset_columns DROP COLUMN sample_values;
ALTER TABLE dataset_columns DROP COLUMN min_value;
ALTER TABLE dataset_columns DROP COLUMN max_value;
```

**ADD to `tables`**:
```sql
ALTER TABLE tables ADD COLUMN external_file_path TEXT;
ALTER TABLE tables ADD COLUMN file_access_method TEXT; -- 'local_path', 's3', 'http', etc.
```

#### 3. Data Access Pattern
**Old (Wrong)**:
```python
# Read from our stored CSV
df = pd.read_csv('data/uploads/file.csv')
```

**New (Correct)**:
```python
# Read from original file path (org's system)
original_path = metadata['external_file_path']
df = pd.read_csv(original_path)  # or read_excel()
```

#### 4. Upload API Response
**Old**:
```json
{
  "file_path": "data/uploads/file.csv",  ❌
  "sample_values": ["value1", "value2"],  ❌
  "min_value": "100",  ❌
  "max_value": "1000"  ❌
}
```

**New**:
```json
{
  "external_file_path": "/Users/niyathnair/Downloads/file.xlsx",  ✅
  "schema": ["col1: integer", "col2: string"],  ✅
  "row_count": 544,  ✅
  "null_counts": {"col1": 0, "col2": 388}  ✅
}
```

## Implementation Steps

### Step 1: Update Database Schema
```sql
-- Remove data storage columns
ALTER TABLE dataset_columns 
  DROP COLUMN IF EXISTS sample_values,
  DROP COLUMN IF EXISTS min_value,
  DROP COLUMN IF EXISTS max_value;

-- Add external reference columns
ALTER TABLE tables 
  ADD COLUMN IF NOT EXISTS external_file_path TEXT,
  ADD COLUMN IF NOT EXISTS file_access_method TEXT DEFAULT 'local_path',
  ADD COLUMN IF NOT EXISTS file_sheet_name TEXT;

-- Update existing uploaded tables to reference original locations
UPDATE tables 
SET file_access_method = 'local_path' 
WHERE system = 'uploaded';
```

### Step 2: Update Upload Handler
```python
def analyze_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze dataframe - METADATA ONLY, no actual data"""
    schema = []
    
    for idx, col in enumerate(df.columns):
        col_data = df[col]
        col_type = self.infer_column_type(col_data)
        
        # ONLY STATISTICS, NO ACTUAL VALUES
        null_count = int(col_data.isnull().sum())
        unique_count = int(col_data.nunique())
        
        schema.append({
            'column_name': col,
            'column_position': idx,
            'data_type': col_type,
            'nullable': null_count > 0,
            'null_count': null_count,
            'unique_values_count': unique_count,
            # REMOVED: sample_values, min_value, max_value
        })
    
    return {
        'columns': schema,
        'row_count': len(df),
        'column_count': len(df.columns)
    }
```

### Step 3: Update File Storage Strategy

**Option A: No Storage (Reference Only)**
```python
def handle_csv_upload(self, original_file_path: str, ...):
    # Don't copy file, just reference it
    df = pd.read_csv(original_file_path)
    schema = self.analyze_dataframe(df)  # In-memory only
    
    # Store ONLY metadata
    self.register_table(
        table_name=...,
        external_file_path=original_file_path,  # Original location
        schema=schema
    )
    # File stays where it was uploaded from
```

**Option B: Temporary Cache (with TTL)**
```python
def handle_csv_upload(self, file_path: Path, ...):
    # Convert Excel to CSV (temporary cache)
    if file_path.suffix == '.xlsx':
        csv_path = self.convert_to_csv(file_path)
        
        # Store metadata
        self.register_table(
            external_file_path=str(file_path),  # Original Excel
            cache_path=str(csv_path),  # Temporary CSV (can be deleted)
            cache_ttl=86400  # 24 hours
        )
```

### Step 4: Update RCA Engine to Read from Original Files

```rust
// In RCA engine
fn load_table_data(&self, table_name: &str) -> Result<DataFrame> {
    // Get table metadata
    let table = self.metadata.get_table(table_name)?;
    
    // Read from ORIGINAL file path (org's system)
    match table.file_access_method {
        "local_path" => {
            let df = CsvReader::from_path(&table.external_file_path)?
                .finish()?;
            Ok(df)
        }
        "s3" => {
            // Read from S3
        }
        _ => Err("Unsupported file access method")
    }
}
```

## Benefits

### Security ✅
- No sensitive data at rest in RCA system
- Complies with data privacy regulations
- Audit trail without data exposure

### Storage ✅
- Minimal disk usage (only metadata)
- No data duplication
- Scales to millions of rows

### Freshness ✅
- Always reads latest data from source
- No sync/replication lag
- Source of truth is always org's system

### Compliance ✅
- GDPR compliant (no personal data stored)
- Data residency requirements met
- Right to be forgotten (delete file, metadata remains)

## What User Should Provide

### For Excel/CSV Uploads
```bash
curl -X POST http://localhost:8081/api/upload/excel \
  -F "file=@/path/to/original/file.xlsx" \
  -F "sheet_name=Sheet1" \
  -F "keep_original_path=true"  # NEW: Don't copy file
```

### For Remote Files
```bash
curl -X POST http://localhost:8081/api/register/external \
  -H "Content-Type: application/json" \
  -d '{
    "file_path": "/company/nfs/share/disbursements.xlsx",
    "sheet_name": "Sheet1",
    "access_method": "local_path"
  }'
```

### For Database Tables (Future)
```bash
curl -X POST http://localhost:8081/api/register/database \
  -H "Content-Type: application/json" \
  -d '{
    "connection_string": "postgresql://host/db",
    "table_name": "disbursements",
    "access_method": "postgresql"
  }'
```

## Migration for Existing Data

```sql
-- Update existing uploads to reference original locations
UPDATE tables 
SET external_file_path = REPLACE(path, 'data/uploads/', '/Users/niyathnair/Downloads/')
WHERE system = 'uploaded';

-- Clean up stored data files
-- (manually delete files from data/uploads/)
```

## Summary

### Current (Wrong) ❌
- Upload file → Store in data/uploads/ → Query from there
- Store actual data, sample values, min/max

### Correct (Metadata-Only) ✅
- Upload file → Analyze schema → Store metadata only → Query from original location
- Store only: column names, types, counts (no actual values)

This aligns with your architecture: **"We're in the middle, helping with analysis, not storing org's data."**

