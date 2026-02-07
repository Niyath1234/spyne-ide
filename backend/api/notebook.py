"""
Notebook API Endpoints

Trino SQL Notebook endpoints for cell-based SQL composition.
"""

import re
import json
import os
import threading
from typing import Dict, List, Optional, Any
from flask import Blueprint, request, jsonify

notebook_router = Blueprint('notebook', __name__)

# File-based notebook storage (shared across Gunicorn workers)
NOTEBOOKS_FILE = os.getenv('NOTEBOOKS_STORAGE_PATH', '/tmp/rca_notebooks.json')
_notebooks_lock = threading.Lock()

def _load_notebooks() -> Dict[str, Dict[str, Any]]:
    """Load notebooks from file."""
    if os.path.exists(NOTEBOOKS_FILE):
        try:
            with open(NOTEBOOKS_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def _save_notebooks(notebooks: Dict[str, Dict[str, Any]]):
    """Save notebooks to file."""
    import logging
    logger = logging.getLogger('notebook_api')
    try:
        # Ensure directory exists
        dir_path = os.path.dirname(NOTEBOOKS_FILE)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        # Use atomic write: write to temp file then rename
        temp_file = NOTEBOOKS_FILE + '.tmp'
        logger.info(f'Attempting to save {len(notebooks)} notebooks to {NOTEBOOKS_FILE}')
        with open(temp_file, 'w') as f:
            json.dump(notebooks, f, indent=2)
        os.replace(temp_file, NOTEBOOKS_FILE)
        logger.info(f'✅ Successfully saved {len(notebooks)} notebooks to {NOTEBOOKS_FILE}')
        # Verify file exists
        if not os.path.exists(NOTEBOOKS_FILE):
            logger.error(f'❌ File {NOTEBOOKS_FILE} does not exist after save operation!')
    except (IOError, OSError) as e:
        logger.error(f'❌ Failed to save notebooks to {NOTEBOOKS_FILE}: {e}', exc_info=True)
        # Don't raise - allow in-memory operation as fallback
    except Exception as e:
        logger.error(f'❌ Unexpected error saving notebooks: {type(e).__name__}: {e}', exc_info=True)

def _get_notebooks() -> Dict[str, Dict[str, Any]]:
    """Get notebooks dict (thread-safe)."""
    with _notebooks_lock:
        return _load_notebooks()

def _set_notebook(notebook_id: str, notebook: Dict[str, Any]):
    """Set a notebook (thread-safe)."""
    import logging
    logger = logging.getLogger('notebook_api')
    with _notebooks_lock:
        notebooks = _load_notebooks()
        notebooks[notebook_id] = notebook
        logger.info(f'Saving notebook {notebook_id} to {NOTEBOOKS_FILE}')
        _save_notebooks(notebooks)
        # Verify it was saved
        if os.path.exists(NOTEBOOKS_FILE):
            logger.info(f'Notebook {notebook_id} saved successfully')
        else:
            logger.warning(f'Notebook {notebook_id} save completed but file {NOTEBOOKS_FILE} does not exist')

def _delete_notebook(notebook_id: str):
    """Delete a notebook (thread-safe)."""
    with _notebooks_lock:
        notebooks = _load_notebooks()
        if notebook_id in notebooks:
            del notebooks[notebook_id]
            _save_notebooks(notebooks)

# Execution router instance
_executor = None


def init_notebook(executor=None):
    """Initialize notebook endpoints with executor."""
    global _executor
    _executor = executor


def _validate_cell_sql(sql: str) -> tuple[bool, Optional[str]]:
    """
    Validate that a cell contains valid SQL that returns a table.
    
    Rules:
    - Must end with a SELECT statement
    - Cannot contain CREATE, DROP, ALTER, INSERT, UPDATE, DELETE
    - Must be valid SQL syntax
    """
    sql = sql.strip()
    if not sql:
        return False, "Cell cannot be empty"
    
    # Check for disallowed statements
    disallowed_patterns = [
        r'\bCREATE\s+TABLE\b',
        r'\bDROP\s+TABLE\b',
        r'\bALTER\s+TABLE\b',
        r'\bINSERT\s+INTO\b',
        r'\bUPDATE\b',
        r'\bDELETE\s+FROM\b',
        r'\bTRUNCATE\b',
    ]
    
    sql_upper = sql.upper()
    for pattern in disallowed_patterns:
        if re.search(pattern, sql_upper):
            return False, f"Cell cannot contain mutation statements: {pattern}"
    
    # Must end with SELECT (allowing for WITH clauses before)
    # This is a simple check - in production, use proper SQL parser
    if not re.search(r'\bSELECT\b', sql_upper):
        return False, "Cell must contain a SELECT statement"
    
    return True, None


def _extract_refs(sql: str) -> List[tuple[str, str]]:
    """
    Extract %%ref directives from SQL.
    
    Supports formats:
    - %%ref <cell_id> AS <alias>
    - %%ref-cell_id AS <alias> (legacy, for compatibility)
    
    Returns list of (cell_id, alias) tuples.
    """
    refs = []
    lines = sql.split('\n')
    for line in lines:
        line = line.strip()
        if line.startswith('%%ref'):
            # Parse: %%ref <cell_id> AS <alias> (space-separated, correct format)
            match = re.match(r'%%ref\s+(\w+)\s+AS\s+(\w+)', line, re.IGNORECASE)
            if match:
                cell_id = match.group(1)
                alias = match.group(2)
                refs.append((cell_id, alias))
            else:
                # Try legacy format: %%ref-cell_id AS <alias> (for error message clarity)
                legacy_match = re.match(r'%%ref-(\w+)\s+AS\s+(\w+)', line, re.IGNORECASE)
                if legacy_match:
                    # Provide helpful error message
                    raise ValueError(
                        f"Invalid %%ref syntax: '{line}'. "
                        f"Use space-separated format: '%%ref {legacy_match.group(1)} AS {legacy_match.group(2)}'"
                    )
    return refs


def _compile_notebook(notebook: Dict[str, Any], target_cell_id: Optional[str] = None) -> tuple[str, Optional[str]]:
    """
    Compile notebook cells into a single Trino query.
    
    Architecture: 
    - Each referenced cell is wrapped as a black-box subquery CTE.
    - CTE names use user-provided aliases from %%ref directives, never cell_ids.
    - Referenced cells compile to: <alias> AS (SELECT * FROM (<cell SQL>) <alias>_subq)
    - No internal identifiers (cell_id) appear in generated SQL.
    
    If target_cell_id is provided, compiles up to and including that cell.
    Otherwise, compiles all cells.
    
    Returns (compiled_sql, error_message)
    """
    cells = notebook.get('cells', [])
    if not cells:
        return "", "Notebook has no cells"
    
    # If target_cell_id is provided, only compile up to that cell
    if target_cell_id:
        target_idx = None
        for idx, cell in enumerate(cells):
            if cell.get('id') == target_cell_id:
                target_idx = idx
                break
        if target_idx is None:
            return "", f"Cell {target_cell_id} not found"
        cells = cells[:target_idx + 1]
    
    # Build cell map (internal use only - cell_ids never appear in generated SQL)
    cell_map = {cell['id']: cell for cell in cells}
    
    # Extract refs for each cell and build dependency order
    # This is simple ordered resolution of explicit %%ref directives, not a DAG
    refs_map: Dict[str, List[tuple[str, str]]] = {}  # cell_id -> [(ref_cell_id, alias), ...]
    cell_sql_map: Dict[str, str] = {}  # cell_id -> cleaned SQL
    
    for cell in cells:
        cell_id = cell['id']
        sql = cell.get('sql', '').strip()
        
        # Remove %%ref lines and extract them
        lines = sql.split('\n')
        sql_lines = []
        refs = []
        
        for line in lines:
            stripped = line.strip()
            # Check for %%ref directive
            if stripped.startswith('%%ref'):
                match = re.match(r'%%ref\s+(\w+)\s+AS\s+(\w+)', stripped, re.IGNORECASE)
                if match:
                    ref_cell_id = match.group(1)
                    alias = match.group(2)
                    refs.append((ref_cell_id, alias))
                else:
                    # Invalid %%ref syntax
                    return "", (
                        f"Cell {cell_id}: Invalid %%ref syntax: '{stripped}'. "
                        f"Use format: '%%ref <cell_id> AS <alias>'"
                    )
            elif stripped.startswith('ref ') and not stripped.startswith('%%ref'):
                # User wrote 'ref' instead of '%%ref' - provide helpful error
                return "", (
                    f"Cell {cell_id}: Missing '%%' prefix in ref directive: '{stripped}'. "
                    f"Use format: '%%ref <cell_id> AS <alias>' (with double percent signs). "
                    f"Example: '%%ref {stripped.replace('ref ', '').split()[0] if stripped.split() else 'cell_id'} AS base'"
                )
            else:
                sql_lines.append(line)
        
        cell_sql = '\n'.join(sql_lines).strip()
        
        # Validate cell SQL
        is_valid, error = _validate_cell_sql(cell_sql)
        if not is_valid:
            return "", f"Cell {cell_id}: {error}"
        
        # Validate %%ref syntax
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('%%ref-'):
                return "", (
                    f"Cell {cell_id}: Invalid %%ref syntax. "
                    f"Use space-separated format: '%%ref <cell_id> AS <alias>' "
                    f"instead of '%%ref-<cell_id> AS <alias>'"
                )
        
        # Check that all referenced cells exist
        for ref_cell_id, alias in refs:
            if ref_cell_id not in cell_map:
                return "", f"Cell {cell_id} references unknown cell {ref_cell_id}"
        
        refs_map[cell_id] = refs
        cell_sql_map[cell_id] = cell_sql
    
    # Determine final cell (the one being executed)
    final_cell = cells[-1]
    final_cell_id = final_cell['id']
    final_refs = refs_map.get(final_cell_id, [])
    final_sql = cell_sql_map[final_cell_id]
    
    # Collect all referenced cells (cells that need to become CTEs)
    # Map: alias -> (ref_cell_id, cell_sql)
    # Each %%ref directive creates a CTE with the specified alias
    referenced_cells: Dict[str, tuple[str, str]] = {}
    
    def collect_references(cell_id: str):
        """Recursively collect all referenced cells."""
        refs = refs_map.get(cell_id, [])
        for ref_cell_id, alias in refs:
            # Check for alias conflicts (same alias used for different cells)
            if alias in referenced_cells:
                existing_cell_id, _ = referenced_cells[alias]
                if existing_cell_id != ref_cell_id:
                    return f"Alias '{alias}' used for multiple cells: {existing_cell_id} and {ref_cell_id}"
            
            # Add this referenced cell with its alias
            if alias not in referenced_cells:
                referenced_cells[alias] = (ref_cell_id, cell_sql_map[ref_cell_id])
                # Recursively collect its references
                error = collect_references(ref_cell_id)
                if error:
                    return error
        return None
    
    # Collect all references from final cell
    error = collect_references(final_cell_id)
    if error:
        return "", error
    
    # Build CTEs: each referenced cell becomes a CTE named by its alias
    # Format: <alias> AS (SELECT * FROM (<cell SQL>) <alias>_subq)
    # When building CTEs, we need to replace cell_id references in cell SQL with aliases
    cte_parts = []
    
    # Map: cell_id -> alias (for replacing references in SQL)
    cell_to_alias: Dict[str, str] = {}
    for alias, (ref_cell_id, _) in referenced_cells.items():
        cell_to_alias[ref_cell_id] = alias
    
    def replace_cell_refs_in_sql(sql: str, cell_id: str) -> str:
        """
        Replace cell_id references in SQL with their aliases.
        
        For a given cell, replace references to other cells (by cell_id) 
        with the aliases defined in this cell's %%ref directives.
        """
        refs = refs_map.get(cell_id, [])
        modified_sql = sql
        for ref_cell_id, alias in refs:
            # Replace references to ref_cell_id with alias
            # This handles cases where cell SQL uses cell_id as a table name
            pattern = rf'\b{re.escape(ref_cell_id)}\b'
            modified_sql = re.sub(pattern, alias, modified_sql, flags=re.IGNORECASE)
        return modified_sql
    
    # Process referenced cells ensuring dependencies come first
    # This is ordered resolution of explicit %%ref directives
    processed_aliases = set()
    
    def process_refs(cell_id: str):
        """Process references for a cell, ensuring dependencies come first."""
        refs = refs_map.get(cell_id, [])
        for ref_cell_id, alias in refs:
            if alias not in processed_aliases:
                # Process dependencies first (cells referenced by this cell)
                process_refs(ref_cell_id)
                
                # Get the referenced cell's SQL and replace its internal references
                ref_cell_sql = cell_sql_map[ref_cell_id]
                # Replace any cell_id references in this cell's SQL with their aliases
                ref_cell_sql = replace_cell_refs_in_sql(ref_cell_sql, ref_cell_id)
                
                # Create CTE for this referenced cell
                # Format: <alias> AS (SELECT * FROM (<cell SQL>) <alias>_subq)
                cte_sql = f"{alias} AS (\n  SELECT *\n  FROM (\n    {ref_cell_sql}\n  ) {alias}_subq\n)"
                cte_parts.append(cte_sql)
                processed_aliases.add(alias)
    
    # Process all references from final cell
    process_refs(final_cell_id)
    
    # Replace references in final SQL with alias names
    # References to cell_ids should be replaced with alias names
    modified_final_sql = replace_cell_refs_in_sql(final_sql, final_cell_id)
    
    # Build final query
    if cte_parts:
        # WITH clause with all referenced cells as CTEs
        with_clause = "WITH\n  " + ",\n  ".join(cte_parts)
        compiled = f"{with_clause}\n{modified_final_sql}"
    else:
        # No references: just use final SQL directly
        compiled = modified_final_sql
    
    return compiled, None


# List notebooks (GET /notebooks) - must come before /notebooks/<id> routes
@notebook_router.route('/notebooks', methods=['GET'])
def list_notebooks():
    """List all notebooks."""
    notebooks = _get_notebooks()
    return jsonify({
        'success': True,
        'notebooks': list(notebooks.values())
    })


# Create notebook (POST /notebooks)
@notebook_router.route('/notebooks', methods=['POST'])
def create_notebook():
    """Create a new notebook."""
    data = request.get_json() or {}
    notebooks = _get_notebooks()
    notebook_id = data.get('id') or f"notebook_{len(notebooks) + 1}"
    
    notebook = {
        'id': notebook_id,
        'engine': data.get('engine', 'trino'),  # Use engine from request
        'cells': data.get('cells', []),
        'metadata': data.get('metadata', {}),
        'created_at': data.get('created_at'),
        'updated_at': data.get('updated_at'),
    }
    
    _set_notebook(notebook_id, notebook)
    
    # Verify it was saved
    import logging
    logger = logging.getLogger('notebook_api')
    saved_notebooks = _get_notebooks()
    if notebook_id in saved_notebooks:
        logger.info(f'Notebook {notebook_id} verified in storage after create')
    else:
        logger.error(f'CRITICAL: Notebook {notebook_id} NOT found in storage after create!')
    
    return jsonify({
        'success': True,
        'notebook': notebook
    }), 201


# Get notebook by ID (GET /notebooks/<id>) - more specific route comes after list
@notebook_router.route('/notebooks/<notebook_id>', methods=['GET'])
def get_notebook(notebook_id: str):
    """Get a notebook by ID."""
    notebooks = _get_notebooks()
    notebook = notebooks.get(notebook_id)
    if not notebook:
        return jsonify({
            'success': False,
            'error': f'Notebook {notebook_id} not found'
        }), 404
    
    return jsonify({
        'success': True,
        'notebook': notebook
    })


@notebook_router.route('/notebooks/<notebook_id>', methods=['PUT'])
def update_notebook(notebook_id: str):
    """Update a notebook."""
    notebooks = _get_notebooks()
    notebook = notebooks.get(notebook_id)
    if not notebook:
        return jsonify({
            'success': False,
            'error': f'Notebook {notebook_id} not found'
        }), 404
    
    data = request.get_json() or {}
    
    # Update cells
    if 'cells' in data:
        notebook['cells'] = data['cells']
    
    # Update metadata
    if 'metadata' in data:
        notebook['metadata'] = {**notebook.get('metadata', {}), **data['metadata']}
    
    notebook['updated_at'] = data.get('updated_at')
    
    _set_notebook(notebook_id, notebook)
    
    return jsonify({
        'success': True,
        'notebook': notebook
    })


@notebook_router.route('/notebooks/<notebook_id>/compile', methods=['POST'])
def compile_notebook(notebook_id: str):
    """Compile a notebook to Trino SQL."""
    notebooks = _get_notebooks()
    notebook = notebooks.get(notebook_id)
    if not notebook:
        return jsonify({
            'success': False,
            'error': f'Notebook {notebook_id} not found'
        }), 404
    
    data = request.get_json() or {}
    target_cell_id = data.get('cell_id')  # Optional: compile up to specific cell
    
    compiled_sql, error = _compile_notebook(notebook, target_cell_id)
    
    if error:
        return jsonify({
            'success': False,
            'error': error
        }), 400
    
    return jsonify({
        'success': True,
        'sql': compiled_sql
    })


@notebook_router.route('/notebooks/<notebook_id>/execute', methods=['POST'])
def execute_notebook(notebook_id: str):
    """Execute a notebook cell."""
    notebooks = _get_notebooks()
    notebook = notebooks.get(notebook_id)
    if not notebook:
        return jsonify({
            'success': False,
            'error': f'Notebook {notebook_id} not found'
        }), 404
    
    data = request.get_json() or {}
    cell_id = data.get('cell_id')
    
    if not cell_id:
        return jsonify({
            'success': False,
            'error': 'Missing required field: cell_id'
        }), 400
    
    # Find cell
    cell = None
    for c in notebook.get('cells', []):
        if c.get('id') == cell_id:
            cell = c
            break
    
    if not cell:
        return jsonify({
            'success': False,
            'error': f'Cell {cell_id} not found'
        }), 404
    
    # Compile notebook up to this cell
    compiled_sql, error = _compile_notebook(notebook, cell_id)
    
    if error:
        return jsonify({
            'success': False,
            'error': error,
            'cell_id': cell_id
        }), 400
    
    # Execute query
    # Get engine from notebook metadata (defaults to 'trino')
    notebook_engine = notebook.get('engine', 'trino')
    
    if not _executor:
        # Try to use Trino directly via HTTP
        try:
            import requests
            import logging
            logger = logging.getLogger(__name__)
            # Use Trino service name from docker-compose (or localhost:8081 for local)
            # In docker: trino:8080 (internal), locally: localhost:8081 (external)
            # Check if we're in Docker by looking for service name
            trino_url = os.getenv('TRINO_COORDINATOR_URL')
            if not trino_url:
                # Try docker service name first, fallback to localhost
                try:
                    import socket
                    socket.gethostbyname('trino')
                    trino_url = 'http://trino:8080'  # Docker internal
                    logger.info('Using Docker internal Trino URL: http://trino:8080')
                except socket.gaierror:
                    trino_url = 'http://localhost:8081'  # Local development
                    logger.info('Using local Trino URL: http://localhost:8081')
            else:
                logger.info(f'Using Trino URL from environment: {trino_url}')
            
            trino_user = os.getenv('TRINO_USER', 'admin')
            # Use notebook engine or default catalog
            # For Trino, detect catalog from SQL query or use environment/default
            # Auto-detect catalog from SQL: if query references tpch.* use tpch, if tpcds.* use tpcds
            sql_lower = compiled_sql.lower()
            
            # Auto-detect catalog from SQL query
            if 'tpch.' in sql_lower or 'from tpch' in sql_lower or 'join tpch' in sql_lower:
                detected_catalog = 'tpch'
            elif 'tpcds.' in sql_lower or 'from tpcds' in sql_lower or 'join tpcds' in sql_lower:
                detected_catalog = 'tpcds'
            else:
                # Default: use environment variable or fallback to tpcds
                detected_catalog = None
            
            # Use detected catalog, environment variable, or default
            if notebook_engine == 'trino':
                trino_catalog = os.getenv('TRINO_CATALOG', detected_catalog or 'tpcds')
            else:
                trino_catalog = os.getenv('TRINO_CATALOG', 'memory')
            
            trino_schema = os.getenv('TRINO_SCHEMA', 'tiny')  # Default to 'tiny' schema for both TPCH and TPCDS
            
            logger.info(f'Executing Trino query: URL={trino_url}, Catalog={trino_catalog}, Schema={trino_schema}, User={trino_user}')
            logger.debug(f'Compiled SQL: {compiled_sql[:200]}...' if len(compiled_sql) > 200 else f'Compiled SQL: {compiled_sql}')
            
            headers = {
                'X-Trino-User': trino_user,
                'X-Trino-Catalog': trino_catalog,
                'X-Trino-Schema': trino_schema,
            }
            
            # Trino API: POST to /v1/statement with SQL as text/plain
            try:
                response = requests.post(
                    f'{trino_url}/v1/statement',
                    headers={
                        **headers,
                        'Content-Type': 'text/plain',
                    },
                    data=compiled_sql.encode('utf-8'),
                    timeout=300
                )
            except requests.exceptions.ConnectionError as e:
                logger.error(f'Connection error to Trino at {trino_url}: {str(e)}')
                raise Exception(f'Cannot connect to Trino at {trino_url}. Is Trino running? Error: {str(e)}')
            
            # Check for HTTP errors
            if response.status_code != 200:
                error_text = response.text or f'HTTP {response.status_code}'
                logger.error(f'Trino HTTP error {response.status_code}: {error_text[:500]}')
                raise Exception(f'Trino connection failed (HTTP {response.status_code}): {error_text[:200]}')
            
            # Parse Trino response
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                logger.error(f'Failed to parse Trino response: {response.text[:500]}')
                raise Exception(f'Invalid JSON response from Trino: {response.text[:200]}')
            
            # Check for errors in initial response
            if data.get('error'):
                error_info = data['error']
                error_msg = error_info.get('message', 'Unknown error')
                error_code = error_info.get('errorCode', 'UNKNOWN')
                raise Exception(f'Trino query error [{error_code}]: {error_msg}')
            
            # Check query state
            stats = data.get('stats', {})
            if stats.get('state') == 'FAILED':
                error_info = data.get('error', {})
                error_msg = error_info.get('message', 'Query failed')
                error_code = error_info.get('errorCode', 'UNKNOWN')
                raise Exception(f'Trino query failed [{error_code}]: {error_msg}')
            
            rows = []
            schema = []
            
            # Extract schema from columns (may be in initial response or later)
            if 'columns' in data:
                schema = [{'name': col['name'], 'type': col['type']} for col in data['columns']]
            
            # Extract data if available in initial response (rare but possible)
            if 'data' in data:
                rows.extend(data['data'])
            
            # Trino returns data in chunks via nextUri
            next_uri = data.get('nextUri')
            logger.info(f'Initial response: state={stats.get("state")}, nextUri={next_uri[:80] if next_uri else None}..., has_columns={"columns" in data}, has_data={"data" in data}')
            
            # If no nextUri and query is finished, we're done (rare case)
            if not next_uri and stats.get('state') == 'FINISHED':
                logger.info('Query completed immediately without nextUri')
                cell['result'] = {
                    'schema': schema,
                    'rows': rows[:100],
                    'row_count': len(rows),
                    'execution_time_ms': stats.get('wallTimeMillis', 0),
                }
                cell['status'] = 'success'
                cell['error'] = None
                _set_notebook(notebook_id, notebook)
                return jsonify({
                    'success': True,
                    'cell_id': cell_id,
                    'result': cell['result'],
                    'compiled_sql': compiled_sql
                })
            
            # Fetch all data chunks
            iteration = 0
            chunk_data = None  # Initialize to avoid NameError
            while next_uri:
                iteration += 1
                logger.debug(f'Fetching chunk {iteration}: {next_uri[:80]}...')
                
                # Trino returns nextUri as a full URL, so use it directly
                # If it's a relative path, prepend trino_url
                if next_uri.startswith('http://') or next_uri.startswith('https://'):
                    chunk_url = next_uri
                else:
                    chunk_url = f'{trino_url}{next_uri}'
                
                try:
                    chunk_response = requests.get(chunk_url, headers=headers, timeout=300)
                    if chunk_response.status_code != 200:
                        error_text = chunk_response.text or f'HTTP {chunk_response.status_code}'
                        logger.error(f'Trino chunk fetch failed (HTTP {chunk_response.status_code}): {error_text[:500]}')
                        raise Exception(f'Trino data fetch failed (HTTP {chunk_response.status_code}): {error_text[:200]}')
                    
                    chunk_data = chunk_response.json()
                    logger.debug(f'Chunk {iteration} state: {chunk_data.get("stats", {}).get("state")}, has_data: {"data" in chunk_data}, has_columns: {"columns" in chunk_data}')
                except json.JSONDecodeError as e:
                    logger.error(f'Failed to parse Trino chunk response: {chunk_response.text[:500] if "chunk_response" in locals() else "No response"}')
                    raise Exception(f'Invalid JSON response from Trino chunk: {str(e)}')
                except requests.exceptions.RequestException as e:
                    logger.error(f'Request exception fetching Trino chunk: {str(e)}')
                    raise Exception(f'Failed to fetch Trino data chunk: {str(e)}')
                
                # Check for errors in chunk
                if chunk_data.get('error'):
                    error_info = chunk_data['error']
                    error_msg = error_info.get('message', 'Unknown error')
                    error_code = error_info.get('errorCode', 'UNKNOWN')
                    logger.error(f'Trino query error in chunk {iteration}: [{error_code}] {error_msg}')
                    raise Exception(f'Trino query error [{error_code}]: {error_msg}')
                
                # Check query state
                chunk_stats = chunk_data.get('stats', {})
                if chunk_stats.get('state') == 'FAILED':
                    error_info = chunk_data.get('error', {})
                    error_msg = error_info.get('message', 'Query failed')
                    error_code = error_info.get('errorCode', 'UNKNOWN')
                    logger.error(f'Trino query failed in chunk {iteration}: [{error_code}] {error_msg}')
                    raise Exception(f'Trino query failed [{error_code}]: {error_msg}')
                
                # Extract data if available
                if 'data' in chunk_data:
                    rows.extend(chunk_data['data'])
                
                # Update schema if columns are available (may come in later chunks)
                if 'columns' in chunk_data and not schema:
                    schema = [{'name': col['name'], 'type': col['type']} for col in chunk_data['columns']]
                
                # Check if query is finished
                chunk_stats = chunk_data.get('stats', {})
                query_state = chunk_stats.get('state')
                
                # If query is finished (no more nextUri), break out of loop
                next_uri = chunk_data.get('nextUri')
                if not next_uri or query_state == 'FINISHED':
                    logger.info(f'Query finished after {iteration} chunks: state={query_state}')
                    break
                
                # If still QUEUED or RUNNING, continue polling (small delay for QUEUED)
                if query_state == 'QUEUED':
                    import time
                    time.sleep(0.1)  # Small delay for queued queries
            
            # After loop completes, update cell with final results
            # Get final stats from last chunk if available, otherwise use initial stats
            final_stats = chunk_data.get('stats', {}) if chunk_data else stats
            logger.info(f'Query completed: {len(rows)} rows, {len(schema)} columns')
            cell['result'] = {
                'schema': schema,
                'rows': rows[:100],  # Limit to 100 rows for display
                'row_count': len(rows),
                'execution_time_ms': final_stats.get('wallTimeMillis', 0),
            }
            cell['status'] = 'success'
            cell['error'] = None
            
            # Save notebook
            _set_notebook(notebook_id, notebook)
            
            return jsonify({
                'success': True,
                'cell_id': cell_id,
                'result': cell['result'],
                'compiled_sql': compiled_sql
            })
        except ImportError:
            return jsonify({
                'success': False,
                'error': 'Trino executor not available. Install requests: pip install requests'
            }), 503
        except requests.exceptions.ConnectionError as e:
            # Connection error - Trino might not be running or URL is wrong
            error_msg = f'Cannot connect to Trino at {trino_url}. Is Trino running? Error: {str(e)}'
            cell['status'] = 'error'
            cell['error'] = error_msg
            cell['result'] = None
            _set_notebook(notebook_id, notebook)
            return jsonify({
                'success': False,
                'error': error_msg,
                'cell_id': cell_id,
                'compiled_sql': compiled_sql
            }), 500
        except requests.exceptions.Timeout as e:
            error_msg = f'Trino query timed out after 300 seconds. The query might be too complex or Trino is overloaded.'
            cell['status'] = 'error'
            cell['error'] = error_msg
            cell['result'] = None
            _set_notebook(notebook_id, notebook)
            return jsonify({
                'success': False,
                'error': error_msg,
                'cell_id': cell_id,
                'compiled_sql': compiled_sql
            }), 500
        except Exception as e:
            # Update cell with error
            import traceback
            error_details = str(e)
            error_traceback = traceback.format_exc()
            
            # Log the full exception for debugging
            logger.error(f'Exception executing notebook cell {cell_id}: {error_details}')
            logger.error(f'Traceback: {error_traceback}')
            
            # Include more context for debugging
            if 'trino_url' in locals():
                error_details += f' (Trino URL: {trino_url}, Catalog: {trino_catalog}, Schema: {trino_schema})'
            
            cell['status'] = 'error'
            cell['error'] = error_details
            cell['result'] = None
            
            # Save notebook
            _set_notebook(notebook_id, notebook)
            
            return jsonify({
                'success': False,
                'error': error_details,
                'cell_id': cell_id,
                'compiled_sql': compiled_sql
            }), 500
    else:
        # Use provided executor
        try:
            # Try Rust execution router interface
            if hasattr(_executor, 'execute'):
                engine_selection = {
                    'engine_name': 'trino',
                    'reasoning': ['Notebook execution'],
                }
                result = _executor.execute(compiled_sql, engine_selection)
                
                # Update cell with results
                cell['result'] = {
                    'schema': [{'name': col, 'type': 'unknown'} for col in result.get('columns', [])],
                    'rows': result.get('data', [])[:100],
                    'row_count': result.get('rows_returned', 0),
                    'execution_time_ms': result.get('execution_time_ms', 0),
                }
                cell['status'] = 'success'
                cell['error'] = None
            else:
                raise Exception('Executor does not support execute method')
            
                # Save notebook
                _set_notebook(notebook_id, notebook)
            
            return jsonify({
                'success': True,
                'cell_id': cell_id,
                'result': cell['result'],
                'compiled_sql': compiled_sql
            })
        except Exception as e:
            # Update cell with error
            cell['status'] = 'error'
            cell['error'] = str(e)
            cell['result'] = None
            
            # Save notebook
            _set_notebook(notebook_id, notebook)
            
            return jsonify({
                'success': False,
                'error': str(e),
                'cell_id': cell_id,
                'compiled_sql': compiled_sql
            }), 500


@notebook_router.route('/notebooks/<notebook_id>/cells/<cell_id>/generate-sql', methods=['POST'])
def generate_sql_for_cell(notebook_id: str, cell_id: str):
    """Generate SQL for a cell using new AI SQL System."""
    """Generate SQL for a cell using LLM (Cursor-style - only writes to current cell)."""
    notebooks = _get_notebooks()
    notebook = notebooks.get(notebook_id)
    if not notebook:
        return jsonify({
            'success': False,
            'error': f'Notebook {notebook_id} not found'
        }), 404
    
    data = request.get_json() or {}
    user_query = data.get('query')
    
    if not user_query:
        return jsonify({
            'success': False,
            'error': 'Missing required field: query'
        }), 400
    
    # Find cell
    cell = None
    for c in notebook.get('cells', []):
        if c.get('id') == cell_id:
            cell = c
            break
    
    if not cell:
        return jsonify({
            'success': False,
            'error': f'Cell {cell_id} not found'
        }), 404
    
    # Use new AI SQL System to generate SQL
    try:
        from backend.ai_sql_system.orchestration.graph import LangGraphOrchestrator
        from backend.ai_sql_system.trino.client import TrinoClient
        from backend.ai_sql_system.trino.validator import TrinoValidator
        from backend.ai_sql_system.metadata.semantic_registry import SemanticRegistry
        from backend.ai_sql_system.planning.join_graph import JoinGraph
        from pathlib import Path
        import json
        
        # Initialize orchestrator (with join graph loading)
        trino_client = TrinoClient()
        trino_validator = TrinoValidator(trino_client)
        semantic_registry = SemanticRegistry()
        join_graph = JoinGraph()
        
        # Load join graph from metadata
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
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Could not load join graph: {e}")
        
        orchestrator = LangGraphOrchestrator(
            trino_validator=trino_validator,
            semantic_registry=semantic_registry,
            join_graph=join_graph
        )
        
        # Run pipeline
        result = orchestrator.run(user_query)
        
        if result.get('success') and result.get('sql'):
            # Update cell SQL
            cell['sql'] = result['sql']
            _set_notebook(notebook_id, notebook)
            
            return jsonify({
                'success': True,
                'cell_id': cell_id,
                'sql': result['sql'],
                'intent': result.get('intent'),
                'method': 'langgraph_pipeline'
            })
        else:
            return jsonify({
                'success': False,
                'error': result.get('error', 'Failed to generate SQL')
            }), 500
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': f'Error generating SQL: {str(e)}'
        }), 500
