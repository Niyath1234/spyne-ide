"""
Notebook API Endpoints

Trino SQL Notebook endpoints for cell-based SQL composition.
"""

import re
import json
import os
from typing import Dict, List, Optional, Any
from flask import Blueprint, request, jsonify

notebook_router = Blueprint('notebook', __name__)

# In-memory notebook storage (in production, use database)
_notebooks: Dict[str, Dict[str, Any]] = {}

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
            if stripped.startswith('%%ref'):
                match = re.match(r'%%ref\s+(\w+)\s+AS\s+(\w+)', stripped, re.IGNORECASE)
                if match:
                    ref_cell_id = match.group(1)
                    alias = match.group(2)
                    refs.append((ref_cell_id, alias))
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
    return jsonify({
        'success': True,
        'notebooks': list(_notebooks.values())
    })


# Create notebook (POST /notebooks)
@notebook_router.route('/notebooks', methods=['POST'])
def create_notebook():
    """Create a new notebook."""
    data = request.get_json() or {}
    notebook_id = data.get('id') or f"notebook_{len(_notebooks) + 1}"
    
    notebook = {
        'id': notebook_id,
        'engine': 'trino',
        'cells': data.get('cells', []),
        'metadata': data.get('metadata', {}),
        'created_at': data.get('created_at'),
        'updated_at': data.get('updated_at'),
    }
    
    _notebooks[notebook_id] = notebook
    
    return jsonify({
        'success': True,
        'notebook': notebook
    }), 201


# Get notebook by ID (GET /notebooks/<id>) - more specific route comes after list
@notebook_router.route('/notebooks/<notebook_id>', methods=['GET'])
def get_notebook(notebook_id: str):
    """Get a notebook by ID."""
    notebook = _notebooks.get(notebook_id)
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
    notebook = _notebooks.get(notebook_id)
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
    
    _notebooks[notebook_id] = notebook
    
    return jsonify({
        'success': True,
        'notebook': notebook
    })


@notebook_router.route('/notebooks/<notebook_id>/compile', methods=['POST'])
def compile_notebook(notebook_id: str):
    """Compile a notebook to Trino SQL."""
    notebook = _notebooks.get(notebook_id)
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
    notebook = _notebooks.get(notebook_id)
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
    if not _executor:
        # Try to use Trino directly via HTTP
        try:
            import requests
            trino_url = os.getenv('TRINO_COORDINATOR_URL', 'http://localhost:8080')
            trino_user = os.getenv('TRINO_USER', 'admin')
            trino_catalog = os.getenv('TRINO_CATALOG', 'memory')
            trino_schema = os.getenv('TRINO_SCHEMA', 'default')
            
            headers = {
                'X-Trino-User': trino_user,
                'X-Trino-Catalog': trino_catalog,
                'X-Trino-Schema': trino_schema,
            }
            
            response = requests.post(
                f'{trino_url}/v1/statement',
                headers=headers,
                data=compiled_sql,
                timeout=300
            )
            
            if response.status_code == 200:
                # Parse Trino response
                data = response.json()
                rows = []
                schema = []
                
                # Trino returns data in chunks
                next_uri = data.get('nextUri')
                if 'columns' in data:
                    schema = [{'name': col['name'], 'type': col['type']} for col in data['columns']]
                
                # Fetch all data
                while next_uri:
                    chunk_response = requests.get(f'{trino_url}{next_uri}', headers=headers, timeout=300)
                    chunk_data = chunk_response.json()
                    if 'data' in chunk_data:
                        rows.extend(chunk_data['data'])
                    next_uri = chunk_data.get('nextUri')
                
                # Update cell with results
                cell['result'] = {
                    'schema': schema,
                    'rows': rows[:100],  # Limit to 100 rows for display
                    'row_count': len(rows),
                    'execution_time_ms': 0,  # Trino doesn't provide this in simple response
                }
                cell['status'] = 'success'
                cell['error'] = None
                
                # Save notebook
                _notebooks[notebook_id] = notebook
                
                return jsonify({
                    'success': True,
                    'cell_id': cell_id,
                    'result': cell['result'],
                    'compiled_sql': compiled_sql
                })
            else:
                error_msg = response.text or 'Trino execution failed'
                raise Exception(error_msg)
        except ImportError:
            return jsonify({
                'success': False,
                'error': 'Trino executor not available. Install requests: pip install requests'
            }), 503
        except Exception as e:
            # Update cell with error
            cell['status'] = 'error'
            cell['error'] = str(e)
            cell['result'] = None
            
            # Save notebook
            _notebooks[notebook_id] = notebook
            
            return jsonify({
                'success': False,
                'error': str(e),
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
            _notebooks[notebook_id] = notebook
            
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
            _notebooks[notebook_id] = notebook
            
            return jsonify({
                'success': False,
                'error': str(e),
                'cell_id': cell_id,
                'compiled_sql': compiled_sql
            }), 500


@notebook_router.route('/notebooks/<notebook_id>/cells/<cell_id>/generate-sql', methods=['POST'])
def generate_sql_for_cell(notebook_id: str, cell_id: str):
    """Generate SQL for a cell using LLM (Cursor-style - only writes to current cell)."""
    notebook = _notebooks.get(notebook_id)
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
    
    # Call LLM to generate SQL (only for this cell - no cross-cell references)
    # This follows Cursor-style: LLM only writes SQL in the current cell
    try:
        # Try to use query generation API
        try:
            from query_regeneration_api import generate_sql_from_query
        except ImportError:
            try:
                from backend.query_regeneration_api import generate_sql_from_query
            except ImportError:
                # Fallback: simple SQL generation
                return jsonify({
                    'success': False,
                    'error': 'LLM query generator not available'
                }), 503
        
        # Generate SQL using LLM (use_llm=True to ensure LLM is used)
        sql_result = generate_sql_from_query(user_query, use_llm=True)
        
        if sql_result.get('success') and sql_result.get('sql'):
            # Update cell SQL
            cell['sql'] = sql_result['sql']
            _notebooks[notebook_id] = notebook
            
            return jsonify({
                'success': True,
                'cell_id': cell_id,
                'sql': sql_result['sql']
            })
        else:
            return jsonify({
                'success': False,
                'error': sql_result.get('error', 'Failed to generate SQL')
            }), 500
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': f'Error generating SQL: {str(e)}'
        }), 500
