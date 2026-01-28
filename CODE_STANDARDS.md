# Code Standards & Best Practices

This document outlines the coding standards and best practices for the Spyne IDE project.

## Module Organization

### Import Order

1. **Standard library imports** (alphabetically sorted)
2. **Third-party imports** (alphabetically sorted)  
3. **Local application imports** (using absolute imports)

```python
# Standard library
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

# Third-party
import requests
import tiktoken

# Local application
from backend.metadata_provider import MetadataProvider
from backend.sql_builder import SQLBuilder
```

### Import Rules

- ✅ **DO**: Use absolute imports: `from backend.module import Class`
- ✅ **DO**: Group imports by category (stdlib, third-party, local)
- ✅ **DO**: Sort imports alphabetically within each group
- ❌ **DON'T**: Use `sys.path.insert()` except in entry points
- ❌ **DON'T**: Use relative imports (`from .module import Class`)
- ❌ **DON'T**: Import everything (`from module import *`)

## Module Structure

### File Organization

Each Python module should follow this structure:

```python
#!/usr/bin/env python3
"""
Module docstring describing what the module does.
"""

# Standard library imports
import logging
from typing import Dict, Any

# Third-party imports
import requests

# Local imports
from backend.metadata_provider import MetadataProvider

# Module-level constants
DEFAULT_TIMEOUT = 30

# Module-level logger
logger = logging.getLogger(__name__)

# Classes and functions
class MyClass:
    """Class docstring."""
    pass

def my_function():
    """Function docstring."""
    pass
```

### Package Structure

- All packages must have `__init__.py`
- `__init__.py` should export public API
- Use `__all__` to define public exports

Example `__init__.py`:
```python
"""Package description."""

from backend.module import PublicClass, public_function

__all__ = ["PublicClass", "public_function"]
```

## Naming Conventions

### Variables and Functions

- **snake_case** for variables and functions: `user_query`, `load_metadata()`
- **UPPER_CASE** for constants: `MAX_TOKENS`, `DEFAULT_TIMEOUT`
- **Descriptive names**: `query_text` not `q`, `metadata_provider` not `mp`

### Classes

- **PascalCase** for classes: `LLMQueryGenerator`, `MetadataProvider`
- **Descriptive names**: `QueryIntentValidator` not `Validator`

### Private Members

- **Leading underscore** for private: `_cache`, `_load_metadata()`
- **Double underscore** for name mangling (rarely needed): `__private`

## Type Hints

### Function Signatures

Always use type hints for function parameters and return types:

```python
def process_query(
    query: str,
    metadata: Dict[str, Any],
    options: Optional[Dict[str, Any]] = None
) -> Tuple[str, List[str]]:
    """Process a query and return SQL and reasoning steps."""
    pass
```

### Type Imports

```python
from typing import Dict, List, Any, Optional, Tuple, Union
```

## Documentation

### Docstrings

Use Google-style docstrings:

```python
def generate_sql_intent(
    query: str,
    metadata: Dict[str, Any]
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Generate SQL intent from natural language query.
    
    Args:
        query: Natural language query string
        metadata: Metadata dictionary with tables, metrics, dimensions
        
    Returns:
        Tuple of (intent_dict, reasoning_steps_list)
        
    Raises:
        ValueError: If query is empty or invalid
        RuntimeError: If LLM call fails
    """
    pass
```

### Comments

- Use comments to explain **why**, not **what**
- Code should be self-documenting
- Complex logic should have comments

```python
# Good: Explains why
# Truncate RAG first because it's least critical and can be regenerated
bundle.rag = self._truncate_text(bundle.rag, target_tokens)

# Bad: Explains what (obvious from code)
# Set rag to truncated text
bundle.rag = self._truncate_text(bundle.rag, target_tokens)
```

## Error Handling

### Exception Handling

- **Catch specific exceptions**, not bare `except:`
- **Log exceptions** with `exc_info=True`
- **Re-raise** critical exceptions
- **Graceful degradation** for optional features

```python
try:
    result = risky_operation()
except SpecificError as e:
    logger.error(f"Operation failed: {e}", exc_info=True)
    raise  # Re-raise critical errors
except Exception as e:
    logger.warning(f"Non-critical operation failed: {e}", exc_info=True)
    return default_value  # Graceful degradation
```

### Logging

- Use module-level logger: `logger = logging.getLogger(__name__)`
- Appropriate log levels:
  - `DEBUG`: Detailed information for debugging
  - `INFO`: General informational messages
  - `WARNING`: Warning messages (non-critical issues)
  - `ERROR`: Error messages (failures that don't stop execution)
  - `CRITICAL`: Critical errors (system failures)

```python
logger.debug("Processing query: %s", query)
logger.info("Metadata loaded: %d tables", table_count)
logger.warning("Cache miss, loading from disk")
logger.error("Failed to connect to database", exc_info=True)
```

## Code Organization

### Separation of Concerns

- **Single Responsibility**: Each class/function should do one thing
- **Dependency Injection**: Pass dependencies as parameters
- **Interface Segregation**: Use interfaces for abstractions
- **Don't Repeat Yourself (DRY)**: Extract common logic

### Modularity

- **Loose Coupling**: Modules should depend on interfaces, not implementations
- **High Cohesion**: Related functionality should be grouped together
- **Clear Boundaries**: Public API should be well-defined

### Example: Good Modular Design

```python
# backend/metadata_provider.py
class MetadataProvider:
    """Centralized metadata provider."""
    @staticmethod
    def load() -> Dict[str, Any]:
        """Load metadata with caching."""
        pass

# backend/llm_query_generator.py
from backend.metadata_provider import MetadataProvider

class LLMQueryGenerator:
    def generate_sql_intent(self, query: str, metadata: Dict[str, Any]):
        """Uses MetadataProvider for metadata."""
        metadata = MetadataProvider.load()  # Clear dependency
```

## Testing

### Test Organization

- Tests in `tests/` directory
- Test files: `test_*.py` or `*_test.py`
- Test classes: `TestClassName`
- Test functions: `test_function_name()`

### Test Structure

```python
import pytest
from backend.llm_query_generator import LLMQueryGenerator

def test_generate_sql_intent():
    """Test SQL intent generation."""
    generator = LLMQueryGenerator()
    # Test implementation
    assert result is not None
```

## Performance

### Caching

- Use caching for expensive operations
- Clear cache when data changes
- Document cache invalidation strategy

### Resource Management

- Use context managers for resources
- Close connections, files, etc.
- Handle cleanup in finally blocks or context managers

## Security

### Input Validation

- Validate all user inputs
- Sanitize data before processing
- Use parameterized queries for SQL

### Secrets Management

- Never commit secrets to repository
- Use environment variables for configuration
- Use `.env` files (gitignored) for local development

## Git Practices

### Commit Messages

- Use descriptive commit messages
- Follow conventional commits format:
  - `feat:` New feature
  - `fix:` Bug fix
  - `refactor:` Code refactoring
  - `docs:` Documentation changes
  - `test:` Test changes

### Branching

- `main`: Production-ready code
- `develop`: Development branch
- `feature/*`: Feature branches
- `fix/*`: Bug fix branches

## Checklist

Before submitting code, ensure:

- [ ] All imports are properly organized
- [ ] Type hints are used for all functions
- [ ] Docstrings are present for public functions/classes
- [ ] Code follows PEP 8 style guide
- [ ] No `sys.path` manipulation (except entry points)
- [ ] All exceptions are properly handled and logged
- [ ] Tests are written for new features
- [ ] README is updated if needed

