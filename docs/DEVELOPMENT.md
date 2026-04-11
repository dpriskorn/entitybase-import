# Development Guide

## Project Structure

```
entitybase-import/
├── scripts/
│   └── imports/           # Import scripts
│       ├── __init__.py
│       ├── jsonl_import.py   # Main import script
│       ├── state_manager.py  # SQLite state management
│       └── cli.py             # CLI interface
├── tests/                  # Test files
│   ├── __init__.py
│   └── test_example.py
├── docs/                   # Documentation
│   ├── INSTALLATION.md
│   └── USAGE.md
├── logs/                   # Log files (auto-generated)
├── pyproject.toml          # Poetry configuration
├── pytest.ini              # pytest configuration
├── .gitignore
├── LICENSE
└── README.md
```

## Development Setup

```bash
# Clone and enter directory
git clone https://github.com/your-org/entitybase-import.git
cd entitybase-import

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install with dev dependencies
poetry install --with dev

# Or manually:
pip install httpx pytest pytest-asyncio ruff mypy
```

## Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_example.py

# Run with coverage
pytest --cov=scripts --cov-report=html
```

## Code Quality

### Linting with Ruff

```bash
# Check code
ruff check scripts/imports/

# Auto-fix issues
ruff check --fix scripts/imports/
```

### Type Checking with mypy

```bash
mypy scripts/imports/
```

### Formatting

The project uses standard Python formatting (PEP 8). Configure your editor accordingly.

## Adding Features

### Adding New CLI Options

Edit `jsonl_import.py`:

1. Add argument to parser in `main()`:
```python
parser.add_argument(
    '--new-option',
    type=str,
    default='default_value',
    help='Description of option'
)
```

2. Add parameter to `import_from_jsonl()` function
3. Use the parameter in the import logic

### Adding New Log Levels

Add custom log handlers or formatters in `import_from_jsonl()`:

```python
# Add a custom handler
custom_handler = logging.FileHandler('custom.log')
custom_handler.setLevel(logging.DEBUG)
root_logger.addHandler(custom_handler)
```

### Modifying Retry Logic

Edit constants in `jsonl_import.py`:

```python
MAX_RETRIES = 5  # Increase retry count
RETRY_DELAY = 5.0  # Increase delay between retries
```

## Testing Guide

### Unit Tests

Test individual functions and classes:

```python
def test_state_manager_init():
    """Test that state manager initializes correctly."""
    manager = ImportStateManager(":memory:")
    assert manager.db_path == ":memory:"
```

### Integration Tests

Test with mock API responses:

```python
import pytest
from httpx import ASGITransport, AsyncClient

@pytest.mark.asyncio
@pytest.mark.integration
async def test_import_with_mock_api():
    """Test import with mocked API."""
    # Use httpx.MockTransport or similar
    pass
```

### Test Fixtures

Create reusable test fixtures in `tests/conftest.py`:

```python
@pytest.fixture
def sample_db(tmp_path):
    """Create a temporary database."""
    db_path = tmp_path / "test.db"
    manager = ImportStateManager(str(db_path))
    yield manager
    # Cleanup
```

## Release Process

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Run full test suite
4. Tag release: `git tag v0.1.0`
5. Push tag: `git push origin v0.1.0`
6. Build and publish: `poetry publish`

## Debugging

### Enable Debug Logging

```bash
python scripts/imports/jsonl_import.py data.jsonl --log-level DEBUG
```

### Debug with pdb

Add breakpoints in code:

```python
import pdb; pdb.set_trace()
```

Or use Python 3.7+ breakpoint():

```python
breakpoint()
```

### Inspect Database

Use sqlite3 command line:

```bash
sqlite3 import_state.db
sqlite> SELECT * FROM entities WHERE status = 'failed';
sqlite> SELECT * FROM import_runs;
```

## Performance Profiling

Profile import performance:

```bash
python -m cProfile -o profile.stats scripts/imports/jsonl_import.py data.jsonl
python -c "import pstats; pstats.Stats('profile.stats').sort_stats('cumulative').print_stats(20)"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run linting and type checking
6. Submit a pull request

## Code Style Guidelines

- Follow PEP 8
- Use type hints where possible
- Add docstrings for public functions
- Keep functions focused and small
- Log important events

### Example

```python
def import_entity(
    session: httpx.AsyncClient,
    entity_id: str,
    entity_data: Dict[str, Any],
) -> str:
    """Import a single entity.

    Args:
        session: HTTP client for API requests
        entity_id: ID of entity to import
        entity_data: Entity data as dictionary

    Returns:
        'success', 'skip', or 'failed'
    """
    logger.debug(f"Importing entity: {entity_id}")
    # Implementation
    return 'success'
```
