"""Shared fixtures for entitybase-import tests."""

import sqlite3
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, Mock, patch
import pytest
import httpx

from src.jsonl_import import ProgressTracker
from src.state_manager import ImportStateManager


@pytest.fixture
def sample_jsonl_file(tmp_path: Path) -> Path:
    """Create a sample JSONL file with varied entity data."""
    file_path = tmp_path / "test_entities.jsonl"
    entities = [
        '{"type":"item","id":"Q1","labels":{"en":{"language":"en","value":"Test Item 1"}}}',
        '{"type":"item","id":"Q2","labels":{"en":{"language":"en","value":"Test Item 2"}}}',
        '{"type":"item","id":"Q3","labels":{"en":{"language":"en","value":"Test Item 3"}}}',
        '{"type":"property","id":"P1","labels":{"en":{"language":"en","value":"Test Property"}}}',
        '',
        '{"type":"lexeme","id":"L1","labels":{"en":{"language":"en","value":"Test Lexeme"}}}',
    ]
    file_path.write_text('\n'.join(entities))
    return file_path


@pytest.fixture
def sample_jsonl_file_with_trailing_comma(tmp_path: Path) -> Path:
    """Create JSONL file with trailing commas (edge case)."""
    file_path = tmp_path / "test_entities_comma.jsonl"
    file_path.write_text(
        '{"type":"item","id":"Q1","labels":{"en":{"language":"en","value":"Test Item 1"}}},\n'
        '{"type":"item","id":"Q2","labels":{"en":{"language":"en","value":"Test Item 2"}}},\n'
    )
    return file_path


@pytest.fixture
def temp_db_path(tmp_path: Path) -> str:
    """Create a temporary SQLite database path."""
    return str(tmp_path / "test_import_state.db")


@pytest.fixture
def mock_http_client():
    """Create a mocked httpx.AsyncClient."""
    client = AsyncMock(spec=httpx.AsyncClient)
    return client


@pytest.fixture
def mock_response():
    """Create a mocked HTTP response."""
    response = Mock(spec=httpx.Response)
    response.status_code = 200
    response.text = "OK"
    response.raise_for_status = Mock()
    return response


@pytest.fixture
def sample_entities() -> List[Dict[str, Any]]:
    """Create sample entity data."""
    return [
        {"type": "item", "id": "Q1", "labels": {"en": {"language": "en", "value": "Test Item 1"}}},
        {"type": "item", "id": "Q2", "labels": {"en": {"language": "en", "value": "Test Item 2"}}},
        {"type": "property", "id": "P1", "labels": {"en": {"language": "en", "value": "Test Property"}}},
        {"type": "lexeme", "id": "L1", "labels": {"en": {"language": "en", "value": "Test Lexeme"}}},
    ]


@pytest.fixture
def progress_tracker():
    """Create a ProgressTracker instance."""
    return ProgressTracker(total=100)


@pytest.fixture
def state_manager(temp_db_path):
    """Create a fresh ImportStateManager instance for each test."""
    manager = ImportStateManager(temp_db_path)
    yield manager
    manager.reset_all()


@pytest.fixture
def sample_run_id(state_manager):
    """Create a sample run and return its ID."""
    return state_manager.create_run(
        jsonl_file="test.jsonl",
        total_entities=10,
        concurrency=5,
        api_url="http://test.com/import"
    )


@pytest.fixture
def mock_state_manager():
    """Create a mocked ImportStateManager."""
    manager = MagicMock(spec=ImportStateManager)
    manager.create_run = MagicMock(return_value=1)
    manager.add_entities = MagicMock()
    manager.get_next_batch = MagicMock(return_value=[])
    manager.mark_success = MagicMock()
    manager.mark_failed = MagicMock()
    manager.mark_skipped = MagicMock()
    manager.finish_run = MagicMock()
    return manager


@pytest.fixture
def mock_session():
    """Create a mocked async session."""
    session = AsyncMock()
    response = Mock()
    response.status_code = 200
    response.text = "OK"
    response.raise_for_status = Mock()
    session.post = AsyncMock(return_value=response)
    return session


@pytest.fixture
def mock_input():
    """Mock the built-in input() function."""
    with patch('builtins.input', return_value='y'):
        yield


@pytest.fixture
def mock_input_cancel():
    """Mock input() to cancel (return 'n')."""
    with patch('builtins.input', return_value='n'):
        yield


@pytest.fixture
def sample_run_stats() -> Dict[str, Any]:
    """Sample run statistics dictionary."""
    return {
        'processed': 50,
        'total': 100,
        'percent': 50.0,
        'rate_per_second': 1.0,
        'rate_per_minute': 60.0,
        'rate_per_hour': 3600.0,
        'elapsed_seconds': 50.0,
        'eta_seconds': 50.0,
        'eta_formatted': '50s'
    }


@pytest.fixture
def empty_db_connection(temp_db_path):
    """Create a connection to an empty test database."""
    conn = sqlite3.connect(temp_db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture
def populated_db_connection(state_manager, sample_run_id, sample_entities):
    """Create a connection to a populated test database."""
    state_manager.add_entities(sample_run_id, sample_entities)
    conn = sqlite3.connect(state_manager.db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()
