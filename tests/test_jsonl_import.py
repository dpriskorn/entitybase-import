"""Tests for jsonl_import module - core business logic."""

import logging
from pathlib import Path
import pytest
from unittest.mock import AsyncMock, Mock, patch
import httpx

from src.jsonl_import import (
    ProgressTracker,
    format_rate,
    format_elapsed,
    print_progress_compact,
    print_progress_detailed,
    import_entity,
    import_from_jsonl,
)


class TestProgressTracker:
    """Test ProgressTracker class."""

    def test_progress_tracker_initialization(self):
        """Test that ProgressTracker initializes correctly."""
        tracker = ProgressTracker(total=100)
        assert tracker.total == 100
        assert tracker.processed == 0
        assert tracker.start_time > 0
        assert tracker.last_update > 0
        assert tracker.last_processed == 0

    def test_progress_tracker_update(self):
        """Test progress update calculations."""
        tracker = ProgressTracker(total=100)
        progress = tracker.update(10)
        assert progress['processed'] == 10
        assert progress['total'] == 100
        assert progress['percent'] == 10.0
        assert progress['rate_per_second'] > 0
        assert progress['rate_per_minute'] > 0
        assert progress['rate_per_hour'] > 0
        assert progress['elapsed_seconds'] > 0

    def test_progress_tracker_rate_calculation(self):
        """Test rate calculations per second, minute, and hour."""
        tracker = ProgressTracker(total=1000)
        progress = tracker.update(100)
        
        assert progress['rate_per_second'] > 0
        assert progress['rate_per_minute'] == progress['rate_per_second'] * 60
        assert progress['rate_per_hour'] == progress['rate_per_second'] * 3600

    def test_progress_tracker_eta_calculation(self):
        """Test ETA calculation."""
        tracker = ProgressTracker(total=100)
        tracker.update(50)
        progress = tracker.update(0)
        
        assert progress['eta_seconds'] is not None
        assert progress['eta_seconds'] > 0
        assert progress['eta_formatted'] != 'N/A'

    def test_progress_tracker_completion(self):
        """Test 100% completion scenario."""
        tracker = ProgressTracker(total=100)
        progress = tracker.update(100)
        
        assert progress['processed'] == 100
        assert progress['percent'] == 100.0
        assert progress['eta_seconds'] == 0

    def test_progress_tracker_stores_attributes(self):
        """Test that elapsed_seconds and rate_per_second are stored."""
        tracker = ProgressTracker(total=100)
        tracker.update(10)
        
        assert hasattr(tracker, 'elapsed_seconds')
        assert hasattr(tracker, 'rate_per_second')
        assert tracker.elapsed_seconds >= 0
        assert tracker.rate_per_second >= 0

    def test_progress_tracker_multiple_updates(self):
        """Test multiple batch updates accumulate correctly."""
        tracker = ProgressTracker(total=100)
        tracker.update(25)
        tracker.update(25)
        tracker.update(25)
        
        progress = tracker.update(25)
        assert progress['processed'] == 100
        assert progress['percent'] == 100.0

    def test_progress_tracker_zero_total(self):
        """Test edge case with zero total."""
        tracker = ProgressTracker(total=0)
        progress = tracker.update(0)
        
        assert progress['percent'] == 0

    def test_progress_tracker_format_eta_seconds(self):
        """Test ETA formatting for < 60 seconds."""
        tracker = ProgressTracker(total=60)
        assert tracker._format_eta(30) == "30s"
        assert tracker._format_eta(59) == "59s"

    def test_progress_tracker_format_eta_minutes(self):
        """Test ETA formatting for < 1 hour."""
        tracker = ProgressTracker(total=60)
        assert tracker._format_eta(60) == "1.0m"
        assert tracker._format_eta(90) == "1.5m"
        assert tracker._format_eta(3599) == "60.0m"

    def test_progress_tracker_format_eta_hours(self):
        """Test ETA formatting for >= 1 hour."""
        tracker = ProgressTracker(total=60)
        assert tracker._format_eta(3600) == "1h 0m"
        assert tracker._format_eta(5400) == "2h 30m"
        assert tracker._format_eta(7200) == "2h 0m"


class TestFormatFunctions:
    """Test formatting utility functions."""

    def test_format_rate_per_second(self):
        """Test rate formatting when >= 60/min."""
        assert format_rate(60) == "1.0/sec"
        assert format_rate(120) == "2.0/sec"

    def test_format_rate_per_minute(self):
        """Test rate formatting when 1 <= rate < 60/min."""
        assert format_rate(1) == "1.0/min"
        assert format_rate(30) == "30.0/min"
        assert format_rate(59.9) == "59.9/min"

    def test_format_rate_per_hour(self):
        """Test rate formatting when < 1/min."""
        assert format_rate(0.5) == "30.00/hour"
        assert format_rate(0.1) == "6.00/hour"

    def test_format_elapsed(self):
        """Test elapsed time formatting."""
        assert format_elapsed(0) == "00:00:00"
        assert format_elapsed(59) == "00:00:59"
        assert format_elapsed(60) == "00:01:00"
        assert format_elapsed(3600) == "01:00:00"
        assert format_elapsed(3661) == "01:01:01"
        assert format_elapsed(86399) == "23:59:59"

    def test_print_progress_compact(self, capsys, sample_run_stats):
        """Test compact progress output."""
        print_progress_compact(1, sample_run_stats)
        captured = capsys.readouterr()
        assert "50" in captured.out
        assert "50.0%" in captured.out
        assert "/s" in captured.out

    def test_print_progress_detailed(self, capsys, sample_run_stats):
        """Test detailed progress output."""
        print_progress_detailed(1, sample_run_stats)
        captured = capsys.readouterr()
        assert "Progress Report" in captured.out
        assert "Entities:" in captured.out
        assert "Elapsed:" in captured.out
        assert "Rate:" in captured.out
        assert "ETA:" in captured.out


@pytest.mark.asyncio
class TestImportEntity:
    """Test import_entity function."""

    async def test_import_entity_success(self, mock_session, mock_state_manager):
        """Test successful entity import."""
        response = Mock()
        response.status_code = 200
        response.text = "OK"
        response.raise_for_status = Mock()
        mock_session.post.return_value = response

        result = await import_entity(
            mock_session,
            "Q1",
            {"type": "item", "id": "Q1"},
            "item",
            mock_state_manager,
            1,
            "http://test.com"
        )

        assert result == 'success'
        mock_state_manager.mark_success.assert_called_once_with("Q1", 1)

    async def test_import_entity_skip_409(self, mock_session, mock_state_manager):
        """Test skip on 409 Conflict."""
        response = Mock()
        response.status_code = 409
        response.text = "Already exists"
        
        error = httpx.HTTPStatusError(
            "Conflict",
            request=Mock(),
            response=response
        )
        mock_session.post.side_effect = error

        result = await import_entity(
            mock_session,
            "Q1",
            {"type": "item", "id": "Q1"},
            "item",
            mock_state_manager,
            1,
            "http://test.com"
        )

        assert result == 'skip'
        mock_state_manager.mark_skipped.assert_called_once_with("Q1", 1)

    async def test_import_entity_validation_error_400(self, mock_session, mock_state_manager):
        """Test fail on 400 validation error."""
        response = Mock()
        response.status_code = 400
        response.text = "Invalid data"
        
        error = httpx.HTTPStatusError(
            "Bad Request",
            request=Mock(),
            response=response
        )
        mock_session.post.side_effect = error

        result = await import_entity(
            mock_session,
            "Q1",
            {"type": "item", "id": "Q1"},
            "item",
            mock_state_manager,
            1,
            "http://test.com"
        )

        assert result == 'failed'
        mock_state_manager.mark_failed.assert_called_once()

    async def test_import_entity_retry_on_500(self, mock_session, mock_state_manager):
        """Test retry on 500 server error."""
        response_500 = Mock()
        response_500.status_code = 500
        response_500.text = "Server error"
        
        response_200 = Mock()
        response_200.status_code = 200
        response_200.text = "OK"
        response_200.raise_for_status = Mock()
        
        error = httpx.HTTPStatusError(
            "Internal Server Error",
            request=Mock(),
            response=response_500
        )
        
        mock_session.post.side_effect = [error, response_200]

        result = await import_entity(
            mock_session,
            "Q1",
            {"type": "item", "id": "Q1"},
            "item",
            mock_state_manager,
            1,
            "http://test.com"
        )

        assert result == 'success'
        assert mock_session.post.call_count == 2

    async def test_import_entity_timeout_retry(self, mock_session, mock_state_manager):
        """Test retry on timeout."""
        response_200 = Mock()
        response_200.status_code = 200
        response_200.text = "OK"
        response_200.raise_for_status = Mock()
        
        mock_session.post.side_effect = [
            httpx.TimeoutException("Timeout"),
            response_200
        ]

        result = await import_entity(
            mock_session,
            "Q1",
            {"type": "item", "id": "Q1"},
            "item",
            mock_state_manager,
            1,
            "http://test.com"
        )

        assert result == 'success'
        assert mock_session.post.call_count == 2

    async def test_import_entity_max_retries_exceeded(self, mock_session, mock_state_manager):
        """Test fail after max retries exceeded."""
        response = Mock()
        response.status_code = 500
        response.text = "Server error"
        
        error = httpx.HTTPStatusError(
            "Internal Server Error",
            request=Mock(),
            response=response
        )
        mock_session.post.side_effect = error

        result = await import_entity(
            mock_session,
            "Q1",
            {"type": "item", "id": "Q1"},
            "item",
            mock_state_manager,
            1,
            "http://test.com"
        )

        assert result == 'failed'
        assert mock_session.post.call_count == 3

    async def test_import_entity_unexpected_error(self, mock_session, mock_state_manager):
        """Test handling of unexpected exceptions."""
        mock_session.post.side_effect = Exception("Unexpected error")

        result = await import_entity(
            mock_session,
            "Q1",
            {"type": "item", "id": "Q1"},
            "item",
            mock_state_manager,
            1,
            "http://test.com"
        )

        assert result == 'failed'
        mock_state_manager.mark_failed.assert_called_once()

    async def test_import_entity_logging(self, mock_session, mock_state_manager, caplog):
        """Test logging during import."""
        response = Mock()
        response.status_code = 200
        response.text = "OK"
        response.raise_for_status = Mock()
        mock_session.post.return_value = response

        with caplog.at_level(logging.DEBUG):
            await import_entity(
                mock_session,
                "Q1",
                {"type": "item", "id": "Q1"},
                "item",
                mock_state_manager,
                1,
                "http://test.com"
            )

        assert any("Starting import for item Q1" in record.message for record in caplog.records)


@pytest.mark.asyncio
class TestImportFromJsonl:
    """Test import_from_jsonl function."""

    async def test_import_from_jsonl_basic_workflow(
        self,
        sample_jsonl_file,
        temp_db_path
    ):
        """Test basic import workflow."""
        with patch('src.jsonl_import.httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            response = Mock()
            response.status_code = 200
            response.text = "OK"
            response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=response)

            await import_from_jsonl(
                sample_jsonl_file,
                concurrency=2,
                progress_interval=1,
                api_url="http://test.com",
                db_path=temp_db_path,
                auto_cleanup=True
            )

            assert mock_client.post.called

    async def test_import_from_jsonl_file_parsing(self, sample_jsonl_file, temp_db_path):
        """Test JSONL file parsing with edge cases."""
        with patch('src.jsonl_import.httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            response = Mock()
            response.status_code = 200
            response.text = "OK"
            response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=response)

            await import_from_jsonl(
                sample_jsonl_file,
                concurrency=2,
                api_url="http://test.com",
                db_path=temp_db_path,
                auto_cleanup=True
            )

            call_count = mock_client.post.call_count
            assert call_count > 0

    async def test_import_from_jsonl_with_trailing_comma(
        self, 
        sample_jsonl_file_with_trailing_comma,
        temp_db_path
    ):
        """Test handling of trailing commas in JSONL."""
        with patch('src.jsonl_import.httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            response = Mock()
            response.status_code = 200
            response.text = "OK"
            response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=response)

            await import_from_jsonl(
                sample_jsonl_file_with_trailing_comma,
                concurrency=2,
                api_url="http://test.com",
                db_path=temp_db_path,
                auto_cleanup=True
            )

            assert mock_client.post.call_count == 2

    async def test_import_from_jsonl_progress_interval(self, sample_jsonl_file, temp_db_path):
        """Test progress reporting at intervals."""
        with patch('src.jsonl_import.httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            response = Mock()
            response.status_code = 200
            response.text = "OK"
            response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=response)

            await import_from_jsonl(
                sample_jsonl_file,
                concurrency=2,
                progress_interval=1,
                api_url="http://test.com",
                db_path=temp_db_path,
                auto_cleanup=True
            )

            assert mock_client.post.called

    async def test_import_from_jsonl_custom_log_level(self, sample_jsonl_file, temp_db_path):
        """Test custom log level configuration."""
        with patch('src.jsonl_import.httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            response = Mock()
            response.status_code = 200
            response.text = "OK"
            response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=response)

            await import_from_jsonl(
                sample_jsonl_file,
                concurrency=2,
                api_url="http://test.com",
                db_path=temp_db_path,
                log_level="DEBUG",
                auto_cleanup=True
            )

            assert mock_client.post.called

    async def test_import_from_jsonl_from_line(self, sample_jsonl_file, temp_db_path):
        """Test importing from a specific line."""
        with patch('src.jsonl_import.httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            response = Mock()
            response.status_code = 200
            response.text = "OK"
            response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=response)

            await import_from_jsonl(
                sample_jsonl_file,
                concurrency=2,
                from_line=3,
                api_url="http://test.com",
                db_path=temp_db_path,
                auto_cleanup=True
            )

    async def test_import_from_jsonl_to_line(self, sample_jsonl_file, temp_db_path):
        """Test importing up to a specific line."""
        with patch('src.jsonl_import.httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            response = Mock()
            response.status_code = 200
            response.text = "OK"
            response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=response)

            await import_from_jsonl(
                sample_jsonl_file,
                concurrency=2,
                to_line=2,
                api_url="http://test.com",
                db_path=temp_db_path,
                auto_cleanup=True
            )


@pytest.fixture
def empty_jsonl_file(tmp_path: Path) -> Path:
    """Create an empty JSONL file."""
    file_path = tmp_path / "empty.jsonl"
    file_path.write_text("")
    return file_path


@pytest.mark.asyncio
class TestImportFromJsonlEdgeCases:
    """Test edge cases for import_from_jsonl."""

    async def test_import_empty_file(self, empty_jsonl_file, temp_db_path):
        """Test importing an empty file."""
        with patch('src.jsonl_import.httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            await import_from_jsonl(
                empty_jsonl_file,
                concurrency=2,
                api_url="http://test.com",
                db_path=temp_db_path,
                auto_cleanup=True
            )

            assert mock_client.post.call_count == 0
