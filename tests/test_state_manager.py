"""Tests for state_manager module."""

import sqlite3

from src.state_manager import ImportStateManager, EntityRecord, ImportRun


class TestDatabaseInitialization:
    """Test database initialization."""

    def test_init_db_creates_tables(self, temp_db_path):
        """Verify tables and indexes are created."""
        ImportStateManager(temp_db_path)
        
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        assert 'import_runs' in tables
        assert 'entities' in tables
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall()]
        assert 'idx_entities_status' in indexes
        assert 'idx_entities_type' in indexes
        assert 'idx_entities_run_id' in indexes
        assert 'idx_entities_last_attempt' in indexes
        
        conn.close()

    def test_init_db_existing_db(self, temp_db_path):
        """Handle existing database gracefully."""
        manager1 = ImportStateManager(temp_db_path)
        run_id = manager1.create_run("test.jsonl", 10, 5, "http://test.com")
        assert run_id > 0
        
        manager2 = ImportStateManager(temp_db_path)
        run_id2 = manager2.create_run("test2.jsonl", 20, 10, "http://test2.com")
        assert run_id2 > run_id


class TestRunManagement:
    """Test run management operations."""

    def test_create_run(self, state_manager):
        """Create new import run."""
        run_id = state_manager.create_run(
            jsonl_file="test.jsonl",
            total_entities=100,
            concurrency=10,
            api_url="http://test.com/import"
        )
        assert run_id > 0
        
        run = state_manager.get_run_stats(run_id)
        assert run.run_id == run_id
        assert run.jsonl_file == "test.jsonl"
        assert run.total_entities == 100
        assert run.concurrency == 10
        assert run.api_url == "http://test.com/import"
        assert run.success_count == 0
        assert run.fail_count == 0
        assert run.skip_count == 0

    def test_create_run_multiple(self, state_manager):
        """Create multiple runs with auto-increment."""
        run_id1 = state_manager.create_run("test1.jsonl", 10, 5, "http://test.com")
        run_id2 = state_manager.create_run("test2.jsonl", 20, 10, "http://test.com")
        run_id3 = state_manager.create_run("test3.jsonl", 30, 15, "http://test.com")
        
        assert run_id2 == run_id1 + 1
        assert run_id3 == run_id2 + 1

    def test_finish_run(self, state_manager, sample_run_id):
        """Mark run as complete with stats."""
        state_manager.finish_run(
            run_id=sample_run_id,
            success_count=80,
            fail_count=15,
            skip_count=5
        )
        
        run = state_manager.get_run_stats(sample_run_id)
        assert run.success_count == 80
        assert run.fail_count == 15
        assert run.skip_count == 5
        assert run.end_time is not None

    def test_get_run_stats(self, state_manager, sample_run_id):
        """Retrieve run statistics."""
        state_manager.finish_run(sample_run_id, 50, 25, 25)
        run = state_manager.get_run_stats(sample_run_id)
        
        assert isinstance(run, ImportRun)
        assert run.run_id == sample_run_id

    def test_get_run_stats_nonexistent(self, state_manager):
        """Handle missing run gracefully."""
        run = state_manager.get_run_stats(999)
        assert run is None


class TestEntityManagement:
    """Test entity management operations."""

    def test_add_entities(self, state_manager, sample_run_id, sample_entities):
        """Bulk insert entities."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM entities WHERE run_id = ?", (sample_run_id,))
        count = cursor.fetchone()[0]
        conn.close()
        
        assert count == len(sample_entities)

    def test_add_entities_replace_existing(self, state_manager, sample_run_id, sample_entities):
        """INSERT OR REPLACE behavior."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        updated_entities = sample_entities[:2]
        state_manager.add_entities(sample_run_id, updated_entities)
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM entities WHERE run_id = ?", (sample_run_id,))
        count = cursor.fetchone()[0]
        conn.close()
        
        assert count == len(sample_entities)

    def test_add_entities_various_types(self, state_manager, sample_run_id):
        """Test different entity types."""
        entities = [
            {"type": "item", "id": "Q1", "labels": {}},
            {"type": "property", "id": "P1", "labels": {}},
            {"type": "lexeme", "id": "L1", "labels": {}},
            {"type": "form", "id": "F1", "labels": {}},
        ]
        state_manager.add_entities(sample_run_id, entities)
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT entity_type FROM entities WHERE run_id = ?",
            (sample_run_id,)
        )
        types = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        assert "item" in types
        assert "property" in types
        assert "lexeme" in types
        assert "form" in types

    def test_get_next_batch(self, state_manager, sample_run_id, sample_entities):
        """Fetch pending entities."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        batch = state_manager.get_next_batch(sample_run_id, limit=2)
        
        assert len(batch) == 2
        assert isinstance(batch[0], EntityRecord)
        assert batch[0].status == 'processing'

    def test_get_next_batch_empty(self, state_manager, sample_run_id):
        """No pending entities."""
        batch = state_manager.get_next_batch(sample_run_id, limit=10)
        assert len(batch) == 0

    def test_get_next_batch_processing_status(self, state_manager, sample_run_id, sample_entities):
        """Status transition to 'processing'."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        batch1 = state_manager.get_next_batch(sample_run_id, limit=2)
        assert len(batch1) == 2
        assert batch1[0].status == 'processing'
        
        batch2 = state_manager.get_next_batch(sample_run_id, limit=2)
        assert len(batch2) == 2
        assert batch2[0].status == 'processing'

    def test_get_next_batch_limit(self, state_manager, sample_run_id, sample_entities):
        """Respect batch size limit."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        batch = state_manager.get_next_batch(sample_run_id, limit=10)
        assert len(batch) <= 10


class TestStatusUpdates:
    """Test status update operations."""

    def test_mark_success(self, state_manager, sample_run_id, sample_entities):
        """Update entity to success."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        state_manager.mark_success(sample_entities[0]['id'], sample_run_id)
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status FROM entities WHERE entity_id = ? AND run_id = ?",
            (sample_entities[0]['id'], sample_run_id)
        )
        status = cursor.fetchone()[0]
        conn.close()
        
        assert status == 'success'

    def test_mark_failed(self, state_manager, sample_run_id, sample_entities):
        """Update entity to failed with error."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        error_msg = "Validation error"
        state_manager.mark_failed(sample_entities[0]['id'], sample_run_id, error_msg)
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status, error_message FROM entities WHERE entity_id = ? AND run_id = ?",
            (sample_entities[0]['id'], sample_run_id)
        )
        status, error = cursor.fetchone()
        conn.close()
        
        assert status == 'failed'
        assert error == error_msg

    def test_mark_failed_retry_increment(self, state_manager, sample_run_id, sample_entities):
        """Retry count increment."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        state_manager.mark_failed(sample_entities[0]['id'], sample_run_id, "Error 1")
        state_manager.mark_failed(sample_entities[0]['id'], sample_run_id, "Error 2")
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT retry_count FROM entities WHERE entity_id = ? AND run_id = ?",
            (sample_entities[0]['id'], sample_run_id)
        )
        retry_count = cursor.fetchone()[0]
        conn.close()
        
        assert retry_count == 2

    def test_mark_skipped(self, state_manager, sample_run_id, sample_entities):
        """Update entity to skipped."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        state_manager.mark_skipped(sample_entities[0]['id'], sample_run_id)
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status FROM entities WHERE entity_id = ? AND run_id = ?",
            (sample_entities[0]['id'], sample_run_id)
        )
        status = cursor.fetchone()[0]
        conn.close()
        
        assert status == 'skipped'

    def test_status_update_isolation(self, state_manager, sample_entities):
        """Separate run IDs don't interfere."""
        run_id1 = state_manager.create_run("test1.jsonl", 10, 5, "http://test.com")
        run_id2 = state_manager.create_run("test2.jsonl", 10, 5, "http://test.com")
        
        state_manager.add_entities(run_id1, sample_entities[:2])
        state_manager.add_entities(run_id2, sample_entities[2:])
        
        state_manager.mark_success(sample_entities[0]['id'], run_id1)
        state_manager.mark_failed(sample_entities[2]['id'], run_id2, "Error")
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT status FROM entities WHERE entity_id = ? AND run_id = ?",
            (sample_entities[0]['id'], run_id1)
        )
        status1 = cursor.fetchone()[0]
        
        cursor.execute(
            "SELECT status FROM entities WHERE entity_id = ? AND run_id = ?",
            (sample_entities[2]['id'], run_id2)
        )
        status2 = cursor.fetchone()[0]
        
        conn.close()
        
        assert status1 == 'success'
        assert status2 == 'failed'


class TestQueryMethods:
    """Test query methods."""

    def test_get_failed_entities(self, state_manager, sample_run_id, sample_entities):
        """Retrieve failed entities sorted."""
        state_manager.add_entities(sample_run_id, sample_entities)
        state_manager.mark_failed(sample_entities[0]['id'], sample_run_id, "Error 1")
        state_manager.mark_failed(sample_entities[1]['id'], sample_run_id, "Error 2")
        
        failed = state_manager.get_failed_entities(sample_run_id, limit=10)
        
        assert len(failed) == 2
        assert failed[0].status == 'failed'
        assert failed[0].error_message is not None

    def test_get_failed_entities_limit(self, state_manager, sample_run_id, sample_entities):
        """Respect limit parameter."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        for i, entity in enumerate(sample_entities):
            state_manager.mark_failed(entity['id'], sample_run_id, f"Error {i}")
        
        failed = state_manager.get_failed_entities(sample_run_id, limit=2)
        
        assert len(failed) == 2

    def test_get_stats_summary(self, state_manager, sample_run_id, sample_entities):
        """Aggregate statistics across runs."""
        state_manager.add_entities(sample_run_id, sample_entities)
        state_manager.finish_run(sample_run_id, 2, 1, 1)
        
        stats = state_manager.get_stats_summary()
        
        assert stats['total_runs'] == 1
        assert stats['total_entities'] >= len(sample_entities)
        assert stats['total_success'] == 2
        assert stats['total_failed'] == 1
        assert stats['total_skipped'] == 1

    def test_get_stats_summary_empty(self, state_manager):
        """Empty database handling."""
        state_manager.reset_all()
        stats = state_manager.get_stats_summary()
        
        assert stats['total_runs'] == 0
        assert stats['total_entities'] is None


class TestResetCleanup:
    """Test reset and cleanup operations."""

    def test_reset_run(self, state_manager, sample_run_id, sample_entities):
        """Delete specific run and entities."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        state_manager.reset_run(sample_run_id)
        
        run = state_manager.get_run_stats(sample_run_id)
        assert run is None
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM entities WHERE run_id = ?", (sample_run_id,))
        count = cursor.fetchone()[0]
        conn.close()
        
        assert count == 0

    def test_reset_run_nonexistent(self, state_manager):
        """Handle missing run gracefully."""
        state_manager.reset_run(999)

    def test_reset_all(self, state_manager, sample_run_id, sample_entities):
        """Clear all data."""
        state_manager.add_entities(sample_run_id, sample_entities)
        state_manager.finish_run(sample_run_id, 4, 0, 0)
        
        state_manager.reset_all()
        
        stats = state_manager.get_stats_summary()
        assert stats['total_runs'] == 0
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM entities")
        count = cursor.fetchone()[0]
        conn.close()
        
        assert count == 0

    def test_cascade_delete(self, state_manager, sample_run_id, sample_entities):
        """Foreign key cascade on run deletion."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM entities WHERE run_id = ?", (sample_run_id,))
        count_before = cursor.fetchone()[0]
        conn.close()
        
        state_manager.reset_run(sample_run_id)
        
        conn = sqlite3.connect(state_manager.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM entities WHERE run_id = ?", (sample_run_id,))
        count_after = cursor.fetchone()[0]
        conn.close()
        
        assert count_before > 0
        assert count_after == 0


class TestDataclasses:
    """Test dataclass functionality."""

    def test_entity_record_creation(self):
        """EntityRecord instantiation."""
        record = EntityRecord(
            entity_id="Q1",
            entity_type="item",
            status="pending",
            line_number=1,
            run_id=1,
            last_attempt="2024-01-01 00:00:00",
            retry_count=0,
            error_message=None
        )
        
        assert record.entity_id == "Q1"
        assert record.entity_type == "item"
        assert record.status == "pending"
        assert record.error_message is None

    def test_import_run_creation(self):
        """ImportRun instantiation."""
        run = ImportRun(
            run_id=1,
            start_time="2024-01-01 00:00:00",
            end_time="2024-01-01 01:00:00",
            jsonl_file="test.jsonl",
            total_entities=100,
            success_count=90,
            fail_count=5,
            skip_count=5,
            concurrency=10,
            api_url="http://test.com/import"
        )
        
        assert run.run_id == 1
        assert run.total_entities == 100
        assert run.success_count + run.fail_count + run.skip_count == 100
