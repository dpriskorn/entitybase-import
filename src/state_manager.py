"""Manage import state using SQLite database."""

import sqlite3
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from contextlib import contextmanager
from typing import cast

DB_PATH = "import_state.db"


class EntityRecord(BaseModel):
    entity_id: str
    entity_type: str
    status: str
    entity_data: str = ""
    line_number: int
    run_id: int
    last_attempt: str
    retry_count: int
    error_message: str = ""


class ImportRun(BaseModel):
    run_id: int
    start_time: str
    end_time: Optional[str] = None
    jsonl_file: str
    total_entities: int
    success_count: int
    fail_count: int
    skip_count: int
    concurrency: int
    api_url: str


class ImportStateManager:
    """Manage import state with SQLite database."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _get_connection(self):
        """Get database connection with context manager."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        """Initialize database schema."""
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS import_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    jsonl_file TEXT NOT NULL,
                    total_entities INTEGER NOT NULL,
                    success_count INTEGER DEFAULT 0,
                    fail_count INTEGER DEFAULT 0,
                    skip_count INTEGER DEFAULT 0,
                    concurrency INTEGER,
                    api_url TEXT
                );

                CREATE TABLE IF NOT EXISTS entities (
                    entity_id TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    entity_data TEXT,
                    line_number INTEGER,
                    run_id INTEGER,
                    last_attempt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    retry_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    PRIMARY KEY (entity_id, run_id),
                    FOREIGN KEY (run_id) REFERENCES import_runs(run_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_entities_status ON entities(status);
                CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
                CREATE INDEX IF NOT EXISTS idx_entities_run_id ON entities(run_id);
                CREATE INDEX IF NOT EXISTS idx_entities_last_attempt ON entities(last_attempt);

                CREATE TABLE IF NOT EXISTS file_counts (
                    file_path TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    entity_count INTEGER NOT NULL,
                    counted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (file_path, file_size)
                );
            """)

    def create_run(self, jsonl_file: str, total_entities: int,
                  concurrency: int, api_url: str) -> int:
        """Create a new import run record."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO import_runs
                (jsonl_file, total_entities, concurrency, api_url)
                VALUES (?, ?, ?, ?)
            """, (jsonl_file, total_entities, concurrency, api_url))
            run_id = cursor.lastrowid
            conn.commit()
            return cast(int, run_id)

    def finish_run(self, run_id: int, success_count: int,
                  fail_count: int, skip_count: int):
        """Mark run as complete."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE import_runs
                SET end_time = CURRENT_TIMESTAMP,
                    success_count = ?,
                    fail_count = ?,
                    skip_count = ?
                WHERE run_id = ?
            """, (success_count, fail_count, skip_count, run_id))
            conn.commit()

    def get_entity_count_for_run(self, run_id: int) -> int:
        """Get count of entities loaded for a run."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM entities WHERE run_id = ?
            """, (run_id,))
            return cursor.fetchone()[0]

    def get_loaded_line_numbers(self, run_id: int) -> set:
        """Get set of line numbers already loaded into database for a run."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT line_number FROM entities WHERE run_id = ?
            """, (run_id,))
            return {row[0] for row in cursor.fetchall()}

    def add_entities(self, run_id: int, entities_with_lines: List[tuple]):
        """Bulk add entities in pending state.

        Args:
            run_id: The import run ID
            entities_with_lines: List of (line_number, entity_dict) tuples
        """
        import json
        with self._get_connection() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO entities
                (entity_id, entity_type, status, entity_data, line_number, run_id)
                VALUES (?, ?, 'pending', ?, ?, ?)
            """, [
                (e['id'], e.get('type', 'item'), json.dumps(e), line_num, run_id)
                for line_num, e in entities_with_lines
            ])
            conn.commit()

    def get_imported_line_numbers(self, run_id: int) -> set:
        """Get set of line numbers already successfully imported for a run."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT line_number FROM entities
                WHERE run_id = ? AND status = 'success'
            """, (run_id,))
            return {row[0] for row in cursor.fetchall()}

    def get_next_batch(self, run_id: int, limit: int = 10) -> List[EntityRecord]:
        """Get next batch of pending entities."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE entities
                SET status = 'processing', last_attempt = CURRENT_TIMESTAMP
                WHERE rowid IN (
                    SELECT rowid FROM entities
                    WHERE run_id = ? AND status = 'pending'
                    LIMIT ?
                )
            """, (run_id, limit))
            conn.commit()

            cursor = conn.execute("""
                SELECT entity_id, entity_type, status, entity_data, line_number, run_id,
                       last_attempt, retry_count, error_message
                FROM entities
                WHERE run_id = ? AND status = 'processing'
                LIMIT ?
            """, (run_id, limit))
            rows = cursor.fetchall()
            return [EntityRecord(
                entity_id=row[0],
                entity_type=row[1],
                status=row[2],
                entity_data=row[3] or "",
                line_number=row[4],
                run_id=row[5],
                last_attempt=row[6],
                retry_count=row[7],
                error_message=row[8] or ""
            ) for row in rows]

    def mark_success(self, entity_id: str, run_id: int):
        """Mark entity as successfully imported."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE entities
                SET status = 'success', last_attempt = CURRENT_TIMESTAMP
                WHERE entity_id = ? AND run_id = ?
            """, (entity_id, run_id))
            conn.commit()

    def mark_failed(self, entity_id: str, run_id: int, error: str):
        """Mark entity as failed."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE entities
                SET status = 'failed',
                    last_attempt = CURRENT_TIMESTAMP,
                    retry_count = retry_count + 1,
                    error_message = ?
                WHERE entity_id = ? AND run_id = ?
            """, (error, entity_id, run_id))
            conn.commit()

    def mark_skipped(self, entity_id: str, run_id: int):
        """Mark entity as skipped (already exists)."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE entities
                SET status = 'skipped', last_attempt = CURRENT_TIMESTAMP
                WHERE entity_id = ? AND run_id = ?
            """, (entity_id, run_id))
            conn.commit()

    def get_run_stats(self, run_id: int) -> Optional[ImportRun]:
        """Get statistics for a run."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM import_runs WHERE run_id = ?
            """, (run_id,))
            row = cursor.fetchone()
            return ImportRun(**row) if row else None

    def get_failed_entities(self, run_id: int, limit: int = 100) -> List[EntityRecord]:
        """Get failed entities for a run."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM entities
                WHERE run_id = ? AND status = 'failed'
                ORDER BY last_attempt DESC
                LIMIT ?
            """, (run_id, limit))
            return [EntityRecord(**row) for row in cursor.fetchall()]

    def get_stats_summary(self) -> Dict[str, Any]:
        """Get overall statistics across all runs."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT
                    COUNT(DISTINCT run_id) as total_runs,
                    SUM(total_entities) as total_entities,
                    SUM(success_count) as total_success,
                    SUM(fail_count) as total_failed,
                    SUM(skip_count) as total_skipped
                FROM import_runs
            """)
            row = cursor.fetchone()
            return dict(row)

    def reset_run(self, run_id: int):
        """Reset a run (delete all its records)."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM entities WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM import_runs WHERE run_id = ?", (run_id,))
            conn.commit()

    def reset_all(self):
        """Reset all data."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM entities")
            conn.execute("DELETE FROM import_runs")
            conn.commit()

    def get_cached_count(self, file_path: str, file_size: int) -> Optional[int]:
        """Get cached entity count for a file. Returns None if not cached."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT entity_count FROM file_counts
                WHERE file_path = ? AND file_size = ?
            """, (file_path, file_size))
            row = cursor.fetchone()
            return row[0] if row else None

    def store_count(self, file_path: str, file_size: int, entity_count: int):
        """Store entity count for a file."""
        with self._get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO file_counts
                (file_path, file_size, entity_count, counted_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (file_path, file_size, entity_count))
            conn.commit()

    def find_incomplete_run(self, jsonl_file: str) -> Optional[ImportRun]:
        """Find the last incomplete run for a given file (for --resume)."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM import_runs
                WHERE jsonl_file = ? AND end_time IS NULL
                ORDER BY run_id DESC
                LIMIT 1
            """, (jsonl_file,))
            row = cursor.fetchone()
            return ImportRun(**row) if row else None

    def find_last_run(self, jsonl_file: str) -> Optional[ImportRun]:
        """Find the last run for a given file."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM import_runs
                WHERE jsonl_file = ?
                ORDER BY run_id DESC
                LIMIT 1
            """, (jsonl_file,))
            row = cursor.fetchone()
            return ImportRun(**row) if row else None

    def get_resume_stats(self, run_id: int) -> Dict[str, Any]:
        """Get stats for resuming a run - how many done vs remaining."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT status, COUNT(*) as count
                FROM entities
                WHERE run_id = ?
                GROUP BY status
            """, (run_id,))
            stats = {row['status']: row['count'] for row in cursor.fetchall()}
            return {
                'success': stats.get('success', 0),
                'failed': stats.get('failed', 0),
                'skipped': stats.get('skipped', 0),
                'pending': stats.get('pending', 0),
                'processing': stats.get('processing', 0),
                'total': sum(stats.values())
            }

    def reset_pending_to_failed(self, run_id: int):
        """Reset processing entities back to pending for retry."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE entities
                SET status = 'pending'
                WHERE run_id = ? AND status = 'processing'
            """, (run_id,))
            conn.commit()
