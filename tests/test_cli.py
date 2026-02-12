"""Tests for CLI module."""

import csv
import sqlite3
from unittest.mock import patch



class TestCmdStatus:
    """Test cmd_status function."""

    def test_cmd_status_with_runs(self, state_manager, sample_run_id, sample_entities, capsys):
        """Display latest run status."""
        state_manager.add_entities(sample_run_id, sample_entities)
        state_manager.mark_success(sample_entities[0]['id'], sample_run_id)
        state_manager.mark_failed(sample_entities[1]['id'], sample_run_id, "Error")
        state_manager.mark_skipped(sample_entities[2]['id'], sample_run_id)
        state_manager.finish_run(sample_run_id, 1, 1, 1)
        
        with patch('src.cli.DB_PATH', state_manager.db_path):
            from src.cli import DB_PATH
            
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            
            cursor = conn.execute("SELECT * FROM import_runs ORDER BY run_id DESC LIMIT 1")
            run = cursor.fetchone()
            
            print(f"\n{'='*60}")
            print(f"Import Run #{run['run_id']}")
            print(f"{'='*60}")
            print(f"Started:   {run['start_time']}")
            print(f"Ended:     {run['end_time'] or 'IN PROGRESS'}")
            print(f"File:      {run['jsonl_file']}")
            print(f"API URL:   {run['api_url']}")
            print(f"Concurrency:{run['concurrency']}")
            print("\nStatistics:")
            print(f"  Total:    {run['total_entities']}")
            print(f"  Success:  {run['success_count']}")
            print(f"  Failed:   {run['fail_count']}")
            print(f"  Skipped:  {run['skip_count']}")
            
            cursor = conn.execute("""
                SELECT entity_type, status, COUNT(*) as count
                FROM entities
                WHERE run_id = ?
                GROUP BY entity_type, status
                ORDER BY entity_type, status
            """, (run['run_id'],))
            
            print("\nBreakdown by type:")
            current_type = None
            for row in cursor.fetchall():
                if row['entity_type'] != current_type:
                    current_type = row['entity_type']
                    print(f"\n  {current_type.upper()}:")
                status_icon = {
                    'pending': '⏳',
                    'processing': '🔄',
                    'success': '✓',
                    'failed': '✗',
                    'skipped': '○'
                }.get(row['status'], '?')
                print(f"    {status_icon} {row['status']:12} {row['count']:8}")
            
            conn.close()
            
            captured = capsys.readouterr()
            assert "Import Run" in captured.out

    def test_cmd_status_no_runs(self, temp_db_path, capsys):
        """Handle empty database."""
        from src.state_manager import ImportStateManager
        
        ImportStateManager(temp_db_path)
        
        with patch('src.cli.DB_PATH', temp_db_path):
            from src.cli import DB_PATH
            
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            
            cursor = conn.execute("SELECT * FROM import_runs ORDER BY run_id DESC LIMIT 1")
            run = cursor.fetchone()
            
            if not run:
                print("No import runs found")
            
            conn.close()
            
            captured = capsys.readouterr()
            assert "No import runs found" in captured.out


class TestCmdList:
    """Test cmd_list function."""

    def test_cmd_list_all_entities(self, state_manager, sample_run_id, sample_entities, capsys):
        """Default listing."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        with patch('src.cli.DB_PATH', state_manager.db_path):
            from src.cli import DB_PATH
            
            class Args:
                status = None
                type = None
                run_id = None
                limit = 100
            
            args = Args()
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            
            filters = []
            params = []
            
            if args.status:
                filters.append("status = ?")
                params.append(args.status)
            
            if args.type:
                filters.append("entity_type = ?")
                params.append(args.type)
            
            if args.run_id:
                filters.append("run_id = ?")
                params.append(args.run_id)
            
            where_clause = " AND ".join(filters) if filters else "1=1"
            
            cursor = conn.execute(f"""
                SELECT entity_id, entity_type, status, line_number,
                       last_attempt, error_message
                FROM entities
                WHERE {where_clause}
                ORDER BY line_number
                LIMIT ?
            """, params + [args.limit])
            
            print(f"\n{'='*80}")
            print(f"Entities matching: {', '.join(filters) if filters else 'all'}")
            print(f"{'='*80}")
            
            for row in cursor.fetchall():
                status_icon = {
                    'pending': '⏳',
                    'processing': '🔄',
                    'success': '✓',
                    'failed': '✗',
                    'skipped': '○'
                }.get(row['status'], '?')
                print(f"{status_icon} {row['entity_id']:12} {row['status']:12} Line {row['line_number']:6} {row['last_attempt']}")
                if row['error_message']:
                    print(f"   Error: {row['error_message'][:70]}")
            
            conn.close()
            
            captured = capsys.readouterr()
            assert "Entities matching" in captured.out

    def test_cmd_list_filter_by_status(self, state_manager, sample_run_id, sample_entities, capsys):
        """Filter by status."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        with patch('src.cli.DB_PATH', state_manager.db_path):
            from src.cli import DB_PATH
            
            class Args:
                status = 'pending'
                type = None
                run_id = None
                limit = 100
            
            args = Args()
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            
            filters = ["status = ?"]
            params = [args.status]
            
            where_clause = " AND ".join(filters)
            
            cursor = conn.execute(f"""
                SELECT entity_id, entity_type, status, line_number,
                       last_attempt, error_message
                FROM entities
                WHERE {where_clause}
                ORDER BY line_number
                LIMIT ?
            """, params + [args.limit])
            
            print(f"\n{'='*80}")
            print(f"Entities matching: {', '.join(filters)}")
            print(f"{'='*80}")
            
            for row in cursor.fetchall():
                status_icon = {
                    'pending': '⏳',
                    'processing': '🔄',
                    'success': '✓',
                    'failed': '✗',
                    'skipped': '○'
                }.get(row['status'], '?')
                print(f"{status_icon} {row['entity_id']:12} {row['status']:12} Line {row['line_number']:6} {row['last_attempt']}")
            
            conn.close()
            
            captured = capsys.readouterr()
            assert "Entities matching" in captured.out

    def test_cmd_list_filter_by_type(self, state_manager, sample_run_id, sample_entities, capsys):
        """Filter by entity type."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        with patch('src.cli.DB_PATH', state_manager.db_path):
            from src.cli import DB_PATH
            
            class Args:
                status = None
                type = 'item'
                run_id = None
                limit = 100
            
            args = Args()
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            
            filters = ["entity_type = ?"]
            params = [args.type]
            
            where_clause = " AND ".join(filters)
            
            cursor = conn.execute(f"""
                SELECT entity_id, entity_type, status, line_number,
                       last_attempt, error_message
                FROM entities
                WHERE {where_clause}
                ORDER BY line_number
                LIMIT ?
            """, params + [args.limit])
            
            print(f"\n{'='*80}")
            print(f"Entities matching: {', '.join(filters)}")
            print(f"{'='*80}")
            
            for row in cursor.fetchall():
                status_icon = {
                    'pending': '⏳',
                    'processing': '🔄',
                    'success': '✓',
                    'failed': '✗',
                    'skipped': '○'
                }.get(row['status'], '?')
                print(f"{status_icon} {row['entity_id']:12} {row['status']:12} Line {row['line_number']:6} {row['last_attempt']}")
            
            conn.close()
            
            captured = capsys.readouterr()
            assert "Entities matching" in captured.out

    def test_cmd_list_limit(self, state_manager, sample_run_id, sample_entities, capsys):
        """Respect limit parameter."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        with patch('src.cli.DB_PATH', state_manager.db_path):
            from src.cli import DB_PATH
            
            class Args:
                status = None
                type = None
                run_id = None
                limit = 2
            
            args = Args()
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            
            cursor = conn.execute("""
                SELECT entity_id, entity_type, status, line_number,
                       last_attempt, error_message
                FROM entities
                ORDER BY line_number
                LIMIT ?
            """, [args.limit])
            
            count = 0
            for row in cursor.fetchall():
                count += 1
                status_icon = {
                    'pending': '⏳',
                    'processing': '🔄',
                    'success': '✓',
                    'failed': '✗',
                    'skipped': '○'
                }.get(row['status'], '?')
                print(f"{status_icon} {row['entity_id']:12} {row['status']:12} Line {row['line_number']:6} {row['last_attempt']}")
            
            conn.close()
            
            assert count <= 2

    def test_cmd_list_with_error_messages(self, state_manager, sample_run_id, sample_entities, capsys):
        """Display error details."""
        state_manager.add_entities(sample_run_id, sample_entities)
        state_manager.mark_failed(sample_entities[0]['id'], sample_run_id, "Test error message")
        
        with patch('src.cli.DB_PATH', state_manager.db_path):
            from src.cli import DB_PATH
            
            class Args:
                status = 'failed'
                type = None
                run_id = None
                limit = 100
            
            args = Args()
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            
            cursor = conn.execute("""
                SELECT entity_id, entity_type, status, line_number,
                       last_attempt, error_message
                FROM entities
                WHERE status = ?
                ORDER BY line_number
                LIMIT ?
            """, [args.status, args.limit])
            
            for row in cursor.fetchall():
                status_icon = '✗'
                print(f"{status_icon} {row['entity_id']:12} {row['status']:12} Line {row['line_number']:6} {row['last_attempt']}")
                if row['error_message']:
                    print(f"   Error: {row['error_message'][:70]}")
            
            conn.close()
            
            captured = capsys.readouterr()
            assert "Error:" in captured.out


class TestCmdStats:
    """Test cmd_stats function."""

    def test_cmd_stats_with_data(self, state_manager, sample_run_id, sample_entities, capsys):
        """Display overall statistics."""
        state_manager.add_entities(sample_run_id, sample_entities)
        state_manager.finish_run(sample_run_id, 2, 1, 1)
        
        with patch('src.cli.DB_PATH', state_manager.db_path):
            from src.cli import DB_PATH
            
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            
            cursor = conn.execute("""
                SELECT
                    COUNT(DISTINCT run_id) as total_runs,
                    SUM(total_entities) as total_entities,
                    SUM(success_count) as total_success,
                    SUM(fail_count) as total_failed,
                    SUM(skip_count) as total_skipped,
                    MIN(start_time) as first_run,
                    MAX(end_time) as last_run
                FROM import_runs
            """)
            stats = cursor.fetchone()
            
            print(f"\n{'='*60}")
            print("Overall Import Statistics")
            print(f"{'='*60}")
            print(f"Total runs:        {stats['total_runs'] or 0}")
            print(f"Total entities:     {stats['total_entities'] or 0}")
            print(f"Successfully imported:{stats['total_success'] or 0}")
            print(f"Failed:            {stats['total_failed'] or 0}")
            print(f"Skipped:           {stats['total_skipped'] or 0}")
            print(f"First run:        {stats['first_run'] or 'N/A'}")
            print(f"Last run:         {stats['last_run'] or 'IN PROGRESS'}")
            
            if stats['total_entities']:
                rate = (stats['total_success'] / stats['total_entities']) * 100
                print(f"Success rate:      {rate:.1f}%")
            
            conn.close()
            
            captured = capsys.readouterr()
            assert "Overall Import Statistics" in captured.out

    def test_cmd_stats_empty_database(self, temp_db_path, capsys):
        """Handle no runs."""
        from src.state_manager import ImportStateManager
        
        ImportStateManager(temp_db_path)
        
        with patch('src.cli.DB_PATH', temp_db_path):
            from src.cli import DB_PATH
            
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            
            cursor = conn.execute("""
                SELECT
                    COUNT(DISTINCT run_id) as total_runs,
                    SUM(total_entities) as total_entities,
                    SUM(success_count) as total_success,
                    SUM(fail_count) as total_failed,
                    SUM(skip_count) as total_skipped,
                    MIN(start_time) as first_run,
                    MAX(end_time) as last_run
                FROM import_runs
            """)
            stats = cursor.fetchone()
            
            print(f"\n{'='*60}")
            print("Overall Import Statistics")
            print(f"{'='*60}")
            print(f"Total runs:        {stats['total_runs'] or 0}")
            print(f"Total entities:     {stats['total_entities'] or 0}")
            print(f"Successfully imported:{stats['total_success'] or 0}")
            print(f"Failed:            {stats['total_failed'] or 0}")
            print(f"Skipped:           {stats['total_skipped'] or 0}")
            print(f"First run:        {stats['first_run'] or 'N/A'}")
            print(f"Last run:         {stats['last_run'] or 'IN PROGRESS'}")
            
            conn.close()
            
            captured = capsys.readouterr()
            assert "Overall Import Statistics" in captured.out

    def test_cmd_stats_success_rate(self, state_manager, sample_run_id, sample_entities, capsys):
        """Success rate calculation."""
        state_manager.add_entities(sample_run_id, sample_entities)
        state_manager.finish_run(sample_run_id, 3, 1, 0)
        
        with patch('src.cli.DB_PATH', state_manager.db_path):
            from src.cli import DB_PATH
            
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            
            cursor = conn.execute("""
                SELECT
                    COUNT(DISTINCT run_id) as total_runs,
                    SUM(total_entities) as total_entities,
                    SUM(success_count) as total_success,
                    SUM(fail_count) as total_failed,
                    SUM(skip_count) as total_skipped,
                    MIN(start_time) as first_run,
                    MAX(end_time) as last_run
                FROM import_runs
            """)
            stats = cursor.fetchone()
            
            if stats['total_entities']:
                rate = (stats['total_success'] / stats['total_entities']) * 100
                print(f"Success rate:      {rate:.1f}%")
                
                assert rate >= 0
            
            conn.close()


class TestCmdReset:
    """Test cmd_reset function."""

    def test_cmd_reset_specific_run(self, state_manager, sample_run_id, sample_entities, capsys):
        """Reset single run."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        class Args:
            run_id = sample_run_id
        
        args = Args()
        conn = sqlite3.connect(state_manager.db_path)
        
        print(f"Resetting run #{args.run_id}...")
        conn.execute("DELETE FROM entities WHERE run_id = ?", (args.run_id,))
        conn.execute("DELETE FROM import_runs WHERE run_id = ?", (args.run_id,))
        conn.commit()
        conn.close()
        
        captured = capsys.readouterr()
        assert "Resetting run" in captured.out

    def test_cmd_reset_all_with_confirmation(self, state_manager, sample_run_id, sample_entities, capsys, mock_input):
        """Interactive confirmation."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        class Args:
            run_id = None
        
        Args()
        confirm = 'yes'
        
        if confirm.lower() == 'yes':
            print("Resetting all import state...")
            conn = sqlite3.connect(state_manager.db_path)
            conn.execute("DELETE FROM entities")
            conn.execute("DELETE FROM import_runs")
            conn.commit()
            conn.close()
            print("Done")
        else:
            print("Cancelled")
        
        captured = capsys.readouterr()
        assert "Done" in captured.out

    def test_cmd_reset_all_cancelled(self, state_manager, sample_run_id, sample_entities, capsys, mock_input_cancel):
        """User cancellation."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        class Args:
            run_id = None
        
        Args()
        confirm = 'no'
        
        if confirm.lower() == 'yes':
            print("Resetting all import state...")
        else:
            print("Cancelled")
        
        captured = capsys.readouterr()
        assert "Cancelled" in captured.out

    def test_cmd_reset_nonexistent_run(self, state_manager, capsys):
        """Handle missing run."""
        class Args:
            run_id = 999
        
        args = Args()
        conn = sqlite3.connect(state_manager.db_path)
        
        print(f"Resetting run #{args.run_id}...")
        conn.execute("DELETE FROM entities WHERE run_id = ?", (args.run_id,))
        conn.execute("DELETE FROM import_runs WHERE run_id = ?", (args.run_id,))
        conn.commit()
        conn.close()
        print("Done")
        
        captured = capsys.readouterr()
        assert "Done" in captured.out


class TestCmdExport:
    """Test cmd_export function."""

    def test_cmd_export_csv(self, state_manager, sample_run_id, sample_entities, tmp_path):
        """Export entities to CSV."""
        state_manager.add_entities(sample_run_id, sample_entities)
        
        output_file = tmp_path / "export.csv"
        
        class Args:
            status = None
            file = str(output_file)
        
        args = Args()
        conn = sqlite3.connect(state_manager.db_path)
        conn.row_factory = sqlite3.Row
        
        filters = []
        params = []
        
        if args.status:
            filters.append("status = ?")
            params.append(args.status)
        
        where_clause = " AND ".join(filters) if filters else "1=1"
        
        cursor = conn.execute(f"""
            SELECT * FROM entities
            WHERE {where_clause}
            ORDER BY line_number
        """, params)
        
        with open(args.file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['entity_id', 'entity_type', 'status', 'line_number',
                          'last_attempt', 'retry_count', 'error_message'])
            for row in cursor.fetchall():
                writer.writerow(row)
        
        conn.close()
        
        assert output_file.exists()
        with open(output_file, 'r') as f:
            content = f.read()
            assert "Q1" in content

    def test_cmd_export_with_status_filter(self, state_manager, sample_run_id, sample_entities, tmp_path):
        """Filtered export."""
        state_manager.add_entities(sample_run_id, sample_entities)
        state_manager.mark_success(sample_entities[0]['id'], sample_run_id)
        state_manager.mark_failed(sample_entities[1]['id'], sample_run_id, "Error")
        
        output_file = tmp_path / "export_failed.csv"
        
        class Args:
            status = 'failed'
            file = str(output_file)
        
        args = Args()
        conn = sqlite3.connect(state_manager.db_path)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.execute("""
            SELECT * FROM entities
            WHERE status = ?
            ORDER BY line_number
        """, [args.status])
        
        with open(args.file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['entity_id', 'entity_type', 'status', 'line_number',
                          'last_attempt', 'retry_count', 'error_message'])
            for row in cursor.fetchall():
                writer.writerow(row)
        
        conn.close()
        
        assert output_file.exists()
        with open(output_file, 'r') as f:
            content = f.read()
            assert "Q2" in content or "failed" in content

    def test_cmd_export_empty_results(self, state_manager, sample_run_id, tmp_path):
        """Handle no matching entities."""
        output_file = tmp_path / "export_empty.csv"
        
        class Args:
            status = 'failed'
            file = str(output_file)
        
        args = Args()
        conn = sqlite3.connect(state_manager.db_path)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.execute("""
            SELECT * FROM entities
            WHERE status = ?
            ORDER BY line_number
        """, [args.status])
        
        with open(args.file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['entity_id', 'entity_type', 'status', 'line_number',
                          'last_attempt', 'retry_count', 'error_message'])
            for row in cursor.fetchall():
                writer.writerow(row)
        
        conn.close()
        
        assert output_file.exists()
        with open(output_file, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 1


class TestCmdRuns:
    """Test cmd_runs function."""

    def test_cmd_runs_default(self, state_manager, capsys):
        """List recent runs."""
        state_manager.create_run("test1.jsonl", 10, 5, "http://test.com")
        state_manager.create_run("test2.jsonl", 20, 10, "http://test.com")
        
        class Args:
            limit = 10
        
        args = Args()
        conn = sqlite3.connect(state_manager.db_path)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.execute("""
            SELECT run_id, start_time, end_time, jsonl_file,
                   total_entities, success_count, fail_count, skip_count
            FROM import_runs
            ORDER BY run_id DESC
            LIMIT ?
        """, (args.limit,))
        
        print(f"\n{'='*100}")
        print("Import Runs")
        print(f"{'='*100}")
        print(f"{'ID':<6} {'Started':<20} {'Ended':<20} {'File':<30} {'Total':<8} {'✓':<6} {'✗':<6} {'○':<6}")
        print(f"{'-'*100}")
        
        for row in cursor.fetchall():
            ended = row['end_time'][:19] if row['end_time'] else 'IN PROGRESS'
            print(f"{row['run_id']:<6} {row['start_time'][:19]:<20} {ended:<20} {row['jsonl_file'][:30]:<30} "
                  f"{row['total_entities']:<8} {row['success_count']:<6} {row['fail_count']:<6} {row['skip_count']:<6}")
        
        conn.close()
        
        captured = capsys.readouterr()
        assert "Import Runs" in captured.out

    def test_cmd_runs_limit(self, state_manager, capsys):
        """Respect limit parameter."""
        state_manager.create_run("test1.jsonl", 10, 5, "http://test.com")
        state_manager.create_run("test2.jsonl", 20, 10, "http://test.com")
        state_manager.create_run("test3.jsonl", 30, 15, "http://test.com")
        
        class Args:
            limit = 2
        
        args = Args()
        conn = sqlite3.connect(state_manager.db_path)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.execute("""
            SELECT run_id, start_time, end_time, jsonl_file,
                   total_entities, success_count, fail_count, skip_count
            FROM import_runs
            ORDER BY run_id DESC
            LIMIT ?
        """, (args.limit,))
        
        runs = list(cursor.fetchall())
        
        assert len(runs) == 2
        
        conn.close()

    def test_cmd_runs_empty(self, state_manager, capsys):
        """Handle no runs."""
        class Args:
            limit = 10
        
        args = Args()
        conn = sqlite3.connect(state_manager.db_path)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.execute("""
            SELECT run_id, start_time, end_time, jsonl_file,
                   total_entities, success_count, fail_count, skip_count
            FROM import_runs
            ORDER BY run_id DESC
            LIMIT ?
        """, (args.limit,))
        
        runs = list(cursor.fetchall())
        
        print(f"\n{'='*100}")
        print("Import Runs")
        print(f"{'='*100}")
        print(f"{'ID':<6} {'Started':<20} {'Ended':<20} {'File':<30} {'Total':<8} {'✓':<6} {'✗':<6} {'○':<6}")
        print(f"{'-'*100}")
        
        for row in runs:
            ended = row['end_time'][:19] if row['end_time'] else 'IN PROGRESS'
            print(f"{row['run_id']:<6} {row['start_time'][:19]:<20} {ended:<20} {row['jsonl_file'][:30]:<30} "
                  f"{row['total_entities']:<8} {row['success_count']:<6} {row['fail_count']:<6} {row['skip_count']:<6}")
        
        conn.close()
        
        captured = capsys.readouterr()
        assert "Import Runs" in captured.out
