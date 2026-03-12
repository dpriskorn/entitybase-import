"""CLI interface for import state database."""

import argparse
import sqlite3
from pathlib import Path

DB_PATH = "import_state.db"


def _ensure_db():
    """Ensure database tables exist."""
    conn = sqlite3.connect(DB_PATH)
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
    """)
    conn.commit()
    conn.close()


def cmd_status(args):
    """Show current import status."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    cursor = conn.execute("""
        SELECT * FROM import_runs
        ORDER BY run_id DESC
        LIMIT 1
    """)
    run = cursor.fetchone()

    if not run:
        print("No import runs found")
        return

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


def cmd_list(args):
    """List entities by status."""
    _ensure_db()
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


def cmd_stats(args):
    """Show overall statistics."""
    _ensure_db()
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


def cmd_reset(args):
    """Reset import state."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)

    if args.run_id:
        print(f"Resetting run #{args.run_id}...")
        conn.execute("DELETE FROM entities WHERE run_id = ?", (args.run_id,))
        conn.execute("DELETE FROM import_runs WHERE run_id = ?", (args.run_id,))
    else:
        confirm = input("This will delete ALL import state. Type 'yes' to confirm: ")
        if confirm.lower() != 'yes':
            print("Cancelled")
            return

        print("Resetting all import state...")
        conn.execute("DELETE FROM entities")
        conn.execute("DELETE FROM import_runs")

    conn.commit()
    print("Done")


def cmd_export(args):
    """Export entities to CSV."""
    _ensure_db()
    import csv

    conn = sqlite3.connect(DB_PATH)
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

    print(f"Exported {cursor.rowcount} entities to {args.file}")


def cmd_runs(args):
    """List all import runs."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
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


def cmd_help(args):
    """Show help message."""
    import sys
    sys.argv = ['cli.py', '--help']
    main()


def cmd_import(args):
    """Import entities from JSONL file."""
    import asyncio
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from jsonl_import import import_from_jsonl

    api_url = f"http://{args.host}:{args.port}/{args.version}/entitybase"

    asyncio.run(import_from_jsonl(
        args.jsonl_file,
        concurrency=args.concurrency,
        progress_interval=args.progress_interval,
        api_url=api_url,
        db_path=args.db_path,
        cleanup=args.cleanup,
        auto_cleanup=args.auto_cleanup,
        log_level=args.log_level,
        from_line=args.from_line,
        to_line=args.to_line
    ))


def cmd_download(args):
    """Download Wikidata entities."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent))
    from download_wikidata_entities import cmd_download as download_cmd

    download_cmd(args)


def main():
    parser = argparse.ArgumentParser(description='EntityBase Import CLI')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    import_parser = subparsers.add_parser('import', help='Import entities from JSONL file into EntityBase')
    import_parser.add_argument('jsonl_file', help='Path to JSONL file to import')
    import_parser.add_argument('--concurrency', '-c', type=int, default=10, help='Number of parallel imports')
    import_parser.add_argument('--progress-interval', '-p', type=int, default=10, help='Show progress every N batches')
    import_parser.add_argument('--host', default='localhost', help='EntityBase API host')
    import_parser.add_argument('--port', type=int, default=8083, help='EntityBase API port')
    import_parser.add_argument('--version', default='v1', help='EntityBase API version')
    import_parser.add_argument('--db-path', default='import_state.db', help='Path to SQLite state database')
    import_parser.add_argument('--cleanup', action='store_true', help='Prompt to delete database after import')
    import_parser.add_argument('--auto-cleanup', action='store_true', help='Automatically delete database after import')
    import_parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Logging level')
    import_parser.add_argument('--from', dest='from_line', type=int, help='Start from line number (1-indexed)')
    import_parser.add_argument('--to', dest='to_line', type=int, help='Stop at line number (1-indexed)')

    download_parser = subparsers.add_parser('download', help='Download Wikidata entities to JSONL')
    download_parser.add_argument("entity_ids", nargs="*", help="Specific Wikidata entity IDs (e.g., Q42, P31, L42)")
    download_parser.add_argument("--random-items", "-i", type=int, metavar="N", default=0, help="Download N random items (Q)")
    download_parser.add_argument("--random-properties", "-p", type=int, metavar="N", default=0, help="Download N random properties (P)")
    download_parser.add_argument("--random-lexemes", "-l", type=int, metavar="N", default=0, help="Download N random lexemes (L)")
    download_parser.add_argument("--output", "-o", type=Path, required=True, help="Output JSONL file path (required)")
    download_parser.add_argument("--append", "-a", action="store_true", help="Append to existing JSONL file")
    download_parser.add_argument("--seed", "-s", type=int, default=None, help="Random seed for reproducibility")
    download_parser.add_argument("--verbose", "-v", action="store_true", help="Print verbose output")

    subparsers.add_parser('status', help='Show current import status')

    list_parser = subparsers.add_parser('list', help='List entities')
    list_parser.add_argument('--status', help='Filter by status')
    list_parser.add_argument('--type', help='Filter by entity type')
    list_parser.add_argument('--run-id', type=int, help='Filter by run ID')
    list_parser.add_argument('--limit', type=int, default=100, help='Max results')

    subparsers.add_parser('stats', help='Show overall statistics')

    reset_parser = subparsers.add_parser('reset', help='Reset import state')
    reset_parser.add_argument('--run-id', type=int, help='Reset specific run')

    export_parser = subparsers.add_parser('export', help='Export to CSV')
    export_parser.add_argument('--status', help='Filter by status')
    export_parser.add_argument('--file', required=True, help='Output file path')

    runs_parser = subparsers.add_parser('runs', help='List all import runs')
    runs_parser.add_argument('--limit', type=int, default=10, help='Max runs to show')

    subparsers.add_parser('help', help='Show this help message')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    commands = {
        'help': cmd_help,
        'import': cmd_import,
        'download': cmd_download,
        'status': cmd_status,
        'list': cmd_list,
        'stats': cmd_stats,
        'reset': cmd_reset,
        'export': cmd_export,
        'runs': cmd_runs
    }

    commands[args.command](args)


if __name__ == '__main__':
    main()
