# entitybase-import task runner
# Requires: just (https://github.com/casey/just), poetry

set shell := ["bash", "-cu"]

# List available recipes
@default:
    @just --list --list-heading "Available recipes:" --list-subheading ""

# Setup development environment
setup:
    poetry install

# Run linter
lint:
    poetry run ruff check .

# Run tests
test: lint
    poetry run pytest

# Run type checker
typecheck:
    poetry run mypy .

# Clean cache files
clean:
    find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete
    rm -rf .mypy_cache .pytest_cache .ruff_cache

# Download all Wikidata lexemes (~9M entities, ~600MB compressed)
download-lexemes:
    poetry run python -m src.cli download-dump lexemes

# Download all Wikidata items (~150GB compressed, WARNING: very large)
download-items:
    poetry run python -m src.cli download-dump items

# Ensure lexeme dump is downloaded (skip if exists)
[private]
ensure-lexemes:
    @if ls data/lexemes-*.json.gz 1>/dev/null 2>&1; then \
      echo "Using existing $$(ls -t data/lexemes-*.json.gz | head -1)"; \
    else just download-lexemes; fi

# Ensure items dump is downloaded (skip if exists)
[private]
ensure-items:
    @if ls data/items-*.json.gz 1>/dev/null 2>&1; then \
      echo "Using existing $$(ls -t data/items-*.json.gz | head -1)"; \
    else just download-items; fi

# Import all Wikidata lexemes
import-lexemes: ensure-lexemes
    poetry run python -m src.cli import "$$(ls -t data/lexemes-*.json.gz | head -1)"

# Import all Wikidata lexemes with custom concurrency
import-lexemes-fast concurrency="100": ensure-lexemes
    poetry run python -m src.cli import "$$(ls -t data/lexemes-*.json.gz | head -1)" -c {{concurrency}}

# Import all Wikidata items (WARNING: very large, takes days)
import-items: ensure-items
    poetry run python -m src.cli import "$$(ls -t data/items-*.json.gz | head -1)"

# Import all Wikidata items with custom concurrency
import-items-fast concurrency="100": ensure-items
    poetry run python -m src.cli import "$$(ls -t data/items-*.json.gz | head -1)" -c {{concurrency}}

# Resume interrupted lexeme import
resume-lexemes:
    @test -f "$$(ls -t data/lexemes-*.json.gz 2>/dev/null | head -1)" || (echo "ERROR: No lexemes dump found. Run: just download-lexemes" && exit 1)
    poetry run python -m src.cli import "$$(ls -t data/lexemes-*.json.gz | head -1)" --resume

# Resume interrupted item import
resume-items:
    @test -f "$$(ls -t data/items-*.json.gz 2>/dev/null | head -1)" || (echo "ERROR: No items dump found. Run: just download-items" && exit 1)
    poetry run python -m src.cli import "$$(ls -t data/items-*.json.gz | head -1)" --resume

# Full workflow: download and import lexemes
lexemes: download-lexemes import-lexemes

# Full workflow: download and import items (WARNING: very large)
items: download-items import-items

# Check import status
status:
    poetry run python -m src.cli status

# Show overall statistics
stats:
    poetry run python -m src.cli stats

# List all import runs
runs:
    poetry run python -m src.cli runs

# List failed entities
failed:
    poetry run python -m src.cli list --status failed

# Reset all import state (will prompt for confirmation)
reset:
    poetry run python -m src.cli reset

# Download specific entities
download +entity_ids:
    poetry run python -m src.cli download -o data/entities.jsonl {{entity_ids}}

# Download random items
download-random-items count="100":
    poetry run python -m src.cli download -o data/entities.jsonl --random-items {{count}}

# Import a JSONL file
import jsonl_file:
    poetry run python -m src.cli import {{jsonl_file}}

# Show MariaDB tuning recommendations
mariadb-tuning:
    @echo "=== MariaDB Bulk Import Tuning ==="
    @echo ""
    @echo "1. Create config file:"
    @echo "   sudo tee /etc/my.cnf.d/bulk-import.cnf << 'EOF'"
    @echo "   [mysqld]"
    @echo "   innodb_buffer_pool_size = 6G"
    @echo "   innodb_log_file_size = 1G"
    @echo "   innodb_log_buffer_size = 64M"
    @echo "   innodb_flush_log_at_trx_commit = 2"
    @echo "   innodb_flush_method = O_DIRECT"
    @echo "   innodb_io_capacity = 200"
    @echo "   bulk_insert_buffer_size = 256M"
    @echo "   innodb_autoinc_lock_mode = 2"
    @echo "   sort_buffer_size = 4M"
    @echo "   join_buffer_size = 4M"
    @echo "   tmp_table_size = 256M"
    @echo "   max_heap_table_size = 256M"
    @echo "   thread_cache_size = 16"
    @echo "   table_open_cache = 4096"
    @echo "   max_allowed_packet = 64M"
    @echo "   EOF"
    @echo ""
    @echo "2. Restart MariaDB:  sudo systemctl restart mariadb"
    @echo "3. Drop FKs+indexes: sudo mariadb entitybase < scripts/drop-indexes.sql"
    @echo "4. Run import:       just import-lexemes"
    @echo "5. Validate FKs:     sudo mariadb entitybase < scripts/validate-fks.sql"
    @echo "6. Recreate:         sudo mariadb entitybase < scripts/create-indexes.sql"
    @echo "7. Cleanup:          sudo rm /etc/my.cnf.d/bulk-import.cnf && sudo systemctl restart mariadb"
    @echo ""
    @echo "See BULK_IMPORT_RECOMMENDATIONS.md for full SQL commands"
