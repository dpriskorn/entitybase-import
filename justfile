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

# Import all Wikidata lexemes
import-lexemes:
    poetry run python -m src.cli import data/latest-lexemes.json.gz

# Import all Wikidata lexemes with custom concurrency
import-lexemes-fast concurrency="100":
    poetry run python -m src.cli import data/latest-lexemes.json.gz -c {{concurrency}}

# Import all Wikidata items (WARNING: very large, takes days)
import-items:
    poetry run python -m src.cli import data/latest-all.json.gz

# Import all Wikidata items with custom concurrency
import-items-fast concurrency="100":
    poetry run python -m src.cli import data/latest-all.json.gz -c {{concurrency}}

# Resume interrupted lexeme import
resume-lexemes:
    poetry run python -m src.cli import data/latest-lexemes.json.gz --resume

# Resume interrupted item import
resume-items:
    poetry run python -m src.cli import data/latest-all.json.gz --resume

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
