# entitybase-import

A command-line tool for importing entities into the EntityBase API from JSONL files.

## Architecture

```mermaid
flowchart TD
    subgraph Input
        JSONL[("JSONL File")]
    end

    subgraph Core
        IMP[("jsonl_import.py")]
        SM[("state_manager.py")]
        API[("EntityBase API")]
    end

    subgraph Storage
        DB[(("import_state.db"))]
    end

    subgraph CLI
        CLI[("cli.py")]
    end

    JSONL --> IMP
    IMP --> SM
    SM <--> DB
    IMP --> API
    CLI --> DB

    style JSONL fill:#f9f,stroke:#333
    style IMP fill:#bbf,stroke:#333
    style SM fill:#bfb,stroke:#333
    style API fill:#fbb,stroke:#333
    style DB fill:#ffd,stroke:#333
    style CLI fill:#bff,stroke:#333
```

## Features

- **Parallel Processing**: Configurable concurrency for faster imports
- **Resume Capability**: SQLite-based state management to track progress and resume interrupted imports
- **Retry Logic**: Automatic retry with exponential backoff for failed imports
- **Detailed Logging**: Comprehensive logging to both console and files
- **Status Tracking**: Real-time progress tracking with rate and ETA calculations
- **Cleanup Options**: Automatic or manual database cleanup after import

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd entitybase-import

# Setup virtual environment and install dependencies
make setup

# Or step by step:
make venv          # Create virtual environment
make install       # Install package
```

## Requirements

- Python >= 3.14
- Running EntityBase API instance
- SQLite3 (for state management)

## Quick Start

```bash
# Basic import
python scripts/imports/jsonl_import.py data/entities.jsonl

# With custom concurrency and API URL
python scripts/imports/jsonl_import.py data/entities.jsonl \
  --concurrency 20 \
  --api-url https://api.example.com/v1/entitybase

# With cleanup prompt
python scripts/imports/jsonl_import.py data/entities.jsonl --cleanup

# Auto-cleanup without prompt
python scripts/imports/jsonl_import.py data/entities.jsonl --auto-cleanup

# With custom log file and debug level
python scripts/imports/jsonl_import.py data/entities.jsonl \
  --log-file logs/my_import.log \
  --log-level DEBUG

# Import specific line range
python scripts/imports/jsonl_import.py data/entities.jsonl \
  --from 1000 \
  --to 2000

# Import from line 1000 to end
python scripts/imports/jsonl_import.py data/entities.jsonl \
  --from 1000
```

## Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `jsonl_file` | Required | Path to JSONL file to import |
| `--concurrency, -c` | 10 | Number of parallel imports |
| `--progress-interval, -p` | 10 | Show detailed progress every N batches |
| `--api-url` | `http://localhost:8000/v1/entitybase` | API base URL |
| `--db-path` | `import_state.db` | Path to SQLite state database |
| `--cleanup` | False | Prompt to delete database after import |
| `--auto-cleanup` | False | Automatically delete database (no prompt) |
| `--log-file` | `logs/import_YYYY-MM-DD_HH-MM-SS.log` | Path to log file |
| `--log-level` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `--from-line, --from` | None | Start importing from line number (1-indexed) |
| `--to-line, --to` | None | Stop importing at line number (1-indexed) |

## JSONL Format

Each line should contain a complete JSON entity object:

```json
{"type":"item","id":"Q1","labels":{"en":{"language":"en","value":"Example"}}}
{"type":"item","id":"Q2","labels":{"en":{"language":"en","value":"Another"}}}
```

## State Management

The import tool uses SQLite to track import state:

- **Pending**: Entities waiting to be imported
- **Processing**: Currently being imported
- **Success**: Successfully imported
- **Skipped**: Already exists in the database (409 Conflict)
- **Failed**: Import failed with error details

Use the CLI to manage state:

```bash
python src/cli.py help                  # Show all commands
python src/cli.py status                 # Show current import status
python src/cli.py stats                  # Show overall statistics
python src/cli.py list --status failed   # List failed entities
python src/cli.py runs                   # List all import runs
python src/cli.py reset                  # Reset all state (prompts confirmation)
python src/cli.py reset --run-id 1       # Reset specific run
```

## Development

```bash
# Setup development environment
make setup                    # Create venv and install with dev dependencies
make clean                    # Remove venv and cache files

# Development commands
make install                  # Install package
make lint                     # Run ruff linter
make test                     # Run tests (includes lint first)
make typecheck                # Run mypy type checker

# Quick reference
make venv                     # Show virtual environment info
```

## Logging

Logs are written to both console and file:

- **Console**: Shows progress and errors at the configured log level
- **File**: Contains detailed debug information including:
  - Run ID for correlation
  - Function and line number
  - Request/response details
  - Timing information

Log files are rotated automatically when they exceed 10MB (5 backup files).

## API Integration

The import tool connects to the EntityBase API via the `/import` endpoint:

- **Method**: POST
- **Headers**:
  - `X-User-ID`: "0"
  - `X-Edit-Summary`: "Bulk import"
- **Body**: JSON entity data

## License

This program is licensed under GNU General Public License v3.0 or later. See the [LICENSE](LICENSE) file for details.

## Contributing

[Add contribution guidelines here]
