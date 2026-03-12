# entitybase-import

A command-line tool for importing entities into the EntityBase API from JSONL files.

## Architecture

```mermaid
flowchart TD
    subgraph Input
        JSONL[("JSONL File")]
    end

    subgraph CLI
        CLI[("cli.py")]
    end

    subgraph Core
        IMP[("import_from_jsonl")]
        SM[("state_manager")]
        API[("EntityBase API")]
    end

    subgraph Storage
        DB[(("import_state.db"))]
    end

    JSONL --> CLI
    CLI --> IMP
    IMP --> SM
    SM <--> DB
    IMP --> API
    CLI --> DB

    style JSONL fill:#f9f,stroke:#333
    style CLI fill:#bbf,stroke:#333
    style IMP fill:#bfb,stroke:#333
    style SM fill:#bfb,stroke:#333
    style API fill:#fbb,stroke:#333
    style DB fill:#ffd,stroke:#333
```

## Features

- **Unified CLI**: Single entry point for import and state management
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
```

## Quick Start

```bash
# Show help
python src/cli.py help

# Import entities
python src/cli.py import data/entities.jsonl

# With custom concurrency and API URL
python src/cli.py import data/entities.jsonl -c 20 --api-url https://api.example.com

# Import specific line range
python src/cli.py import data/entities.jsonl --from 1000 --to 2000
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `import` | Import entities from JSONL file |
| `status` | Show current import status |
| `list` | List entities (with filters) |
| `stats` | Show overall statistics |
| `runs` | List all import runs |
| `export` | Export entities to CSV |
| `reset` | Reset import state |
| `help` | Show this help message |

### Examples

```bash
# Check import status
python src/cli.py status

# Show statistics
python src/cli.py stats

# List failed entities
python src/cli.py list --status failed

# List all runs
python src/cli.py runs

# Export failed to CSV
python src/cli.py export --status failed --file failed.csv

# Reset specific run
python src/cli.py reset --run-id 1

# Reset all state (will prompt for confirmation)
python src/cli.py reset
```

## Import Options

| Option | Default | Description |
|--------|---------|-------------|
| `jsonl_file` | Required | Path to JSONL file to import |
| `--concurrency, -c` | 10 | Number of parallel imports |
| `--progress-interval, -p` | 10 | Show progress every N batches |
| `--api-url` | `http://localhost:8000/v1/entitybase` | API base URL |
| `--db-path` | `import_state.db` | Path to SQLite state database |
| `--cleanup` | False | Prompt to delete database after import |
| `--auto-cleanup` | False | Automatically delete database (no prompt) |
| `--log-level` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `--from` | None | Start from line number (1-indexed) |
| `--to` | None | Stop at line number (1-indexed) |

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
```

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
