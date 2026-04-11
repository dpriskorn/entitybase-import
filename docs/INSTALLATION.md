# Installation Guide

## Prerequisites

- Python 3.13 or higher
- Running EntityBase API instance
- SQLite3 (usually included with Python)

## Install from Source

```bash
# Clone the repository
git clone https://github.com/your-org/entitybase-import.git
cd entitybase-import

# Create virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
poetry install
# Or with pip:
pip install httpx
```

## Verify Installation

```bash
# Check Python version
python --version  # Should be >= 3.13

# Test the import script
python scripts/imports/jsonl_import.py --help
```

## Docker Installation (Optional)

```bash
# Build Docker image
docker build -t entitybase-import .

# Run import
docker run -v $(pwd)/data:/data entitybase-import \
  python scripts/imports/jsonl_import.py /data/entities.jsonl
```

## Troubleshooting

### Python Version Error
Ensure you're using Python 3.13+:
```bash
python --version
# If using pyenv:
pyenv global 3.13.0
```

### Import Error
Make sure you've activated the virtual environment:
```bash
source .venv/bin/activate
```

### API Connection Error
Verify the EntityBase API is running:
```bash
curl http://localhost:8000/v1/entitybase/health
```

Adjust the `--api-url` parameter if your API is hosted elsewhere.
