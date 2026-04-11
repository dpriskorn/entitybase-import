# Usage Guide

## Basic Usage

### Simple Import

```bash
python scripts/imports/jsonl_import.py data/entities.jsonl
```

This will:
- Import entities from `data/entities.jsonl`
- Use default concurrency (10 parallel requests)
- Connect to `http://localhost:8000/v1/entitybase`
- Save state to `import_state.db`
- Log to `logs/import_YYYY-MM-DD_HH-MM-SS.log`

### Custom Concurrency

Increase concurrency for faster imports (watch API rate limits):

```bash
python scripts/imports/jsonl_import.py data/entities.jsonl --concurrency 20
```

### Custom API URL

Connect to a remote API:

```bash
python scripts/imports/jsonl_import.py data/entities.jsonl \
  --api-url https://api.example.com/v1/entitybase
```

### Cleanup Database

Prompt to delete the database after import:

```bash
python scripts/imports/jsonl_import.py data/entities.jsonl --cleanup
```

Auto-delete without prompt (useful for scripts/CI):

```bash
python scripts/imports/jsonl_import.py data/entities.jsonl --auto-cleanup
```

## Advanced Usage

### Detailed Logging

Enable debug-level logging:

```bash
python scripts/imports/jsonl_import.py data/entities.jsonl --log-level DEBUG
```

### Custom Log File

Specify a log file path:

```bash
python scripts/imports/jsonl_import.py data/entities.jsonl \
  --log-file logs/production_import.log
```

### Custom Database Path

Use a separate database for each import:

```bash
python scripts/imports/jsonl_import.py data/entities.jsonl \
  --db-path states/feb2025_import.db
```

### Full Example

```bash
python scripts/imports/jsonl_import.py \
  data/production_dump.jsonl \
  --concurrency 30 \
  --progress-interval 5 \
  --api-url https://api.example.com/v1/entitybase \
  --db-path states/prod_import.db \
  --log-file logs/prod_import_2025-02-12.log \
  --log-level DEBUG \
  --cleanup
```

## JSONL File Format

Each line must be a complete JSON object:

```json
{"type":"item","id":"Q1","labels":{"en":{"language":"en","value":"Example Item"}},"descriptions":{"en":{"language":"en","value":"An example item"}}}
{"type":"item","id":"Q2","labels":{"en":{"language":"en","value":"Another Item"}}}
```

### Valid JSONL Features

- Each line is independent (no trailing commas)
- Blank lines are ignored
- Comments are NOT supported
- All standard JSON types are supported

## Status Management

### View Import Status

```bash
python scripts/imports/cli.py status
```

### Resume Interrupted Import

If an import is interrupted (e.g., network failure), simply re-run the command:

```bash
python scripts/imports/jsonl_import.py data/entities.jsonl
```

The tool will:
1. Detect the existing database
2. Skip already imported entities
3. Continue with pending entities
4. Retry failed entities

### Reset Import State

To start fresh (delete all progress):

```bash
rm import_state.db
# Or
python scripts/imports/cli.py reset
```

## Performance Tuning

### Concurrency

- **Low latency networks**: 20-50 concurrent requests
- **High latency networks**: 10-20 concurrent requests
- **Rate-limited APIs**: 5-10 concurrent requests
- **Local API**: 50-100 concurrent requests

### Progress Interval

Adjust how often detailed progress is shown:

- **Large imports**: `--progress-interval 20` (every 20 batches)
- **Small imports**: `--progress-interval 5` (every 5 batches)
- **Quiet mode**: `--progress-interval 999` (only at end)

## Error Handling

### Retry Logic

The tool automatically retries failed requests up to 3 times with exponential backoff:
- Attempt 1: immediate
- Attempt 2: wait 2 seconds
- Attempt 3: wait 4 seconds

### Common Errors

**409 Conflict**: Entity already exists → Automatically skipped
**400 Bad Request**: Invalid entity data → Logged and marked as failed
**Timeout**: Network or API slow → Retried automatically
**5xx Server Error**: API error → Retried automatically

### Troubleshooting

1. **Check logs**: Review `logs/import_*.log` for detailed error messages
2. **Check database**: Use SQLite browser to inspect `import_state.db`
3. **Test API**: Verify API is responding: `curl http://localhost:8000/health`
4. **Reduce concurrency**: Lower `--concurrency` if hitting rate limits
5. **Increase timeout**: Edit `REQUEST_TIMEOUT` in script if entities are large

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Import Entities
on: [push]
jobs:
  import:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.13'
      - run: pip install httpx
      - run: python scripts/imports/jsonl_import.py \
          data/entities.jsonl \
          --api-url ${{ secrets.API_URL }} \
          --auto-cleanup
```

## Monitoring

### Progress Indicators

The tool shows:
- **Processed**: Number of entities completed
- **Total**: Total entities to import
- **Percent**: Completion percentage
- **Rate**: Current import rate (entities/minute)
- **ETA**: Estimated time remaining

### Log Analysis

View failed imports:

```bash
grep "ERROR" logs/import_*.log
```

View skipped entities:

```bash
grep "Skipped" logs/import_*.log
```

View timing:

```bash
grep "in.*s$" logs/import_*.log
```
