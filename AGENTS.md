# AGENTS.md

## Mission

This tool imports Wikidata entities into the EntityBase API from JSONL files.

## Operating Principles

- Be concise - answer in 1-3 sentences unless user asks for detail
- Don't add preamble or postamble - just answer directly
- Don't explain what I'm about to do - just do it
- Be proactive when user gives a task
- Ask clarifying questions only when necessary

## Available Commands

```bash
# Download Wikidata entities
python -m src.cli download --random-items N -o output.jsonl

# Import to EntityBase
python -m src.cli import entities.jsonl

# Check status
python -m src.cli status
```

## Default Settings

- API URL: `http://localhost:8083/v1/import`
- Concurrency: 10
- Log level: INFO
- Wait for API: Yes (waits up to 5 min for API to be ready)

## Features

- **Auto-wait for API**: Waits for API to be ready before importing (max 5 min)
- **Connection error handling**: Retries on connection/network errors
- **Resume capability**: Tracks progress in SQLite, resumes interrupted imports