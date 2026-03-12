.PHONY: help setup install install-dev lint test typecheck clean venv

help:
	@echo "Available targets:"
	@echo "  setup         - Create venv and install with dev dependencies"
	@echo "  install       - Install the package using Poetry"
	@echo "  install-dev  - Install with dev dependencies"
	@echo "  lint         - Run ruff linter"
	@echo "  test         - Run tests with pytest"
	@echo "  typecheck    - Run mypy type checker"
	@echo "  clean        - Remove virtual environment and cache files"
	@echo "  venv         - Show/create virtual environment"

setup: venv install-dev

venv:
	poetry env info || poetry env use python3

install:
	poetry install

install-dev:
	poetry install --with dev

lint:
	poetry run ruff check .

test:
	poetry run pytest

typecheck:
	poetry run mypy .

clean:
	rm -rf .venv
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .mypy_cache .pytest_cache
