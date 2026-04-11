.PHONY: help setup install lint test typecheck clean

help:
	@echo "Available targets:"
	@echo "  setup           - Install dependencies with poetry"
	@echo "  install        - Install the package with poetry"
	@echo "  lint           - Run ruff linter"
	@echo "  test           - Run tests with pytest"
	@echo "  typecheck      - Run mypy type checker"
	@echo "  clean          - Remove cache files"

setup: install

install:
	poetry install

lint:
	poetry run ruff check .

test: lint
	poetry run pytest

typecheck:
	poetry run mypy .

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .mypy_cache .pytest_cache .ruff_cache