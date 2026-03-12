.PHONY: help setup install install-dev lint test typecheck clean venv install-python install-poetry

help:
	@echo "Available targets:"
	@echo "  setup           - Create venv and install with dev dependencies"
	@echo "  install-python - Install Python 3.14 via pyenv"
	@echo "  install-poetry - Reinstall poetry for current Python"
	@echo "  install         - Install the package using Poetry"
	@echo "  install-dev    - Install with dev dependencies"
	@echo "  lint           - Run ruff linter"
	@echo "  test           - Run tests with pytest"
	@echo "  typecheck      - Run mypy type checker"
	@echo "  clean          - Remove virtual environment and cache files"
	@echo "  venv           - Show/create virtual environment"

install-poetry:
	curl -sSL https://install.python-poetry.org | python3.14 -

setup: venv install-dev

install-python:
	pyenv install 3.14.0
	pyenv local 3.14.0

venv:
	poetry env use 3.14 || poetry env use python3.14

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
