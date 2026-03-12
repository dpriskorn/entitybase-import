.PHONY: help setup install lint test typecheck clean venv install-python

help:
	@echo "Available targets:"
	@echo "  setup           - Create venv and install dependencies"
	@echo "  install-python - Install Python 3.14 via pyenv"
	@echo "  install         - Install the package using pip"
	@echo "  lint           - Run ruff linter"
	@echo "  test           - Run tests with pytest"
	@echo "  typecheck      - Run mypy type checker"
	@echo "  clean          - Remove virtual environment and cache files"
	@echo "  venv           - Show/create virtual environment"

setup: venv install

venv:
	test -d .venv || python3.14.3 -m venv .venv

install:
	.venv/bin/pip install -e .

lint:
	.venv/bin/ruff check .

test: lint
	.venv/bin/pytest

typecheck:
	.venv/bin/mypy .

clean:
	rm -rf .venv
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .mypy_cache .pytest_cache
