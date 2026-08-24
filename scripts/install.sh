#!/usr/bin/env bash
set -euo pipefail

echo "=== Updating system packages ==="
apt update && apt upgrade -y
apt install -y git curl build-essential software-properties-common

echo "=== Installing Python 3.13 via deadsnakes PPA ==="
add-apt-repository ppa:deadsnakes/ppa -y
apt update
apt install -y python3.13 python3.13-venv python3.13-dev

echo "=== Installing Poetry ==="
curl -sSL https://install.python-poetry.org | python3.13 -

echo "=== Cloning entitybase-import ==="
git clone https://github.com/dpriskorn/entitybase-import.git
cd entitybase-import

echo "=== Installing project dependencies ==="
poetry install

echo "=== Done ==="
echo "Run 'cd entitybase-import && poetry run entitybase-import --help' to verify."
