# DataWorkflow

A Git-like data versioning system with S3 storage and PostgreSQL tracking, featuring a GitHub-like web interface.

## Setup

This project uses [uv](https://github.com/astral-sh/uv) for dependency management. See [docs/UV_SETUP.md](docs/UV_SETUP.md) for details.

```bash
# Install dependencies
uv sync

# Seed example data
source .venv/bin/activate
PYTHONPATH=. python scripts/seed_data.py

# Start the server
PYTHONPATH=. python src/app.py
```

Visit http://localhost:5001

## Run Tests

```bash
PYTHONPATH=. pytest
```
