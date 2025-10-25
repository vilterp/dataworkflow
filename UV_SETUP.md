# Using UV Package Manager

This project uses [uv](https://github.com/astral-sh/uv) for fast, declarative Python package management.

## Installation

```bash
# Install uv (macOS/Linux)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or via Homebrew
brew install uv
```

## Quick Start

```bash
# Sync dependencies (creates/updates venv automatically)
uv sync --extra dev

# Run tests
uv run pytest

# Run the worker
uv run python sdk/worker.py --server-url http://localhost:5001

# Run the Flask app
uv run python src/app.py

# Add a new dependency
uv add <package-name>

# Add a dev dependency
uv add --dev <package-name>
```

## Migration from requirements.txt

The `pyproject.toml` now contains all dependencies. You can safely remove:
- `requirements.txt` (dependencies now in `pyproject.toml`)
- `setup.py` (replaced by `pyproject.toml`)
- `venv/` (uv manages its own venv at `.venv/`)

## Benefits of UV

- **Fast**: 10-100x faster than pip
- **Declarative**: Single source of truth in `pyproject.toml`
- **Lock file**: Automatic `uv.lock` for reproducible builds
- **Auto venv**: Automatically creates and manages virtual environments
- **Compatible**: Works with existing PyPI packages
