# DataWorkflow

A Git-like data versioning system with S3 storage and PostgreSQL tracking, featuring a GitHub-like web interface.

## Setup

```bash
# Install dependencies
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Seed example data
PYTHONPATH=. python scripts/seed_data.py

# Start the server
PYTHONPATH=. python src/app.py
```

Visit http://localhost:5001

## Run Tests

```bash
PYTHONPATH=. pytest
```
