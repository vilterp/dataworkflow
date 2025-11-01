# DataWorkflow Workflow Engine

The DataWorkflow workflow engine allows you to define and execute data processing workflows that are versioned in your repository.

## Architecture

- **Workflow Source Code**: Python files stored in your repository (e.g., `examples/example_workflow.py`)
- **Workflow Runner**: Separate process (SDK) that polls for pending workflows and executes them
- **Main Server**: Flask app that remains stateless, tracks execution state in PostgreSQL/SQLite
- **Communication**: Runner communicates with server via REST API

## Workflow Structure

Every workflow must define a `main()` function that serves as the entry point:

```python
def main():
    """Entry point for the workflow."""
    # Your workflow logic here
    data = extract_data()
    transformed = transform_data(data)
    load_data(transformed)

    return {"status": "success"}

def extract_data():
    # ... extract logic
    pass

def transform_data(data):
    # ... transform logic
    pass

def load_data(data):
    # ... load logic
    pass
```

## Database Models

### WorkflowRun
Tracks a workflow execution:
- `workflow_file`: Path to the workflow file in the repo
- `commit_hash`: Specific commit to run the workflow from
- `status`: PENDING → CLAIMED → RUNNING → COMPLETED/FAILED
- `runner_id`: ID of the runner executing this workflow

### StageRun
Tracks execution of individual stages within a workflow:
- `stage_name`: Name of the stage function
- `parent_stage_run_id`: Optional parent stage (for nested execution)
- `status`: PENDING → RUNNING → COMPLETED/FAILED
- `result_value`: JSON-encoded result from the stage
- `error_message`: Error details if failed

## API Endpoints

- `GET /api/workflows/pending?repo_name=<repo>` - Get workflows awaiting execution
- `POST /api/workflows/<id>/claim` - Claim a workflow for execution
- `POST /api/workflows/<id>/start` - Mark workflow as started
- `POST /api/workflows/<id>/stages/<name>/start` - Mark stage as started
- `POST /api/workflows/<id>/stages/<name>/finish` - Mark stage as completed
- `POST /api/workflows/<id>/finish` - Mark workflow as completed
- `GET /api/repos/<repo>/blob/<commit>/<path>` - Download workflow source code

## Quick Start

### 1. Run Database Migration

```bash
python scripts/migrate_add_workflows.py
```

### 2. Create a Workflow File

Create `examples/my_workflow.py`:

```python
def main():
    print("Hello from my workflow!")
    return {"status": "success"}
```

### 3. Commit the Workflow to Your Repository

Use the DataWorkflow UI or API to commit your workflow file to a repository.

### 4. Create a Workflow Run

```python
from src.models import WorkflowRun, WorkflowStatus
from src.models.base import create_session
from src.config import Config

db = create_session(Config.DATABASE_URL)

workflow_run = WorkflowRun(
    repository_id=1,  # Your repository ID
    workflow_file='examples/my_workflow.py',
    commit_hash='abc123...',  # Commit containing the workflow
    status=WorkflowStatus.PENDING,
    triggered_by='manual',
    trigger_event='manual'
)
db.add(workflow_run)
db.commit()
db.close()
```

Or use the test script:

```bash
python tests/test_create_workflow_run.py
```

### 5. Start the Workflow Runner

Make sure the Flask server is running:

```bash
python src/app.py
```

In another terminal, start the workflow runner:

```bash
python sdk/run_workflows.py --server http://localhost:5001 --repo test-repo
```

The runner will:
1. Poll for pending workflows
2. Claim and download the workflow file
3. Execute the `main()` function
4. Report results back to the server

## SDK Usage

The SDK provides a `WorkflowRunner` class that can be used programmatically:

```python
from sdk.runner import WorkflowRunner
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create and start runner
runner = WorkflowRunner(
    server_url='http://localhost:5001',
    repo_name='my-repo',
    runner_id='my-runner',  # Optional, auto-generated if not provided
    poll_interval=5  # Poll every 5 seconds
)

runner.start()  # Blocks and runs forever
```

## Decorator Usage (Optional)

While not required, you can use the `@stage` decorator to mark functions as stages:

```python
from sdk import stage

@stage()
def extract():
    return load_data()

@stage()
def transform(data):
    return process(data)

def main():
    data = extract()
    result = transform(data)
    return result
```

Note: The decorator is currently for documentation purposes only. The runner will only execute `main()`.

## Example Workflow

See [examples/example_workflow.py](examples/example_workflow.py) for a complete example that demonstrates:
- Data extraction
- Data transformation
- Data loading
- Returning results

## Future Enhancements

- **Docker/K8s sandboxing**: Run workflows in isolated containers
- **Parallel stage execution**: Execute independent stages concurrently
- **Workflow scheduling**: Trigger workflows on a schedule
- **Workflow dependencies**: Chain workflows together
- **UI for monitoring**: View workflow runs and stage execution in the web interface
