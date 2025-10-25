# Distributed Execution System

## Overview

The workflow system has been overhauled to support distributed execution, where each stage invocation can potentially run in a separate Python process.

## Architecture Changes

### Previous Model (Legacy)
- Workflows executed in a single process
- `WorkflowRunner` managed the entire workflow lifecycle
- Decorator sent messages to control plane just for tracking
- Parent-child relationships tracked via `StageContext` call stack

### New Model (Distributed)
- Each stage invocation runs as an independent "call"
- When a stage invokes another stage, it sends a request to the control plane
- Workers poll for pending calls and execute them
- Caller blocks and polls for completion

## API Endpoints

### `GET /api/calls?status=pending`
Get list of pending calls waiting to be executed.

**Response:**
```json
{
  "calls": [
    {
      "invocation_id": "123",
      "function_name": "extract_data",
      "parent_invocation_id": "122",
      "arguments": {"args": [], "kwargs": {}},
      "created_at": "2025-01-15T10:30:00Z",
      "status": "pending"
    }
  ]
}
```

### `POST /api/call`
Create a new call invocation.

**Request:**
```json
{
  "caller_id": "122",
  "function_name": "extract_data",
  "arguments": {"args": [], "kwargs": {}}
}
```

**Response:**
```json
{
  "invocation_id": "123",
  "status": "pending",
  "created": true
}
```

### `GET /api/call/<invocation_id>`
Get status and result of a call.

**Response:**
```json
{
  "invocation_id": "123",
  "function_name": "extract_data",
  "status": "completed",
  "result": [1, 2, 3, 4, 5],
  "created_at": "2025-01-15T10:30:00Z",
  "started_at": "2025-01-15T10:30:01Z",
  "completed_at": "2025-01-15T10:30:05Z"
}
```

### `POST /api/call/<invocation_id>/start`
Worker claims and starts executing a call.

**Request:**
```json
{
  "worker_id": "worker-abc123"
}
```

### `POST /api/call/<invocation_id>/finish`
Worker reports call completion.

**Request:**
```json
{
  "status": "completed",
  "result": [1, 2, 3, 4, 5]
}
```

Or for failures:
```json
{
  "status": "failed",
  "error": "ValueError: Invalid data\n<traceback>"
}
```

## Execution Flow

### 1. Decorator Behavior

When a `@stage` decorated function is called:

```python
@stage
def extract_data():
    return [1, 2, 3]

# When called:
result = extract_data()
```

The decorator:
1. Serializes arguments to JSON: `{"args": [], "kwargs": {}}`
2. Gets current invocation context (caller_id)
3. POSTs to `/api/call` with function name, arguments, and caller_id
4. Receives back an `invocation_id`
5. Sets this as the current invocation ID (for nested calls)
6. Polls `GET /api/call/<invocation_id>` until status is `completed` or `failed`
7. Returns the result value
8. Restores previous invocation context

### 2. Worker Behavior

The `CallWorker` process:
1. Loads the workflow module once at startup
2. Continuously polls `GET /api/calls?status=pending`
3. When a call is found:
   - POSTs to `/api/call/<id>/start` to claim it
   - Extracts function name and arguments
   - Gets the unwrapped function from the module
   - Executes: `func(*args, **kwargs)`
   - POSTs result to `/api/call/<id>/finish` with status `completed` or `failed`

### 3. Parent-Child Tracking

Parent-child relationships are maintained through:
- `parent_stage_run_id` field in database (foreign key to `stage_runs.id`)
- When creating a call, the `caller_id` is set to the current invocation's `id`
- This forms a tree structure in the database

Example:
```
main (id=1, parent=NULL)
  └─ extract_data (id=2, parent=1)
  └─ transform_data (id=3, parent=1)
  └─ load_data (id=4, parent=1)
```

## Database Schema

### `stage_runs` Table

Now supports both legacy workflow mode and new distributed mode:

```sql
CREATE TABLE stage_runs (
    id INTEGER PRIMARY KEY,
    workflow_run_id INTEGER NULL,              -- NULL for distributed mode
    parent_stage_run_id INTEGER NULL,          -- Parent call's ID
    stage_name VARCHAR(255) NOT NULL,
    arguments TEXT NULL,                        -- JSON: {"args": [...], "kwargs": {...}}
    status VARCHAR(20) NOT NULL,               -- pending/running/completed/failed
    started_at DATETIME NULL,
    completed_at DATETIME NULL,
    result_value TEXT NULL,                    -- JSON result
    error_message TEXT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    FOREIGN KEY (workflow_run_id) REFERENCES workflow_runs(id),
    FOREIGN KEY (parent_stage_run_id) REFERENCES stage_runs(id)
);
```

**Key points:**
- `id` serves as the invocation ID (no separate hash-based ID needed)
- `workflow_run_id` is NULL for distributed calls (no parent workflow concept)
- `arguments` stores serialized function arguments
- `parent_stage_run_id` tracks the calling invocation

## Running the System

### 1. Run Migration

```bash
python scripts/migrate_add_invocation_fields.py
```

### 2. Start Control Plane

```bash
python src/app.py
```

### 3. Start Workers

```bash
python -m sdk.worker \
  --server-url http://localhost:5001 \
  --repo-name my-repo \
  --workflow-file workflows/example.py \
  --commit-hash abc123def456
```

You can start multiple workers for parallel execution.

### 4. Configure and Run Workflows

In your workflow code:

```python
from sdk.decorators import stage, set_control_plane_url

# Configure control plane URL
set_control_plane_url('http://localhost:5001')

@stage
def main():
    data = extract_data()
    result = transform_data(data)
    return result

@stage
def extract_data():
    return [1, 2, 3]

@stage
def transform_data(data):
    return [x * 2 for x in data]

if __name__ == '__main__':
    # Execute the workflow
    result = main()
    print(f"Result: {result}")
```

## Standalone Mode

If no control plane is configured, stages execute locally (backward compatible):

```python
from sdk.decorators import stage

# No set_control_plane_url() call

@stage
def process():
    return "result"

# Executes directly, no distributed execution
result = process()
```

## Migration Path

1. **Immediate:** Run the database migration
2. **Compatible:** Old `WorkflowRunner` still works with legacy `workflow_runs` API
3. **Gradual:** New workflows can use distributed mode with `CallWorker`
4. **Future:** Deprecate old `WorkflowRunner` and workflow-centric APIs

## Advantages

- **Scalability:** Each invocation can run on different machines
- **Isolation:** Failures in one call don't crash entire workflow
- **Flexibility:** Workers can be dynamically added/removed
- **Visibility:** Full audit trail of all function calls
- **Debugging:** Can inspect and retry individual calls

## Limitations

- **Latency:** Polling introduces delays (configurable poll interval)
- **Serialization:** All arguments and results must be JSON-serializable
- **State:** No shared state between calls (must pass everything via args)
- **Networking:** Requires reliable network connection to control plane
