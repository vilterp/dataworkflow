# Workflow System Overhaul - Summary

## Overview

The workflow system has been completely overhauled to support distributed execution where each stage invocation can run in a separate Python process. Previously, workflows executed within a single process with decorators just sending tracking messages. Now, when one stage invokes another, it sends a request to the control plane service, which creates a new invocation that workers can pick up and execute.

## Files Modified

### 1. `/src/routes/workflows.py` - NEW CALL-BASED API

**Previous:** Workflow-centric endpoints for managing `WorkflowRun` lifecycle
**Now:** Call-centric endpoints for managing individual function invocations

New endpoints:
- `GET /api/calls?status=pending` - List pending calls for workers to pick up
- `POST /api/call` - Create new invocation (returns invocation ID)
- `GET /api/call/<id>` - Get call status and result
- `POST /api/call/<id>/start` - Worker claims/starts a call
- `POST /api/call/<id>/finish` - Worker reports completion/failure

The API uses the existing `stage_runs` table, treating its `id` field as the invocation ID.

### 2. `/src/models/workflow.py` - UPDATED STAGERUN MODEL

**Changes:**
- Added `arguments` TEXT field to store JSON-serialized function arguments
- Made `workflow_run_id` nullable (not needed for distributed calls)
- `id` serves as the invocation identifier
- `parent_stage_run_id` tracks parent-child relationships

The model now supports both:
- Legacy workflow-based execution (with `workflow_run_id`)
- New distributed call-based execution (`workflow_run_id` can be NULL)

### 3. `/sdk/decorators.py` - COMPLETELY REWRITTEN DECORATOR

**Previous behavior:**
- Checked for runner context
- Routed through `_runner_execute_stage` if in workflow
- Otherwise executed directly

**New behavior:**
- Checks if control plane URL is configured
- If yes: Creates call via `POST /api/call`, polls for completion, returns result
- If no: Executes directly (standalone mode, backward compatible)
- Maintains invocation context for nested calls via global `_current_invocation_id`

Key functions added:
- `set_control_plane_url(url)` - Configure control plane
- `create_call(function_name, arguments, caller_id)` - Create invocation
- `poll_call_status(invocation_id)` - Wait for completion
- `set_current_invocation_id()` / `get_current_invocation_id()` - Track context

### 4. `/sdk/worker.py` - NEW WORKER PROCESS

**New file:** Replaces/augments the old `WorkflowRunner` for distributed mode

Key features:
- Polls `GET /api/calls?status=pending` for work
- Loads workflow module once at startup
- For each call:
  - Claims via `POST /api/call/<id>/start`
  - Executes the function directly (not through decorator)
  - Reports result via `POST /api/call/<id>/finish`
- Can run multiple workers for parallelism

Command-line interface:
```bash
python -m sdk.worker \
  --server-url http://localhost:5001 \
  --repo-name my-repo \
  --workflow-file workflows/example.py \
  --commit-hash abc123 \
  --poll-interval 2
```

### 5. `/scripts/migrate_add_invocation_fields.py` - DATABASE MIGRATION

**New file:** SQL migration script

Changes:
- Adds `arguments` TEXT column to `stage_runs`
- Makes `workflow_run_id` nullable
- No indexes needed (uses existing ID and foreign key)

Run with: `python scripts/migrate_add_invocation_fields.py`

### 6. `/src/models/api_schemas.py` - API PAYLOAD MODELS

**New file:** Pydantic models for type safety and validation

Models defined:
- `CreateCallRequest` / `CreateCallResponse` - Call creation
- `CallInfo` - Call status/metadata
- `GetCallsResponse` - List of calls
- `StartCallRequest` / `StartCallResponse` - Claiming calls
- `FinishCallRequest` / `FinishCallResponse` - Reporting results
- `ErrorResponse` - Standard error format

### 7. `/examples/distributed_workflow.py` - EXAMPLE USAGE

**New file:** Demonstrates the distributed execution model

Shows:
- How to configure control plane URL
- How stages invoke each other via control plane
- ETL pipeline with extract/transform/load stages
- Parent-child call relationships

## Execution Flow

### Old Flow (Legacy)
1. `WorkflowRunner` polls for `WorkflowRun`
2. Claims and downloads workflow file
3. Executes `main()` in same process
4. When `@stage` function called, decorator routes through runner's `_execute_stage`
5. Runner creates `StageRun` record and executes inline
6. Uses call stack (`StageContext`) to track parent-child relationships

### New Flow (Distributed)

**From workflow code:**
1. Call `set_control_plane_url('http://localhost:5001')`
2. Invoke `main()` (or any `@stage` function)
3. Decorator POSTs to `/api/call` with function name and arguments
4. Control plane creates `StageRun` record with status `PENDING`
5. Decorator polls `/api/call/<id>` until complete
6. Returns result to caller

**From worker:**
1. Worker polls `/api/calls?status=pending`
2. Finds pending call, POSTs to `/api/call/<id>/start` to claim
3. Extracts function from pre-loaded workflow module
4. Executes function directly: `func(*args, **kwargs)`
5. POSTs result to `/api/call/<id>/finish`

## Key Architectural Decisions

### 1. Use Existing `id` Field as Invocation ID
**Decision:** Don't add a separate `invocation_id` hash field. Use the existing auto-increment `id`.
**Rationale:** Simpler schema, uses existing indexes, ID-as-string is sufficient for API

### 2. Nullable `workflow_run_id`
**Decision:** Make `workflow_run_id` nullable for distributed calls
**Rationale:** Distributed calls don't belong to a parent "workflow run" - they're independent invocations

### 3. Arguments Stored as JSON
**Decision:** Store arguments as `{"args": [...], "kwargs": {...}}` JSON in TEXT column
**Rationale:** Flexible, no schema changes for different function signatures

### 4. Parent Tracking via `parent_stage_run_id`
**Decision:** Reuse existing `parent_stage_run_id` foreign key
**Rationale:** Already indexed, supports tree queries, no new columns needed

### 5. Polling for Completion
**Decision:** Caller polls control plane until call completes
**Rationale:** Simpler than webhooks/callbacks, works across processes, acceptable latency

### 6. Workers Load Module Once
**Decision:** Worker loads workflow module at startup, reuses for all calls
**Rationale:** Faster execution, avoids repeated downloads/imports

### 7. Execute Unwrapped Functions
**Decision:** Workers execute the original function, not the decorated wrapper
**Rationale:** Avoids infinite loop (decorator would create another call)

## Migration Path

### Phase 1: Database Migration (REQUIRED)
```bash
python scripts/migrate_add_invocation_fields.py
```

### Phase 2: Both Models Coexist (CURRENT)
- Old `WorkflowRunner` + workflow-centric API still works
- New `CallWorker` + call-centric API available
- Choose per workflow which model to use

### Phase 3: Gradual Adoption
- New workflows use distributed mode
- Existing workflows continue with legacy mode
- Monitor and tune distributed system

### Phase 4: Deprecation (FUTURE)
- Remove old `WorkflowRunner`
- Remove workflow-centric API endpoints
- All execution through call-based API

## Usage Examples

### Standalone Mode (No Distribution)
```python
from sdk.decorators import stage

@stage
def main():
    return extract_data()

@stage
def extract_data():
    return [1, 2, 3]

# Just run it
result = main()  # Executes directly, no control plane needed
```

### Distributed Mode
```python
from sdk.decorators import stage, set_control_plane_url

set_control_plane_url('http://localhost:5001')

@stage
def main():
    return extract_data()  # POSTs to control plane, waits for worker

@stage
def extract_data():
    return [1, 2, 3]

# Start workers first:
# python -m sdk.worker --server-url http://localhost:5001 \
#   --repo-name my-repo --workflow-file workflow.py --commit-hash abc123

# Then run:
result = main()  # Distributed execution
```

## Benefits

1. **Scalability:** Run many workers, scale horizontally
2. **Isolation:** Failures don't crash entire workflow
3. **Flexibility:** Add/remove workers dynamically
4. **Observability:** Full audit trail of all calls
5. **Debugging:** Inspect and retry individual calls
6. **Resource Management:** Different calls can run on different machines with different resources

## Limitations

1. **Latency:** Polling adds overhead (~0.5-2s per call)
2. **Serialization:** All args/results must be JSON-serializable
3. **Statelessness:** No shared memory between calls
4. **Network Dependency:** Requires stable connection to control plane
5. **Complexity:** More moving parts to manage

## Testing

The existing test suite (`test_workflow_integration.py`) tests the **legacy** workflow mode. New tests should be added for:
- Call creation and status checking
- Worker execution
- Parent-child relationship tracking
- Error handling in distributed mode
- Polling and timeout behavior

## Next Steps

1. ✅ Run database migration
2. ✅ Test new API endpoints manually
3. ⬜ Update integration tests for distributed mode
4. ⬜ Add Pydantic validation to API routes
5. ⬜ Performance testing and optimization
6. ⬜ Add metrics/monitoring for distributed execution
7. ⬜ Document deployment patterns (how many workers, etc.)
8. ⬜ Handle edge cases (worker crashes, network failures, etc.)
