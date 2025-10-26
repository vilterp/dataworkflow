"""Call-based API routes for DataWorkflow distributed execution"""
from flask import Blueprint, jsonify, request, current_app, send_file
from datetime import datetime, timezone
import json
import hashlib
import io
from src.models import StageRun, StageRunStatus, StageFile
from src.models.base import create_session
from src.models.api_schemas import (
    CallInfo, GetCallsResponse, CreateCallResponse,
    StartCallResponse, FinishCallResponse, ErrorResponse,
    StageFileInfo, CreateStageFileResponse, ListStageFilesResponse
)
from src.config import Config

workflows_bp = Blueprint('workflows_api', __name__)


def get_db():
    """Get a database session for API routes."""
    database_url = current_app.config.get('DATABASE_URL', Config.DATABASE_URL)
    debug = current_app.config.get('DEBUG', Config.DEBUG)
    return create_session(database_url, echo=debug)


@workflows_bp.route('/api/calls', methods=['GET'])
def get_pending_calls():
    """
    Get list of pending calls to be picked up by workers.

    Query parameters:
        status: Filter by status (default: 'pending')
        limit: Maximum number of calls to return (default: 100)

    Returns:
        List of call invocations with status 'pending'
    """
    db = get_db()

    try:
        status_filter = request.args.get('status', 'pending')
        limit = int(request.args.get('limit', 100))

        # Map status string to enum
        try:
            status_enum = StageRunStatus[status_filter.upper()]
        except KeyError:
            error = ErrorResponse(error=f'Invalid status: {status_filter}')
            return jsonify(error.model_dump()), 400

        # Query pending calls (stage runs)
        pending_calls = db.query(StageRun).filter(
            StageRun.status == status_enum
        ).order_by(StageRun.created_at).limit(limit).all()

        call_infos = [
            CallInfo(
                invocation_id=str(call.id),
                function_name=call.stage_name,
                parent_invocation_id=str(call.parent_stage_run_id) if call.parent_stage_run_id else None,
                arguments=json.loads(call.arguments) if call.arguments else {},
                repo_name=call.repo_name,
                commit_hash=call.commit_hash,
                workflow_file=call.workflow_file,
                created_at=call.created_at.isoformat(),
                status=call.status.value
            )
            for call in pending_calls
        ]

        response = GetCallsResponse(calls=call_infos)
        return jsonify(response.model_dump()), 200
    finally:
        db.close()


@workflows_bp.route('/api/call', methods=['POST'])
def create_call():
    """
    Create a new call invocation.

    Expected JSON body:
    {
        "caller_id": "parent-invocation-id",  // optional, null for root calls (string of int)
        "function_name": "function_to_call",
        "arguments": {...}  // JSON object of arguments
    }

    Returns:
        {
            "invocation_id": "123"  // string of the DB id
        }
    """
    db = get_db()
    data = request.get_json()

    if not data:
        error = ErrorResponse(error='Request body required')
        return jsonify(error.model_dump()), 400

    if 'function_name' not in data:
        error = ErrorResponse(error='function_name required in request body')
        return jsonify(error.model_dump()), 400

    if 'arguments' not in data:
        error = ErrorResponse(error='arguments required in request body')
        return jsonify(error.model_dump()), 400
    if 'repo_name' not in data:
        error = ErrorResponse(error='repo_name required in request body')
        return jsonify(error.model_dump()), 400
    if 'commit_hash' not in data:
        error = ErrorResponse(error='commit_hash required in request body')
        return jsonify(error.model_dump()), 400
    if 'workflow_file' not in data:
        error = ErrorResponse(error='workflow_file required in request body')
        return jsonify(error.model_dump()), 400

    caller_id = data.get('caller_id')
    function_name = data['function_name']
    arguments = data['arguments']
    repo_name = data['repo_name']
    commit_hash = data['commit_hash']
    workflow_file = data['workflow_file']

    if not isinstance(arguments, dict):
        error = ErrorResponse(error='arguments must be a JSON object')
        return jsonify(error.model_dump()), 400

    try:
        # Serialize arguments deterministically
        args_json = json.dumps(arguments, sort_keys=True, separators=(',', ':'))

        # Compute content-addressable ID
        stage_id = StageRun.compute_id(
            parent_stage_run_id=caller_id,  # caller_id is already a string (hash)
            commit_hash=commit_hash,
            workflow_file=workflow_file,
            stage_name=function_name,
            arguments=args_json
        )

        # Check if this exact invocation already exists
        existing_call = db.query(StageRun).filter(StageRun.id == stage_id).first()
        if existing_call:
            response = CreateCallResponse(
                invocation_id=existing_call.id,
                status=existing_call.status.value,
                created=False
            )
            return jsonify(response.model_dump()), 200

        # Create new call record
        new_call = StageRun(
            id=stage_id,
            parent_stage_run_id=caller_id,
            stage_name=function_name,
            arguments=args_json,
            repo_name=repo_name,
            commit_hash=commit_hash,
            workflow_file=workflow_file,
            status=StageRunStatus.PENDING,
            created_at=datetime.now(timezone.utc)
        )
        db.add(new_call)
        db.commit()

        response = CreateCallResponse(
            invocation_id=new_call.id,
            status='pending',
            created=True
        )
        return jsonify(response.model_dump()), 201
    finally:
        db.close()


@workflows_bp.route('/api/call/<invocation_id>', methods=['GET'])
def get_call_status(invocation_id):
    """
    Get the status and result of a call invocation.

    Returns:
    {
        "invocation_id": "123",
        "function_name": "...",
        "status": "pending" | "running" | "completed" | "failed",
        "result": {...},  // only present if status is completed
        "error": "...",   // only present if status is failed
        "created_at": "...",
        "completed_at": "..."  // only present if completed or failed
    }
    """
    db = get_db()

    try:
        # invocation_id is now a hash (string)
        call = db.query(StageRun).filter(StageRun.id == invocation_id).first()

        if not call:
            error = ErrorResponse(error='Call invocation not found')
            return jsonify(error.model_dump()), 404

        response = CallInfo(
            invocation_id=call.id,
            function_name=call.stage_name,
            parent_invocation_id=call.parent_stage_run_id,
            arguments=json.loads(call.arguments) if call.arguments else {},
            repo_name=call.repo_name,
            commit_hash=call.commit_hash,
            workflow_file=call.workflow_file,
            status=call.status.value,
            created_at=call.created_at.isoformat(),
            started_at=call.started_at.isoformat() if call.started_at else None,
            completed_at=call.completed_at.isoformat() if call.completed_at else None,
            result=json.loads(call.result_value) if call.status == StageRunStatus.COMPLETED and call.result_value else None,
            error=call.error_message if call.status == StageRunStatus.FAILED and call.error_message else None
        )

        return jsonify(response.model_dump(exclude_none=True)), 200
    finally:
        db.close()


@workflows_bp.route('/api/call/<invocation_id>/start', methods=['POST'])
def start_call(invocation_id):
    """
    Mark a call as started (claimed by a worker).

    Expected JSON body:
    {
        "worker_id": "unique-worker-identifier"  // optional
    }
    """
    db = get_db()
    data = request.get_json() or {}

    try:
        # invocation_id is now a hash (string)
        call = db.query(StageRun).filter(StageRun.id == invocation_id).first()

        if not call:
            error = ErrorResponse(error='Call invocation not found')
            return jsonify(error.model_dump()), 404

        if call.status != StageRunStatus.PENDING:
            error = ErrorResponse(error=f'Call is not pending (current status: {call.status.value})')
            return jsonify(error.model_dump()), 409

        # Mark as running
        call.status = StageRunStatus.RUNNING
        call.started_at = datetime.now(timezone.utc)

        # Optionally track worker ID (would need to add this column)
        # call.worker_id = data.get('worker_id')

        db.commit()

        response = StartCallResponse(success=True)
        return jsonify(response.model_dump()), 200
    finally:
        db.close()


@workflows_bp.route('/api/call/<invocation_id>/finish', methods=['POST'])
def finish_call(invocation_id):
    """
    Mark a call as finished with result or error.

    Expected JSON body:
    {
        "status": "completed" | "failed",
        "result": {...},  // required if status is completed
        "error": "..."    // required if status is failed
    }
    """
    db = get_db()
    data = request.get_json()

    if not data:
        error = ErrorResponse(error='Request body required')
        return jsonify(error.model_dump()), 400

    if 'status' not in data:
        error = ErrorResponse(error='status required in request body')
        return jsonify(error.model_dump()), 400

    status_str = data['status']
    if status_str not in ['completed', 'failed']:
        error = ErrorResponse(error='status must be one of: completed, failed')
        return jsonify(error.model_dump()), 400

    try:
        # invocation_id is now a hash (string)
        call = db.query(StageRun).filter(StageRun.id == invocation_id).first()

        if not call:
            error = ErrorResponse(error='Call invocation not found')
            return jsonify(error.model_dump()), 404

        if call.status not in [StageRunStatus.RUNNING, StageRunStatus.PENDING]:
            error = ErrorResponse(error=f'Call is already finished (current status: {call.status.value})')
            return jsonify(error.model_dump()), 409

        # Update status
        call.status = StageRunStatus[status_str.upper()]
        call.completed_at = datetime.now(timezone.utc)

        if status_str == 'completed':
            if 'result' not in data:
                error = ErrorResponse(error='result required for completed status')
                return jsonify(error.model_dump()), 400
            call.result_value = json.dumps(data['result'])

        if status_str == 'failed':
            if 'error' not in data:
                error = ErrorResponse(error='error required for failed status')
                return jsonify(error.model_dump()), 400
            call.error_message = data['error']

        db.commit()

        response = FinishCallResponse(success=True)
        return jsonify(response.model_dump()), 200
    finally:
        db.close()


@workflows_bp.route('/api/stages/<stage_run_id>/files', methods=['POST'])
def create_stage_file(stage_run_id):
    """
    Create a file associated with a stage run.

    This endpoint is called by stages via StageContext.write_file() to upload
    files that should be stored and associated with the stage run.

    Expected multipart form data:
        file: The file content (binary)
        file_path: The logical path for the file (e.g., "output/results.csv")

    Returns:
        {
            "file_id": "...",
            "file_path": "...",
            "size": 1234,
            "content_hash": "..."
        }
    """
    from src.app import get_storage

    db = get_db()

    try:
        # Verify the stage run exists
        stage_run = db.query(StageRun).filter(StageRun.id == stage_run_id).first()
        if not stage_run:
            error = ErrorResponse(error='Stage run not found')
            return jsonify(error.model_dump()), 404

        # Get file from request
        if 'file' not in request.files:
            error = ErrorResponse(error='file required in request')
            return jsonify(error.model_dump()), 400

        file = request.files['file']
        file_path = request.form.get('file_path')

        if not file_path:
            error = ErrorResponse(error='file_path required in request')
            return jsonify(error.model_dump()), 400

        # Read file content
        content = file.read()
        size = len(content)

        # Compute content hash
        content_hash = hashlib.sha256(content).hexdigest()

        # Store the file using the storage backend
        storage = get_storage()
        _, storage_key, _ = storage.store(content)

        # Compute stage file ID
        stage_file_id = StageFile.compute_id(stage_run_id, file_path)

        # Check if this file already exists
        existing_file = db.query(StageFile).filter(StageFile.id == stage_file_id).first()
        if existing_file:
            # Update existing file
            existing_file.content_hash = content_hash
            existing_file.storage_key = storage_key
            existing_file.size = size
            db.commit()

            response = CreateStageFileResponse(
                file_id=existing_file.id,
                file_path=existing_file.file_path,
                size=existing_file.size,
                content_hash=existing_file.content_hash,
                updated=True
            )
            return jsonify(response.model_dump()), 200

        # Create new stage file record
        stage_file = StageFile(
            id=stage_file_id,
            stage_run_id=stage_run_id,
            file_path=file_path,
            content_hash=content_hash,
            storage_key=storage_key,
            size=size,
            created_at=datetime.now(timezone.utc)
        )
        db.add(stage_file)
        db.commit()

        response = CreateStageFileResponse(
            file_id=stage_file.id,
            file_path=stage_file.file_path,
            size=stage_file.size,
            content_hash=stage_file.content_hash,
            created=True
        )
        return jsonify(response.model_dump()), 201

    finally:
        db.close()


@workflows_bp.route('/api/stages/<stage_run_id>/files', methods=['GET'])
def list_stage_files(stage_run_id):
    """
    List all files created by a stage run.

    Returns:
        {
            "files": [
                {
                    "id": "...",
                    "file_path": "...",
                    "size": 1234,
                    "content_hash": "...",
                    "created_at": "..."
                },
                ...
            ]
        }
    """
    db = get_db()

    try:
        # Verify the stage run exists
        stage_run = db.query(StageRun).filter(StageRun.id == stage_run_id).first()
        if not stage_run:
            error = ErrorResponse(error='Stage run not found')
            return jsonify(error.model_dump()), 404

        # Get all files for this stage run
        files = db.query(StageFile).filter(
            StageFile.stage_run_id == stage_run_id
        ).order_by(StageFile.created_at).all()

        file_infos = [
            StageFileInfo(
                id=f.id,
                file_path=f.file_path,
                size=f.size,
                content_hash=f.content_hash,
                created_at=f.created_at.isoformat()
            )
            for f in files
        ]

        response = ListStageFilesResponse(files=file_infos)
        return jsonify(response.model_dump()), 200

    finally:
        db.close()


@workflows_bp.route('/api/stage-files/<file_id>/download', methods=['GET'])
def download_stage_file(file_id):
    """
    Download a file created by a stage run.

    Returns the file content as a binary stream.
    """
    from src.app import get_storage

    db = get_db()

    try:
        # Get the stage file
        stage_file = db.query(StageFile).filter(StageFile.id == file_id).first()
        if not stage_file:
            error = ErrorResponse(error='Stage file not found')
            return jsonify(error.model_dump()), 404

        # Retrieve file content from storage
        storage = get_storage()
        content = storage.retrieve(stage_file.content_hash)

        if content is None:
            error = ErrorResponse(error='File content not found in storage')
            return jsonify(error.model_dump()), 404

        # Return file as binary stream
        return send_file(
            io.BytesIO(content),
            mimetype='application/octet-stream',
            as_attachment=True,
            download_name=stage_file.file_path.split('/')[-1]
        )

    finally:
        db.close()


