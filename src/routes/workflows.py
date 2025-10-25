"""Call-based API routes for DataWorkflow distributed execution"""
from flask import Blueprint, jsonify, request, current_app
from datetime import datetime, timezone
import json
from src.models import StageRun, StageRunStatus
from src.models.base import create_session
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
            return jsonify({'error': f'Invalid status: {status_filter}'}), 400

        # Query pending calls (stage runs)
        pending_calls = db.query(StageRun).filter(
            StageRun.status == status_enum
        ).order_by(StageRun.created_at).limit(limit).all()

        result = []
        for call in pending_calls:
            result.append({
                'invocation_id': str(call.id),
                'function_name': call.stage_name,
                'parent_invocation_id': str(call.parent_stage_run_id) if call.parent_stage_run_id else None,
                'arguments': json.loads(call.arguments) if call.arguments else {},
                'created_at': call.created_at.isoformat(),
                'status': call.status.value
            })

        return jsonify({'calls': result}), 200
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
        return jsonify({'error': 'Request body required'}), 400

    if 'function_name' not in data:
        return jsonify({'error': 'function_name required in request body'}), 400

    if 'arguments' not in data:
        return jsonify({'error': 'arguments required in request body'}), 400

    caller_id = data.get('caller_id')
    function_name = data['function_name']
    arguments = data['arguments']

    if not isinstance(arguments, dict):
        return jsonify({'error': 'arguments must be a JSON object'}), 400

    try:
        # Convert caller_id string to int if provided
        parent_stage_run_id = int(caller_id) if caller_id else None

        # Create new call record
        new_call = StageRun(
            parent_stage_run_id=parent_stage_run_id,
            stage_name=function_name,
            arguments=json.dumps(arguments),
            status=StageRunStatus.PENDING,
            created_at=datetime.now(timezone.utc)
        )
        db.add(new_call)
        db.commit()
        db.refresh(new_call)  # Get the auto-generated ID

        return jsonify({
            'invocation_id': str(new_call.id),
            'status': 'pending',
            'created': True
        }), 201
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
        # Convert invocation_id string to int
        call_id = int(invocation_id)
        call = db.query(StageRun).filter(StageRun.id == call_id).first()

        if not call:
            return jsonify({'error': 'Call invocation not found'}), 404

        response = {
            'invocation_id': str(call.id),
            'function_name': call.stage_name,
            'status': call.status.value,
            'created_at': call.created_at.isoformat()
        }

        if call.status == StageRunStatus.COMPLETED and call.result_value:
            response['result'] = json.loads(call.result_value)

        if call.status == StageRunStatus.FAILED and call.error_message:
            response['error'] = call.error_message

        if call.completed_at:
            response['completed_at'] = call.completed_at.isoformat()

        if call.started_at:
            response['started_at'] = call.started_at.isoformat()

        return jsonify(response), 200
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
        call_id = int(invocation_id)
        call = db.query(StageRun).filter(StageRun.id == call_id).first()

        if not call:
            return jsonify({'error': 'Call invocation not found'}), 404

        if call.status != StageRunStatus.PENDING:
            return jsonify({
                'error': f'Call is not pending (current status: {call.status.value})'
            }), 409

        # Mark as running
        call.status = StageRunStatus.RUNNING
        call.started_at = datetime.now(timezone.utc)

        # Optionally track worker ID (would need to add this column)
        # call.worker_id = data.get('worker_id')

        db.commit()

        return jsonify({'success': True}), 200
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
        return jsonify({'error': 'Request body required'}), 400

    if 'status' not in data:
        return jsonify({'error': 'status required in request body'}), 400

    status_str = data['status']
    if status_str not in ['completed', 'failed']:
        return jsonify({'error': 'status must be one of: completed, failed'}), 400

    try:
        call_id = int(invocation_id)
        call = db.query(StageRun).filter(StageRun.id == call_id).first()

        if not call:
            return jsonify({'error': 'Call invocation not found'}), 404

        if call.status not in [StageRunStatus.RUNNING, StageRunStatus.PENDING]:
            return jsonify({
                'error': f'Call is already finished (current status: {call.status.value})'
            }), 409

        # Update status
        call.status = StageRunStatus[status_str.upper()]
        call.completed_at = datetime.now(timezone.utc)

        if status_str == 'completed':
            if 'result' not in data:
                return jsonify({'error': 'result required for completed status'}), 400
            call.result_value = json.dumps(data['result'])

        if status_str == 'failed':
            if 'error' not in data:
                return jsonify({'error': 'error required for failed status'}), 400
            call.error_message = data['error']

        db.commit()

        return jsonify({'success': True}), 200
    finally:
        db.close()


