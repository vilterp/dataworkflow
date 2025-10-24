"""Workflow API routes for DataWorkflow"""
from flask import Blueprint, jsonify, request, current_app
from datetime import datetime, timezone
from src.models import WorkflowRun, StageRun, WorkflowStatus, StageRunStatus
from src.models.base import create_session
from src.config import Config

workflows_bp = Blueprint('workflows_api', __name__)


def get_db():
    """Get a database session for API routes."""
    database_url = current_app.config.get('DATABASE_URL', Config.DATABASE_URL)
    debug = current_app.config.get('DEBUG', Config.DEBUG)
    return create_session(database_url, echo=debug)


@workflows_bp.route('/api/workflows/pending', methods=['GET'])
def get_pending_workflows():
    """
    Get list of workflows that need to be run.

    Returns pending workflows that haven't been claimed or have been
    claimed but not started within a timeout period.
    """
    from src.app import get_repository

    # For now, we'll accept repo_name as a query parameter
    # Later this could be extended to handle multiple repos
    repo_name = request.args.get('repo_name')

    if not repo_name:
        return jsonify({'error': 'repo_name parameter required'}), 400

    repo, db = get_repository(repo_name)
    if not repo:
        return jsonify({'error': f'Repository {repo_name} not found'}), 404

    try:
        # Get pending workflows (not claimed or stale claims)
        # A claim is considered stale if it's been more than 5 minutes without starting
        stale_threshold = datetime.now(timezone.utc)

        pending_workflows = db.query(WorkflowRun).filter(
            WorkflowRun.repository_id == repo.repository_id,
            WorkflowRun.status == WorkflowStatus.PENDING
        ).order_by(WorkflowRun.created_at).all()

        result = []
        for workflow in pending_workflows:
            result.append({
                'id': workflow.id,
                'workflow_file': workflow.workflow_file,
                'commit_hash': workflow.commit_hash,
                'created_at': workflow.created_at.isoformat(),
                'triggered_by': workflow.triggered_by,
                'trigger_event': workflow.trigger_event
            })

        return jsonify({'workflows': result}), 200
    finally:
        db.close()


@workflows_bp.route('/api/workflows/<int:workflow_id>/claim', methods=['POST'])
def claim_workflow(workflow_id):
    """
    Claim a workflow for execution by a runner.

    Expected JSON body:
    {
        "runner_id": "unique-runner-identifier"
    }
    """
    from src.app import get_repository

    data = request.get_json()
    if not data or 'runner_id' not in data:
        return jsonify({'error': 'runner_id required in request body'}), 400

    runner_id = data['runner_id']
    db = get_db()

    try:
        workflow = db.query(WorkflowRun).filter(WorkflowRun.id == workflow_id).first()

        if not workflow:
            return jsonify({'error': 'Workflow not found'}), 404

        if workflow.status != WorkflowStatus.PENDING:
            return jsonify({'error': f'Workflow is not pending (current status: {workflow.status.value})'}), 409

        # Claim the workflow
        workflow.status = WorkflowStatus.CLAIMED
        workflow.runner_id = runner_id
        workflow.claimed_at = datetime.now(timezone.utc)
        db.commit()

        return jsonify({
            'success': True,
            'workflow_id': workflow.id,
            'workflow_file': workflow.workflow_file,
            'commit_hash': workflow.commit_hash
        }), 200
    finally:
        db.close()


@workflows_bp.route('/api/workflows/<int:workflow_id>/start', methods=['POST'])
def start_workflow(workflow_id):
    """
    Mark a workflow as started.

    Called by runner when it begins executing the workflow.
    """
    db = get_db()

    try:
        workflow = db.query(WorkflowRun).filter(WorkflowRun.id == workflow_id).first()

        if not workflow:
            return jsonify({'error': 'Workflow not found'}), 404

        workflow.status = WorkflowStatus.RUNNING
        workflow.started_at = datetime.now(timezone.utc)
        db.commit()

        return jsonify({'success': True}), 200
    finally:
        db.close()


@workflows_bp.route('/api/workflows/<int:workflow_id>/stages/<stage_name>/start', methods=['POST'])
def stage_started(workflow_id, stage_name):
    """
    Mark a stage as started within a workflow run.

    Expected JSON body:
    {
        "parent_stage_run_id": 123  // optional, ID of parent stage that invoked this
    }
    """
    db = get_db()
    data = request.get_json() or {}

    try:
        workflow = db.query(WorkflowRun).filter(WorkflowRun.id == workflow_id).first()

        if not workflow:
            return jsonify({'error': 'Workflow not found'}), 404

        parent_stage_run_id = data.get('parent_stage_run_id')

        # Create new stage run
        stage_run = StageRun(
            workflow_run_id=workflow_id,
            parent_stage_run_id=parent_stage_run_id,
            stage_name=stage_name,
            status=StageRunStatus.RUNNING,
            started_at=datetime.now(timezone.utc)
        )
        db.add(stage_run)
        db.commit()

        return jsonify({
            'success': True,
            'stage_run_id': stage_run.id
        }), 200
    finally:
        db.close()


@workflows_bp.route('/api/workflows/<int:workflow_id>/stages/<stage_name>/finish', methods=['POST'])
def stage_finished(workflow_id, stage_name):
    """
    Mark a stage as finished within a workflow run.

    Expected JSON body:
    {
        "status": "completed" | "failed",
        "result_value": "any JSON-serializable value",  // optional
        "error_message": "error details"  // required if status is failed
    }
    """
    db = get_db()
    data = request.get_json()
    if not data or 'status' not in data:
        return jsonify({'error': 'status required in request body'}), 400

    status_str = data['status']
    if status_str not in ['completed', 'failed', 'skipped']:
        return jsonify({'error': 'status must be one of: completed, failed, skipped'}), 400

    try:
        stage_run = db.query(StageRun).filter(
            StageRun.workflow_run_id == workflow_id,
            StageRun.stage_name == stage_name
        ).first()

        if not stage_run:
            return jsonify({'error': 'Stage run not found'}), 404

        # Update stage run
        stage_run.status = StageRunStatus[status_str.upper()]
        stage_run.completed_at = datetime.now(timezone.utc)

        if 'result_value' in data:
            import json
            stage_run.result_value = json.dumps(data['result_value'])

        if 'error_message' in data:
            stage_run.error_message = data['error_message']

        db.commit()

        return jsonify({'success': True}), 200
    finally:
        db.close()


@workflows_bp.route('/api/workflows/<int:workflow_id>/finish', methods=['POST'])
def finish_workflow(workflow_id):
    """
    Mark a workflow as finished.

    Expected JSON body:
    {
        "status": "completed" | "failed" | "cancelled",
        "error_message": "error details"  // optional, for failed status
    }
    """
    db = get_db()
    data = request.get_json()
    if not data or 'status' not in data:
        return jsonify({'error': 'status required in request body'}), 400

    status_str = data['status']
    if status_str not in ['completed', 'failed', 'cancelled']:
        return jsonify({'error': 'status must be one of: completed, failed, cancelled'}), 400

    try:
        workflow = db.query(WorkflowRun).filter(WorkflowRun.id == workflow_id).first()

        if not workflow:
            return jsonify({'error': 'Workflow not found'}), 404

        workflow.status = WorkflowStatus[status_str.upper()]
        workflow.completed_at = datetime.now(timezone.utc)

        if 'error_message' in data:
            workflow.error_message = data['error_message']

        db.commit()

        return jsonify({'success': True}), 200
    finally:
        db.close()


@workflows_bp.route('/api/repos/<repo_name>/blob/<commit_hash>/<path:file_path>')
def get_file_content(repo_name, commit_hash, file_path):
    """
    Get raw file content from a specific commit.

    Used by workflow runners to download workflow source code.
    """
    from src.app import get_repository
    from flask import Response

    repo, db = get_repository(repo_name)
    if not repo:
        return jsonify({'error': f'Repository {repo_name} not found'}), 404

    try:
        # Get the commit
        commit = repo.get_commit(commit_hash)
        if not commit:
            return jsonify({'error': 'Commit not found'}), 404

        # Navigate to the file through the tree
        path_parts = file_path.split('/')
        current_tree_hash = commit.tree_hash

        # Navigate through directories
        for i, part in enumerate(path_parts[:-1]):
            tree_entries = repo.get_tree_contents(current_tree_hash)
            found = False
            for entry in tree_entries:
                if entry.name == part and entry.type.value == 'tree':
                    current_tree_hash = entry.hash
                    found = True
                    break
            if not found:
                return jsonify({'error': f'Path not found: {"/".join(path_parts[:i+1])}'}), 404

        # Find the file in the final directory
        tree_entries = repo.get_tree_contents(current_tree_hash)
        file_name = path_parts[-1]
        blob_hash = None
        for entry in tree_entries:
            if entry.name == file_name and entry.type.value == 'blob':
                blob_hash = entry.hash
                break

        if not blob_hash:
            return jsonify({'error': f'File not found: {file_path}'}), 404

        # Get blob content
        blob = repo.get_blob(blob_hash)
        if not blob:
            return jsonify({'error': 'Blob not found'}), 404

        # Return raw content
        return Response(blob.content, mimetype='application/octet-stream')
    finally:
        db.close()
