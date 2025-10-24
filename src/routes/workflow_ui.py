"""Workflow UI routes for DataWorkflow"""
from flask import Blueprint, render_template, redirect, url_for, flash, request
from src.models import WorkflowRun, StageRun, WorkflowStatus, StageRunStatus
from datetime import datetime, timezone

workflow_ui_bp = Blueprint('workflow_ui', __name__)


@workflow_ui_bp.route('/<repo_name>/workflows')
def workflows_list(repo_name):
    """List all workflow runs for a repository"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Get all workflow runs for this repository
        workflow_runs = db.query(WorkflowRun).filter(
            WorkflowRun.repository_id == repo.repository_id
        ).order_by(WorkflowRun.created_at.desc()).all()

        return render_template(
            'workflows/workflows_list.html',
            repo_name=repo_name,
            workflow_runs=workflow_runs,
            active_tab='workflows'
        )
    finally:
        db.close()


@workflow_ui_bp.route('/<repo_name>/workflows/new', methods=['GET', 'POST'])
def workflow_dispatch(repo_name):
    """Dispatch a new workflow run"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        if request.method == 'POST':
            workflow_file = request.form.get('workflow_file')
            branch = request.form.get('branch', 'main')
            triggered_by = request.form.get('triggered_by', 'manual')

            if not workflow_file:
                flash('Workflow file is required', 'error')
                return redirect(url_for('workflow_ui.workflow_dispatch', repo_name=repo_name))

            # Get the branch ref to get commit hash
            ref_name = f'refs/heads/{branch}' if not branch.startswith('refs/') else branch
            ref = repo.get_ref(ref_name)

            if not ref:
                flash(f'Branch {branch} not found', 'error')
                return redirect(url_for('workflow_ui.workflow_dispatch', repo_name=repo_name))

            # Create workflow run
            workflow_run = WorkflowRun(
                repository_id=repo.repository_id,
                workflow_file=workflow_file,
                commit_hash=ref.commit_hash,
                status=WorkflowStatus.PENDING,
                triggered_by=triggered_by,
                trigger_event='manual'
            )
            db.add(workflow_run)
            db.commit()

            flash(f'Workflow dispatched successfully (Run #{workflow_run.id})', 'success')
            return redirect(url_for('workflow_ui.workflow_detail', repo_name=repo_name, run_id=workflow_run.id))

        # GET request - show form
        branches = repo.list_branches()

        # Get list of Python files that might be workflows
        # For now, we'll look in the latest commit on main
        workflow_files = []
        main_ref = repo.get_ref('refs/heads/main')
        if main_ref:
            commit = repo.get_commit(main_ref.commit_hash)
            if commit:
                # Simple approach: list all .py files in the tree
                def find_python_files(tree_hash, prefix=''):
                    files = []
                    entries = repo.get_tree_contents(tree_hash)
                    for entry in entries:
                        full_path = f"{prefix}/{entry.name}" if prefix else entry.name
                        if entry.type.value == 'blob' and entry.name.endswith('.py'):
                            files.append(full_path)
                        elif entry.type.value == 'tree':
                            files.extend(find_python_files(entry.hash, full_path))
                    return files

                workflow_files = find_python_files(commit.tree_hash)

        return render_template(
            'workflows/workflow_dispatch.html',
            repo_name=repo_name,
            branches=branches,
            workflow_files=workflow_files,
            active_tab='workflows'
        )
    finally:
        db.close()


@workflow_ui_bp.route('/<repo_name>/workflows/<int:run_id>')
def workflow_detail(repo_name, run_id):
    """View workflow run details"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        workflow_run = db.query(WorkflowRun).filter(
            WorkflowRun.id == run_id,
            WorkflowRun.repository_id == repo.repository_id
        ).first()

        if not workflow_run:
            flash('Workflow run not found', 'error')
            return redirect(url_for('workflow_ui.workflows_list', repo_name=repo_name))

        # Get all stage runs for this workflow
        stage_runs = db.query(StageRun).filter(
            StageRun.workflow_run_id == run_id
        ).order_by(StageRun.started_at).all()

        return render_template(
            'workflows/workflow_detail.html',
            repo_name=repo_name,
            workflow_run=workflow_run,
            stage_runs=stage_runs,
            active_tab='workflows'
        )
    finally:
        db.close()
