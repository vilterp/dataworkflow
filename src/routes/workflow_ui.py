"""Workflow UI routes for DataWorkflow"""
from flask import Blueprint, render_template, redirect, url_for, flash, request
from src.models import StageRun, StageFile
from src.core.workflows import create_stage_run_with_entry_point, find_python_files_in_tree
from datetime import datetime, timezone

workflow_ui_bp = Blueprint('workflow_ui', __name__)


@workflow_ui_bp.route('/<repo_name>/workflows')
def workflows_list(repo_name):
    """List all root stage runs (workflow entry points) for a repository"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Get filter parameters from query string
        workflow_file = request.args.get('workflow_file')
        commit_hash = request.args.get('commit_hash')

        # Build query for root stage runs
        query = db.query(StageRun).filter(
            StageRun.parent_stage_run_id == None,
            StageRun.repo_name == repo_name
        )

        # Apply filters
        if workflow_file:
            query = query.filter(StageRun.workflow_file == workflow_file)
        if commit_hash:
            query = query.filter(StageRun.commit_hash == commit_hash)

        root_stages = query.order_by(StageRun.created_at.desc()).all()

        # Add branch names for each stage run
        stage_runs_with_branches = []
        for stage in root_stages:
            branch_name = repo.get_branch_for_commit(stage.commit_hash) or stage.commit_hash
            stage_runs_with_branches.append({
                'stage_run': stage,
                'branch_name': branch_name
            })

        return render_template(
            'workflows/workflows_list.html',
            repo_name=repo_name,
            workflow_runs=root_stages,  # Keep var name for template compatibility
            stage_runs_with_branches=stage_runs_with_branches,
            workflow_file=workflow_file,  # For displaying filter
            commit_hash=commit_hash,  # For displaying filter
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

            # Create root stage run (entry point)
            root_stage, created = create_stage_run_with_entry_point(
                repo_name=repo_name,
                repo=repo,
                db=db,
                workflow_file=workflow_file,
                commit_hash=ref.commit_hash,
                entry_point='main',
                arguments=None,
                triggered_by=triggered_by,
                trigger_event='manual'
            )

            if created:
                flash(f'Workflow dispatched successfully (Run {root_stage.short_id})', 'success')
            else:
                flash(f'This workflow already exists (Run {root_stage.short_id})', 'info')

            # Redirect to new stage browsing interface
            # Format: /<repo>/stage/<branch>/<workflow_file>/<entry_point>
            # Try to use branch name instead of commit hash
            branch_name = repo.get_branch_for_commit(ref.commit_hash) or ref.commit_hash
            stage_path = f"{workflow_file}/main"
            return redirect(url_for('repo.stage_view', repo_name=repo_name, branch=branch_name, stage_path=stage_path))

        # GET request - show form
        branches = repo.list_branches()

        # Get list of Python files that might be workflows
        workflow_files = []
        main_ref = repo.get_ref('refs/heads/main')
        if main_ref:
            commit = repo.get_commit(main_ref.commit_hash)
            if commit:
                workflow_files = find_python_files_in_tree(repo, commit.tree_hash)

        # Get pre-fill values from query params
        prefill_workflow_file = request.args.get('workflow_file')
        prefill_branch = request.args.get('branch')

        return render_template(
            'workflows/workflow_dispatch.html',
            repo_name=repo_name,
            branches=branches,
            workflow_files=workflow_files,
            prefill_workflow_file=prefill_workflow_file,
            prefill_branch=prefill_branch,
            active_tab='workflows'
        )
    finally:
        db.close()


# Removed: Old stage detail page - replaced by new stage browsing at /<repo>/stage/<ref>/<path>/<call_stack>
