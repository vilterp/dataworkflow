"""Workflow UI routes for DataWorkflow"""
from flask import Blueprint, render_template, redirect, url_for, flash, request
from src.models import StageRun
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

        return render_template(
            'workflows/workflows_list.html',
            repo_name=repo_name,
            workflow_runs=root_stages,  # Keep var name for template compatibility
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
            return redirect(url_for('workflow_ui.workflow_detail', repo_name=repo_name, run_id=root_stage.id))

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


@workflow_ui_bp.route('/<repo_name>/workflows/<run_id>')
def workflow_detail(repo_name, run_id):
    """View workflow run details (root stage and its descendants)"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Get the root stage run
        root_stage = db.query(StageRun).filter(
            StageRun.id == run_id
        ).first()

        if not root_stage:
            flash('Workflow run not found', 'error')
            return redirect(url_for('workflow_ui.workflows_list', repo_name=repo_name))

        # Get all descendant stage runs (recursively get all children)
        def get_all_descendants(stage_id):
            """Recursively get all descendant stage runs."""
            descendants = []
            children = db.query(StageRun).filter(
                StageRun.parent_stage_run_id == stage_id
            ).all()
            for child in children:
                descendants.append(child)
                descendants.extend(get_all_descendants(child.id))
            return descendants

        all_stages = [root_stage] + get_all_descendants(root_stage.id)

        # Build tree structure for display
        def build_tree(parent_id=None, depth=0):
            """Recursively build tree of stage runs."""
            tree = []
            for stage_run in all_stages:
                if stage_run.parent_stage_run_id == parent_id:
                    tree.append({
                        'stage_run': stage_run,
                        'depth': depth,
                        'children': build_tree(stage_run.id, depth + 1)
                    })
            return tree

        def flatten_tree(tree):
            """Flatten tree into list with depth info."""
            flat = []
            for node in tree:
                flat.append({'stage_run': node['stage_run'], 'depth': node['depth']})
                flat.extend(flatten_tree(node['children']))
            return flat

        stage_runs_tree = flatten_tree(build_tree())

        return render_template(
            'workflows/workflow_detail.html',
            repo_name=repo_name,
            workflow_run=root_stage,  # Keep var name for template compatibility
            stage_runs=stage_runs_tree,
            active_tab='workflows'
        )
    finally:
        db.close()


@workflow_ui_bp.route('/<repo_name>/workflows/<run_id>/stages/<stage_id>')
def stage_run_detail(repo_name, run_id, stage_id):
    """View details for a specific stage run"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Get the stage run
        stage_run = db.query(StageRun).filter(StageRun.id == stage_id).first()
        if not stage_run:
            flash('Stage run not found', 'error')
            return redirect(url_for('workflow_ui.workflows_list', repo_name=repo_name))

        # Get the root stage run (for breadcrumb/navigation)
        root_stage = db.query(StageRun).filter(StageRun.id == run_id).first()
        if not root_stage:
            flash('Workflow run not found', 'error')
            return redirect(url_for('workflow_ui.workflows_list', repo_name=repo_name))

        # Get parent stage run if exists
        parent_stage_run = None
        if stage_run.parent_stage_run_id:
            parent_stage_run = db.query(StageRun).filter(
                StageRun.id == stage_run.parent_stage_run_id
            ).first()

        # Get child stage runs
        child_stage_runs = db.query(StageRun).filter(
            StageRun.parent_stage_run_id == stage_id
        ).order_by(StageRun.started_at).all()

        return render_template(
            'workflows/stage_run_detail.html',
            repo_name=repo_name,
            workflow_run=root_stage,  # Keep var name for template compatibility
            stage_run=stage_run,
            parent_stage_run=parent_stage_run,
            child_stage_runs=child_stage_runs,
            active_tab='workflows'
        )
    finally:
        db.close()
