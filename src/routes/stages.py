"""Stage-related routes for DataWorkflow"""
from flask import Blueprint, render_template, redirect, url_for, flash, request
from src.models import Stage, StageFile
from src.core.stage_operations import commit_stage, get_stage_file_statuses

stages_bp = Blueprint('stages', __name__)


@stages_bp.route('/<repo_name>/stages')
@stages_bp.route('/<repo_name>/stages/<filter_type>')
def stages_list(repo_name, filter_type='active'):
    """List all stages for a repository"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repo', repo_name=repo_name))

    try:
        # Get stages based on filter
        if filter_type == 'committed':
            stages = db.query(Stage).filter(
                Stage.repository_id == repo.repository_id,
                Stage.committed == True
            ).order_by(Stage.committed_at.desc()).all()
        else:
            # Default to active stages
            filter_type = 'active'
            stages = db.query(Stage).filter(
                Stage.repository_id == repo.repository_id,
                Stage.committed == False
            ).order_by(Stage.created_at.desc()).all()

        return render_template(
            'stages/stages_list.html',
            repo_name=repo_name,
            stages=stages,
            filter_type=filter_type,
            active_tab='stages'
        )
    finally:
        db.close()


@stages_bp.route('/<repo_name>/stages/new', methods=['GET', 'POST'])
def stage_create(repo_name):
    """Create a new stage"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        if request.method == 'POST':
            name = request.form.get('name')
            base_ref = request.form.get('base_ref', 'refs/heads/main')
            description = request.form.get('description', '')

            if not name:
                flash('Stage name is required', 'error')
                return redirect(url_for('stages.stage_create', repo_name=repo_name))

            # Normalize base_ref to full ref name
            if not base_ref.startswith('refs/'):
                base_ref = f'refs/heads/{base_ref}'

            # Create the stage
            stage = Stage(
                repository_id=repo.repository_id,
                name=name,
                base_ref=base_ref,
                description=description
            )
            db.add(stage)
            db.commit()

            flash(f'Stage "{name}" created successfully', 'success')
            return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage.id))

        # GET request - show form
        branches = repo.list_branches()
        return render_template(
            'stages/stage_create.html',
            repo_name=repo_name,
            branches=branches,
            active_tab='stages'
        )
    finally:
        db.close()


@stages_bp.route('/<repo_name>/stages/<int:stage_id>')
def stage_detail(repo_name, stage_id):
    """View and manage a stage"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        stage = db.query(Stage).filter(Stage.id == stage_id).first()
        if not stage or stage.repository_id != repo.repository_id:
            flash('Stage not found', 'error')
            return redirect(url_for('stages.stages_list', repo_name=repo_name))

        # Get files in the stage
        files = db.query(StageFile).filter(StageFile.stage_id == stage_id).order_by(StageFile.path).all()

        # Determine file statuses using the stage_operations function
        file_statuses = get_stage_file_statuses(repo, db, stage)

        return render_template(
            'stages/stage_detail.html',
            repo_name=repo_name,
            stage=stage,
            files=files,
            file_statuses=file_statuses,
            active_tab='stages'
        )
    finally:
        db.close()


@stages_bp.route('/<repo_name>/stages/<int:stage_id>/upload', methods=['POST'])
def stage_upload_file(repo_name, stage_id):
    """Upload a file to a stage"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        stage = db.query(Stage).filter(Stage.id == stage_id).first()
        if not stage or stage.repository_id != repo.repository_id:
            flash('Stage not found', 'error')
            return redirect(url_for('stages.stages_list', repo_name=repo_name))

        if stage.committed:
            flash('Cannot modify a committed stage', 'error')
            return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))

        # Get file from form
        file = request.files.get('file')
        file_path = request.form.get('path')

        if not file or not file_path:
            flash('File and path are required', 'error')
            return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))

        # Create blob from file content
        content = file.read()
        blob = repo.create_blob(content)

        # Check if file already exists in stage
        existing_file = db.query(StageFile).filter(
            StageFile.stage_id == stage_id,
            StageFile.path == file_path
        ).first()

        if existing_file:
            # Update existing file
            existing_file.blob_hash = blob.hash
            existing_file.updated_at = datetime.now(timezone.utc)
        else:
            # Create new file entry
            stage_file = StageFile(
                stage_id=stage_id,
                path=file_path,
                blob_hash=blob.hash
            )
            db.add(stage_file)

        db.commit()
        flash(f'File "{file_path}" added to stage', 'success')
        return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))
    finally:
        db.close()


@stages_bp.route('/<repo_name>/stages/<int:stage_id>/files/<int:file_id>/delete', methods=['POST'])
def stage_delete_file(repo_name, stage_id, file_id):
    """Remove a file from a stage"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        stage = db.query(Stage).filter(Stage.id == stage_id).first()
        if not stage or stage.repository_id != repo.repository_id:
            flash('Stage not found', 'error')
            return redirect(url_for('stages.stages_list', repo_name=repo_name))

        if stage.committed:
            flash('Cannot modify a committed stage', 'error')
            return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))

        stage_file = db.query(StageFile).filter(StageFile.id == file_id).first()
        if not stage_file or stage_file.stage_id != stage_id:
            flash('File not found', 'error')
            return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))

        file_path = stage_file.path
        db.delete(stage_file)
        db.commit()

        flash(f'File "{file_path}" removed from stage', 'success')
        return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))
    finally:
        db.close()


@stages_bp.route('/<repo_name>/stages/<int:stage_id>/commit', methods=['POST'])
def stage_commit(repo_name, stage_id):
    """Commit a stage to create a new commit"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        stage = db.query(Stage).filter(Stage.id == stage_id).first()
        if not stage or stage.repository_id != repo.repository_id:
            flash('Stage not found', 'error')
            return redirect(url_for('stages.stages_list', repo_name=repo_name))

        if stage.committed:
            flash('Stage has already been committed', 'error')
            return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))

        # Get files in the stage
        files = db.query(StageFile).filter(StageFile.stage_id == stage_id).all()
        if not files:
            flash('Cannot commit an empty stage', 'error')
            return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))

        # Get commit message and author from form
        message = request.form.get('message')
        author = request.form.get('author', 'Anonymous')
        author_email = request.form.get('author_email', 'anonymous@example.com')
        commit_target = request.form.get('commit_target', 'base')  # 'base' or 'new_branch'
        new_branch_name = request.form.get('new_branch_name', '')

        if not message:
            flash('Commit message is required', 'error')
            return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))

        # Validate branch name if creating new branch
        if commit_target == 'new_branch':
            if not new_branch_name:
                flash('Branch name is required when creating a new branch', 'error')
                return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))
            # Check if branch already exists
            new_ref_name = f'refs/heads/{new_branch_name}'
            if repo.get_ref(new_ref_name):
                flash(f'Branch "{new_branch_name}" already exists', 'error')
                return redirect(url_for('stages.stage_detail', repo_name=repo_name, stage_id=stage_id))

        # Determine which ref to update
        if commit_target == 'new_branch':
            target_ref = f'refs/heads/{new_branch_name}'
            flash_message = f'Created new branch "{new_branch_name}"'
        else:
            target_ref = stage.base_ref
            branch_name = stage.base_ref.replace('refs/heads/', '')
            flash_message = f'Committed to "{branch_name}"'

        # Use the commit_stage function to handle the commit logic
        commit_hash, committed_ref = commit_stage(
            repo=repo,
            db=db,
            stage=stage,
            message=message,
            author=author,
            author_email=author_email,
            target_ref=target_ref
        )

        flash(f'{flash_message} as {commit_hash[:7]}', 'success')
        return redirect(url_for('repo.commit_detail', repo_name=repo_name, commit_hash=commit_hash))
    finally:
        db.close()
