"""Repository and data browsing routes for DataWorkflow"""
from flask import Blueprint, render_template, redirect, url_for, flash, request
from markupsafe import Markup
from werkzeug.utils import secure_filename
import markdown
from src.models import Repository as RepositoryModel
from src.core.repository import FileEntry
from src.diff import DiffGenerator

repo_bp = Blueprint('repo', __name__)


@repo_bp.route('/')
def repositories_list():
    """List all repositories"""
    from src.app import create_session
    from src.config import Config
    from flask import current_app

    # Use Flask app config if available, otherwise use global config
    database_url = current_app.config.get('DATABASE_URL', Config.DATABASE_URL)
    debug = current_app.config.get('DEBUG', Config.DEBUG)

    db = create_session(database_url, echo=debug)
    try:
        repositories = db.query(RepositoryModel).order_by(RepositoryModel.name).all()
        return render_template('repositories.html', repositories=repositories)
    finally:
        db.close()


@repo_bp.route('/<repo_name>')
def repo(repo_name):
    """Repository homepage - show file browser like GitHub"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Get main branch (default branch)
        main_ref = repo.get_ref('refs/heads/main')

        if not main_ref:
            # No commits yet - show empty state
            branches = repo.list_branches()
            tags = repo.list_tags()
            return render_template(
                'repo.html',
                branches=branches,
                tags=tags,
                latest_commit=None,
                tree_entries=[]
            )

        # Get latest commit
        latest_commit = repo.get_commit(main_ref.commit_hash)

        # Get tree entries and their commits using shared method
        file_entries = repo.get_tree_entries_with_commits(main_ref.commit_hash)

        # Get branch and tag counts
        branches = repo.list_branches()
        tags = repo.list_tags()

        # Get commit count for the main branch
        commit_count = len(repo.get_commit_history(main_ref.commit_hash, limit=1000))

        # Look for README.md in the tree
        readme_content = None
        for entry in file_entries:
            if entry.name.lower() == 'readme.md' and entry.type.value == 'blob':
                # Get the README content
                content = repo.get_blob_content(entry.hash)
                if content:
                    try:
                        # Convert markdown to HTML
                        readme_text = content.decode('utf-8')
                        readme_content = Markup(markdown.markdown(readme_text))
                    except UnicodeDecodeError:
                        pass
                break

        # Get stage run stats for latest commit
        from dataclasses import asdict
        stats = repo.get_commit_stage_run_stats(latest_commit.hash)

        return render_template(
            'repo.html',
            repo_name=repo_name,
            branches=branches,
            tags=tags,
            latest_commit=latest_commit,
            file_entries=file_entries,
            current_branch='main',
            readme_content=readme_content,
            commit_count=commit_count,
            **asdict(stats),
            active_tab='data'
        )
    finally:
        db.close()


@repo_bp.route('/<repo_name>/branches')
def branches(repo_name):
    """List all branches"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        branches = repo.list_branches()
        return render_template('data/branches.html', repo_name=repo_name, branches=branches, active_tab='data')
    finally:
        db.close()


@repo_bp.route('/<repo_name>/commits/<branch>')
@repo_bp.route('/<repo_name>/commits/<branch>/<path:file_path>')
def commits(repo_name, branch='main', file_path=None):
    """Show commit history for a branch, optionally filtered by path"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))
    try:
        # Handle short ref names (e.g., 'main' -> 'refs/heads/main')
        ref_name = f'refs/heads/{branch}' if not branch.startswith('refs/') else branch

        ref = repo.get_ref(ref_name)
        if not ref:
            flash(f'Branch {branch} not found', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        # Get all commits for the branch
        all_commits = repo.get_commit_history(ref.commit_hash, limit=100)

        # Filter commits by path if specified
        if file_path:
            diff_gen = DiffGenerator(repo)
            filtered_commits = [
                commit for commit in all_commits
                if diff_gen.commit_affects_path(commit.hash, file_path)
            ]
            commits = filtered_commits
        else:
            commits = all_commits

        # Get stage run stats for each commit
        commit_stats = {}
        for commit in commits:
            stats = repo.get_commit_stage_run_stats(commit.hash)
            commit_stats[commit.hash] = {
                'count': stats.stage_run_count,
                'has_failed': stats.has_failed,
                'has_running': stats.has_running,
                'has_completed': stats.has_completed,
            }

        return render_template(
            'data/commits.html',
            repo_name=repo_name,
            branch=branch,
            ref=ref,
            commits=commits,
            file_path=file_path,
            commit_stats=commit_stats,
            active_tab='data'
        )
    finally:
        db.close()


@repo_bp.route('/<repo_name>/commit/<commit_hash>')
def commit_detail(repo_name, commit_hash):
    """Show commit details with diff"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        commit = repo.get_commit(commit_hash)
        if not commit:
            flash(f'Commit {commit_hash} not found', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        # Generate diff
        diff_gen = DiffGenerator(repo)
        file_diffs = diff_gen.get_commit_diff(commit_hash)

        # Get stage run stats for this commit
        from dataclasses import asdict
        stats = repo.get_commit_stage_run_stats(commit.hash)

        return render_template(
            'data/commit_detail.html',
            repo_name=repo_name,
            commit=commit,
            file_diffs=file_diffs,
            **asdict(stats),
            active_tab='data'
        )
    finally:
        db.close()


@repo_bp.route('/<repo_name>/tree/<branch>/')
@repo_bp.route('/<repo_name>/tree/<branch>/<path:dir_path>')
def tree_view(repo_name, branch, dir_path=''):
    """Browse tree contents at a specific branch and path"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Resolve branch name or commit hash
        commit, branch_display = repo.resolve_ref_or_commit(branch)
        if not commit:
            flash(f'Branch or commit {branch} not found', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        # Get entries and their commits using shared method
        file_entries = repo.get_tree_entries_with_commits(commit.hash, dir_path)

        if not file_entries and dir_path:
            flash(f'Directory not found: {dir_path}', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        # Get the latest commit and commit count for the current path
        if dir_path:
            latest_commit_for_dir, commit_count = repo.get_path_commit_info(commit.hash, dir_path)
            if not latest_commit_for_dir:
                latest_commit_for_dir = commit
        else:
            latest_commit_for_dir = commit
            commit_count = len(repo.get_commit_history(commit.hash, limit=1000))

        # Get stage run stats for the commit
        from dataclasses import asdict
        stats = repo.get_commit_stage_run_stats(latest_commit_for_dir.hash)

        return render_template(
            'data/tree_view.html',
            repo_name=repo_name,
            branch=branch,
            dir_path=dir_path,
            commit=latest_commit_for_dir,
            file_entries=file_entries,
            commit_count=commit_count,
            **asdict(stats),
            active_tab='data'
        )
    finally:
        db.close()


@repo_bp.route('/<repo_name>/blob/<branch>/<path:file_path>')
def blob_view(repo_name, branch, file_path):
    """View blob content at a specific branch and path"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Resolve branch name or commit hash
        commit, branch_display = repo.resolve_ref_or_commit(branch)
        if not commit:
            flash(f'Branch or commit {branch} not found', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        # Get blob hash from path
        blob_hash = repo.get_blob_hash_from_path(commit.tree_hash, file_path)

        if not blob_hash:
            flash(f'File not found: {file_path}', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        # Get blob content
        blob = repo.get_blob(blob_hash)
        content = repo.get_blob_content(blob_hash)

        # Try to decode as text
        try:
            text_content = content.decode('utf-8') if content else ''
            is_binary = False
        except UnicodeDecodeError:
            text_content = None
            is_binary = True

        # Get the latest commit and commit count for this file
        latest_commit_for_file, commit_count = repo.get_path_commit_info(commit.hash, file_path)

        # If no commit found affecting this file, fall back to branch head
        if not latest_commit_for_file:
            latest_commit_for_file = commit

        # Check if this is a Python file
        is_python_file = file_path.endswith('.py')

        # Get stage run stats for this commit
        stats = repo.get_commit_stage_run_stats(latest_commit_for_file.hash)
        stage_run_count = stats.stage_run_count
        has_failed = stats.has_failed
        has_running = stats.has_running
        has_completed = stats.has_completed

        # Get stage run count for this specific file (for greying out the view runs button)
        from src.models import StageRun
        file_stage_runs = db.query(StageRun).filter(
            StageRun.workflow_file == file_path,
            StageRun.parent_stage_run_id == None  # Only root stages
        ).all()
        file_stage_run_count = len(file_stage_runs)

        return render_template(
            'data/blob_view.html',
            repo_name=repo_name,
            branch=branch,
            file_path=file_path,
            commit=latest_commit_for_file,
            blob=blob,
            content=text_content,
            is_binary=is_binary,
            is_python_file=is_python_file,
            commit_count=commit_count,
            stage_run_count=stage_run_count,
            has_failed=has_failed,
            has_running=has_running,
            has_completed=has_completed,
            file_stage_run_count=file_stage_run_count,
            active_tab='data'
        )
    finally:
        db.close()


@repo_bp.route('/<repo_name>/download/<branch>/<path:file_path>')
def download_blob(repo_name, branch, file_path):
    """Download blob content at a specific branch and path"""
    from src.app import get_repository
    from flask import Response
    import os

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Resolve branch name or commit hash
        commit, _ = repo.resolve_ref_or_commit(branch)
        if not commit:
            flash(f'Branch or commit {branch} not found', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        # Get blob hash from path
        blob_hash = repo.get_blob_hash_from_path(commit.tree_hash, file_path)

        if not blob_hash:
            flash(f'File not found: {file_path}', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        # Get blob content
        content = repo.get_blob_content(blob_hash)
        if content is None:
            flash('Blob content not found in storage', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        # Extract filename from path
        filename = os.path.basename(file_path)

        # Return content as downloadable file
        return Response(
            content,
            mimetype='application/octet-stream',
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
    finally:
        db.close()


@repo_bp.route('/api/repos/<repo_name>/blob/<commit_hash>/<path:file_path>')
def get_file_content_api(repo_name, commit_hash, file_path):
    """
    Get raw file content from a specific commit (API endpoint).

    Used by workflow runners to download workflow source code.
    Returns raw file content as application/octet-stream.
    """
    from src.app import get_repository
    from flask import Response, jsonify

    repo, db = get_repository(repo_name)
    if not repo:
        return jsonify({'error': f'Repository {repo_name} not found'}), 404

    try:
        # Get the commit
        commit = repo.get_commit(commit_hash)
        if not commit:
            return jsonify({'error': 'Commit not found'}), 404

        # Get blob hash from path
        blob_hash = repo.get_blob_hash_from_path(commit.tree_hash, file_path)

        if not blob_hash:
            return jsonify({'error': f'File not found: {file_path}'}), 404

        # Get blob content using repo method
        content = repo.get_blob_content(blob_hash)
        if content is None:
            return jsonify({'error': 'Blob content not found in storage'}), 404

        # Return raw content
        return Response(content, mimetype='application/octet-stream')
    finally:
        db.close()


@repo_bp.route('/<repo_name>/add_file/<ref>/<path:dir_path>', methods=['GET', 'POST'])
@repo_bp.route('/<repo_name>/add_file/<ref>', methods=['GET', 'POST'])
def add_file(repo_name, ref, dir_path=''):
    """Add a new file to the repository"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        if request.method == 'POST':
            # Get uploaded file
            if 'file' not in request.files:
                flash('No file provided', 'error')
                return redirect(request.url)

            file = request.files['file']
            if file.filename == '':
                flash('No file selected', 'error')
                return redirect(request.url)

            # Get form data
            filename = request.form.get('filename', file.filename)
            commit_message = request.form.get('commit_message', f'Add {filename}')
            target_branch = request.form.get('target_branch', ref)
            create_new_branch = request.form.get('create_new_branch') == 'on'
            new_branch_name = request.form.get('new_branch_name', '')

            # Secure the filename
            filename = secure_filename(filename)
            file_path = f"{dir_path}/{filename}" if dir_path else filename

            # Read file content
            content = file.read()

            # Determine which branch to commit to
            if create_new_branch and new_branch_name:
                # Get base commit from source ref
                source_ref_obj = repo.get_ref(f'refs/heads/{ref}')
                if not source_ref_obj:
                    flash(f'Source branch {ref} not found', 'error')
                    return redirect(url_for('repo.tree_view', repo_name=repo_name, branch=ref, dir_path=dir_path))

                # Create new branch from source
                try:
                    repo.create_branch(new_branch_name, source_ref_obj.commit_hash)
                except ValueError as e:
                    flash(f'Error creating branch: {str(e)}', 'error')
                    return redirect(url_for('repo.tree_view', repo_name=repo_name, branch=ref, dir_path=dir_path))

                branch_name = new_branch_name
            else:
                branch_name = target_branch

            # Add/update the file using the repository method
            try:
                repo.update_file(
                    branch=branch_name,
                    file_path=file_path,
                    content=content,
                    commit_message=commit_message,
                    author_name='Web UI',
                    author_email='webui@dataworkflow.local'
                )
                flash(f'Successfully added {filename}', 'success')
            except ValueError as e:
                flash(f'Error adding file: {str(e)}', 'error')
                return redirect(url_for('repo.tree_view', repo_name=repo_name, branch=ref, dir_path=dir_path))
            return redirect(url_for('repo.tree_view', repo_name=repo_name, branch=branch_name, dir_path=dir_path))

        # GET request - show upload form
        return render_template(
            'data/add_file.html',
            repo_name=repo_name,
            ref=ref,
            dir_path=dir_path
        )
    finally:
        db.close()


@repo_bp.route('/<repo_name>/replace_file/<ref>/<path:file_path>', methods=['GET', 'POST'])
def replace_file(repo_name, ref, file_path):
    """Replace an existing file in the repository"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        if request.method == 'POST':
            # Get uploaded file
            if 'file' not in request.files:
                flash('No file provided', 'error')
                return redirect(request.url)

            file = request.files['file']
            if file.filename == '':
                flash('No file selected', 'error')
                return redirect(request.url)

            # Get form data
            commit_message = request.form.get('commit_message', f'Update {file_path}')
            target_branch = request.form.get('target_branch', ref)
            create_new_branch = request.form.get('create_new_branch') == 'on'
            new_branch_name = request.form.get('new_branch_name', '')

            # Read file content
            content = file.read()

            # Determine which branch to commit to
            if create_new_branch and new_branch_name:
                # Get base commit from source ref
                source_ref_obj = repo.get_ref(f'refs/heads/{ref}')
                if not source_ref_obj:
                    flash(f'Source branch {ref} not found', 'error')
                    return redirect(url_for('repo.blob_view', repo_name=repo_name, branch=ref, file_path=file_path))

                # Create new branch from source
                try:
                    repo.create_branch(new_branch_name, source_ref_obj.commit_hash)
                except ValueError as e:
                    flash(f'Error creating branch: {str(e)}', 'error')
                    return redirect(url_for('repo.blob_view', repo_name=repo_name, branch=ref, file_path=file_path))

                branch_name = new_branch_name
            else:
                branch_name = target_branch

            # Update the file using the repository method
            try:
                repo.update_file(
                    branch=branch_name,
                    file_path=file_path,
                    content=content,
                    commit_message=commit_message,
                    author_name='Web UI',
                    author_email='webui@dataworkflow.local'
                )
                flash(f'Successfully updated {file_path}', 'success')
            except ValueError as e:
                flash(f'Error updating file: {str(e)}', 'error')
                return redirect(url_for('repo.blob_view', repo_name=repo_name, branch=ref, file_path=file_path))
            return redirect(url_for('repo.blob_view', repo_name=repo_name, branch=branch_name, file_path=file_path))

        # GET request - show upload form
        return render_template(
            'data/replace_file.html',
            repo_name=repo_name,
            ref=ref,
            file_path=file_path
        )
    finally:
        db.close()


@repo_bp.route('/<repo_name>/delete_file/<ref>/<path:file_path>', methods=['POST'])
def delete_file(repo_name, ref, file_path):
    """Delete a file from the repository"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Get the branch ref
        ref_name = f'refs/heads/{ref}' if not ref.startswith('refs/') else ref
        ref_obj = repo.get_ref(ref_name)
        if not ref_obj:
            flash(f'Branch {ref} not found', 'error')
            return redirect(url_for('repo.repositories_list'))

        # Delete the file and create a commit
        try:
            commit = repo.delete_file(
                base_commit_hash=ref_obj.commit_hash,
                file_path=file_path,
                message=f'Delete {file_path}',
                author='Web UI',
                author_email='webui@dataworkflow.local'
            )

            # Update the branch reference
            repo.create_or_update_ref(ref_name, commit.hash)

            flash(f'Successfully deleted {file_path}', 'success')

            # Redirect to the tree view of the parent directory
            if '/' in file_path:
                parent_dir = '/'.join(file_path.split('/')[:-1])
                return redirect(url_for('repo.tree_view', repo_name=repo_name, branch=ref, dir_path=parent_dir))
            else:
                # File was in root, redirect to repo home
                return redirect(url_for('repo.repo', repo_name=repo_name))

        except ValueError as e:
            flash(f'Error deleting file: {str(e)}', 'error')
            return redirect(url_for('repo.blob_view', repo_name=repo_name, branch=ref, file_path=file_path))

    finally:
        db.close()
