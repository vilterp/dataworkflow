"""Blob editing and workflow-related routes for DataWorkflow"""
import logging
from flask import Blueprint, render_template, redirect, url_for, flash, request
from datetime import datetime, timezone
from src.models import StageRun

logger = logging.getLogger(__name__)

# Import this blueprint in app.py
repo_edit_bp = Blueprint('repo_edit', __name__)


@repo_edit_bp.route('/<repo_name>/edit/<branch>/<path:file_path>', methods=['GET', 'POST'])
def edit_blob(repo_name, branch, file_path):
    """Edit a blob and commit changes"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Get the branch ref
        ref_name = f'refs/heads/{branch}' if not branch.startswith('refs/') else branch
        ref = repo.get_ref(ref_name)
        if not ref:
            flash(f'Branch {branch} not found', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        if request.method == 'POST':
            content = request.form.get('content', '')
            commit_message = request.form.get('commit_message')
            commit_option = request.form.get('commit_option')
            new_branch = request.form.get('new_branch')

            if not commit_message:
                flash('Commit message is required', 'error')
                return redirect(url_for('repo_edit.edit_blob', repo_name=repo_name, branch=branch, file_path=file_path))

            # Determine target branch
            target_branch = branch
            if commit_option == 'new_branch':
                if not new_branch:
                    flash('New branch name is required', 'error')
                    return redirect(url_for('repo_edit.edit_blob', repo_name=repo_name, branch=branch, file_path=file_path))
                target_branch = new_branch
                # Create new branch from current branch
                try:
                    repo.create_branch(new_branch, ref.commit_hash)
                except ValueError as e:
                    flash(f'Error creating branch: {str(e)}', 'error')
                    return redirect(url_for('repo_edit.edit_blob', repo_name=repo_name, branch=branch, file_path=file_path))

            # Update file and commit
            try:
                commit = repo.update_file(
                    branch=target_branch,
                    file_path=file_path,
                    content=content.encode('utf-8'),
                    commit_message=commit_message,
                    author_name="Web Editor",
                    author_email="editor@dataworkflow"
                )
                flash(f'File updated successfully in {target_branch}', 'success')
                return redirect(url_for('repo.blob_view', repo_name=repo_name, branch=target_branch, file_path=file_path))
            except Exception as e:
                logger.error(f'Error updating file {file_path} in {repo_name}/{target_branch}: {str(e)}', exc_info=True)
                flash(f'Error updating file: {str(e)}', 'error')
                return redirect(url_for('repo_edit.edit_blob', repo_name=repo_name, branch=branch, file_path=file_path))

        # GET request - show editor
        # Get the commit
        commit = repo.get_commit(ref.commit_hash)
        if not commit:
            flash(f'Commit not found', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

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
                flash(f'Path not found: {"/".join(path_parts[:i+1])}', 'error')
                return redirect(url_for('repo.repo', repo_name=repo_name))

        # Find the file in the final directory
        tree_entries = repo.get_tree_contents(current_tree_hash)
        file_name = path_parts[-1]
        blob_hash = None
        for entry in tree_entries:
            if entry.name == file_name and entry.type.value == 'blob':
                blob_hash = entry.hash
                break

        if not blob_hash:
            flash(f'File not found: {file_path}', 'error')
            return redirect(url_for('repo.repo', repo_name=repo_name))

        # Get blob content
        content = repo.get_blob_content(blob_hash)

        # Try to decode as text
        try:
            text_content = content.decode('utf-8') if content else ''
        except UnicodeDecodeError:
            flash('Cannot edit binary files', 'error')
            return redirect(url_for('repo.blob_view', repo_name=repo_name, branch=branch, file_path=file_path))

        return render_template(
            'data/blob_edit.html',
            repo_name=repo_name,
            branch=branch,
            file_path=file_path,
            content=text_content
        )
    finally:
        db.close()


# These routes just redirect to workflows list with filters
@repo_edit_bp.route('/<repo_name>/blob-runs/<path:file_path>')
def blob_stage_runs(repo_name, file_path):
    """Redirect to workflows list filtered by file"""
    return redirect(url_for('workflow_ui.workflows_list', repo_name=repo_name, workflow_file=file_path))


@repo_edit_bp.route('/<repo_name>/commit-runs/<commit_hash>')
def commit_stage_runs(repo_name, commit_hash):
    """Redirect to workflows list filtered by commit"""
    return redirect(url_for('workflow_ui.workflows_list', repo_name=repo_name, commit_hash=commit_hash))
