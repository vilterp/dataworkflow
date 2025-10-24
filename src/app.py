from flask import Flask, render_template, redirect, url_for, flash
from markupsafe import Markup
import markdown
from datetime import datetime, timezone
from src.config import Config
from src.models.base import create_session
from src.models import Repository as RepositoryModel
from src.storage import S3Storage, FilesystemStorage
from src.repository import Repository
from src.diff import DiffGenerator

app = Flask(__name__)
app.config.from_object(Config)


@app.template_filter('timeago')
def timeago_filter(dt):
    """Convert a datetime to a relative time string (e.g., '2 hours ago')"""
    if not dt:
        return ''

    now = datetime.now(timezone.utc)
    # Ensure dt is timezone-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    diff = now - dt
    seconds = diff.total_seconds()

    if seconds < 60:
        return 'just now'
    elif seconds < 3600:  # Less than 1 hour
        minutes = int(seconds / 60)
        return f'{minutes} minute{"s" if minutes != 1 else ""} ago'
    elif seconds < 86400:  # Less than 1 day
        hours = int(seconds / 3600)
        return f'{hours} hour{"s" if hours != 1 else ""} ago'
    elif seconds < 2592000:  # Less than 30 days
        days = int(seconds / 86400)
        return f'{days} day{"s" if days != 1 else ""} ago'
    elif seconds < 31536000:  # Less than 1 year
        months = int(seconds / 2592000)
        return f'{months} month{"s" if months != 1 else ""} ago'
    else:
        years = int(seconds / 31536000)
        return f'{years} year{"s" if years != 1 else ""} ago'


def get_storage():
    """Get storage backend - S3 if configured, otherwise filesystem"""
    if Config.S3_BUCKET:
        return S3Storage()
    else:
        return FilesystemStorage()


def get_repository(repo_name: str):
    """Get repository instance with DB session and storage"""
    # Use Flask app config if available, otherwise use global config
    database_url = app.config.get('DATABASE_URL', Config.DATABASE_URL)
    debug = app.config.get('DEBUG', Config.DEBUG)
    
    db = create_session(database_url, echo=debug)
    storage = get_storage()

    # Look up repository by name
    repo_model = db.query(RepositoryModel).filter(RepositoryModel.name == repo_name).first()
    if not repo_model:
        return None, db

    return Repository(db, storage, repo_model.id), db


@app.teardown_appcontext
def shutdown_session(exception=None):
    """Close database session"""
    pass


@app.route('/')
def repositories_list():
    """List all repositories"""
    # Use Flask app config if available, otherwise use global config
    database_url = app.config.get('DATABASE_URL', Config.DATABASE_URL)
    debug = app.config.get('DEBUG', Config.DEBUG)
    
    db = create_session(database_url, echo=debug)
    try:
        repositories = db.query(RepositoryModel).order_by(RepositoryModel.name).all()
        return render_template('repositories.html', repositories=repositories)
    finally:
        db.close()


@app.route('/<repo_name>')
def index(repo_name):
    """Repository homepage - show file browser like GitHub"""
    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repositories_list'))

    try:
        # Get main branch (default branch)
        main_ref = repo.get_ref('refs/heads/main')

        if not main_ref:
            # No commits yet - show empty state
            branches = repo.list_branches()
            tags = repo.list_tags()
            return render_template(
                'index.html',
                branches=branches,
                tags=tags,
                latest_commit=None,
                tree_entries=[]
            )

        # Get latest commit
        latest_commit = repo.get_commit(main_ref.commit_hash)

        # Get tree entries (files and directories at root)
        tree_entries = repo.get_tree_contents(latest_commit.tree_hash)

        # Get branch and tag counts
        branches = repo.list_branches()
        tags = repo.list_tags()

        # Get commit count for the main branch
        commit_count = len(repo.get_commit_history(main_ref.commit_hash, limit=1000))

        # Get latest commit info for each entry
        diff_gen = DiffGenerator(repo)
        entry_commits = {}
        for entry in tree_entries:
            commit_for_entry = diff_gen.get_latest_commit_for_path(main_ref.commit_hash, entry.name)
            if commit_for_entry:
                entry_commits[entry.name] = commit_for_entry

        # Look for README.md in the tree
        readme_content = None
        for entry in tree_entries:
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

        return render_template(
            'index.html',
            repo_name=repo_name,
            branches=branches,
            tags=tags,
            latest_commit=latest_commit,
            tree_entries=tree_entries,
            entry_commits=entry_commits,
            current_branch='main',
            readme_content=readme_content,
            commit_count=commit_count
        )
    finally:
        db.close()


@app.route('/<repo_name>/branches')
def branches(repo_name):
    """List all branches"""
    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repositories_list'))

    try:
        branches = repo.list_branches()
        return render_template('branches.html', repo_name=repo_name, branches=branches)
    finally:
        db.close()


@app.route('/<repo_name>/commits/<branch>')
@app.route('/<repo_name>/commits/<branch>/<path:file_path>')
def commits(repo_name, branch='main', file_path=None):
    """Show commit history for a branch, optionally filtered by path"""
    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repositories_list'))
    try:
        # Handle short ref names (e.g., 'main' -> 'refs/heads/main')
        ref_name = f'refs/heads/{branch}' if not branch.startswith('refs/') else branch

        ref = repo.get_ref(ref_name)
        if not ref:
            flash(f'Branch {branch} not found', 'error')
            return redirect(url_for('index', repo_name=repo_name))

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

        return render_template(
            'commits.html',
            repo_name=repo_name,
            branch=branch,
            ref=ref,
            commits=commits,
            file_path=file_path
        )
    finally:
        db.close()


@app.route('/<repo_name>/commit/<commit_hash>')
def commit_detail(repo_name, commit_hash):
    """Show commit details with diff"""
    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repositories_list'))

    try:
        commit = repo.get_commit(commit_hash)
        if not commit:
            flash(f'Commit {commit_hash} not found', 'error')
            return redirect(url_for('index', repo_name=repo_name))

        # Generate diff
        diff_gen = DiffGenerator(repo)
        file_diffs = diff_gen.get_commit_diff(commit_hash)

        return render_template(
            'commit_detail.html',
            repo_name=repo_name,
            commit=commit,
            file_diffs=file_diffs
        )
    finally:
        db.close()


@app.route('/<repo_name>/tree/<branch>/')
@app.route('/<repo_name>/tree/<branch>/<path:dir_path>')
def tree_view(repo_name, branch, dir_path=''):
    """Browse tree contents at a specific branch and path"""
    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repositories_list'))

    try:
        # Get the branch ref
        ref_name = f'refs/heads/{branch}' if not branch.startswith('refs/') else branch
        ref = repo.get_ref(ref_name)
        if not ref:
            flash(f'Branch {branch} not found', 'error')
            return redirect(url_for('index', repo_name=repo_name))

        # Get the commit
        commit = repo.get_commit(ref.commit_hash)
        if not commit:
            flash(f'Commit not found', 'error')
            return redirect(url_for('index', repo_name=repo_name))

        # Navigate to the directory through the tree
        current_tree_hash = commit.tree_hash

        if dir_path:
            path_parts = dir_path.split('/')
            # Navigate through directories
            for i, part in enumerate(path_parts):
                tree_entries = repo.get_tree_contents(current_tree_hash)
                found = False
                for entry in tree_entries:
                    if entry.name == part and entry.type.value == 'tree':
                        current_tree_hash = entry.hash
                        found = True
                        break
                if not found:
                    flash(f'Directory not found: {"/".join(path_parts[:i+1])}', 'error')
                    return redirect(url_for('index', repo_name=repo_name))

        # Get entries in the current directory
        entries = repo.get_tree_contents(current_tree_hash)

        # Get the latest commit affecting this directory (if dir_path specified)
        diff_gen = DiffGenerator(repo)
        if dir_path:
            latest_commit_for_dir = diff_gen.get_latest_commit_for_path(ref.commit_hash, dir_path)
            if not latest_commit_for_dir:
                latest_commit_for_dir = commit

            # Get commit count for this directory's history
            all_commits = repo.get_commit_history(ref.commit_hash, limit=1000)
            dir_commits = [c for c in all_commits if diff_gen.commit_affects_path(c.hash, dir_path)]
            commit_count = len(dir_commits)
        else:
            latest_commit_for_dir = commit
            commit_count = len(repo.get_commit_history(ref.commit_hash, limit=1000))

        return render_template(
            'tree_view.html',
            repo_name=repo_name,
            branch=branch,
            dir_path=dir_path,
            commit=latest_commit_for_dir,
            entries=entries,
            commit_count=commit_count
        )
    finally:
        db.close()


@app.route('/<repo_name>/blob/<branch>/<path:file_path>')
def blob_view(repo_name, branch, file_path):
    """View blob content at a specific branch and path"""
    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repositories_list'))

    try:
        # Get the branch ref
        ref_name = f'refs/heads/{branch}' if not branch.startswith('refs/') else branch
        ref = repo.get_ref(ref_name)
        if not ref:
            flash(f'Branch {branch} not found', 'error')
            return redirect(url_for('index', repo_name=repo_name))

        # Get the commit
        commit = repo.get_commit(ref.commit_hash)
        if not commit:
            flash(f'Commit not found', 'error')
            return redirect(url_for('index', repo_name=repo_name))

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
                return redirect(url_for('index', repo_name=repo_name))

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
            return redirect(url_for('index', repo_name=repo_name))

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

        # Get download URL
        download_url = repo.storage.get_download_url(blob_hash)

        # Get the latest commit affecting this file
        diff_gen = DiffGenerator(repo)
        latest_commit_for_file = diff_gen.get_latest_commit_for_path(ref.commit_hash, file_path)

        # If no commit found affecting this file, fall back to branch head
        if not latest_commit_for_file:
            latest_commit_for_file = commit

        # Get commit count for this file's history
        all_commits = repo.get_commit_history(ref.commit_hash, limit=1000)
        file_commits = [c for c in all_commits if diff_gen.commit_affects_path(c.hash, file_path)]
        commit_count = len(file_commits)

        return render_template(
            'blob_view.html',
            repo_name=repo_name,
            branch=branch,
            file_path=file_path,
            commit=latest_commit_for_file,
            blob=blob,
            content=text_content,
            is_binary=is_binary,
            download_url=download_url,
            commit_count=commit_count
        )
    finally:
        db.close()


if __name__ == '__main__':
    app.run(debug=Config.DEBUG, host='0.0.0.0', port=Config.PORT)
