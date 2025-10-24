from flask import Flask, render_template, redirect, url_for, flash
from src.config import Config
from src.models.base import create_session
from src.storage import S3Storage
from src.repository import Repository

app = Flask(__name__)
app.config.from_object(Config)


def get_repository():
    """Get repository instance with DB session and S3 storage"""
    db = create_session(Config.DATABASE_URL, echo=Config.DEBUG)
    storage = S3Storage()
    return Repository(db, storage), db


@app.teardown_appcontext
def shutdown_session(exception=None):
    """Close database session"""
    pass


@app.route('/')
def index():
    """Homepage - show branches and recent commits"""
    repo, db = get_repository()
    try:
        branches = repo.list_branches()
        tags = repo.list_tags()

        # Get latest commit from main branch if it exists
        main_ref = repo.get_ref('refs/heads/main')
        recent_commits = []
        if main_ref:
            recent_commits = repo.get_commit_history(main_ref.commit_hash, limit=10)

        return render_template(
            'index.html',
            branches=branches,
            tags=tags,
            recent_commits=recent_commits
        )
    finally:
        db.close()


@app.route('/branches')
def branches():
    """List all branches"""
    repo, db = get_repository()
    try:
        branches = repo.list_branches()
        return render_template('branches.html', branches=branches)
    finally:
        db.close()


@app.route('/commits')
@app.route('/commits/<ref_name>')
def commits(ref_name='refs/heads/main'):
    """Show commit history for a branch"""
    repo, db = get_repository()
    try:
        # Handle short ref names (e.g., 'main' -> 'refs/heads/main')
        if not ref_name.startswith('refs/'):
            ref_name = f'refs/heads/{ref_name}'

        ref = repo.get_ref(ref_name)
        if not ref:
            flash(f'Reference {ref_name} not found', 'error')
            return redirect(url_for('index'))

        commits = repo.get_commit_history(ref.commit_hash, limit=50)
        return render_template(
            'commits.html',
            ref=ref,
            commits=commits
        )
    finally:
        db.close()


@app.route('/commit/<commit_hash>')
def commit_detail(commit_hash):
    """Show commit details"""
    repo, db = get_repository()
    try:
        commit = repo.get_commit(commit_hash)
        if not commit:
            flash(f'Commit {commit_hash} not found', 'error')
            return redirect(url_for('index'))

        tree = repo.get_tree(commit.tree_hash)
        tree_entries = repo.get_tree_contents(commit.tree_hash) if tree else []

        return render_template(
            'commit_detail.html',
            commit=commit,
            tree_entries=tree_entries
        )
    finally:
        db.close()


@app.route('/tree/<tree_hash>')
def tree_view(tree_hash):
    """Browse tree contents"""
    repo, db = get_repository()
    try:
        tree = repo.get_tree(tree_hash)
        if not tree:
            flash(f'Tree {tree_hash} not found', 'error')
            return redirect(url_for('index'))

        entries = repo.get_tree_contents(tree_hash)
        return render_template(
            'tree_view.html',
            tree=tree,
            entries=entries
        )
    finally:
        db.close()


@app.route('/blob/<blob_hash>')
def blob_view(blob_hash):
    """View blob content"""
    repo, db = get_repository()
    try:
        blob = repo.get_blob(blob_hash)
        if not blob:
            flash(f'Blob {blob_hash} not found', 'error')
            return redirect(url_for('index'))

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

        return render_template(
            'blob_view.html',
            blob=blob,
            content=text_content,
            is_binary=is_binary,
            download_url=download_url
        )
    finally:
        db.close()


if __name__ == '__main__':
    app.run(debug=Config.DEBUG, host='0.0.0.0', port=5000)
