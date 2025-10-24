from flask import Flask, render_template, redirect, url_for, flash
from markupsafe import Markup
import markdown
from src.config import Config
from src.models.base import create_session
from src.models import Repository as RepositoryModel
from src.storage import S3Storage, FilesystemStorage
from src.repository import Repository

app = Flask(__name__)
app.config.from_object(Config)


def get_storage():
    """Get storage backend - S3 if configured, otherwise filesystem"""
    if Config.S3_BUCKET:
        return S3Storage()
    else:
        return FilesystemStorage()


def get_repository(repo_name: str):
    """Get repository instance with DB session and storage"""
    db = create_session(Config.DATABASE_URL, echo=Config.DEBUG)
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
    db = create_session(Config.DATABASE_URL, echo=Config.DEBUG)
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
            current_branch='main',
            readme_content=readme_content
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


@app.route('/<repo_name>/commits')
@app.route('/<repo_name>/commits/<ref_name>')
def commits(repo_name, ref_name='refs/heads/main'):
    """Show commit history for a branch"""
    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repositories_list'))
    try:
        # Handle short ref names (e.g., 'main' -> 'refs/heads/main')
        if not ref_name.startswith('refs/'):
            ref_name = f'refs/heads/{ref_name}'

        ref = repo.get_ref(ref_name)
        if not ref:
            flash(f'Reference {ref_name} not found', 'error')
            return redirect(url_for('index', repo_name=repo_name))

        commits = repo.get_commit_history(ref.commit_hash, limit=50)
        return render_template(
            'commits.html',
            repo_name=repo_name,
            ref=ref,
            commits=commits
        )
    finally:
        db.close()


@app.route('/<repo_name>/commit/<commit_hash>')
def commit_detail(repo_name, commit_hash):
    """Show commit details"""
    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repositories_list'))

    try:
        commit = repo.get_commit(commit_hash)
        if not commit:
            flash(f'Commit {commit_hash} not found', 'error')
            return redirect(url_for('index', repo_name=repo_name))

        tree = repo.get_tree(commit.tree_hash)
        tree_entries = repo.get_tree_contents(commit.tree_hash) if tree else []

        return render_template(
            'commit_detail.html',
            repo_name=repo_name,
            commit=commit,
            tree_entries=tree_entries
        )
    finally:
        db.close()


@app.route('/<repo_name>/tree/<tree_hash>')
def tree_view(repo_name, tree_hash):
    """Browse tree contents"""
    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repositories_list'))

    try:
        tree = repo.get_tree(tree_hash)
        if not tree:
            flash(f'Tree {tree_hash} not found', 'error')
            return redirect(url_for('index', repo_name=repo_name))

        entries = repo.get_tree_contents(tree_hash)
        return render_template(
            'tree_view.html',
            repo_name=repo_name,
            tree=tree,
            entries=entries
        )
    finally:
        db.close()


@app.route('/<repo_name>/blob/<blob_hash>')
def blob_view(repo_name, blob_hash):
    """View blob content"""
    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repositories_list'))

    try:
        blob = repo.get_blob(blob_hash)
        if not blob:
            flash(f'Blob {blob_hash} not found', 'error')
            return redirect(url_for('index', repo_name=repo_name))

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
            repo_name=repo_name,
            blob=blob,
            content=text_content,
            is_binary=is_binary,
            download_url=download_url
        )
    finally:
        db.close()


if __name__ == '__main__':
    app.run(debug=Config.DEBUG, host='0.0.0.0', port=Config.PORT)
