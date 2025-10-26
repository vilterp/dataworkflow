from flask import Flask
from src.config import Config
from src.models.base import create_session
from src.models import Repository as RepositoryModel
from src.storage import S3Storage, FilesystemStorage
from src.core import Repository
from src.utils import timeago_filter
from src.routes import repo_bp
from src.routes.workflows import workflows_bp
from src.routes.workflow_ui import workflow_ui_bp
from src.routes.repo_edit import repo_edit_bp

app = Flask(__name__)
app.config.from_object(Config)

# Register blueprints
app.register_blueprint(repo_bp)
app.register_blueprint(workflows_bp)
app.register_blueprint(workflow_ui_bp)
app.register_blueprint(repo_edit_bp)

# Register template filters
app.template_filter('timeago')(timeago_filter)


def get_storage():
    """Get storage backend - S3 if configured, otherwise filesystem"""
    # Use Flask app config if available, otherwise use global config
    s3_bucket = app.config.get('S3_BUCKET', Config.S3_BUCKET)
    storage_base_path = app.config.get('STORAGE_BASE_PATH', '.dataworkflow/objects')

    if s3_bucket:
        return S3Storage()
    else:
        return FilesystemStorage(base_path=storage_base_path)


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
