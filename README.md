# DataWorkflow

A Git-like data versioning system with S3 storage and PostgreSQL tracking, featuring a GitHub-like web interface.

## Features

- **Content-Addressable Storage**: Stores data blobs in S3 using SHA-256 hashing
- **Git-like Architecture**: Implements commits, trees, blobs, and refs similar to Git
- **PostgreSQL Tracking**: All metadata stored in PostgreSQL using SQLAlchemy
- **GitHub-like UI**: Web interface built with Flask and server-side templates
- **Deduplication**: Automatic content deduplication via content-addressable storage

## Architecture

The system mirrors Git's architecture:

- **Blobs**: Store file content in S3 (content-addressable)
- **Trees**: Represent directory structures (stored in PostgreSQL)
- **Commits**: Snapshots of trees with metadata (stored in PostgreSQL)
- **Refs**: Named pointers to commits, like branches and tags (stored in PostgreSQL)

## Setup

### 1. Prerequisites

- Python 3.8+
- PostgreSQL database (optional - SQLite works for development)
- AWS S3 bucket (or use filesystem storage for development)
- AWS credentials with S3 access (if using S3)

### 2. Install Dependencies

Create a virtual environment and install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Note: PostgreSQL support (`psycopg2-binary`) is optional. Uncomment it in `requirements.txt` if you need PostgreSQL.

### 3. Configure Environment

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

Edit `.env` with your settings:

```
# Database Configuration
# For PostgreSQL:
# DATABASE_URL=postgresql://user:password@localhost:5432/dataworkflow
# For SQLite (development):
DATABASE_URL=sqlite:///./dataworkflow.db

# S3 Configuration (optional - uses FilesystemStorage if not configured)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET=your-bucket-name

# Flask Configuration
FLASK_SECRET_KEY=your-secret-key-here
FLASK_ENV=development
PORT=5001
```

### 4. Run Tests

Verify everything is working:

```bash
PYTHONPATH=. pytest tests/ -v
```

## Running the Application

Start the Flask development server:

```bash
source venv/bin/activate
PYTHONPATH=. python src/app.py
```

The application will be available at `http://localhost:5001` (or whatever PORT you set in `.env`)

**Note:** Make sure your database and storage backend are properly configured in `.env` before running the app.

You can change the port by setting the `PORT` environment variable:
```bash
PORT=8080 PYTHONPATH=. python src/app.py
```

## Project Structure

```
dataworkflow/
├── src/
│   ├── models/           # SQLAlchemy models
│   │   ├── base.py       # Database connection and base
│   │   ├── blob.py       # Blob model (file content)
│   │   ├── tree.py       # Tree and TreeEntry models
│   │   ├── commit.py     # Commit model
│   │   └── ref.py        # Reference model (branches/tags)
│   ├── storage/          # S3 storage layer
│   │   └── s3_storage.py # S3 operations
│   ├── repository/       # Core Git-like operations
│   │   └── repository.py # Repository class
│   ├── templates/        # Flask HTML templates
│   │   ├── base.html     # Base template
│   │   ├── index.html    # Homepage
│   │   ├── branches.html # Branch list
│   │   ├── commits.html  # Commit history
│   │   ├── commit_detail.html
│   │   ├── tree_view.html
│   │   └── blob_view.html
│   ├── config.py         # Configuration
│   └── app.py            # Flask application
├── scripts/
│   ├── init_db.py        # Database initialization
│   └── example_commit.py # Example data generator
├── requirements.txt
├── .env.example
└── README.md
```

## Usage Examples

### Creating a Blob

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models.base import Base
from src.storage import FilesystemStorage  # or S3Storage
from src.repository import Repository

# Setup database
engine = create_engine('sqlite:///./dataworkflow.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
db = Session()

# Setup storage (filesystem or S3)
storage = FilesystemStorage()  # or S3Storage()

# Create repository
repo = Repository(db, storage)

# Create a blob from content
blob = repo.create_blob(b"Hello, World!")
print(f"Created blob: {blob.hash}")
```

### Creating a Tree

```python
# Create tree entries
entries = [
    {'name': 'file1.txt', 'type': 'blob', 'hash': blob1.hash, 'mode': '100644'},
    {'name': 'file2.txt', 'type': 'blob', 'hash': blob2.hash, 'mode': '100644'},
]

tree = repo.create_tree(entries)
print(f"Created tree: {tree.hash}")
```

### Creating a Commit

```python
commit = repo.create_commit(
    tree_hash=tree.hash,
    message="Initial commit",
    author="Your Name",
    author_email="you@example.com",
    parent_hash=None  # No parent for first commit
)
print(f"Created commit: {commit.hash}")
```

### Creating a Branch

```python
# Create or update a branch
ref = repo.create_or_update_ref('refs/heads/main', commit.hash)
print(f"Created branch: {ref.name}")
```

## Web Interface

The web UI provides:

- **Homepage**: Overview of branches, tags, and recent commits
- **Branches**: List all branches with their latest commits
- **Commits**: View commit history for a branch
- **Commit Details**: See commit metadata, message, and tree contents
- **Tree Browser**: Explore directory structures
- **Blob Viewer**: View file contents (with download for binary files)

## Future Enhancements

- REST API for programmatic access
- Diff view between commits
- Merge functionality
- Search across commits and content
- User authentication and permissions
- Garbage collection for orphaned objects
- Alembic migrations for schema changes

## License

MIT
