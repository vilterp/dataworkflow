"""Stage file model - represents files created by stage runs."""
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.orm import relationship
import hashlib
from .base import Base


class StageFile(Base):
    """
    A file created by a stage run during workflow execution.

    Files are stored in the same storage backend as blobs, and the ID
    is a hash of the stage_run_id and file_path to ensure uniqueness
    within a stage run.
    """
    __tablename__ = 'stage_files'

    # Content-addressable ID (hash of stage_run_id + file_path)
    id = Column(String(64), primary_key=True)

    # Reference to the stage run that created this file
    stage_run_id = Column(String(64), ForeignKey('stage_runs.id'), nullable=False, index=True)

    # File path as specified by the workflow (e.g., "output/results.csv")
    file_path = Column(String(500), nullable=False)

    # Content hash (SHA-256) of the file contents
    content_hash = Column(String(64), nullable=False)

    # Storage key where the file is stored (e.g., S3 key or filesystem path)
    storage_key = Column(String(255), nullable=False)

    # File size in bytes
    size = Column(Integer, nullable=False)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    stage_run = relationship("StageRun", backref="stage_files")

    @staticmethod
    def compute_id(stage_run_id: str, file_path: str) -> str:
        """
        Compute content-addressable ID for a stage file.

        The ID is a SHA256 hash of stage_run_id + file_path, ensuring that
        each file path within a stage run has a unique ID.

        Args:
            stage_run_id: ID of the stage run that created the file
            file_path: Path of the file as specified by the workflow

        Returns:
            64-character hex string (SHA256 hash)
        """
        hash_input = f"{stage_run_id}|{file_path}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

    @property
    def short_id(self) -> str:
        """Return shortened ID for display (first 8 characters)."""
        return self.id[:8]

    def __repr__(self):
        return f"<StageFile(id={self.short_id}, file_path='{self.file_path}', size={self.size})>"
