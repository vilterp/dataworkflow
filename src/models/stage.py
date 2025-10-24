"""Stage model - represents a staging area for creating commits."""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from .base import Base


class Stage(Base):
    """A staging area for preparing a commit."""
    __tablename__ = 'stages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    repository_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)
    name = Column(String(255), nullable=False)
    base_ref = Column(String(255), nullable=False)  # e.g., 'refs/heads/main'
    description = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    committed = Column(Boolean, default=False)  # Whether this stage has been committed
    committed_at = Column(DateTime, nullable=True)
    commit_hash = Column(String(64), nullable=True)  # Hash of the commit created from this stage

    # Relationships
    files = relationship("StageFile", back_populates="stage", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Stage(id={self.id}, name='{self.name}', base_ref='{self.base_ref}', committed={self.committed})>"


class StageFile(Base):
    """A file in a staging area."""
    __tablename__ = 'stage_files'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stage_id = Column(Integer, ForeignKey('stages.id'), nullable=False)
    path = Column(String(500), nullable=False)  # File path within the repository
    blob_hash = Column(String(64), nullable=False)  # Hash of the blob containing the file content
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    stage = relationship("Stage", back_populates="files")

    def __repr__(self):
        return f"<StageFile(id={self.id}, path='{self.path}', blob_hash='{self.blob_hash}')>"
