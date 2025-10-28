from sqlalchemy import Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class Blob(Base):
    """
    Represents a blob (file content) stored in S3.
    Similar to Git blobs, this stores the actual file content.
    """
    __tablename__ = 'blobs'

    # Composite primary key: repository + hash
    repository_id = Column(Integer, ForeignKey('repositories.id'), primary_key=True)
    hash = Column(String(64), primary_key=True)

    # Size in bytes
    size = Column(Integer, nullable=False)

    # S3 key where content is stored
    s3_key = Column(String(255), nullable=False, unique=True)

    # Commit that first created this blob
    created_by_commit_hash = Column(String(64), nullable=True)

    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    repository = relationship("Repository")

    def __repr__(self):
        return f"<Blob(hash='{self.hash[:8]}...', size={self.size})>"
