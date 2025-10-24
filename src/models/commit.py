from sqlalchemy import Column, String, Text, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class Commit(Base):
    """
    Represents a commit (snapshot in time).
    Similar to Git commits, this captures a tree state with metadata.
    """
    __tablename__ = 'commits'

    # SHA-256 hash of commit content
    hash = Column(String(64), primary_key=True)

    # Tree this commit points to
    tree_hash = Column(String(64), ForeignKey('trees.hash'), nullable=False)

    # Parent commit (null for initial commit)
    parent_hash = Column(String(64), ForeignKey('commits.hash'), nullable=True)

    # Commit metadata
    author = Column(String(255), nullable=False)
    author_email = Column(String(255), nullable=False)
    message = Column(Text, nullable=False)

    # Timestamps
    committed_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    tree = relationship("Tree", foreign_keys=[tree_hash])
    parent = relationship("Commit", remote_side=[hash], foreign_keys=[parent_hash])
    refs = relationship("Ref", back_populates="commit")

    def __repr__(self):
        return f"<Commit(hash='{self.hash[:8]}...', message='{self.message[:50]}...')>"
