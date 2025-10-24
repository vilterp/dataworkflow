from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class Commit(Base):
    """
    Represents a commit (snapshot in time).
    Similar to Git commits, this captures a tree state with metadata.
    """
    __tablename__ = 'commits'

    # Composite primary key: repository + hash
    repository_id = Column(Integer, ForeignKey('repositories.id'), primary_key=True)
    hash = Column(String(64), primary_key=True)

    # Tree this commit points to (composite foreign key)
    tree_hash = Column(String(64), nullable=False)

    # Parent commit (null for initial commit, composite foreign key)
    parent_hash = Column(String(64), nullable=True)

    # Commit metadata
    author = Column(String(255), nullable=False)
    author_email = Column(String(255), nullable=False)
    message = Column(Text, nullable=False)

    # Timestamps
    committed_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Table args for composite foreign keys
    __table_args__ = (
        ForeignKeyConstraint(['repository_id', 'tree_hash'], ['trees.repository_id', 'trees.hash']),
        ForeignKeyConstraint(['repository_id', 'parent_hash'], ['commits.repository_id', 'commits.hash']),
    )

    # Relationships
    repository = relationship("Repository")
    tree = relationship("Tree", foreign_keys=[repository_id, tree_hash])
    parent = relationship("Commit", remote_side=[repository_id, hash], foreign_keys=[repository_id, parent_hash])
    refs = relationship("Ref", back_populates="commit")

    def __repr__(self):
        return f"<Commit(hash='{self.hash[:8]}...', message='{self.message[:50]}...')>"
