from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class Ref(Base):
    """
    Represents a reference (branch or tag).
    Similar to Git refs, this provides named pointers to commits.
    """
    __tablename__ = 'refs'

    # Composite primary key: repository + ref id
    repository_id = Column(Integer, ForeignKey('repositories.id'), primary_key=True)
    id = Column(String(255), primary_key=True)  # e.g., 'refs/heads/main', 'refs/tags/v1.0'

    # Commit this ref points to (composite foreign key)
    commit_hash = Column(String(64), nullable=False)

    # Metadata
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Table args for composite foreign key
    __table_args__ = (
        ForeignKeyConstraint(['repository_id', 'commit_hash'], ['commits.repository_id', 'commits.hash']),
    )

    # Relationships
    repository = relationship("Repository")
    commit = relationship("Commit", back_populates="refs", foreign_keys=[repository_id, commit_hash])

    def __repr__(self):
        return f"<Ref(id='{self.id}', commit='{self.commit_hash[:8]}...')>"

    @property
    def name(self):
        """Get short name (e.g., 'main' from 'refs/heads/main')"""
        return self.id.split('/')[-1]

    @property
    def is_branch(self):
        """Check if this is a branch ref"""
        return self.id.startswith('refs/heads/')

    @property
    def is_tag(self):
        """Check if this is a tag ref"""
        return self.id.startswith('refs/tags/')
