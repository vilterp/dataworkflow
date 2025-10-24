from sqlalchemy import Column, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class Ref(Base):
    """
    Represents a reference (branch or tag).
    Similar to Git refs, this provides named pointers to commits.
    """
    __tablename__ = 'refs'

    id = Column(String(255), primary_key=True)  # e.g., 'refs/heads/main', 'refs/tags/v1.0'

    # Commit this ref points to
    commit_hash = Column(String(64), ForeignKey('commits.hash'), nullable=False)

    # Metadata
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    commit = relationship("Commit", back_populates="refs")

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
