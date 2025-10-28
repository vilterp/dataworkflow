"""Repository model - represents a Git-like repository."""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Text, DateTime
from .base import Base


class Repository(Base):
    """A repository that contains commits, trees, and blobs."""
    __tablename__ = 'repositories'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=True)
    main_branch = Column(String(255), nullable=False, default='main')
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return f"<Repository(id={self.id}, name='{self.name}')>"
