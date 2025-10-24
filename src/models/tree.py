from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, ForeignKeyConstraint, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
from .base import Base


class EntryType(enum.Enum):
    """Type of tree entry"""
    BLOB = "blob"
    TREE = "tree"


class Tree(Base):
    """
    Represents a tree (directory) structure.
    Similar to Git trees, this represents a directory snapshot.
    """
    __tablename__ = 'trees'

    # Composite primary key: repository + hash
    repository_id = Column(Integer, ForeignKey('repositories.id'), primary_key=True)
    hash = Column(String(64), primary_key=True)

    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    repository = relationship("Repository")
    entries = relationship("TreeEntry", back_populates="tree", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Tree(hash='{self.hash[:8]}...', entries={len(self.entries)})>"


class TreeEntry(Base):
    """
    Represents an entry in a tree (a file or subdirectory).
    """
    __tablename__ = 'tree_entries'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Parent tree (composite foreign key)
    repository_id = Column(Integer, nullable=False)
    tree_hash = Column(String(64), nullable=False)
    __table_args__ = (
        ForeignKeyConstraint(['repository_id', 'tree_hash'], ['trees.repository_id', 'trees.hash']),
    )

    # Entry name (filename or directory name)
    name = Column(String(255), nullable=False)

    # Type (blob or tree)
    type = Column(Enum(EntryType), nullable=False)

    # Hash of the blob or tree this entry points to
    hash = Column(String(64), nullable=False)

    # File mode (e.g., 100644 for regular file, 040000 for directory)
    mode = Column(String(6), nullable=False, default='100644')

    # Relationships
    tree = relationship("Tree", back_populates="entries", foreign_keys=[repository_id, tree_hash])

    def __repr__(self):
        return f"<TreeEntry(name='{self.name}', type={self.type.value}, hash='{self.hash[:8]}...')>"
