from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
from .base import Base


class PullRequestStatus(enum.Enum):
    """Status of a pull request"""
    OPEN = "open"
    CLOSED = "closed"
    MERGED = "merged"


class PullRequest(Base):
    """
    Represents a pull request to merge changes from one branch to another.
    Similar to GitHub pull requests.
    """
    __tablename__ = 'pull_requests'

    id = Column(Integer, primary_key=True, autoincrement=True)
    repository_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)

    # Pull request number within the repository (1, 2, 3, etc.)
    number = Column(Integer, nullable=False)

    # Branch names
    base_branch = Column(String(255), nullable=False)  # Target branch (e.g., 'main')
    head_branch = Column(String(255), nullable=False)  # Source branch (e.g., 'feature-xyz')

    # Metadata
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    author = Column(String(255), nullable=False)
    author_email = Column(String(255), nullable=False)

    # Status
    status = Column(SQLEnum(PullRequestStatus), nullable=False, default=PullRequestStatus.OPEN)

    # Merge information (populated when merged)
    merge_commit_hash = Column(String(64), nullable=True)
    merged_at = Column(DateTime(timezone=True), nullable=True)
    merged_by = Column(String(255), nullable=True)
    merged_by_email = Column(String(255), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    closed_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    repository = relationship("Repository", back_populates="pull_requests")
    comments = relationship("PullRequestComment", back_populates="pull_request", cascade="all, delete-orphan", order_by="PullRequestComment.created_at")

    def __repr__(self):
        return f"<PullRequest(#{self.number}, '{self.title}', {self.status.value})>"


class PullRequestComment(Base):
    """
    Represents a comment on a pull request.
    Similar to GitHub PR comments.
    """
    __tablename__ = 'pull_request_comments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    pull_request_id = Column(Integer, ForeignKey('pull_requests.id'), nullable=False)

    # Comment content
    body = Column(Text, nullable=False)

    # Author information
    author = Column(String(255), nullable=False)
    author_email = Column(String(255), nullable=False)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    pull_request = relationship("PullRequest", back_populates="comments")

    def __repr__(self):
        return f"<PullRequestComment(pr=#{self.pull_request_id}, author='{self.author}')>"
