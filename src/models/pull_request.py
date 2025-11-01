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
    checks = relationship("PullRequestCheck", back_populates="pull_request", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<PullRequest(#{self.number}, '{self.title}', {self.status.value})>"


class PullRequestCheckStatus(enum.Enum):
    """Status of a pull request check"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"


class PullRequestCheck(Base):
    """
    Represents a check that must pass before a PR can be merged.
    Links to a StageRun that validates the PR.
    """
    __tablename__ = 'pull_request_checks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    pull_request_id = Column(Integer, ForeignKey('pull_requests.id'), nullable=False)

    # Name of the check (from the PR check configuration file)
    check_name = Column(String(255), nullable=False)

    # Reference to the stage run that executes this check
    stage_run_id = Column(String(64), ForeignKey('stage_runs.id'), nullable=True)

    # Status (can be derived from stage_run, but stored for easier querying)
    status = Column(SQLEnum(PullRequestCheckStatus), nullable=False, default=PullRequestCheckStatus.PENDING)

    # Additional context
    error_message = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    pull_request = relationship("PullRequest", back_populates="checks")
    stage_run = relationship("StageRun")

    def __repr__(self):
        return f"<PullRequestCheck(name='{self.check_name}', status={self.status.value})>"
