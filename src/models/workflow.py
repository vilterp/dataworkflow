"""Workflow models - represents workflow execution runs and stage runs."""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, Enum as SQLEnum
from sqlalchemy.orm import relationship
import enum
from .base import Base


class WorkflowStatus(enum.Enum):
    """Status of a workflow run."""
    PENDING = "pending"      # Waiting to be picked up by a runner
    CLAIMED = "claimed"      # Claimed by a runner but not started
    RUNNING = "running"      # Currently executing
    COMPLETED = "completed"  # Successfully completed
    FAILED = "failed"        # Failed with an error
    CANCELLED = "cancelled"  # Cancelled by user


class StageRunStatus(enum.Enum):
    """Status of a stage run within a workflow."""
    PENDING = "pending"      # Waiting to run
    RUNNING = "running"      # Currently executing
    COMPLETED = "completed"  # Successfully completed
    FAILED = "failed"        # Failed with an error
    SKIPPED = "skipped"      # Skipped due to conditional logic


class WorkflowRun(Base):
    """A workflow execution run."""
    __tablename__ = 'workflow_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    repository_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)

    # Workflow identification
    workflow_file = Column(String(500), nullable=False)  # Path to the workflow file in the repo
    commit_hash = Column(String(64), nullable=False)     # Commit hash to run workflow from

    # Execution metadata
    status = Column(SQLEnum(WorkflowStatus), default=WorkflowStatus.PENDING, nullable=False)
    runner_id = Column(String(255), nullable=True)       # ID of the runner that claimed this workflow
    claimed_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Trigger information
    triggered_by = Column(String(255), nullable=True)    # User or event that triggered this run
    trigger_event = Column(String(100), nullable=True)   # e.g., "manual", "commit", "schedule"

    # Results
    error_message = Column(Text, nullable=True)          # Error message if failed

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    stage_runs = relationship("StageRun", back_populates="workflow_run", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<WorkflowRun(id={self.id}, workflow_file='{self.workflow_file}', status='{self.status.value}')>"


class StageRun(Base):
    """
    A stage execution / call invocation.

    Supports both legacy workflow_run_id mode and new distributed invocation mode.
    In distributed mode, workflow_run_id can be NULL, and id serves as the invocation_id.
    """
    __tablename__ = 'stage_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Workflow run support (optional for distributed mode)
    workflow_run_id = Column(Integer, ForeignKey('workflow_runs.id'), nullable=True)
    parent_stage_run_id = Column(Integer, ForeignKey('stage_runs.id'), nullable=True)  # Parent call ID

    # New distributed invocation support
    arguments = Column(Text, nullable=True)  # JSON-encoded function arguments

    # Stage identification
    stage_name = Column(String(255), nullable=False)     # Name of the stage function

    # Execution metadata
    status = Column(SQLEnum(StageRunStatus), default=StageRunStatus.PENDING, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Results
    result_value = Column(Text, nullable=True)           # JSON-encoded result from stage execution
    error_message = Column(Text, nullable=True)          # Error message if failed

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    workflow_run = relationship("WorkflowRun", back_populates="stage_runs")
    parent_stage_run = relationship("StageRun", remote_side=[id], backref="child_stage_runs")

    def __repr__(self):
        return f"<StageRun(id={self.id}, stage_name='{self.stage_name}', status='{self.status.value}')>"
