"""Workflow models - represents stage runs."""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, Enum as SQLEnum
from sqlalchemy.orm import relationship
import enum
from .base import Base


class StageRunStatus(enum.Enum):
    """Status of a stage run within a workflow."""
    PENDING = "pending"      # Waiting to run
    RUNNING = "running"      # Currently executing
    COMPLETED = "completed"  # Successfully completed
    FAILED = "failed"        # Failed with an error
    SKIPPED = "skipped"      # Skipped due to conditional logic


class StageRun(Base):
    """
    A stage execution / call invocation.
    """
    __tablename__ = 'stage_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Parent stage support for nested calls
    parent_stage_run_id = Column(Integer, ForeignKey('stage_runs.id'), nullable=True)  # Parent call ID

    # New distributed invocation support
    arguments = Column(Text, nullable=False)  # JSON-encoded function arguments
    repo_name = Column(String(255), nullable=False)  # Repository name
    commit_hash = Column(String(64), nullable=False)  # Git commit hash
    workflow_file = Column(String(500), nullable=False)  # Path to workflow file

    # Trigger information (moved from WorkflowRun)
    triggered_by = Column(String(255), nullable=True)    # User or event that triggered this run
    trigger_event = Column(String(100), nullable=True)   # e.g., "manual", "commit", "schedule"

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
    parent_stage_run = relationship("StageRun", remote_side=[id], backref="child_stage_runs")

    def __repr__(self):
        return f"<StageRun(id={self.id}, stage_name='{self.stage_name}', status='{self.status.value}')>"
