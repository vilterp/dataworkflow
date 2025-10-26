"""Workflow models - represents stage runs."""
from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, ForeignKey, Text, Enum as SQLEnum
from sqlalchemy.orm import relationship
import enum
import json
import hashlib
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

    Content-addressable: The ID is a hash of the execution parameters,
    allowing automatic deduplication of identical invocations.
    """
    __tablename__ = 'stage_runs'

    # Content-addressable ID (hash of execution parameters)
    id = Column(String(64), primary_key=True)

    # Parent stage support for nested calls
    parent_stage_run_id = Column(String(64), ForeignKey('stage_runs.id'), nullable=True)

    # Execution parameters (used to compute the content hash)
    arguments = Column(Text, nullable=False)  # JSON-encoded function arguments
    repo_name = Column(String(255), nullable=False)  # Repository name
    commit_hash = Column(String(64), nullable=False)  # Git commit hash
    workflow_file = Column(String(500), nullable=False)  # Path to workflow file
    stage_name = Column(String(255), nullable=False)     # Name of the stage function

    # Trigger information (only for root stages)
    triggered_by = Column(String(255), nullable=True)    # User or event that triggered this run
    trigger_event = Column(String(100), nullable=True)   # e.g., "manual", "commit", "schedule"

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

    @staticmethod
    def compute_id(
        parent_stage_run_id: str | None,
        commit_hash: str,
        workflow_file: str,
        stage_name: str,
        arguments: str
    ) -> str:
        """
        Compute content-addressable ID for a stage run.

        The ID is a SHA256 hash of the execution parameters, ensuring that
        identical invocations have the same ID.

        Args:
            parent_stage_run_id: Parent stage run ID (None for root stages)
            commit_hash: Git commit hash
            workflow_file: Path to workflow file
            stage_name: Name of the stage function
            arguments: JSON-encoded arguments (must be deterministically serialized)

        Returns:
            64-character hex string (SHA256 hash)
        """
        # Parse and re-serialize arguments to ensure deterministic JSON
        args_dict = json.loads(arguments)
        canonical_args = json.dumps(args_dict, sort_keys=True, separators=(',', ':'))

        # Compute hash of all execution parameters
        hash_input = f"{parent_stage_run_id or ''}|{commit_hash}|{workflow_file}|{stage_name}|{canonical_args}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

    def __repr__(self):
        return f"<StageRun(id={self.id[:12]}..., stage_name='{self.stage_name}', status='{self.status.value}')>"
