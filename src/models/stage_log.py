"""Stage log model - represents log lines from stage runs."""
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from .base import Base


class StageLogLine(Base):
    """
    A log line emitted during a stage run's execution.

    Log lines are captured from the stage's stdout/stderr and stored
    with timestamps and sequential indices for ordering and tailing.
    """
    __tablename__ = 'stage_log_lines'

    # Auto-incrementing ID for ordering
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Reference to the stage run that created this log line
    stage_run_id = Column(String(64), ForeignKey('stage_runs.id'), nullable=False, index=True)

    # Sequential index within the stage run (0-based)
    log_line_index = Column(Integer, nullable=False, index=True)

    # Timestamp when the log line was emitted
    timestamp = Column(DateTime, nullable=False)

    # Log content (text line)
    log_contents = Column(Text, nullable=False)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    stage_run = relationship("StageRun", backref="log_lines")

    def __repr__(self):
        preview = self.log_contents[:50] + '...' if len(self.log_contents) > 50 else self.log_contents
        return f"<StageLogLine(stage_run_id={self.stage_run_id[:8]}..., index={self.log_line_index}, content='{preview}')>"
