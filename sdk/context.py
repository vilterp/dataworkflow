"""Thread-local context for tracking stage execution stack."""
import threading
from typing import Optional
from dataclasses import dataclass


@dataclass
class StageStackFrame:
    """Represents a stage in the execution stack."""
    workflow_id: int
    stage_name: str
    stage_run_id: int
    parent: Optional['StageStackFrame'] = None


class StageContext:
    """Thread-local context for tracking stage execution."""

    def __init__(self):
        self._local = threading.local()

    def push_frame(self, workflow_id: int, stage_name: str, stage_run_id: int) -> StageStackFrame:
        """
        Push a new stage frame onto the stack.

        Args:
            workflow_id: The workflow run ID
            stage_name: The name of the stage being executed
            stage_run_id: The stage run ID from the database

        Returns:
            The new stack frame
        """
        current = self.get_current_frame()
        frame = StageStackFrame(
            workflow_id=workflow_id,
            stage_name=stage_name,
            stage_run_id=stage_run_id,
            parent=current
        )
        self._local.current_frame = frame
        return frame

    def pop_frame(self) -> Optional[StageStackFrame]:
        """
        Pop the current frame from the stack.

        Returns:
            The popped frame, or None if stack was empty
        """
        current = self.get_current_frame()
        if current:
            self._local.current_frame = current.parent
        return current

    def get_current_frame(self) -> Optional[StageStackFrame]:
        """
        Get the current stack frame without modifying the stack.

        Returns:
            The current frame, or None if stack is empty
        """
        return getattr(self._local, 'current_frame', None)

    def get_current_stage_run_id(self) -> Optional[int]:
        """
        Get the stage run ID of the current stage.

        Returns:
            The current stage's stage_run_id, or None if no current stage
        """
        current = self.get_current_frame()
        return current.stage_run_id if current else None

    def get_parent_stage_run_id(self) -> Optional[int]:
        """
        Get the stage run ID of the parent stage.

        Returns:
            The parent's stage_run_id, or None if no parent
        """
        current = self.get_current_frame()
        if current and current.parent:
            return current.parent.stage_run_id
        return None

    def clear(self):
        """Clear the entire stack."""
        self._local.current_frame = None


# Global singleton instance
_context = StageContext()


def get_stage_context() -> StageContext:
    """Get the global stage context instance."""
    return _context
