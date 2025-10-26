"""Pydantic models for API payloads between control plane and workers."""
from typing import Optional, Dict, Any, List
from pydantic import BaseModel
from datetime import datetime


# ============================================================================
# Call Creation
# ============================================================================

class CreateCallRequest(BaseModel):
    """Request to create a new call invocation."""

    caller_id: Optional[str]
    """ID of the calling invocation (None for root calls)"""

    function_name: str
    """Name of the function to invoke"""

    arguments: Dict[str, Any]
    """Arguments to pass (must include 'args' and 'kwargs' keys)"""

    repo_name: str
    """Name of the repository containing the workflow code"""

    commit_hash: str
    """Git commit hash to load the workflow from"""

    workflow_file: str
    """Path to the workflow file in the repo"""


class CreateCallResponse(BaseModel):
    """Response from creating a call."""

    invocation_id: str
    """Unique ID for this call (string of database ID)"""

    status: str
    """Current status of the call"""

    created: bool
    """Whether this was newly created (vs. already existed)"""


# ============================================================================
# Call Status
# ============================================================================

class CallInfo(BaseModel):
    """Information about a call invocation."""

    invocation_id: str
    """Unique ID for this call invocation"""

    function_name: str
    """Name of the function being invoked"""

    parent_invocation_id: Optional[str]
    """ID of the parent call that invoked this one"""

    arguments: Dict[str, Any]
    """Arguments passed to the function"""

    repo_name: str
    """Repository containing the workflow code"""

    commit_hash: str
    """Commit hash to load workflow from"""

    workflow_file: str
    """Path to workflow file in repo"""

    status: str
    """Status: pending, running, completed, or failed"""

    created_at: str
    """ISO 8601 timestamp when call was created"""

    started_at: Optional[str] = None
    """ISO 8601 timestamp when call started executing"""

    completed_at: Optional[str] = None
    """ISO 8601 timestamp when call finished"""

    result: Optional[Any] = None
    """Result value (only present if status is completed)"""

    error: Optional[str] = None
    """Error message (only present if status is failed)"""


class GetCallsResponse(BaseModel):
    """Response containing list of calls."""

    calls: List[CallInfo]
    """List of call invocations matching the query"""


# ============================================================================
# Call Lifecycle
# ============================================================================

class StartCallRequest(BaseModel):
    """Request to start/claim a call."""

    worker_id: Optional[str] = None
    """Unique identifier for the worker claiming this call"""


class StartCallResponse(BaseModel):
    """Response from starting a call."""

    success: bool
    """Whether the call was successfully claimed"""


class FinishCallRequest(BaseModel):
    """Request to finish a call with result or error."""

    status: str
    """Final status: 'completed' or 'failed'"""

    result: Optional[Any] = None
    """Result value (required if status is completed)"""

    error: Optional[str] = None
    """Error message with traceback (required if status is failed)"""

    def validate_status(self):
        """Validate that result/error match the status."""
        if self.status == 'completed' and self.result is None:
            raise ValueError("result is required when status is 'completed'")
        if self.status == 'failed' and self.error is None:
            raise ValueError("error is required when status is 'failed'")
        if self.status not in ['completed', 'failed']:
            raise ValueError("status must be 'completed' or 'failed'")


class FinishCallResponse(BaseModel):
    """Response from finishing a call."""

    success: bool
    """Whether the call was successfully marked as finished"""


# ============================================================================
# Stage Files
# ============================================================================

class StageFileInfo(BaseModel):
    """Information about a file created by a stage run."""

    id: str
    """Unique ID for this stage file"""

    file_path: str
    """Path of the file as specified by the workflow"""

    size: int
    """File size in bytes"""

    content_hash: str
    """SHA-256 hash of the file contents"""

    created_at: str
    """ISO 8601 timestamp when file was created"""


class CreateStageFileResponse(BaseModel):
    """Response from creating a stage file."""

    file_id: str
    """Unique ID for the created file"""

    file_path: str
    """Path of the file"""

    size: int
    """File size in bytes"""

    content_hash: str
    """SHA-256 hash of the file contents"""

    created: Optional[bool] = None
    """Whether this was newly created (vs. updated)"""

    updated: Optional[bool] = None
    """Whether this was updated (vs. newly created)"""


class ListStageFilesResponse(BaseModel):
    """Response containing list of stage files."""

    files: List[StageFileInfo]
    """List of files created by the stage run"""


# ============================================================================
# Error Response
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response."""

    error: str
    """Error message describing what went wrong"""
