"""Pydantic models for API payloads between control plane and workers."""
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime


# ============================================================================
# Call Creation
# ============================================================================

class CreateCallRequest(BaseModel):
    """Request to create a new call invocation."""
    caller_id: Optional[str] = Field(
        None,
        description="ID of the calling invocation (None for root calls)"
    )
    function_name: str = Field(
        ...,
        description="Name of the function to invoke"
    )
    arguments: Dict[str, Any] = Field(
        ...,
        description="Arguments to pass to the function (must include 'args' and 'kwargs' keys)"
    )


class CreateCallResponse(BaseModel):
    """Response from creating a call."""
    invocation_id: str = Field(
        ...,
        description="Unique ID for this call invocation (string of database ID)"
    )
    status: str = Field(
        ...,
        description="Current status of the call"
    )
    created: bool = Field(
        ...,
        description="Whether this was newly created (vs. already existed)"
    )


# ============================================================================
# Call Status
# ============================================================================

class CallInfo(BaseModel):
    """Information about a call invocation."""
    invocation_id: str = Field(
        ...,
        description="Unique ID for this call invocation"
    )
    function_name: str = Field(
        ...,
        description="Name of the function being invoked"
    )
    parent_invocation_id: Optional[str] = Field(
        None,
        description="ID of the parent call that invoked this one"
    )
    arguments: Dict[str, Any] = Field(
        ...,
        description="Arguments passed to the function"
    )
    status: str = Field(
        ...,
        description="Status: pending, running, completed, or failed"
    )
    created_at: str = Field(
        ...,
        description="ISO 8601 timestamp when call was created"
    )
    started_at: Optional[str] = Field(
        None,
        description="ISO 8601 timestamp when call started executing"
    )
    completed_at: Optional[str] = Field(
        None,
        description="ISO 8601 timestamp when call finished"
    )
    result: Optional[Any] = Field(
        None,
        description="Result value (only present if status is completed)"
    )
    error: Optional[str] = Field(
        None,
        description="Error message (only present if status is failed)"
    )


class GetCallsResponse(BaseModel):
    """Response containing list of calls."""
    calls: List[CallInfo] = Field(
        ...,
        description="List of call invocations matching the query"
    )


# ============================================================================
# Call Lifecycle
# ============================================================================

class StartCallRequest(BaseModel):
    """Request to start/claim a call."""
    worker_id: Optional[str] = Field(
        None,
        description="Unique identifier for the worker claiming this call"
    )


class StartCallResponse(BaseModel):
    """Response from starting a call."""
    success: bool = Field(
        ...,
        description="Whether the call was successfully claimed"
    )


class FinishCallRequest(BaseModel):
    """Request to finish a call with result or error."""
    status: str = Field(
        ...,
        description="Final status: 'completed' or 'failed'"
    )
    result: Optional[Any] = Field(
        None,
        description="Result value (required if status is completed)"
    )
    error: Optional[str] = Field(
        None,
        description="Error message with traceback (required if status is failed)"
    )

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
    success: bool = Field(
        ...,
        description="Whether the call was successfully marked as finished"
    )


# ============================================================================
# Error Response
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response."""
    error: str = Field(
        ...,
        description="Error message describing what went wrong"
    )
