"""
Path segment classes for virtual file system paths.

Provides path segment types that distinguish between regular tree nodes,
stage runs, and files in both base git data and derived workflow data.
"""
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod


class SegmentType(Enum):
    """Type of path segment in a VFS path."""
    TREE = "tree"           # Normal tree/directory (base git data)
    STAGERUN = "stagerun"   # Stage run (derived data)
    FILE = "file"           # File (can be base or derived)


@dataclass
class PathSegment(ABC):
    """Base class for path segments in a VFS path."""
    name: str  # Name of this path segment

    @property
    @abstractmethod
    def segment_type(self) -> SegmentType:
        """Get the type of this segment."""
        pass


@dataclass
class TreeSegment(PathSegment):
    """A normal tree/directory path segment (base git data)."""

    @property
    def segment_type(self) -> SegmentType:
        return SegmentType.TREE


@dataclass
class StageRunSegment(PathSegment):
    """A stage run path segment (derived data)."""
    status: str  # Status of the stage run (COMPLETED, FAILED, etc.)

    @property
    def segment_type(self) -> SegmentType:
        return SegmentType.STAGERUN


@dataclass
class FileSegment(PathSegment):
    """A file path segment (final segment in the path)."""
    is_derived: bool  # True if this is a derived file (stage output)

    @property
    def segment_type(self) -> SegmentType:
        return SegmentType.FILE
