import hashlib
from pathlib import Path
from typing import Optional
from .base import StorageBackend


class FilesystemStorage(StorageBackend):
    """
    Filesystem-based storage backend.
    Stores blobs in a local directory with git-like structure.
    """

    def __init__(self, base_path: str = '.dataworkflow/objects'):
        """
        Initialize filesystem storage.

        Args:
            base_path: Base directory for storing objects
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _compute_hash(self, content: bytes) -> str:
        """Compute SHA-256 hash of content"""
        return hashlib.sha256(content).hexdigest()

    def _make_path(self, hash: str) -> Path:
        """
        Create filesystem path from hash.
        Uses git-like directory structure: base/first2/rest
        e.g., .dataworkflow/objects/ab/cdef123456...
        """
        return self.base_path / hash[:2] / hash[2:]

    def store(self, content: bytes) -> tuple[str, str, int]:
        """
        Store content in filesystem and return (hash, path, size).

        Args:
            content: Binary content to store

        Returns:
            Tuple of (hash, storage_key, size)
        """
        hash = self._compute_hash(content)
        path = self._make_path(hash)
        size = len(content)

        # Check if already exists (content-addressable storage deduplication)
        if path.exists():
            return hash, str(path), size

        # Create parent directory
        path.parent.mkdir(parents=True, exist_ok=True)

        # Write content
        try:
            path.write_bytes(content)
        except Exception as e:
            raise Exception(f"Failed to write to filesystem: {e}")

        return hash, str(path), size

    def retrieve(self, hash: str) -> Optional[bytes]:
        """
        Retrieve content from filesystem by hash.

        Args:
            hash: SHA-256 hash of the content

        Returns:
            Binary content or None if not found
        """
        path = self._make_path(hash)

        try:
            if not path.exists():
                return None
            return path.read_bytes()
        except Exception as e:
            raise Exception(f"Failed to read from filesystem: {e}")

    def exists(self, hash: str) -> bool:
        """
        Check if content with given hash exists in filesystem.

        Args:
            hash: SHA-256 hash of the content

        Returns:
            True if exists, False otherwise
        """
        return self._make_path(hash).exists()

    def delete(self, hash: str) -> bool:
        """
        Delete content from filesystem by hash.

        Args:
            hash: SHA-256 hash of the content

        Returns:
            True if deleted, False if not found
        """
        path = self._make_path(hash)

        if not path.exists():
            return False

        try:
            path.unlink()
            # Try to remove empty parent directories
            try:
                path.parent.rmdir()
            except OSError:
                pass  # Directory not empty
            return True
        except Exception as e:
            raise Exception(f"Failed to delete from filesystem: {e}")

    def get_download_url(self, hash: str, expires_in: int = 3600) -> Optional[str]:
        """
        Get filesystem path for downloading content.

        Args:
            hash: SHA-256 hash of the content
            expires_in: Not used for filesystem (included for interface compatibility)

        Returns:
            Filesystem path or None if not found
        """
        if not self.exists(hash):
            return None

        path = self._make_path(hash)
        return f"file://{path.absolute()}"
