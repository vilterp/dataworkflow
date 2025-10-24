from abc import ABC, abstractmethod
from typing import Optional


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.
    Implementations can use S3, filesystem, or any other storage system.
    """

    @abstractmethod
    def store(self, content: bytes) -> tuple[str, str, int]:
        """
        Store content and return (hash, storage_key, size).

        Args:
            content: Binary content to store

        Returns:
            Tuple of (hash, storage_key, size)
        """
        pass

    @abstractmethod
    def retrieve(self, hash: str) -> Optional[bytes]:
        """
        Retrieve content by hash.

        Args:
            hash: SHA-256 hash of the content

        Returns:
            Binary content or None if not found
        """
        pass

    @abstractmethod
    def exists(self, hash: str) -> bool:
        """
        Check if content with given hash exists.

        Args:
            hash: SHA-256 hash of the content

        Returns:
            True if exists, False otherwise
        """
        pass

    @abstractmethod
    def delete(self, hash: str) -> bool:
        """
        Delete content by hash.

        Args:
            hash: SHA-256 hash of the content

        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    def get_download_url(self, hash: str, expires_in: int = 3600) -> Optional[str]:
        """
        Generate a URL for downloading content.

        Args:
            hash: SHA-256 hash of the content
            expires_in: URL expiration time in seconds (default: 1 hour)

        Returns:
            Download URL or None if not found
        """
        pass
