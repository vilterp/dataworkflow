import hashlib
import boto3
from botocore.exceptions import ClientError
from typing import Optional
from src.config import Config
from .base import StorageBackend


class S3Storage(StorageBackend):
    """
    Handles storage and retrieval of blob content in S3.
    Content is stored using content-addressable storage (hash-based keys).
    """

    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
            region_name=Config.AWS_REGION
        )
        self.bucket = Config.S3_BUCKET

    def _compute_hash(self, content: bytes) -> str:
        """Compute SHA-256 hash of content"""
        return hashlib.sha256(content).hexdigest()

    def _make_s3_key(self, hash: str) -> str:
        """
        Create S3 key from hash.
        Uses git-like directory structure: first 2 chars / rest of hash
        e.g., ab/cdef123456...
        """
        return f"blobs/{hash[:2]}/{hash[2:]}"

    def store(self, content: bytes) -> tuple[str, str, int]:
        """
        Store content in S3 and return (hash, s3_key, size).

        Args:
            content: Binary content to store

        Returns:
            Tuple of (hash, s3_key, size)
        """
        hash = self._compute_hash(content)
        s3_key = self._make_s3_key(hash)
        size = len(content)

        # Check if already exists (content-addressable storage deduplication)
        if self.exists(hash):
            return hash, s3_key, size

        # Upload to S3
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=content,
                ContentType='application/octet-stream',
                Metadata={
                    'hash': hash,
                    'size': str(size)
                }
            )
        except ClientError as e:
            raise Exception(f"Failed to upload to S3: {e}")

        return hash, s3_key, size

    def retrieve(self, hash: str) -> Optional[bytes]:
        """
        Retrieve content from S3 by hash.

        Args:
            hash: SHA-256 hash of the content

        Returns:
            Binary content or None if not found
        """
        s3_key = self._make_s3_key(hash)

        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            return response['Body'].read()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise Exception(f"Failed to retrieve from S3: {e}")

    def exists(self, hash: str) -> bool:
        """
        Check if content with given hash exists in S3.

        Args:
            hash: SHA-256 hash of the content

        Returns:
            True if exists, False otherwise
        """
        s3_key = self._make_s3_key(hash)

        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise Exception(f"Failed to check S3: {e}")

    def delete(self, hash: str) -> bool:
        """
        Delete content from S3 by hash.

        Args:
            hash: SHA-256 hash of the content

        Returns:
            True if deleted, False if not found
        """
        s3_key = self._make_s3_key(hash)

        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            raise Exception(f"Failed to delete from S3: {e}")

    def get_download_url(self, hash: str, expires_in: int = 3600) -> Optional[str]:
        """
        Generate a presigned URL for downloading content.

        Args:
            hash: SHA-256 hash of the content
            expires_in: URL expiration time in seconds (default: 1 hour)

        Returns:
            Presigned URL or None if not found
        """
        if not self.exists(hash):
            return None

        s3_key = self._make_s3_key(hash)

        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': s3_key},
                ExpiresIn=expires_in
            )
            return url
        except ClientError as e:
            raise Exception(f"Failed to generate presigned URL: {e}")
