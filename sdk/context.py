"""Stage execution context for file I/O operations."""
import requests
from typing import Optional
from urllib.parse import urljoin
import io


class StageContext:
    """
    Context object passed to stage functions, providing file I/O capabilities.

    This context allows stages to:
    - Read files from the repository at specific commits
    - Write output files that will be stored and associated with the stage run
    - Read files created by other stages in the workflow

    The context is automatically injected as the first argument to stage functions
    by the @stage decorator.

    Example:
        @stage
        def process_data(ctx: StageContext):
            # Read input file from repo
            data = ctx.read_file("data/input.csv")

            # Process the data
            result = process(data)

            # Write output file
            ctx.write_file("output/result.csv", result)

            return {"status": "success"}
    """

    def __init__(self, control_plane_url: str, stage_run_id: str,
                 repo_name: str, commit_hash: str):
        """
        Initialize the stage context.

        Args:
            control_plane_url: URL of the control plane server
            stage_run_id: ID of the current stage run
            repo_name: Repository name
            commit_hash: Commit hash for reading files from repo
        """
        self.control_plane_url = control_plane_url
        self.stage_run_id = stage_run_id
        self.repo_name = repo_name
        self.commit_hash = commit_hash

    def read_file(self, file_path: str, encoding: Optional[str] = 'utf-8') -> bytes | str:
        """
        Read a file from the repository at the current commit.

        This reads files that were committed to the repository, not files
        created by other stages. Use read_stage_file() for that.

        Args:
            file_path: Path to the file in the repository (e.g., "data/input.csv")
            encoding: Text encoding to use. If None, returns bytes. Default is 'utf-8'.

        Returns:
            File contents as string (if encoding specified) or bytes (if encoding is None)

        Raises:
            RuntimeError: If the file cannot be read
        """
        try:
            url = urljoin(
                self.control_plane_url,
                f'/api/repos/{self.repo_name}/blob/{self.commit_hash}/{file_path}'
            )
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            if encoding is not None:
                return response.content.decode(encoding)
            else:
                return response.content

        except requests.RequestException as e:
            raise RuntimeError(f"Failed to read file '{file_path}': {e}")

    def write_file(self, file_path: str, content: bytes | str, encoding: Optional[str] = 'utf-8'):
        """
        Write a file that will be stored and associated with this stage run.

        The file will be uploaded to the control plane's storage backend and
        linked to this stage run, making it available in the UI and to other
        stages.

        Args:
            file_path: Path for the file (e.g., "output/results.csv")
            content: File content as string or bytes
            encoding: Text encoding to use if content is string. Default is 'utf-8'.

        Raises:
            RuntimeError: If the file cannot be written
        """
        # Convert string to bytes if needed
        if isinstance(content, str):
            if encoding is None:
                raise ValueError("encoding must be specified when content is a string")
            content_bytes = content.encode(encoding)
        else:
            content_bytes = content

        try:
            url = urljoin(
                self.control_plane_url,
                f'/api/stages/{self.stage_run_id}/files'
            )

            # Send file as multipart form data
            files = {'file': (file_path, io.BytesIO(content_bytes))}
            data = {'file_path': file_path}

            response = requests.post(url, files=files, data=data, timeout=60)
            response.raise_for_status()

        except requests.RequestException as e:
            raise RuntimeError(f"Failed to write file '{file_path}': {e}")

    def read_stage_file(self, stage_file_id: str, encoding: Optional[str] = 'utf-8') -> bytes | str:
        """
        Read a file created by a stage run.

        Args:
            stage_file_id: ID of the stage file (can be obtained from stage run metadata)
            encoding: Text encoding to use. If None, returns bytes. Default is 'utf-8'.

        Returns:
            File contents as string (if encoding specified) or bytes (if encoding is None)

        Raises:
            RuntimeError: If the file cannot be read
        """
        try:
            url = urljoin(
                self.control_plane_url,
                f'/api/stage-files/{stage_file_id}/download'
            )
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            if encoding is not None:
                return response.content.decode(encoding)
            else:
                return response.content

        except requests.RequestException as e:
            raise RuntimeError(f"Failed to read stage file '{stage_file_id}': {e}")

    def list_files(self) -> list[dict]:
        """
        List all files created by this stage run.

        Returns:
            List of file metadata dictionaries with keys:
            - id: Stage file ID
            - file_path: Path of the file
            - size: Size in bytes
            - content_hash: SHA-256 hash of the content
            - created_at: ISO format timestamp

        Raises:
            RuntimeError: If files cannot be listed
        """
        try:
            url = urljoin(
                self.control_plane_url,
                f'/api/stages/{self.stage_run_id}/files'
            )
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            return response.json()['files']

        except requests.RequestException as e:
            raise RuntimeError(f"Failed to list files: {e}")
