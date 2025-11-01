"""
Core operations for pull requests.
"""

from typing import Optional, List, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.models import (
    Repository as RepositoryModel, PullRequest, PullRequestStatus, PullRequestCheck,
    PullRequestCheckStatus, Ref, Commit, StageRun, StageRunStatus
)
from src.core.pr_checks_config import load_pr_checks_config, PR_CHECKS_CONFIG_FILE
from src.core import Repository


def get_next_pr_number(session: Session, repository_id: int) -> int:
    """Get the next PR number for a repository."""
    max_number = session.query(func.max(PullRequest.number)).filter(
        PullRequest.repository_id == repository_id
    ).scalar()
    return (max_number or 0) + 1


def create_pull_request(
    session: Session,
    repository_id: int,
    base_branch: str,
    head_branch: str,
    title: str,
    description: Optional[str],
    author: str,
    author_email: str
) -> PullRequest:
    """
    Create a new pull request.

    Args:
        session: Database session
        repository_id: ID of the repository
        base_branch: Target branch (e.g., 'main')
        head_branch: Source branch (e.g., 'feature-xyz')
        title: PR title
        description: PR description
        author: Author name
        author_email: Author email

    Returns:
        The created PullRequest object
    """
    number = get_next_pr_number(session, repository_id)

    pr = PullRequest(
        repository_id=repository_id,
        number=number,
        base_branch=base_branch,
        head_branch=head_branch,
        title=title,
        description=description,
        author=author,
        author_email=author_email,
        status=PullRequestStatus.OPEN
    )
    session.add(pr)
    session.flush()  # Get the PR ID

    # Load PR checks configuration and create checks
    dispatch_pr_checks(session, pr)

    return pr


def dispatch_pr_checks(session: Session, pr: PullRequest) -> List[PullRequestCheck]:
    """
    Dispatch checks for a pull request based on the repository's PR checks configuration.

    Args:
        session: Database session
        pr: Pull request to dispatch checks for

    Returns:
        List of created PullRequestCheck objects
    """
    from src.app import get_storage

    repo_model = session.query(RepositoryModel).get(pr.repository_id)
    if not repo_model:
        return []

    # Create Repository instance
    storage = get_storage()
    repo = Repository(session, storage, repo_model.id)

    # Try to load PR checks config from the base branch
    base_ref = session.query(Ref).filter(
        Ref.repository_id == pr.repository_id,
        Ref.id == f"refs/heads/{pr.base_branch}"
    ).first()

    if not base_ref:
        # No base branch found, can't load config
        return []

    # Try to get the PR checks config file
    try:
        # Get the commit's tree
        commit = repo.get_commit(base_ref.commit_hash)
        if not commit:
            return []

        # Get the blob hash for the config file
        blob_hash = repo.get_blob_hash_from_path(commit.tree_hash, PR_CHECKS_CONFIG_FILE)
        if not blob_hash:
            # No config file, no checks to run
            return []

        # Get the blob content
        config_bytes = repo.get_blob_content(blob_hash)
        if not config_bytes:
            return []

        config_content = config_bytes.decode('utf-8')
        config = load_pr_checks_config(config_content)
    except (ValueError, UnicodeDecodeError):
        # Invalid config or file not found
        return []

    # Create checks for each configured check
    checks = []
    for check_config in config.checks:
        check = PullRequestCheck(
            pull_request_id=pr.id,
            check_name=check_config.name,
            status=PullRequestCheckStatus.PENDING
        )
        session.add(check)
        checks.append(check)

    session.flush()

    # TODO: Actually dispatch the stage runs
    # This would involve creating StageRun entries and triggering execution
    # For now, we just create the check records

    return checks


def update_pr_check_from_stage_run(
    session: Session,
    check: PullRequestCheck,
    stage_run: StageRun
) -> None:
    """
    Update a PR check status based on a stage run.

    Args:
        session: Database session
        check: Pull request check to update
        stage_run: Stage run to get status from
    """
    check.stage_run_id = stage_run.id

    # Map stage run status to check status
    status_mapping = {
        StageRunStatus.PENDING: PullRequestCheckStatus.PENDING,
        StageRunStatus.RUNNING: PullRequestCheckStatus.RUNNING,
        StageRunStatus.COMPLETED: PullRequestCheckStatus.SUCCESS,
        StageRunStatus.FAILED: PullRequestCheckStatus.FAILURE,
        StageRunStatus.SKIPPED: PullRequestCheckStatus.SKIPPED,
    }
    check.status = status_mapping[stage_run.status]

    if stage_run.started_at:
        check.started_at = stage_run.started_at
    if stage_run.completed_at:
        check.completed_at = stage_run.completed_at
    if stage_run.error_message:
        check.error_message = stage_run.error_message


def can_merge_pr(pr: PullRequest) -> Tuple[bool, Optional[str]]:
    """
    Check if a pull request can be merged.

    Args:
        pr: Pull request to check

    Returns:
        Tuple of (can_merge, reason) where reason is None if can merge
    """
    if pr.status != PullRequestStatus.OPEN:
        return False, f"Pull request is {pr.status.value}"

    # Check if all required checks have passed
    required_checks = [c for c in pr.checks]
    if not required_checks:
        # No checks configured, can merge
        return True, None

    pending_checks = [c for c in required_checks if c.status == PullRequestCheckStatus.PENDING]
    if pending_checks:
        return False, f"{len(pending_checks)} check(s) still pending"

    running_checks = [c for c in required_checks if c.status == PullRequestCheckStatus.RUNNING]
    if running_checks:
        return False, f"{len(running_checks)} check(s) still running"

    failed_checks = [c for c in required_checks if c.status == PullRequestCheckStatus.FAILURE]
    if failed_checks:
        check_names = ", ".join(c.check_name for c in failed_checks)
        return False, f"Check(s) failed: {check_names}"

    return True, None


def merge_pull_request(
    session: Session,
    pr: PullRequest,
    merged_by: str,
    merged_by_email: str
) -> Tuple[bool, Optional[str]]:
    """
    Merge a pull request.

    Args:
        session: Database session
        pr: Pull request to merge
        merged_by: Name of user merging
        merged_by_email: Email of user merging

    Returns:
        Tuple of (success, error_message)
    """
    can_merge, reason = can_merge_pr(pr)
    if not can_merge:
        return False, reason

    # Use Repository method to perform the merge
    from src.app import get_storage
    storage = get_storage()
    repo = Repository(session, storage, pr.repository_id)

    success, error = repo.merge_branches(pr.base_branch, pr.head_branch)
    if not success:
        return False, error

    # Get the head ref to capture the merge commit hash
    head_ref = session.query(Ref).filter(
        Ref.repository_id == pr.repository_id,
        Ref.id == f"refs/heads/{pr.head_branch}"
    ).first()

    # Update PR status
    pr.status = PullRequestStatus.MERGED
    pr.merge_commit_hash = head_ref.commit_hash if head_ref else None
    pr.merged_at = datetime.now(timezone.utc)
    pr.merged_by = merged_by
    pr.merged_by_email = merged_by_email

    session.flush()
    return True, None


def close_pull_request(session: Session, pr: PullRequest) -> None:
    """Close a pull request without merging."""
    pr.status = PullRequestStatus.CLOSED
    pr.closed_at = datetime.now(timezone.utc)
    session.flush()


def reopen_pull_request(session: Session, pr: PullRequest) -> Tuple[bool, Optional[str]]:
    """
    Reopen a closed pull request.

    Args:
        session: Database session
        pr: Pull request to reopen

    Returns:
        Tuple of (success, error_message)
    """
    if pr.status == PullRequestStatus.MERGED:
        return False, "Cannot reopen a merged pull request"

    pr.status = PullRequestStatus.OPEN
    pr.closed_at = None
    session.flush()
    return True, None


def get_pr_commits(session: Session, pr: PullRequest) -> List[Commit]:
    """
    Get all commits in a pull request (commits in head branch not in base branch).

    Args:
        session: Database session
        pr: Pull request

    Returns:
        List of commits
    """
    # Get base and head commits
    base_ref = session.query(Ref).filter(
        Ref.repository_id == pr.repository_id,
        Ref.id == f"refs/heads/{pr.base_branch}"
    ).first()

    head_ref = session.query(Ref).filter(
        Ref.repository_id == pr.repository_id,
        Ref.id == f"refs/heads/{pr.head_branch}"
    ).first()

    if not base_ref or not head_ref:
        return []

    # Use Repository method to get commits between base and head
    from src.app import get_storage
    storage = get_storage()
    repo = Repository(session, storage, pr.repository_id)
    return repo.get_commits_between(base_ref.commit_hash, head_ref.commit_hash)
