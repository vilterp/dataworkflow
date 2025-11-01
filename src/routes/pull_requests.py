"""Pull request routes for DataWorkflow"""
import logging
from flask import Blueprint, render_template, redirect, url_for, flash, request
from src.models import PullRequest, PullRequestStatus, PullRequestComment, Repository as RepositoryModel
from src.core.pull_requests import (
    create_pull_request, merge_pull_request, close_pull_request,
    reopen_pull_request, get_pr_commits, can_merge_pr, dispatch_pr_checks
)
from src.core.vfs_diff_view import get_commit_diff_view
from src.core.vfs_diff import diff_commits

logger = logging.getLogger(__name__)
pull_requests_bp = Blueprint('pull_requests', __name__)


@pull_requests_bp.route('/<repo_name>/pulls')
def pull_requests_list(repo_name):
    """List all pull requests for a repository"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Get all pull requests, ordered by number (descending)
        prs = db.query(PullRequest).filter(
            PullRequest.repository_id == repo.repository_id
        ).order_by(PullRequest.number.desc()).all()

        # Separate by status
        open_prs = [pr for pr in prs if pr.status == PullRequestStatus.OPEN]
        closed_prs = [pr for pr in prs if pr.status == PullRequestStatus.CLOSED]
        merged_prs = [pr for pr in prs if pr.status == PullRequestStatus.MERGED]

        # Get the current tab from query params
        tab = request.args.get('tab', 'open')

        return render_template(
            'pull_requests/list.html',
            repo_name=repo_name,
            active_tab='pulls',
            open_prs=open_prs,
            closed_prs=closed_prs,
            merged_prs=merged_prs,
            current_tab=tab
        )
    finally:
        db.close()


@pull_requests_bp.route('/<repo_name>/pulls/new')
def new_pull_request(repo_name):
    """Form to create a new pull request"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Get the repository model for accessing main_branch
        repo_model = db.query(RepositoryModel).get(repo.repository_id)

        # Get branches
        branches = repo.list_branches()

        # Get comparison from query params (e.g., ?compare=main...feature-branch)
        comparison = request.args.get('compare', '')
        base_branch = None
        head_branch = None

        if '...' in comparison:
            parts = comparison.split('...')
            if len(parts) == 2:
                base_branch = parts[0]
                head_branch = parts[1]

        # Default to main if not specified
        if not base_branch:
            base_branch = repo_model.main_branch if repo_model else 'main'

        return render_template(
            'pull_requests/new.html',
            repo_name=repo_name,
            active_tab='pulls',
            branches=branches,
            base_branch=base_branch,
            head_branch=head_branch
        )
    finally:
        db.close()


@pull_requests_bp.route('/<repo_name>/pulls/create', methods=['POST'])
def create_pull_request_route(repo_name):
    """Create a new pull request"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        base_branch = request.form.get('base_branch')
        head_branch = request.form.get('head_branch')
        title = request.form.get('title')
        description = request.form.get('description', '')
        author = request.form.get('author', 'Unknown')
        author_email = request.form.get('author_email', 'unknown@example.com')

        # Validate inputs
        if not all([base_branch, head_branch, title]):
            flash('Base branch, head branch, and title are required', 'error')
            return redirect(url_for('pull_requests.new_pull_request', repo_name=repo_name))

        if base_branch == head_branch:
            flash('Base and head branches must be different', 'error')
            return redirect(url_for('pull_requests.new_pull_request', repo_name=repo_name))

        # Create the pull request
        pr = create_pull_request(
            db,
            repo.repository_id,
            base_branch,
            head_branch,
            title,
            description,
            author,
            author_email
        )
        db.commit()

        flash(f'Pull request #{pr.number} created successfully', 'success')
        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr.number))
    except Exception as e:
        db.rollback()
        logger.error(f'Error creating pull request: {e}', exc_info=True)
        flash('Error creating pull request. Please try again.', 'error')
        return redirect(url_for('pull_requests.new_pull_request', repo_name=repo_name))
    finally:
        db.close()


@pull_requests_bp.route('/<repo_name>/pull/<int:pr_number>')
def pull_request_detail(repo_name, pr_number):
    """Show pull request detail page"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        # Get the pull request
        pr = db.query(PullRequest).filter(
            PullRequest.repository_id == repo.repository_id,
            PullRequest.number == pr_number
        ).first()

        if not pr:
            flash(f'Pull request #{pr_number} not found', 'error')
            return redirect(url_for('pull_requests.pull_requests_list', repo_name=repo_name))

        # Get the current tab
        tab = request.args.get('tab', 'conversation')

        # Get commits in this PR
        commits = get_pr_commits(db, pr)

        # Get base and head commits for diff
        base_ref = repo.get_ref(f'refs/heads/{pr.base_branch}')
        head_ref = repo.get_ref(f'refs/heads/{pr.head_branch}')

        file_diffs = []
        base_commit = None
        head_commit = None

        if base_ref and head_ref:
            base_commit = repo.get_commit(base_ref.commit_hash)
            head_commit = repo.get_commit(head_ref.commit_hash)

            # Generate diff if we're on the files tab
            if tab == 'files':
                # Get diff between base and head commits
                # head_commit is the "new" commit, base_commit is the "old" (parent)
                file_diffs = get_commit_diff_view(repo, head_commit.hash, base_commit.hash)

        # Check if PR can be merged and get PR checks
        from src.core.pull_requests import get_pr_checks
        can_merge, merge_error = can_merge_pr(db, pr)
        pr_checks = get_pr_checks(db, pr)

        # For conversation tab, create a merged timeline of commits and comments
        timeline_items = []
        if tab == 'conversation':
            # Add commits to timeline with type marker
            for commit in commits:
                timeline_items.append({
                    'type': 'commit',
                    'data': commit,
                    'timestamp': commit.committed_at
                })

            # Add comments to timeline with type marker
            for comment in pr.comments:
                timeline_items.append({
                    'type': 'comment',
                    'data': comment,
                    'timestamp': comment.created_at
                })

            # Sort by timestamp
            timeline_items.sort(key=lambda x: x['timestamp'])

        # Choose template based on tab
        template_map = {
            'conversation': 'pull_requests/detail_conversation.html',
            'commits': 'pull_requests/detail_commits.html',
            'files': 'pull_requests/detail_files.html',
        }
        template = template_map.get(tab, 'pull_requests/detail_conversation.html')

        return render_template(
            template,
            repo_name=repo_name,
            active_tab='pulls',
            pr=pr,
            commits=commits,
            base_commit=base_commit,
            head_commit=head_commit,
            file_diffs=file_diffs,
            current_tab=tab,
            can_merge=can_merge,
            merge_error=merge_error,
            timeline_items=timeline_items,
            pr_checks=pr_checks
        )
    finally:
        db.close()


@pull_requests_bp.route('/<repo_name>/pull/<int:pr_number>/merge', methods=['POST'])
def merge_pull_request_route(repo_name, pr_number):
    """Merge a pull request"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        pr = db.query(PullRequest).filter(
            PullRequest.repository_id == repo.repository_id,
            PullRequest.number == pr_number
        ).first()

        if not pr:
            flash(f'Pull request #{pr_number} not found', 'error')
            return redirect(url_for('pull_requests.pull_requests_list', repo_name=repo_name))

        # Get merge user info from form
        merged_by = request.form.get('merged_by', 'Unknown')
        merged_by_email = request.form.get('merged_by_email', 'unknown@example.com')

        success, error = merge_pull_request(db, pr, merged_by, merged_by_email)

        if success:
            db.commit()
            flash(f'Pull request #{pr_number} merged successfully', 'success')
        else:
            flash(f'Cannot merge pull request: {error}', 'error')

        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))
    except Exception as e:
        db.rollback()
        logger.error(f'Error merging pull request #{pr_number}: {e}', exc_info=True)
        flash('Error merging pull request. Please try again.', 'error')
        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))
    finally:
        db.close()


@pull_requests_bp.route('/<repo_name>/pull/<int:pr_number>/close', methods=['POST'])
def close_pull_request_route(repo_name, pr_number):
    """Close a pull request without merging"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        pr = db.query(PullRequest).filter(
            PullRequest.repository_id == repo.repository_id,
            PullRequest.number == pr_number
        ).first()

        if not pr:
            flash(f'Pull request #{pr_number} not found', 'error')
            return redirect(url_for('pull_requests.pull_requests_list', repo_name=repo_name))

        close_pull_request(db, pr)
        db.commit()
        flash(f'Pull request #{pr_number} closed', 'success')

        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))
    except Exception as e:
        db.rollback()
        logger.error(f'Error closing pull request #{pr_number}: {e}', exc_info=True)
        flash('Error closing pull request. Please try again.', 'error')
        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))
    finally:
        db.close()


@pull_requests_bp.route('/<repo_name>/pull/<int:pr_number>/reopen', methods=['POST'])
def reopen_pull_request_route(repo_name, pr_number):
    """Reopen a closed pull request"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        pr = db.query(PullRequest).filter(
            PullRequest.repository_id == repo.repository_id,
            PullRequest.number == pr_number
        ).first()

        if not pr:
            flash(f'Pull request #{pr_number} not found', 'error')
            return redirect(url_for('pull_requests.pull_requests_list', repo_name=repo_name))

        success, error = reopen_pull_request(db, pr)

        if success:
            db.commit()
            flash(f'Pull request #{pr_number} reopened', 'success')
        else:
            flash(f'Cannot reopen pull request: {error}', 'error')

        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))
    except Exception as e:
        db.rollback()
        logger.error(f'Error reopening pull request #{pr_number}: {e}', exc_info=True)
        flash('Error reopening pull request. Please try again.', 'error')
        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))
    finally:
        db.close()


@pull_requests_bp.route('/<repo_name>/pull/<int:pr_number>/checks/dispatch', methods=['POST'])
def dispatch_checks_route(repo_name, pr_number):
    """Re-dispatch checks for a pull request"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        pr = db.query(PullRequest).filter(
            PullRequest.repository_id == repo.repository_id,
            PullRequest.number == pr_number
        ).first()

        if not pr:
            flash(f'Pull request #{pr_number} not found', 'error')
            return redirect(url_for('pull_requests.pull_requests_list', repo_name=repo_name))

        # Clear existing checks and re-dispatch
        for check in pr.checks:
            db.delete(check)

        dispatch_pr_checks(db, pr)
        db.commit()

        flash('Checks dispatched successfully', 'success')
        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))
    except Exception as e:
        db.rollback()
        logger.error(f'Error dispatching checks for PR #{pr_number}: {e}', exc_info=True)
        flash('Error dispatching checks. Please try again.', 'error')
        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))
    finally:
        db.close()


@pull_requests_bp.route('/<repo_name>/pull/<int:pr_number>/comment', methods=['POST'])
def add_comment_route(repo_name, pr_number):
    """Add a comment to a pull request"""
    from src.app import get_repository

    repo, db = get_repository(repo_name)
    if not repo:
        flash(f'Repository {repo_name} not found', 'error')
        return redirect(url_for('repo.repositories_list'))

    try:
        pr = db.query(PullRequest).filter(
            PullRequest.repository_id == repo.repository_id,
            PullRequest.number == pr_number
        ).first()

        if not pr:
            flash(f'Pull request #{pr_number} not found', 'error')
            return redirect(url_for('pull_requests.pull_requests_list', repo_name=repo_name))

        # Get comment data from form
        body = request.form.get('body', '').strip()
        author = request.form.get('author', 'Unknown')
        author_email = request.form.get('author_email', 'unknown@example.com')

        if not body:
            flash('Comment body cannot be empty', 'error')
            return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))

        # Create the comment
        comment = PullRequestComment(
            pull_request_id=pr.id,
            body=body,
            author=author,
            author_email=author_email
        )
        db.add(comment)
        db.commit()

        flash('Comment added successfully', 'success')
        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))
    except Exception as e:
        db.rollback()
        logger.error(f'Error adding comment to PR #{pr_number}: {e}', exc_info=True)
        flash('Error adding comment. Please try again.', 'error')
        return redirect(url_for('pull_requests.pull_request_detail', repo_name=repo_name, pr_number=pr_number))
    finally:
        db.close()
