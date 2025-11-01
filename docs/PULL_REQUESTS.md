# Pull Requests Feature

This document describes the pull request feature that has been added to DataWorkflow.

## Overview

The pull request system allows you to:
- Create pull requests to merge changes from one branch to another
- View and manage open, closed, and merged pull requests
- Configure automated checks that must pass before a PR can be merged
- View diffs and commit lists for pull requests
- Merge pull requests with validation

## Database Models

### PullRequest
Located in [src/models/pull_request.py](../src/models/pull_request.py)

- `id`: Primary key
- `repository_id`: Foreign key to repositories
- `number`: PR number within the repository (1, 2, 3, etc.)
- `base_branch`: Target branch (e.g., 'main')
- `head_branch`: Source branch (e.g., 'feature-xyz')
- `title`: PR title
- `description`: PR description (optional)
- `author`: Author name
- `author_email`: Author email
- `status`: One of OPEN, CLOSED, or MERGED
- `merge_commit_hash`: Hash of merge commit (when merged)
- `merged_at`, `merged_by`, `merged_by_email`: Merge metadata
- `created_at`, `updated_at`, `closed_at`: Timestamps

### PullRequestCheck
Located in [src/models/pull_request.py](../src/models/pull_request.py)

- `id`: Primary key
- `pull_request_id`: Foreign key to pull_requests
- `check_name`: Name of the check (from config)
- `stage_run_id`: Foreign key to stage_runs (optional)
- `status`: One of PENDING, RUNNING, SUCCESS, FAILURE, or SKIPPED
- `error_message`: Error message if failed
- `created_at`, `updated_at`, `started_at`, `completed_at`: Timestamps

## PR Checks Configuration

Pull request checks are configured via a YAML file (`.pr-checks.yml`) in the repository root.

### Configuration Format

```yaml
version: "1"
checks:
  - name: "tests"
    workflow_file: "workflows/ci.py"
    stage_name: "test"
    required: true

  - name: "lint"
    workflow_file: "workflows/ci.py"
    stage_name: "lint"
    required: true

  - name: "build"
    workflow_file: "workflows/build.py"
    stage_name: "build"
    arguments:
      target: "production"
    required: true
```

### Configuration Schema

Defined in [src/core/pr_checks_config.py](../src/core/pr_checks_config.py)

- `version`: Configuration format version (currently "1")
- `checks`: List of check configurations
  - `name`: Unique name for the check
  - `workflow_file`: Path to the workflow file
  - `stage_name`: Name of the stage to run
  - `arguments`: Optional arguments to pass to the stage
  - `required`: Whether this check must pass for PR to be mergeable (default: true)

The configuration is validated using Pydantic models.

## Routes

All routes are defined in [src/routes/pull_requests.py](../src/routes/pull_requests.py)

- `GET /<repo>/pulls` - List all pull requests with tabs for open/closed/merged
- `GET /<repo>/pulls/new` - Form to create a new pull request
- `POST /<repo>/pulls/create` - Create a new pull request
- `GET /<repo>/pull/<number>` - View pull request details with tabs for conversation/commits/files
- `POST /<repo>/pull/<number>/merge` - Merge a pull request
- `POST /<repo>/pull/<number>/close` - Close a pull request
- `POST /<repo>/pull/<number>/reopen` - Reopen a closed pull request
- `POST /<repo>/pull/<number>/checks/dispatch` - Re-dispatch checks for a pull request

## Core Operations

Located in [src/core/pull_requests.py](../src/core/pull_requests.py)

- `create_pull_request()` - Create a new PR with automatic check dispatch
- `dispatch_pr_checks()` - Dispatch checks based on .pr-checks.yml config
- `can_merge_pr()` - Check if a PR can be merged (all checks passed)
- `merge_pull_request()` - Merge a PR (fast-forward merge to base branch)
- `close_pull_request()` - Close a PR without merging
- `reopen_pull_request()` - Reopen a closed PR
- `get_pr_commits()` - Get all commits in a PR (commits in head not in base)
- `update_pr_check_from_stage_run()` - Update check status from stage run

## Templates

Located in [src/templates/pull_requests/](../src/templates/pull_requests/)

- `list.html` - Pull request list with tabs for open/closed/merged PRs
- `detail.html` - Pull request detail page with tabs for conversation/commits/files
- `new.html` - Form to create a new pull request

All templates extend `base.html` and use the existing `file_diff_list` macro for rendering diffs.

## UI Features

### Pull Request List
- Tabs to filter by status (open, closed, merged)
- Shows PR number, title, author, status, and check status
- Click-through to PR detail page

### Pull Request Detail
- Three tabs: Conversation, Commits, Files Changed
- **Conversation tab**:
  - PR description
  - Check status in sidebar
  - Merge button (enabled only when all checks pass)
  - Close/Reopen button
- **Commits tab**:
  - List of all commits in the PR
  - Links to individual commit pages
- **Files Changed tab**:
  - Reuses existing diff macro from compare view
  - Shows all file changes between base and head

### Checks Sidebar
- Shows status of each configured check
- Color-coded status icons (green=success, red=failure, yellow=running, gray=pending)
- Re-run button to re-dispatch checks

## Database Migration

Run the migration to create the new tables:

```bash
python scripts/migrate_add_pull_requests.py
```

This creates:
- `pull_requests` table
- `pull_request_checks` table

## How It Works

### Creating a Pull Request

1. User navigates to `/<repo>/pulls/new`
2. Selects base and head branches
3. Enters title and description
4. On submit, `create_pull_request()` is called which:
   - Creates a new PullRequest record with an auto-incremented number
   - Calls `dispatch_pr_checks()` to load `.pr-checks.yml` and create PullRequestCheck records
   - Returns the created PR

### Checking If a PR Can Merge

The `can_merge_pr()` function checks:
1. PR status is OPEN
2. All configured checks have completed
3. All checks have status SUCCESS (not FAILURE)

If any check is pending, running, or failed, the PR cannot be merged.

### Merging a PR

The `merge_pull_request()` function:
1. Validates that the PR can be merged
2. Updates the base branch ref to point to the head branch commit (fast-forward merge)
3. Updates PR status to MERGED
4. Records merge metadata (merged_at, merged_by, merge_commit_hash)

**Note**: Currently implements a simple fast-forward merge. A real implementation would create a merge commit.

### Check Dispatch

When a PR is created or checks are re-dispatched:
1. Load `.pr-checks.yml` from the base branch
2. Parse and validate the configuration
3. Create a PullRequestCheck record for each configured check
4. Set initial status to PENDING

**TODO**: Actually create and dispatch StageRun entries for the checks. Currently only creates the PullRequestCheck records.

## Future Enhancements

1. **Actual check execution**: Wire up PullRequestCheck to StageRun creation and execution
2. **Merge commit creation**: Instead of fast-forward, create proper merge commits
3. **PR comments**: Add ability to comment on PRs
4. **File-level comments**: Add inline comments on diff lines
5. **Review system**: Add approve/request changes workflow
6. **Draft PRs**: Support draft pull requests
7. **Auto-merge**: Auto-merge when checks pass
8. **Squash/rebase merge**: Support different merge strategies
9. **PR labels**: Add labels to categorize PRs
10. **Notifications**: Notify users when PRs are created, updated, or merged

## Example Workflow

1. Create two branches in your repository
2. Add `.pr-checks.yml` to the base branch with check configurations
3. Make some commits on the head branch
4. Navigate to `/<repo>/pulls` and click "New Pull Request"
5. Select branches and create the PR
6. View the PR detail page to see commits, checks, and diff
7. Once checks pass, merge the PR

## Testing

The implementation has been tested with:
- Database migration successfully creates tables
- App imports without errors
- Routes are registered correctly
- Templates extend base layout properly

To test the full workflow, you'll need to:
1. Create a repository with multiple branches
2. Add a `.pr-checks.yml` configuration file
3. Create a pull request
4. Verify the UI displays correctly
