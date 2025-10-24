#!/usr/bin/env python3
"""
CLI tool to run the workflow runner.

Usage:
    python sdk/run_workflows.py --server http://localhost:5001 --repo my-repo
"""
import argparse
import logging
import sys
from runner import WorkflowRunner


def main():
    parser = argparse.ArgumentParser(description='Run DataWorkflow workflow runner')
    parser.add_argument('--server', required=True, help='Server URL (e.g., http://localhost:5001)')
    parser.add_argument('--repo', required=True, help='Repository name')
    parser.add_argument('--runner-id', help='Unique runner ID (auto-generated if not provided)')
    parser.add_argument('--poll-interval', type=int, default=5, help='Poll interval in seconds (default: 5)')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level (default: INFO)')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Create and start the runner
    runner = WorkflowRunner(
        server_url=args.server,
        repo_name=args.repo,
        runner_id=args.runner_id,
        poll_interval=args.poll_interval
    )

    try:
        runner.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        sys.exit(0)


if __name__ == '__main__':
    main()
