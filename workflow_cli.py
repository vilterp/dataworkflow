#!/usr/bin/env python3
"""
CLI tool to kick off workflow executions.

Usage:
    python workflow_cli.py \
        --repo test-repo \
        --commit <commit-hash> \
        --file examples/distributed_workflow.py \
        --function main \
        --control-plane http://localhost:5001
"""
import argparse
import requests
import json
import time
from urllib.parse import urljoin


def create_workflow_call(control_plane_url: str, repo_name: str, commit_hash: str,
                        workflow_file: str, function_name: str = 'main'):
    """Create a workflow call and poll for completion."""
    url = urljoin(control_plane_url, '/api/call')
    payload = {
        'caller_id': None,  # Root call
        'function_name': function_name,
        'arguments': {'args': [], 'kwargs': {}},
        'repo_name': repo_name,
        'commit_hash': commit_hash,
        'workflow_file': workflow_file
    }

    print(f"Creating call for {function_name}() in {workflow_file}@{commit_hash[:8]}...")
    response = requests.post(url, json=payload)
    response.raise_for_status()

    invocation_id = response.json()['invocation_id']
    print(f"✓ Created invocation {invocation_id}")
    print(f"Waiting for completion...")

    # Poll for completion
    status_url = urljoin(control_plane_url, f'/api/call/{invocation_id}')

    while True:
        response = requests.get(status_url)
        response.raise_for_status()

        data = response.json()
        status = data.get('status')

        if status == 'COMPLETED':
            result_json = data.get('result_value')
            result = json.loads(result_json) if result_json else None
            print(f"\n✓ Workflow completed successfully!")
            print(f"Result: {json.dumps(result, indent=2)}")
            return result
        elif status == 'FAILED':
            error = data.get('error_message', 'Unknown error')
            print(f"\n✗ Workflow failed: {error}")
            raise RuntimeError(error)
        elif status == 'RUNNING':
            print(".", end="", flush=True)

        time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description='Kick off a workflow execution')
    parser.add_argument('--repo', required=True, help='Repository name')
    parser.add_argument('--commit', required=True, help='Git commit hash')
    parser.add_argument('--file', required=True, help='Workflow file path')
    parser.add_argument('--function', default='main', help='Entry point function (default: main)')
    parser.add_argument('--control-plane', default='http://localhost:5001',
                       help='Control plane URL (default: http://localhost:5001)')

    args = parser.parse_args()

    try:
        create_workflow_call(
            control_plane_url=args.control_plane,
            repo_name=args.repo,
            commit_hash=args.commit,
            workflow_file=args.file,
            function_name=args.function
        )
    except Exception as e:
        print(f"\nError: {e}")
        exit(1)


if __name__ == '__main__':
    main()
