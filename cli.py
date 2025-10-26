#!/usr/bin/env python3
"""DataWorkflow CLI - Unified command-line interface for all operations."""
import argparse
import logging
import sys
import os


def setup_logging(log_level: str):
    """Configure logging for the CLI."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def cmd_control_plane(args):
    """Start the control plane server."""
    from src.app import app
    from src.config import Config

    setup_logging(args.log_level)

    host = args.host or '0.0.0.0'
    port = args.port or Config.PORT
    debug = args.debug if args.debug is not None else Config.DEBUG

    logger = logging.getLogger(__name__)
    logger.info(f"Starting control plane on {host}:{port}")

    app.run(debug=debug, host=host, port=port)


def cmd_worker(args):
    """Start a worker that executes workflow tasks."""
    from sdk.worker import CallWorker

    setup_logging(args.log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting DataWorkflow worker")

    # Create and start worker
    worker = CallWorker(
        server_url=args.server_url,
        worker_id=args.worker_id,
        poll_interval=args.poll_interval
    )

    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='DataWorkflow - Distributed workflow execution engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start the control plane
  %(prog)s control-plane --port 5001

  # Start a worker
  %(prog)s worker --server-url http://localhost:5001

  # Start worker with custom settings
  %(prog)s worker --server-url http://localhost:5001 --poll-interval 5 --worker-id my-worker
"""
    )

    # Global options
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the logging level (default: INFO)'
    )

    # Create subparsers for commands
    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
        required=True
    )

    # Control plane command
    cp_parser = subparsers.add_parser(
        'control-plane',
        help='Start the control plane server',
        description='Start the DataWorkflow control plane server'
    )
    cp_parser.add_argument(
        '--host',
        help='Host to bind to (default: 0.0.0.0)'
    )
    cp_parser.add_argument(
        '--port',
        type=int,
        help='Port to bind to (default: from config)'
    )
    cp_parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode'
    )
    cp_parser.add_argument(
        '--no-debug',
        dest='debug',
        action='store_false',
        help='Disable debug mode'
    )
    cp_parser.set_defaults(func=cmd_control_plane, debug=None)

    # Worker command
    worker_parser = subparsers.add_parser(
        'worker',
        help='Start a workflow worker',
        description='Start a DataWorkflow worker that executes workflow tasks'
    )
    worker_parser.add_argument(
        '--server-url',
        required=True,
        help='Control plane server URL (e.g., http://localhost:5001)'
    )
    worker_parser.add_argument(
        '--worker-id',
        help='Worker ID (auto-generated if not provided)'
    )
    worker_parser.add_argument(
        '--poll-interval',
        type=int,
        default=2,
        help='Polling interval in seconds (default: 2)'
    )
    worker_parser.set_defaults(func=cmd_worker)

    # Parse arguments
    args = parser.parse_args()

    # Execute the command
    args.func(args)


if __name__ == '__main__':
    main()
