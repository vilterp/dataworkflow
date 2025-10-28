"""Test log tailing functionality."""
import pytest
from datetime import datetime, timezone
from flask import Flask
from src.app import app as flask_app
from src.models import StageRun, StageRunStatus, StageLogLine, Repository as RepositoryModel
from src.models.api_schemas import LogLineData, CreateStageLogsRequest, GetStageLogsResponse
from src.models.base import init_db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture
def app(temp_dir):
    """Create and configure a test Flask app"""
    db_path = f"{temp_dir}/test.db"
    database_url = f'sqlite:///{db_path}'

    flask_app.config['TESTING'] = True
    flask_app.config['DATABASE_URL'] = database_url
    flask_app.config['STORAGE_BASE_PATH'] = f"{temp_dir}/objects"

    # Setup database
    init_db(database_url, echo=False)

    yield flask_app


@pytest.fixture
def client(app):
    """Create a test client"""
    return app.test_client()


@pytest.fixture
def db_session(app):
    """Create a database session for direct DB access in tests"""
    engine = create_engine(app.config['DATABASE_URL'], echo=False)
    Session = sessionmaker(bind=engine)
    db = Session()
    yield db
    db.close()


def test_create_stage_logs(client, db_session):
    """Test creating log lines for a stage run."""
    # Create a test stage run
    stage_run = StageRun(
        id='test_stage_run_hash_123',
        repo_name='test_repo',
        commit_hash='abc123',
        workflow_file='test_workflow.py',
        stage_name='test_stage',
        arguments='{}',
        status=StageRunStatus.RUNNING
    )
    db_session.add(stage_run)
    db_session.commit()

    # Create log lines
    logs = [
        {'index': 0, 'timestamp': '2024-01-01T12:00:00Z', 'content': 'Starting stage'},
        {'index': 1, 'timestamp': '2024-01-01T12:00:01Z', 'content': 'Processing data'},
        {'index': 2, 'timestamp': '2024-01-01T12:00:02Z', 'content': 'Stage completed'},
    ]

    response = client.post(
        f'/api/stages/{stage_run.id}/logs',
        json={'logs': logs}
    )

    assert response.status_code == 201
    data = response.get_json()
    assert data['success'] is True
    assert data['count'] == 3

    # Verify logs were stored
    stored_logs = db_session.query(StageLogLine).filter(
        StageLogLine.stage_run_id == stage_run.id
    ).order_by(StageLogLine.log_line_index).all()

    assert len(stored_logs) == 3
    assert stored_logs[0].log_contents == 'Starting stage'
    assert stored_logs[1].log_contents == 'Processing data'
    assert stored_logs[2].log_contents == 'Stage completed'


def test_get_stage_logs(client, db_session):
    """Test retrieving log lines for a stage run."""
    # Create a test stage run
    stage_run = StageRun(
        id='test_stage_run_hash_456',
        repo_name='test_repo',
        commit_hash='abc123',
        workflow_file='test_workflow.py',
        stage_name='test_stage',
        arguments='{}',
        status=StageRunStatus.RUNNING
    )
    db_session.add(stage_run)
    db_session.commit()

    # Add some log lines directly
    for i in range(5):
        log_line = StageLogLine(
            stage_run_id=stage_run.id,
            log_line_index=i,
            timestamp=datetime.now(timezone.utc),
            log_contents=f'Log line {i}',
            created_at=datetime.now(timezone.utc)
        )
        db_session.add(log_line)
    db_session.commit()

    # Get all logs
    response = client.get(f'/api/stages/{stage_run.id}/logs')

    assert response.status_code == 200
    data = response.get_json()
    assert len(data['logs']) == 5
    assert data['has_more'] is False
    assert data['logs'][0]['content'] == 'Log line 0'
    assert data['logs'][4]['content'] == 'Log line 4'


def test_get_stage_logs_with_tailing(client, db_session):
    """Test log tailing with since_index parameter."""
    # Create a test stage run
    stage_run = StageRun(
        id='test_stage_run_hash_789',
        repo_name='test_repo',
        commit_hash='abc123',
        workflow_file='test_workflow.py',
        stage_name='test_stage',
        arguments='{}',
        status=StageRunStatus.RUNNING
    )
    db_session.add(stage_run)
    db_session.commit()

    # Add some log lines
    for i in range(10):
        log_line = StageLogLine(
            stage_run_id=stage_run.id,
            log_line_index=i,
            timestamp=datetime.now(timezone.utc),
            log_contents=f'Log line {i}',
            created_at=datetime.now(timezone.utc)
        )
        db_session.add(log_line)
    db_session.commit()

    # Get logs since index 5
    response = client.get(f'/api/stages/{stage_run.id}/logs?since_index=5')

    assert response.status_code == 200
    data = response.get_json()
    assert len(data['logs']) == 4  # Indices 6, 7, 8, 9
    assert data['logs'][0]['index'] == 6
    assert data['logs'][3]['index'] == 9
    assert data['has_more'] is False


def test_get_stage_logs_with_limit(client, db_session):
    """Test log retrieval with limit."""
    # Create a test stage run
    stage_run = StageRun(
        id='test_stage_run_hash_abc',
        repo_name='test_repo',
        commit_hash='abc123',
        workflow_file='test_workflow.py',
        stage_name='test_stage',
        arguments='{}',
        status=StageRunStatus.RUNNING
    )
    db_session.add(stage_run)
    db_session.commit()

    # Add many log lines
    for i in range(50):
        log_line = StageLogLine(
            stage_run_id=stage_run.id,
            log_line_index=i,
            timestamp=datetime.now(timezone.utc),
            log_contents=f'Log line {i}',
            created_at=datetime.now(timezone.utc)
        )
        db_session.add(log_line)
    db_session.commit()

    # Get logs with limit
    response = client.get(f'/api/stages/{stage_run.id}/logs?limit=10')

    assert response.status_code == 200
    data = response.get_json()
    assert len(data['logs']) == 10
    assert data['has_more'] is True


def test_pydantic_validation():
    """Test Pydantic models for log API."""
    # Test LogLineData
    log_data = LogLineData(
        index=0,
        timestamp='2024-01-01T12:00:00Z',
        content='Test log line'
    )
    assert log_data.index == 0
    assert log_data.content == 'Test log line'

    # Test CreateStageLogsRequest
    log_request = CreateStageLogsRequest(logs=[
        LogLineData(index=0, timestamp='2024-01-01T12:00:00Z', content='Line 1'),
        LogLineData(index=1, timestamp='2024-01-01T12:00:01Z', content='Line 2'),
    ])
    assert len(log_request.logs) == 2

    # Test GetStageLogsResponse
    response = GetStageLogsResponse(
        logs=[log_data],
        has_more=False
    )
    assert len(response.logs) == 1
    assert response.has_more is False
