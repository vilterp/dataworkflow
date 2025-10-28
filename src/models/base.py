from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


def create_session(database_url: str, echo: bool = False):
    """
    Create a database session.

    Args:
        database_url: SQLAlchemy database URL
        echo: Whether to echo SQL statements

    Returns:
        Database session
    """
    # For SQLite, we need to allow sharing connections across threads in tests
    connect_args = {}
    if database_url.startswith('sqlite'):
        connect_args['check_same_thread'] = False

    engine = create_engine(database_url, echo=echo, connect_args=connect_args)
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return Session()


def init_db(database_url: str, echo: bool = False):
    """
    Initialize database tables.

    Args:
        database_url: SQLAlchemy database URL
        echo: Whether to echo SQL statements
    """
    # For SQLite, we need to allow sharing connections across threads in tests
    connect_args = {}
    if database_url.startswith('sqlite'):
        connect_args['check_same_thread'] = False

    engine = create_engine(database_url, echo=echo, connect_args=connect_args)
    Base.metadata.create_all(bind=engine)
