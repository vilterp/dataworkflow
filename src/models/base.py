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
    engine = create_engine(database_url, echo=echo)
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return Session()


def init_db(database_url: str, echo: bool = False):
    """
    Initialize database tables.

    Args:
        database_url: SQLAlchemy database URL
        echo: Whether to echo SQL statements
    """
    engine = create_engine(database_url, echo=echo)
    Base.metadata.create_all(bind=engine)
