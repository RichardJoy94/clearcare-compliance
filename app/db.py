"""
Database configuration and session management for ClearCare Compliance MVP
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, Session
from typing import Generator

# Database URL configuration
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./app/data/app.db")

# Create engine
if DATABASE_URL.startswith("sqlite"):
    # SQLite specific configuration
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=False  # Set to True for SQL debugging
    )
else:
    # PostgreSQL/other databases
    engine = create_engine(DATABASE_URL, echo=False)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_session() -> Generator[Session, None, None]:
    """
    Dependency to get database session.
    Yields a database session and ensures it's closed after use.
    """
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def init_db() -> None:
    """
    Initialize the database by creating all tables.
    This should be called once at application startup.
    """
    # Ensure the data directory exists for SQLite
    if DATABASE_URL.startswith("sqlite"):
        data_dir = os.path.dirname(DATABASE_URL.replace("sqlite:///", ""))
        os.makedirs(data_dir, exist_ok=True)
    
    # Create all tables
    SQLModel.metadata.create_all(engine)
    print(f"Database initialized: {DATABASE_URL}")


def get_db_session() -> Session:
    """
    Get a database session for direct use (not as a dependency).
    Remember to close the session when done.
    """
    return SessionLocal()
