"""
Database configuration and session management for ClearCare Compliance MVP
"""

import os
from sqlalchemy import create_engine, text, inspect
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


def column_exists(table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        return any(col['name'] == column_name for col in columns)
    except Exception:
        return False

def add_missing_columns() -> None:
    """Add missing columns to existing tables."""
    try:
        with engine.connect() as conn:
            # Check and add cms_csv_ok column to runs table
            if not column_exists("runs", "cms_csv_ok"):
                print("Adding cms_csv_ok column to runs table...")
                if DATABASE_URL.startswith("postgresql"):
                    alter_sql = "ALTER TABLE runs ADD COLUMN cms_csv_ok BOOLEAN"
                else:  # SQLite
                    alter_sql = "ALTER TABLE runs ADD COLUMN cms_csv_ok BOOLEAN"
                
                conn.execute(text(alter_sql))
                conn.commit()
                print("✅ Added cms_csv_ok column")
            else:
                print("✅ cms_csv_ok column already exists")
    except Exception as e:
        print(f"Warning: Could not add missing columns: {e}")

def init_db() -> None:
    """
    Initialize the database by creating all tables and adding missing columns.
    This should be called once at application startup.
    """
    # Ensure the data directory exists for SQLite
    if DATABASE_URL.startswith("sqlite"):
        data_dir = os.path.dirname(DATABASE_URL.replace("sqlite:///", ""))
        os.makedirs(data_dir, exist_ok=True)
    
    # Create all tables
    SQLModel.metadata.create_all(engine)
    print(f"Database initialized: {DATABASE_URL}")
    
    # Add any missing columns (for schema migrations)
    add_missing_columns()


def get_db_session() -> Session:
    """
    Get a database session for direct use (not as a dependency).
    Remember to close the session when done.
    """
    return SessionLocal()
