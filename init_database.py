#!/usr/bin/env python3
"""
Database initialization script that handles both new installations and migrations.
This script will:
1. Create tables if they don't exist
2. Add missing columns if they exist
3. Handle both SQLite and PostgreSQL
"""

import os
import sys
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import OperationalError

def get_database_url():
    """Get database URL from environment or use default."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        # Default to SQLite for local development
        database_url = "sqlite:///./app/data/app.db"
    return database_url

def table_exists(engine, table_name):
    """Check if a table exists in the database."""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()

def column_exists(engine, table_name, column_name):
    """Check if a column exists in a table."""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    return any(col['name'] == column_name for col in columns)

def add_missing_columns(engine):
    """Add missing columns to existing tables."""
    database_url = get_database_url()
    is_postgresql = "postgresql" in database_url
    
    if not table_exists(engine, "runs"):
        print("❌ Runs table doesn't exist. Please run the application first to create tables.")
        return False
    
    print("🔍 Checking for missing columns...")
    
    # Check and add cms_csv_ok column
    if not column_exists(engine, "runs", "cms_csv_ok"):
        print("Adding cms_csv_ok column to runs table...")
        try:
            with engine.connect() as conn:
                if is_postgresql:
                    alter_sql = "ALTER TABLE runs ADD COLUMN cms_csv_ok BOOLEAN"
                else:  # SQLite
                    alter_sql = "ALTER TABLE runs ADD COLUMN cms_csv_ok BOOLEAN"
                
                conn.execute(text(alter_sql))
                conn.commit()
                print("✅ Added cms_csv_ok column")
        except Exception as e:
            print(f"❌ Failed to add cms_csv_ok column: {e}")
            return False
    else:
        print("✅ cms_csv_ok column already exists")
    
    return True

def main():
    """Main initialization function."""
    print("🔄 Database Initialization and Migration")
    print("=" * 50)
    
    database_url = get_database_url()
    print(f"Database URL: {database_url}")
    
    try:
        engine = create_engine(database_url)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful")
        
        # Add missing columns
        if add_missing_columns(engine):
            print("\n✅ Database initialization completed successfully!")
            print("You can now restart your application.")
        else:
            print("\n❌ Database initialization failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
