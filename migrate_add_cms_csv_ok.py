#!/usr/bin/env python3
"""
Database migration script to add cms_csv_ok column to runs table.
Run this script to update your existing database schema.
"""

import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

def get_database_url():
    """Get database URL from environment or use default."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        # Default to SQLite for local development
        database_url = "sqlite:///./app/data/app.db"
    return database_url

def add_cms_csv_ok_column():
    """Add cms_csv_ok column to runs table."""
    database_url = get_database_url()
    
    print(f"Connecting to database: {database_url}")
    
    try:
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            # Check if column already exists
            if "postgresql" in database_url:
                check_sql = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'runs' AND column_name = 'cms_csv_ok'
                """
            else:  # SQLite
                check_sql = """
                PRAGMA table_info(runs)
                """
            
            result = conn.execute(text(check_sql))
            
            if "postgresql" in database_url:
                column_exists = result.fetchone() is not None
            else:  # SQLite
                columns = [row[1] for row in result.fetchall()]
                column_exists = 'cms_csv_ok' in columns
            
            if column_exists:
                print("✅ Column 'cms_csv_ok' already exists in runs table")
                return True
            
            # Add the column
            if "postgresql" in database_url:
                alter_sql = """
                ALTER TABLE runs 
                ADD COLUMN cms_csv_ok BOOLEAN
                """
            else:  # SQLite
                alter_sql = """
                ALTER TABLE runs 
                ADD COLUMN cms_csv_ok BOOLEAN
                """
            
            print("Adding cms_csv_ok column to runs table...")
            conn.execute(text(alter_sql))
            conn.commit()
            
            print("✅ Successfully added cms_csv_ok column to runs table")
            return True
            
    except OperationalError as e:
        print(f"❌ Database error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def main():
    """Main migration function."""
    print("🔄 Running database migration: Add cms_csv_ok column")
    print("=" * 50)
    
    success = add_cms_csv_ok_column()
    
    if success:
        print("\n✅ Migration completed successfully!")
        print("You can now restart your application.")
    else:
        print("\n❌ Migration failed!")
        print("Please check the error messages above and try again.")
        sys.exit(1)

if __name__ == "__main__":
    main()
