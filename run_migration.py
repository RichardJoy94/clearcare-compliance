#!/usr/bin/env python3
"""
Quick migration script to add the cms_csv_ok column.
Run this script to fix the database schema issue.
"""

import sys
import os

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from db import init_db

if __name__ == "__main__":
    print("🔄 Running database migration...")
    try:
        init_db()
        print("✅ Migration completed successfully!")
        print("You can now restart your application and try uploading files again.")
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)
