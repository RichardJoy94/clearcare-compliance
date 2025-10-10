#!/usr/bin/env python3
"""
Install script for the clearcare_compliance package.
This can be run during deployment to ensure the package is available.
"""

import subprocess
import sys
import os

def install_package():
    """Install the clearcare_compliance package."""
    try:
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Change to the project root directory
        os.chdir(script_dir)
        
        print("Installing clearcare_compliance package...")
        
        # Install the package in development mode
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", "-e", "."
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ clearcare_compliance package installed successfully")
            return True
        else:
            print(f"❌ Failed to install package: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error installing package: {e}")
        return False

if __name__ == "__main__":
    success = install_package()
    sys.exit(0 if success else 1)
