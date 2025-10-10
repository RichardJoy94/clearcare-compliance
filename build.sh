#!/bin/bash
# Build script for deployment

# Install dependencies
pip install -r requirements.txt

# Install the clearcare_compliance package
pip install -e .

# Run any necessary setup
python init_database.py

echo "Build completed successfully"
