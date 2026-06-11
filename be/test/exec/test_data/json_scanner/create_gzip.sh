#!/bin/bash

# Script to create gzip compressed test file
# This script should be run from the StarRocks root directory

echo "Creating gzip compressed test file..."

# Check if gzip is available
if ! command -v gzip &> /dev/null; then
    echo "Error: gzip command not found. Please install gzip first."
    exit 1
fi

# Navigate to test data directory
cd be/test/exec/test_data/json_scanner/

# Compress the JSON file
if [ -f "books.json" ]; then
    echo "Compressing books.json to books.json.gz..."
    gzip -c books.json > books.json.gz
    echo "Successfully created books.json.gz"
    echo "File size: $(du -h books.json.gz | cut -f1)"
else
    echo "Error: books.json not found in current directory"
    exit 1
fi

echo "Done!"
