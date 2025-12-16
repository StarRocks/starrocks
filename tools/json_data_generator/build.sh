#!/bin/bash
# Simple build script for JSON generator C++ program

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building JSON generator C++ program..."

# Check for C++ compiler
if ! command -v g++ &> /dev/null && ! command -v clang++ &> /dev/null; then
    echo "Error: C++ compiler (g++ or clang++) not found."
    exit 1
fi

# Use g++ if available, otherwise clang++
CXX=$(command -v g++ || command -v clang++)

# Build
echo "Compiling with $CXX..."
$CXX -std=c++17 -O3 -Wall -o json_generator json_generator.cpp

if [ -f json_generator ]; then
    echo "Build successful! Executable: ./json_generator"
    chmod +x json_generator
else
    echo "Build failed!"
    exit 1
fi
