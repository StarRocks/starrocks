#!/bin/bash
# Simple build script for JSON generator C++ program

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building JSON generator C++ program..."

CXX=$(command -v clang++ || command -v g++)
FMT_LIB="-lfmt"

# Build
echo "Compiling with $CXX..."
$CXX -std=c++17 -O3 -gdwarf-5 -fno-omit-frame-pointer -Wall -o json_generator json_generator.cpp $FMT_LIB

if [ -f json_generator ]; then
    echo "Build successful! Executable: ./json_generator"
    chmod +x json_generator
else
    echo "Build failed!"
    exit 1
fi
