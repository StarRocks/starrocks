#!/bin/bash

# Simple script to test if the fuzzy test can compile
set -e

echo "Testing compilation of builtin_functions_fuzzy_test.cpp..."

cd /workspace/be

# Create build directory if it doesn't exist
if [ ! -d "build" ]; then
    mkdir build
fi

cd build

# Run cmake if needed
if [ ! -f "Makefile" ]; then
    echo "Running cmake..."
    cmake .. -DCMAKE_BUILD_TYPE=Debug
fi

# Try to compile just our fuzzy test
echo "Compiling fuzzy test..."
make builtin_functions_fuzzy_test -j4

echo "Compilation test completed successfully!"