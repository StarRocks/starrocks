#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script detects changed C++ files for incremental GCOV coverage
# Usage: source detect_incremental_files.sh
# Sets the following variables:
#   GCOV_INCREMENTAL_FILES - semicolon-separated list of changed files (relative to be/)
#   WITH_GCOV - set to ON if falling back to full coverage
#   WITH_GCOV_INCREMENTAL - set to OFF if falling back to full coverage

detect_incremental_files() {
    local context="$1"  # "build" or "unit-test" for debug file naming
    
    echo "Detecting changed files for incremental coverage..."
    
    # Check if we're in a git repository
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        echo "Warning: Not in a git repository, falling back to full coverage"
        WITH_GCOV=ON
        WITH_GCOV_INCREMENTAL=OFF
        GCOV_INCREMENTAL_FILES=""
        return
    fi
    
    # Try to find base branch for comparison
    BASE_BRANCH=""
    if git show-ref --verify --quiet refs/heads/main; then
        BASE_BRANCH="main"
    else
        BASE_BRANCH="HEAD~1"
    fi
    
    echo "Using base branch: $BASE_BRANCH"
    
    # Get changed C++ files in be/ directory
    CHANGED_FILES=$(git diff --name-only $BASE_BRANCH...HEAD 2>/dev/null | grep -E '^be/.*\.(cpp|cc|h|hpp)$' || true)
    
    if [[ -z "$CHANGED_FILES" ]]; then
        echo "No changed C++ files detected, falling back to full coverage"
        WITH_GCOV=ON
        WITH_GCOV_INCREMENTAL=OFF
        GCOV_INCREMENTAL_FILES=""
        return
    fi
    
    # Count changed files
    FILE_COUNT=$(echo "$CHANGED_FILES" | wc -l)
    echo "Found $FILE_COUNT changed C++ files"
    
    # If too many files changed, fall back to full coverage
    if [[ $FILE_COUNT -gt 1000 ]]; then
        echo "Too many files changed ($FILE_COUNT), falling back to full coverage"
        WITH_GCOV=ON
        WITH_GCOV_INCREMENTAL=OFF
        GCOV_INCREMENTAL_FILES=""
        return
    fi
    
    # Convert to CMake-friendly format (relative to be/ directory)
    GCOV_INCREMENTAL_FILES=$(echo "$CHANGED_FILES" | sed 's|^be/||' | tr '\n' ';')
    echo "Incremental coverage will instrument $FILE_COUNT files"
    echo "Changed files: $(echo "$CHANGED_FILES" | tr '\n' ' ')"
    
    # Write debug information to file
    GCOV_DEBUG_FILE="${STARROCKS_HOME}/.gcov_incremental_debug.txt"
    cat > "$GCOV_DEBUG_FILE" << EOF
# GCOV Incremental Coverage Debug Information ($context)
# Generated on: $(date)
# Base branch: $BASE_BRANCH
# Git command: git diff --name-only $BASE_BRANCH...HEAD
# File count: $FILE_COUNT

# Changed C++ files (relative to be/ directory):
$(echo "$CHANGED_FILES" | sed 's|^be/||')

# CMake format (semicolon-separated):
$GCOV_INCREMENTAL_FILES
EOF
    echo "Debug information written to: $GCOV_DEBUG_FILE"
}

# Export the function so it can be used by other scripts
export -f detect_incremental_files
