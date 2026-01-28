#!/bin/bash

# Script to generate JaCoCo include patterns based on git diff
# This reduces instrumentation overhead by only targeting changed files

set -e

# Configuration
BASE_BRANCH=${1:-"HEAD~1"}  # Default to previous commit, can be overridden
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_FILE=${2:-"${PROJECT_ROOT}/target/jacoco-includes.txt"}
PROPERTIES_FILE=${3:-"${PROJECT_ROOT}/target/jacoco-includes.properties"}

# Create output directory if it doesn't exist
mkdir -p "$(dirname "$OUTPUT_FILE")"

# Get list of changed Java files
echo "Generating JaCoCo includes based on git diff from $BASE_BRANCH..."

# Get changed Java files in src/main/java
CHANGED_FILES=$(git diff --name-only "$BASE_BRANCH" -- "src/main/java/**/*.java" 2>/dev/null || echo "")

if [ -z "$CHANGED_FILES" ]; then
    echo "No changed Java files found. Using default includes."
    # Fallback to default includes if no changes detected
    cat > "$OUTPUT_FILE" << EOF
# JaCoCo includes generated from git diff
# Base branch: $BASE_BRANCH
# Generated on: $(date)
# No changes detected, using full coverage

com/starrocks/**
EOF
    echo "jacoco.includes=com/starrocks/**" > "$PROPERTIES_FILE"
    echo "Generated JaCoCo includes file: $OUTPUT_FILE"
    echo "Generated properties file: $PROPERTIES_FILE"
    echo "Maven property: jacoco.includes=com/starrocks/**"
    exit 0
fi

# Convert file paths to class patterns for JaCoCo includes
echo "Changed files:"
echo "$CHANGED_FILES"

# Generate include patterns
INCLUDE_PATTERNS=""
{
    echo "# JaCoCo includes generated from git diff"
    echo "# Base branch: $BASE_BRANCH"
    echo "# Generated on: $(date)"
    echo ""
    
    # Convert file paths to class patterns
    while IFS= read -r file; do
        if [ -n "$file" ]; then
            # Convert fe/fe-core/src/main/java/com/starrocks/example/Class.java to com/starrocks/example/**
            class_pattern=$(echo "$file" | sed 's|^fe/fe-core/src/main/java/||' | sed 's|/[^/]*\.java$|/**|')
            echo "$class_pattern"
            if [ -z "$INCLUDE_PATTERNS" ]; then
                INCLUDE_PATTERNS="$class_pattern"
            else
                INCLUDE_PATTERNS="$INCLUDE_PATTERNS,$class_pattern"
            fi
        fi
    done <<< "$CHANGED_FILES"
} > "$OUTPUT_FILE"

# Generate properties file for Maven
echo "jacoco.includes=$INCLUDE_PATTERNS" > "$PROPERTIES_FILE"

echo "Generated JaCoCo includes file: $OUTPUT_FILE"
echo "Generated properties file: $PROPERTIES_FILE"
echo "Include patterns:"
cat "$OUTPUT_FILE"
echo "Maven property: jacoco.includes=$INCLUDE_PATTERNS"
