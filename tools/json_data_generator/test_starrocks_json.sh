#!/usr/bin/env bash

# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# StarRocks JSON Data Generator Test Script
# This script performs a complete test workflow:
# 1. Create database and table in StarRocks
# 2. Generate JSON test data
# 3. Import data via Stream Load
# 4. Run query tests

set -e

# Configuration - can be overridden by environment variables
SR_HOST=${SR_HOST:-"127.0.0.1"}
SR_QUERY_PORT=${SR_QUERY_PORT:-"9030"}
SR_HTTP_PORT=${SR_HTTP_PORT:-"8030"}
SR_USER=${SR_USER:-"root"}
SR_PASSWORD=${SR_PASSWORD:-""}
SR_DATABASE=${SR_DATABASE:-"json_test_db"}
SR_TABLE=${SR_TABLE:-"json_test_table"}

# Data generation parameters
NUM_RECORDS=${NUM_RECORDS:-10000}
NUM_FIELDS=${NUM_FIELDS:-30}
SPARSITY=${SPARSITY:-0.2}
MAX_DEPTH=${MAX_DEPTH:-3}
NEST_PROBABILITY=${NEST_PROBABILITY:-0.3}
HIGH_CARD_FIELDS=${HIGH_CARD_FIELDS:-5}
LOW_CARD_FIELDS=${LOW_CARD_FIELDS:-10}
SEED=${SEED:-42}

# Query generation parameters
NUM_QUERIES=${NUM_QUERIES:-10}
QUERY_TYPES=${QUERY_TYPES:-"filter,aggregation,select"}

# Segment generation parameters
NUM_SEGMENTS=${NUM_SEGMENTS:-1}

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILE="${SCRIPT_DIR}/test_data.jsonl"
GENERATOR_BIN="${SCRIPT_DIR}/json_generator"
QUERY_FILE="${SCRIPT_DIR}/generated_queries.sql"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print functions
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check dependencies
check_dependencies() {
    print_info "Checking dependencies..."
    
    if ! command -v mysql &> /dev/null; then
        print_error "mysql client is not installed. Please install mysql-client."
        exit 1
    fi
    
    if ! command -v curl &> /dev/null; then
        print_error "curl is not installed. Please install curl."
        exit 1
    fi

    if [ ! -x "$GENERATOR_BIN" ]; then
        print_error "JSON generator binary not found or not executable: $GENERATOR_BIN"
        print_error "Please build it first by running: (cd $SCRIPT_DIR && ./build.sh)"
        exit 1
    fi
    
    print_info "All dependencies are available."
}

# Execute SQL command
execute_sql() {
    local sql="$1"
    local password_arg=""
    local show_output="${2:-true}"  # Second parameter: whether to show output (default: true)
    
    if [ -n "$SR_PASSWORD" ]; then
        password_arg="-p${SR_PASSWORD}"
    fi
    
    if [ "$show_output" = "true" ]; then
        mysql -h"$SR_HOST" -P"$SR_QUERY_PORT" -u"$SR_USER" $password_arg -N -e "$sql" 2>&1
    else
        mysql -h"$SR_HOST" -P"$SR_QUERY_PORT" -u"$SR_USER" $password_arg -N -e "$sql" >/dev/null 2>&1
    fi
}

# Step 1: Create database and table
create_table() {
    print_info "Step 1: Creating database and table..."
    
    # Create database
    execute_sql "CREATE DATABASE IF NOT EXISTS ${SR_DATABASE};"
    print_info "Database '${SR_DATABASE}' created or already exists."
    
    # Create table
    local create_table_sql="
    USE ${SR_DATABASE};
    DROP TABLE IF EXISTS ${SR_TABLE};
    CREATE TABLE ${SR_TABLE} (
        id BIGINT NOT NULL AUTO_INCREMENT,
        json_data JSON NULL COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    PROPERTIES('replication_num'='1')
    "
    
    execute_sql "$create_table_sql"
    print_info "Table '${SR_TABLE}' created successfully."
}

# Step 2: Generate JSON data (optionally with queries)
generate_data() {
    local generate_queries="${1:-false}"  # First parameter: whether to generate queries (default: false)
    
    if [ "$generate_queries" = "true" ]; then
        print_info "Generating JSON test data and queries..."
        print_info "Parameters: records=${NUM_RECORDS}, fields=${NUM_FIELDS}, sparsity=${SPARSITY}"
        
        # Parse query types to calculate queries per type
        IFS=',' read -ra TYPES <<< "$QUERY_TYPES"
        local num_types=${#TYPES[@]}
        local queries_per_type=$((NUM_QUERIES / num_types))
        
        # Calculate total queries (may be slightly less than NUM_QUERIES if not evenly divisible)
        local total_queries=$((queries_per_type * num_types))
        
        print_info "Generating data and queries for types: $QUERY_TYPES"
        print_info "Queries per type: $queries_per_type (total: $total_queries)"
        
        # Generate data and all query types together in a single call
        # Suppress stdout to avoid printing all generated data, but keep stderr for errors
        if ! "$GENERATOR_BIN" \
            --num-records "$NUM_RECORDS" \
            --num-fields "$NUM_FIELDS" \
            --sparsity "$SPARSITY" \
            --max-depth "$MAX_DEPTH" \
            --nest-probability "$NEST_PROBABILITY" \
            --field-types string,int,bool,datetime,array,object \
            --high-cardinality-fields "$HIGH_CARD_FIELDS" \
            --low-cardinality-fields "$LOW_CARD_FIELDS" \
            --seed "$SEED" \
            --output "$DATA_FILE" \
            --gen-query-type "$QUERY_TYPES" \
            --gen-query-num "$queries_per_type" \
            --gen-query-table "$SR_TABLE" \
            --gen-query-column "json_data" \
            --gen-query-output "$QUERY_FILE" >/dev/null 2>&1; then
            print_error "Failed to generate data and queries."
            exit 1
        fi
        
        if [ ! -f "$QUERY_FILE" ] || [ ! -s "$QUERY_FILE" ]; then
            print_error "Failed to generate queries."
            exit 1
        fi
        
        local query_count=$(wc -l < "$QUERY_FILE" | tr -d ' ')
        print_info "Generated $query_count queries in $QUERY_FILE"
    else
        print_info "Generating JSON test data..."
        print_info "Parameters: records=${NUM_RECORDS}, fields=${NUM_FIELDS}, sparsity=${SPARSITY}"
        
        # Generate data only (no queries)
        if ! "$GENERATOR_BIN" \
            --num-records "$NUM_RECORDS" \
            --num-fields "$NUM_FIELDS" \
            --sparsity "$SPARSITY" \
            --max-depth "$MAX_DEPTH" \
            --nest-probability "$NEST_PROBABILITY" \
            --field-types string,int,bool,datetime,array,object \
            --high-cardinality-fields "$HIGH_CARD_FIELDS" \
            --low-cardinality-fields "$LOW_CARD_FIELDS" \
            --seed "$SEED" \
            --output "$DATA_FILE" >/dev/null 2>&1; then
            print_error "Failed to generate data."
            exit 1
        fi
    fi
    
    if [ ! -f "$DATA_FILE" ]; then
        print_error "Failed to generate data file."
        exit 1
    fi
    
    local file_size=$(du -h "$DATA_FILE" | cut -f1)
    print_info "Data file generated: $DATA_FILE (size: $file_size)"
}

# Step 3: Import data via Stream Load
import_data() {
    print_info "Step 3: Importing data via Stream Load..."
    
    local label="json_test_$(date +%s)"
    local url="http://${SR_HOST}:${SR_HTTP_PORT}/api/${SR_DATABASE}/${SR_TABLE}/_stream_load"
    local auth=""
    
    if [ -n "$SR_PASSWORD" ]; then
        auth="${SR_USER}:${SR_PASSWORD}"
    else
        auth="${SR_USER}:"
    fi
    
    local response=$(curl -s --location-trusted -u "$auth" \
        -H "label:${label}" \
        -H "Expect:100-continue" \
        -H "format: json" \
        -H "columns: json_data" \
        -H "jsonpaths: [\"$\"]" \
        -H "ignore_json_size: true" \
        -T "$DATA_FILE" -XPUT \
        "$url" 2>&1)
    
    # Check if import was successful
    if echo "$response" | grep -q '"Status": "Success"'; then
        print_info "Data imported successfully."
        
        # Extract import statistics
        local num_rows=$(echo "$response" | grep -o '"NumberTotalRows":"[0-9]*"' | grep -o '[0-9]*' || echo "N/A")
        local num_loaded=$(echo "$response" | grep -o '"NumberLoadedRows":"[0-9]*"' | grep -o '[0-9]*' || echo "N/A")
        print_info "Import statistics: Total rows: $num_rows, Loaded rows: $num_loaded"
    else
        print_error "Data import failed."
        echo "$response" | head -20
        exit 1
    fi
}

# Step 4: Run generated queries
run_queries() {
    print_info "Step 4: Running generated queries..."
    
    local password_arg=""
    if [ -n "$SR_PASSWORD" ]; then
        password_arg="-p${SR_PASSWORD}"
    fi
    
    local query_num=1
    local success_count=0
    local fail_count=0
    
    set +e
    while IFS= read -r query; do
        # Skip empty lines
        [ -z "$query" ] && continue
        
        # Remove trailing semicolon if present for display
        local query_display="${query%;}"
        print_info "Query $query_num: ${query_display:0:80}..."
        
        # Execute query and capture output with timing info
        local query_result
        query_result=$(mysql -vvv -h"$SR_HOST" -P"$SR_QUERY_PORT" -u"$SR_USER" $password_arg -N -e "USE ${SR_DATABASE}; $query" 2>&1)
        local query_exit_code=$?
        
        # Extract execution time from mysql -vvv output (format: "Query OK, ... (X.XX sec)")
        local exec_time=$(echo "$query_result" | grep -oE '\([0-9]+\.[0-9]+ sec\)' | tail -1 | sed 's/[()]//g' || echo "")
        
        if [ $query_exit_code -eq 0 ]; then
            success_count=$((success_count + 1))
            if [ -n "$exec_time" ]; then
                print_info "  ✓ Query $query_num executed successfully (${exec_time})"
            else
                print_info "  ✓ Query $query_num executed successfully"
            fi
        else
            fail_count=$((fail_count + 1))
            if [ -n "$exec_time" ]; then
                print_warn "  ✗ Query $query_num failed (${exec_time})"
            else
                print_warn "  ✗ Query $query_num failed"
            fi
            # Show the query that failed and error message
            print_warn "  Failed query: ${query_display:0:100}..."
            if [ -n "$query_result" ]; then
                local error_msg=$(echo "$query_result" | grep -v "Query OK" | head -1 | sed 's/^[[:space:]]*//')
                [ -n "$error_msg" ] && print_warn "  Error: $error_msg"
            fi
        fi
        
        query_num=$((query_num + 1))
    done < "$QUERY_FILE"
    set -e
    
    print_info "Query execution summary:"
    print_info "  Total queries: $((query_num - 1))"
    print_info "  Successful: $success_count"
    print_info "  Failed: $fail_count"
    
    if [ $fail_count -gt 0 ]; then
        print_warn "Some queries failed. Check the output above for details."
    fi
}

# Cleanup function
cleanup() {
    if [ "$1" != "--keep-data" ]; then
        print_info "Cleaning up temporary files..."
        [ -f "$DATA_FILE" ] && rm -f "$DATA_FILE"
        [ -f "$QUERY_FILE" ] && rm -f "$QUERY_FILE"
    fi
}

# Main function
main() {
    print_info "=========================================="
    print_info "StarRocks JSON Data Generator Test"
    print_info "=========================================="
    print_info "Configuration:"
    print_info "  Host: ${SR_HOST}"
    print_info "  Query Port: ${SR_QUERY_PORT}"
    print_info "  HTTP Port: ${SR_HTTP_PORT}"
    print_info "  User: ${SR_USER}"
    print_info "  Database: ${SR_DATABASE}"
    print_info "  Table: ${SR_TABLE}"
    print_info "  Num Segments: ${NUM_SEGMENTS}"
    print_info "=========================================="
    
    check_dependencies
    create_table
    
    # Generate and import data segments
    local segment=1
    while [ $segment -le $NUM_SEGMENTS ]; do
        print_info "=========================================="
        print_info "Processing segment $segment of $NUM_SEGMENTS"
        print_info "=========================================="
        
        # Generate queries only on the last segment
        if [ $segment -eq $NUM_SEGMENTS ]; then
            generate_data "true"
        else
            generate_data "false"
        fi
        
        import_data
        
        segment=$((segment + 1))
    done
    
    # Run queries (only if queries were generated)
    if [ -f "$QUERY_FILE" ] && [ -s "$QUERY_FILE" ]; then
        run_queries
    fi
    
    print_info "=========================================="
    print_info "Test completed successfully!"
    print_info "=========================================="
    print_info "Data file: $DATA_FILE"
    print_info "Query file: $QUERY_FILE"
    print_info "To keep the data and query files, run with --keep-data flag"
    
    # Cleanup
    if [[ "$*" != *"--keep-data"* ]]; then
        cleanup
    fi
}

# Handle script arguments
if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    echo "Usage: $0 [--keep-data]"
    echo ""
    echo "Environment variables:"
    echo "  SR_HOST          StarRocks FE host (default: 127.0.0.1)"
    echo "  SR_QUERY_PORT   StarRocks query port (default: 9030)"
    echo "  SR_HTTP_PORT     StarRocks HTTP port (default: 8030)"
    echo "  SR_USER          StarRocks username (default: root)"
    echo "  SR_PASSWORD      StarRocks password (default: empty)"
    echo "  SR_DATABASE      Database name (default: json_test_db)"
    echo "  SR_TABLE         Table name (default: json_test_table)"
    echo "  NUM_RECORDS       Number of records to generate (default: 10000)"
    echo "  NUM_FIELDS        Number of fields per record (default: 30)"
    echo "  SPARSITY          Sparsity ratio 0.0-1.0 (default: 0.2)"
    echo "  MAX_DEPTH         Max nesting depth (default: 3)"
    echo "  NEST_PROBABILITY   Nesting probability 0.0-1.0 (default: 0.3)"
    echo "  HIGH_CARD_FIELDS  High cardinality fields count (default: 5)"
    echo "  LOW_CARD_FIELDS   Low cardinality fields count (default: 10)"
    echo "  SEED              Random seed (default: 42)"
    echo "  NUM_QUERIES        Number of queries to generate (default: 10)"
    echo "  QUERY_TYPES        Comma-separated query types: filter,aggregation,select (default: filter,aggregation,select)"
    echo "  NUM_SEGMENTS       Number of data segments to generate and import (default: 1)"
    echo ""
    echo "Example:"
    echo "  SR_HOST=192.168.1.100 SR_PASSWORD=mypass $0"
    echo "  SR_HOST=192.168.1.100 NUM_QUERIES=20 QUERY_TYPES=filter,aggregation $0"
    exit 0
fi

# Run main function
main "$@"
