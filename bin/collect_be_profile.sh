#!/bin/bash
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

# Early error handler - output to stderr before log system is initialized
early_error() {
    echo "[ERROR] $1" >&2
    exit 1
}

# Configuration check mode - enable strict error checking
check_config_mode() {
    set -e
}

# Runtime mode - disable strict error checking to allow graceful degradation
runtime_mode() {
    set +e
}

# Default configuration
DEFAULT_DURATION=10
DEFAULT_BRPC_PORT=8060
DEFAULT_CLEANUP_DAYS=1
DEFAULT_CLEANUP_SIZE=2147483648  # 2GB in bytes
DEFAULT_INTERVAL=60

# Global variables
DURATION=$DEFAULT_DURATION
OUTPUT_DIR=""
BRPC_PORT=$DEFAULT_BRPC_PORT
CLEANUP_DAYS=$DEFAULT_CLEANUP_DAYS
CLEANUP_SIZE=$DEFAULT_CLEANUP_SIZE
DAEMON_MODE=false
INTERVAL=$DEFAULT_INTERVAL
INTERVAL_SET_BY_USER=false  # Track if interval was set via command line
PID_FILE=""
LOG_FILE="/dev/stdout" 
PROFILING_TYPE="cpu"  # Default profiling type
BE_CONF_PATH=""      # Custom be.conf path


usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Collect CPU and contention profiles from StarRocks BE BRPC server"
    echo ""
    echo "Options:"
    echo "  --profiling-type <type>  Profiling type: cpu, contention, or both (default: cpu)"
    echo "  --duration <seconds>     Profile duration (default: $DEFAULT_DURATION)"
    echo "  --output-dir <path>      Output directory (default: read from be.conf)"
    echo "  --be-conf <path>         Path to be.conf file (default: \$STARROCKS_HOME/conf/be.conf)"
    echo "  --brpc-port <port>       BRPC port (default: $DEFAULT_BRPC_PORT)"
    echo "  --cleanup-days <days>    Delete files older than N days (default: $DEFAULT_CLEANUP_DAYS)"
    echo "  --cleanup-size <bytes>   Max total size to retain (default: 2GB)"
    echo "  --daemon                 Run continuously in background"
    echo "  --interval <seconds>     Collection interval in daemon mode (default: $DEFAULT_INTERVAL seconds)"
    echo "  --help                   Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --profiling-type cpu --duration 30"
    echo "  $0 --profiling-type both --daemon --interval 1800"
    echo "  $0 --profiling-type contention"
    echo "  $0 --profiling-type contention --be-conf /custom/path/be.conf"
    exit 1
}

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg" >> "$LOG_FILE" 2>&1
}

log_warn() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $1"
    echo "$msg" >> "$LOG_FILE" 2>&1
}

log_error() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1"
    echo "$msg" >> "$LOG_FILE" 2>&1
}

# Check if a directory or file is writable, exit if not
check_writable() {
    local path="$1"
    local path_type="$2"  # "directory" or "file"
    
    if [ -z "$path" ]; then
        return 1
    fi
    
    # If it's a file path, check the directory containing it
    if [ "$path_type" = "file" ]; then
        local dir_path=$(dirname "$path")
        if [ -z "$dir_path" ] || [ "$dir_path" = "." ]; then
            dir_path="$(pwd)"
        fi
        path="$dir_path"
        path_type="directory"
    fi
    
    # Check if directory exists
    if [ -d "$path" ]; then
        # Directory exists, check if it's writable
        if [ ! -w "$path" ]; then
            early_error "Directory is not writable: $path. Please check permissions or choose a different location"
        fi
    else
        # Directory doesn't exist, check if parent directory is writable
        local parent_dir=$(dirname "$path")
        if [ -z "$parent_dir" ] || [ "$parent_dir" = "." ]; then
            parent_dir="$(pwd)"
        fi
        
        if [ ! -d "$parent_dir" ]; then
            early_error "Parent directory does not exist: $parent_dir"
        fi
        
        if [ ! -w "$parent_dir" ]; then
            early_error "Parent directory is not writable: $parent_dir. Cannot create directory: $path. Please check permissions or choose a different location"
        fi
    fi
}

# Read configuration from be.conf
read_config() {
    # Enable strict error checking during configuration phase
    check_config_mode
    
    if [ -z "$STARROCKS_HOME" ]; then
        STARROCKS_HOME=$(dirname $(dirname $(readlink -f "$0")))
    fi
    
    if [ -z "$STARROCKS_HOME" ] || [ ! -d "$STARROCKS_HOME" ]; then
        early_error "STARROCKS_HOME is not set or invalid: $STARROCKS_HOME"
    fi
    
    # Source common.sh to get configuration loading functions
    # Note: These warnings use echo because log system is not initialized yet
    if [ ! -f "$STARROCKS_HOME/bin/common.sh" ]; then
        echo "[WARNING] Cannot find common.sh at: $STARROCKS_HOME/bin/common.sh, some features may not work" >&2
        runtime_mode  # Disable strict checking if common.sh is missing
    else
        # Try to source common.sh, but don't fail if it doesn't work
        runtime_mode  # Disable strict checking for source operations
        if ! source "$STARROCKS_HOME/bin/common.sh" 2>/dev/null; then
            echo "[WARNING] Failed to source common.sh, continuing with defaults" >&2
        else
            # Try to export shared envvars, but don't fail if it doesn't work
            if ! export_shared_envvars 2>/dev/null; then
                echo "[WARNING] Failed to export shared environment variables, continuing with defaults" >&2
            fi
        fi
    fi
    
    # Use custom be.conf path if provided, otherwise use default
    local be_conf
    if [ -n "$BE_CONF_PATH" ]; then
        be_conf="$BE_CONF_PATH"
    else
        be_conf="$STARROCKS_HOME/conf/be.conf"
    fi
    
    if [ -f "$be_conf" ]; then
        log "Reading configuration from: $be_conf"
        
        # Load all uppercase environment variables from be.conf
        # Runtime mode allows graceful failure
        export_env_from_conf "$be_conf" 2>/dev/null || true
        
        # Read sys_log_dir using read_var_from_conf
        if [ -z "$OUTPUT_DIR" ]; then
            read_var_from_conf sys_log_dir "$be_conf" 2>/dev/null || true
            OUTPUT_DIR="${sys_log_dir:-$STARROCKS_HOME/log}"
        fi
        
        # Read brpc_port using read_var_from_conf
        if [ "$BRPC_PORT" = "$DEFAULT_BRPC_PORT" ]; then
            read_var_from_conf brpc_port "$be_conf" 2>/dev/null || true
            if [ -n "$brpc_port" ]; then
                BRPC_PORT=$brpc_port
            fi
        fi
        
        # Read COLLECT_BE_PROFILE_INTERVAL if not set by user
        # Only support uppercase environment variable (from export_env_from_conf)
        if [ "$INTERVAL_SET_BY_USER" != true ]; then
            if [ -n "$COLLECT_BE_PROFILE_INTERVAL" ] && [ "$COLLECT_BE_PROFILE_INTERVAL" -gt 0 ] 2>/dev/null; then
                INTERVAL=$COLLECT_BE_PROFILE_INTERVAL
                log "Using COLLECT_BE_PROFILE_INTERVAL from be.conf: $INTERVAL seconds"
            fi
        fi
    else
        if [ -z "$OUTPUT_DIR" ]; then
            OUTPUT_DIR="$STARROCKS_HOME/log"
        fi
    fi
    
    OUTPUT_DIR="$OUTPUT_DIR/proc_profile"
    PID_FILE="$STARROCKS_HOME/bin/collect_be_profile.pid"
    # Set LOG_FILE: use file path in daemon mode, keep /dev/stdout in non-daemon mode
    if [ "$DAEMON_MODE" = true ] || [ "$LOG_FILE" != "/dev/stdout" ]; then
        LOG_FILE="$OUTPUT_DIR/../collect_be_profile.log"
    fi
    
    # Create all necessary directories before checking write permissions
    mkdir -p "$OUTPUT_DIR" "$(dirname "$PID_FILE")" 2>/dev/null || true
    
    check_config_mode
    check_writable "$OUTPUT_DIR" "directory"
    # Skip writable check for /dev/stdout as it's always writable
    if [ "$LOG_FILE" != "/dev/stdout" ]; then
        check_writable "$LOG_FILE" "file"
        touch "$LOG_FILE" 2>/dev/null || true
    fi
    check_writable "$PID_FILE" "file"
    
    # Switch to runtime mode after configuration is complete
    runtime_mode
    
    # Log configuration values
    log "BRPC_PORT: $BRPC_PORT"
    log "OUTPUT_DIR: $OUTPUT_DIR"
    log "Collection interval: $INTERVAL seconds"
}

# Check if BRPC server is accessible
check_brpc_server() {
    local url="http://localhost:$BRPC_PORT/pprof/cpu?seconds=1"
    if ! curl -s --connect-timeout 5 "$url" > /dev/null 2>&1; then
        log_error "Cannot connect to BRPC server at localhost:$BRPC_PORT"
        log_error "Please ensure StarRocks BE is running and BRPC port is correct"
        return 1
    fi
    return 0
}

# Generate timestamp for filename
get_timestamp() {
    date '+%Y%m%d-%H%M%S'
}

# Format bytes into human-readable format (portable alternative to numfmt)
format_bytes() {
    local bytes=$1
    local units=("B" "KB" "MB" "GB" "TB")
    local unit_index=0
    
    while [ $bytes -ge 1024 ] && [ $unit_index -lt $((${#units[@]} - 1)) ]; do
        bytes=$((bytes / 1024))
        unit_index=$((unit_index + 1))
    done
    
    echo "${bytes}${units[$unit_index]}"
}

# Generic profile collection function
collect_profile() {
    local profile_type="$1"  # cpu or contention
    local timestamp=$(get_timestamp)
    local base_name="${profile_type}-profile-$timestamp"
    
    log "Collecting ${profile_type} profile for ${DURATION} seconds..."
    
    # Determine the correct endpoint based on profile type
    local endpoint
    if [ "$profile_type" = "cpu" ]; then
        endpoint="profile"
    elif [ "$profile_type" = "contention" ]; then
        endpoint="contention"
    else
        log_error "Unknown profile type: $profile_type"
        return 1
    fi
    
    # Collect pprof format (default format from BE)
    local pprof_file="$OUTPUT_DIR/${base_name}-pprof.gz"
    local pprof_url="http://localhost:$BRPC_PORT/pprof/${endpoint}?seconds=$DURATION"
    
    log "Executing curl command: curl -s \"$pprof_url\" | gzip > \"$pprof_file\""
    
    # Check if curl command succeeds
    if ! curl -s "$pprof_url" | gzip > "$pprof_file"; then
        log_error "Failed to collect ${profile_type} profile from $pprof_url"
        return 1
    fi
    
    # Check if file was created and has content
    if [ ! -f "$pprof_file" ] || [ ! -s "$pprof_file" ]; then
        log_error "Profile file was not created or is empty: $pprof_file"
        return 1
    fi
    
    log "${profile_type^} pprof saved: $pprof_file"
    return 0
}

# Collect CPU profile
collect_cpu_profile() {
    collect_profile "cpu"
}

# Collect contention profile
collect_contention_profile() {
    collect_profile "contention"
}

# Cleanup old files
cleanup_old_files() {
    log "Cleaning up old profile files..."
    
    local total_size=0
    local files_to_delete=()
    
    # Get all profile files sorted by modification time (oldest first)
    while IFS= read -r -d '' file; do
        if [[ "$file" =~ (cpu-profile-|contention-profile-).*\.gz$ ]]; then
            local file_size=$(stat -c%s "$file" 2>/dev/null || echo 0)
            local file_age=$(($(date +%s) - $(stat -c%Y "$file" 2>/dev/null || echo 0)))
            local file_age_days=$((file_age / 86400))
            
            # Check if file is too old
            if [ $file_age_days -gt $CLEANUP_DAYS ]; then
                files_to_delete+=("$file")
                log "Marking for deletion (age: ${file_age_days} days): $(basename "$file")"
            else
                total_size=$((total_size + file_size))
            fi
        fi
    done < <(find "$OUTPUT_DIR" -name "*.gz" -type f -print0 | sort -z)
    
    # Delete files that are too old
    for file in "${files_to_delete[@]}"; do
        rm -f "$file"
        log "Deleted old file: $(basename "$file")"
    done
    
    # Delete files if total size exceeds limit (oldest first)
    if [ $total_size -gt $CLEANUP_SIZE ]; then
        log_warn "Total size ($(format_bytes $total_size)) exceeds limit ($(format_bytes $CLEANUP_SIZE))"
        
        while IFS= read -r -d '' file; do
            if [[ "$file" =~ (cpu-profile-|contention-profile-).*\.gz$ ]]; then
                local file_size=$(stat -c%s "$file" 2>/dev/null || echo 0)
                rm -f "$file"
                total_size=$((total_size - file_size))
                log "Deleted file to reduce size: $(basename "$file")"
                
                if [ $total_size -le $CLEANUP_SIZE ]; then
                    break
                fi
            fi
        done < <(find "$OUTPUT_DIR" -name "*.gz" -type f -print0 | sort -z)
    fi
}

# Daemon mode functions
start_daemon() {
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log_error "Daemon is already running (PID: $pid)"
            exit 1
        else
            rm -f "$PID_FILE"
        fi
    fi
    
    log "Starting profile collection daemon..."
    log "Output directory: $OUTPUT_DIR"
    log "BRPC port: $BRPC_PORT"
    log "Collection interval: $INTERVAL seconds"
    log "Log file: $LOG_FILE"
    
    # Start daemon in background
    nohup "$0" --daemon-internal "$@" > "$LOG_FILE" 2>&1 &
    local daemon_pid=$!
    echo "$daemon_pid" > "$PID_FILE"
    
    log "Daemon started with PID: $daemon_pid"
    log "Use 'kill $daemon_pid' or 'pkill -f collect_be_profile.sh' to stop"
}

# Internal daemon loop
daemon_loop() {
    log "Profile collection daemon started (PID: $$)"
    
    # Set up signal handlers
    trap 'log "Received SIGTERM, shutting down..."; cleanup_and_exit' TERM
    trap 'log "Received SIGINT, shutting down..."; cleanup_and_exit' INT
    
    while true; do
        # Check if BRPC server is still accessible
        if ! check_brpc_server; then
            log_error "BRPC server not accessible, waiting for next interval..."
            sleep 60
            continue
        fi
        
        # Collect profiles
        if [ "$PROFILING_TYPE" = "cpu" ] || [ "$PROFILING_TYPE" = "both" ]; then
            collect_cpu_profile
        fi
        
        if [ "$PROFILING_TYPE" = "contention" ] || [ "$PROFILING_TYPE" = "both" ]; then
            collect_contention_profile
        fi
        
        # Cleanup old files
        cleanup_old_files
        
        # Wait for next interval
        log "Waiting $INTERVAL seconds until next collection..."
        sleep $INTERVAL
    done
}

cleanup_and_exit() {
    log "Cleaning up and exiting..."
    rm -f "$PID_FILE"
    exit 0
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --duration)
                DURATION="$2"
                shift 2
                ;;
            --output-dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --brpc-port)
                BRPC_PORT="$2"
                shift 2
                ;;
            --cleanup-days)
                CLEANUP_DAYS="$2"
                shift 2
                ;;
            --cleanup-size)
                CLEANUP_SIZE="$2"
                shift 2
                ;;
            --profiling-type)
                PROFILING_TYPE="$2"
                if [ "$PROFILING_TYPE" != "cpu" ] && [ "$PROFILING_TYPE" != "contention" ] && [ "$PROFILING_TYPE" != "both" ]; then
                    log_error "Invalid profiling type: $PROFILING_TYPE. Must be 'cpu', 'contention', or 'both'"
                    exit 1
                fi
                shift 2
                ;;
            --be-conf)
                BE_CONF_PATH="$2"
                shift 2
                ;;
            --daemon)
                DAEMON_MODE=true
                shift
                ;;
            --daemon-internal)
                DAEMON_MODE=true
                DAEMON_INTERNAL=true
                shift
                ;;
            --interval)
                INTERVAL="$2"
                INTERVAL_SET_BY_USER=true
                shift 2
                ;;
            --help)
                usage
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done
}

# Main execution
main() {
    # Start in runtime mode (no strict error checking) - allows graceful degradation
    runtime_mode
    
    # Trap errors and output to stderr before exiting
    # Note: This trap uses echo because it may fire before log system is initialized
    trap 'echo "[ERROR] Script failed at line $LINENO. Command: $BASH_COMMAND" >&2; exit 1' ERR
    
    parse_args "$@"
    
    read_config
    
    if [ "$DAEMON_MODE" = true ]; then
        if [ "$DAEMON_INTERNAL" = true ]; then
            # In daemon internal mode, redirect all output to log file
            exec >> "$LOG_FILE" 2>&1
            daemon_loop
        else
            start_daemon "$@"
        fi
    else
        # Single collection mode - log messages go to both stdout and log file
        if ! check_brpc_server; then
            log_error "BRPC server check failed"
            exit 1
        fi
        
        if [ "$PROFILING_TYPE" = "cpu" ] || [ "$PROFILING_TYPE" = "both" ]; then
            if ! collect_cpu_profile; then
                log_error "Failed to collect CPU profile"
                exit 1
            fi
        fi
        
        if [ "$PROFILING_TYPE" = "contention" ] || [ "$PROFILING_TYPE" = "both" ]; then
            if ! collect_contention_profile; then
                log_error "Failed to collect contention profile"
                exit 1
            fi
        fi
        
        cleanup_old_files
        log "Profile collection completed"
    fi
}

# Run main function
main "$@"
