#!/usr/bin/env bash

# Structured JSON logger for bash (similar to Go's slog)
# Usage: slog <level> <message> [key=value] [key=value] ...

## Source the script
#source logger.sh
#
## Set log level (optional, defaults to INFO)
#export LOG_LEVEL=0  # DEBUG
#
## Basic logging
#log_info "User logged in"
#
## With structured data
#log_info "User logged in" "user_id=12345" "username=johndoe"
#
## Mixed data types
#log_error "Connection failed" "host=localhost" "port=5432" "timeout=30" "ssl=true"
#
## Direct log function
#log "warn" "High CPU usage" "cpu_percent=85.5" "threshold=80"

# Output Example:
# {"time":"2024-07-09T10:30:45.123Z","level":"INFO","msg":"User logged in","hostname":"myhost","pid":1234,"user_id":12345,"username":"johndoe"}

# Configuration
LOG_LEVEL_DEBUG=0
LOG_LEVEL_INFO=1
LOG_LEVEL_WARN=2
LOG_LEVEL_ERROR=3

# Default log level (can be overridden with LOG_LEVEL environment variable)
CURRENT_LOG_LEVEL=${LOG_LEVEL:-$LOG_LEVEL_INFO}

# Convert log level string to number
get_log_level_num() {
    case "$1" in
        "debug"|"DEBUG") echo $LOG_LEVEL_DEBUG ;;
        "info"|"INFO") echo $LOG_LEVEL_INFO ;;
        "warn"|"WARN"|"warning"|"WARNING") echo $LOG_LEVEL_WARN ;;
        "error"|"ERROR") echo $LOG_LEVEL_ERROR ;;
        *) echo $LOG_LEVEL_INFO ;;
    esac
}

# Escape JSON string
escape_json() {
    local str="$1"
    # Escape backslashes, quotes, and control characters
    printf '%s' "$str" | sed 's/\\/\\\\/g; s/"/\\"/g; s/\x08/\\b/g; s/\x0c/\\f/g; s/\x0a/\\n/g; s/\x0d/\\r/g; s/\x09/\\t/g'
}

# Main logging function
slog() {
    local level="$1"
    local message="$2"
    shift 2

    # Check if we should log this level
    local level_num=$(get_log_level_num "$level")
    if [ "$level_num" -lt "$CURRENT_LOG_LEVEL" ]; then
        return 0
    fi

    # Get current timestamp in ISO 8601 format
    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")

    # Start building JSON
    local json_parts=()
    json_parts+=("\"time\":\"$timestamp\"")
    json_parts+=("\"level\":\"$(echo "$level" | tr '[:lower:]' '[:upper:]')\"")
    json_parts+=("\"msg\":\"$(escape_json "$message")\"")

    # Add hostname and process info
    json_parts+=("\"hostname\":\"$(hostname)\"")
    json_parts+=("\"pid\":$$")

    # Parse additional key-value pairs
    local key_value_pairs=()
    for arg in "$@"; do
        if [[ "$arg" == *"="* ]]; then
            local key="${arg%%=*}"
            local value="${arg#*=}"

            # Try to detect if value is a number or boolean
            if [[ "$value" =~ ^[0-9]+$ ]]; then
                # Integer
                key_value_pairs+=("\"$key\":$value")
            elif [[ "$value" =~ ^[0-9]+\.[0-9]+$ ]]; then
                # Float
                key_value_pairs+=("\"$key\":$value")
            elif [[ "$value" == "true" || "$value" == "false" ]]; then
                # Boolean
                key_value_pairs+=("\"$key\":$value")
            else
                # String
                key_value_pairs+=("\"$key\":\"$(escape_json "$value")\"")
            fi
        fi
    done

    # Combine all parts
    local all_parts=("${json_parts[@]}" "${key_value_pairs[@]}")
    local json_output="{"
    local first=true
    for part in "${all_parts[@]}"; do
        if [ "$first" = true ]; then
            first=false
        else
            json_output+=","
        fi
        json_output+="$part"
    done
    json_output+="}"

    # Output to stderr (standard for logging)
    echo "$json_output" >&2
}

# Convenience functions for different log levels
slog_debug() {
    slog "debug" "$@"
}

slog_info() {
    slog "info" "$@"
}

slog_warn() {
    slog "warn" "$@"
}

slog_error() {
    slog "error" "$@"
}

# Example usage:
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "=== Structured JSON Logger Demo ===" >&2
    echo "Setting LOG_LEVEL to DEBUG for demo" >&2
    export LOG_LEVEL=$LOG_LEVEL_DEBUG

    slog_info "Application started" "version=1.0.0" "env=production"
    slog_debug "Processing user request" "user_id=12345" "request_id=abc-123"
    slog_warn "High memory usage detected" "memory_usage=85.5" "threshold=80"
    slog_error "Database connection failed" "host=db.example.com" "port=5432" "timeout=30"

    # Example with mixed data types
    slog_info "User login successful" "user_id=67890" "username=johndoe" "admin=true" "login_count=42"

    # Example with special characters
    slog_info "Processing file" "filename=test file with spaces.txt" "path=/home/user/documents"
fi
