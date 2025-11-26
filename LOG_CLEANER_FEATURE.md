# Automatic Log Cleaner Based on Disk Usage

## Overview

This feature implements an automatic log cleaning mechanism that monitors disk usage and cleans log files when disk space becomes critical. The cleaner runs as a background daemon on the Frontend (FE) leader node and helps prevent disk space exhaustion from log file accumulation.

## Features

- **Automatic Disk Usage Monitoring**: Periodically checks disk usage for each configured log directory
- **Threshold-Based Cleaning**: Triggers log cleaning when disk usage exceeds a configurable threshold (default: 80%)
- **Target-Based Stopping**: Continues cleaning until disk usage drops below a target threshold (default: 60%)
- **Per-Directory Processing**: Each log directory is checked and cleaned independently
- **Audit Log Protection**: Audit log files are protected with a minimum retention period (default: 3 days)
- **All Log Types Support**: Scans and cleans all log file types in each directory, regardless of configuration

## Configuration

The following configuration parameters are available in `fe.conf`:

### `log_cleaner_enable`
- **Type**: boolean
- **Default**: `true`
- **Description**: Enable or disable automatic log cleaning based on disk usage
- **Mutable**: Yes

### `log_cleaner_disk_usage_threshold`
- **Type**: int
- **Default**: `80`
- **Description**: Disk usage threshold (percentage) to trigger log cleaning. When disk usage exceeds this threshold, log cleaning will start
- **Mutable**: Yes

### `log_cleaner_disk_usage_target`
- **Type**: int
- **Default**: `60`
- **Description**: Target disk usage (percentage) after log cleaning. Log cleaning will continue until disk usage drops below this threshold
- **Mutable**: Yes

### `log_cleaner_audit_log_min_retention_days`
- **Type**: int
- **Default**: `3`
- **Description**: Minimum retention days for audit log files. Audit log files newer than this will not be deleted even if disk usage is high
- **Mutable**: Yes

### `log_cleaner_check_interval_second`
- **Type**: int
- **Default**: `300` (5 minutes)
- **Description**: Interval in seconds to check disk usage and clean logs
- **Mutable**: Yes

## How It Works

1. **Directory Scanning**: The cleaner scans all configured log directories:
   - `sys_log_dir`
   - `audit_log_dir`
   - `internal_log_dir`
   - `dump_log_dir`
   - `big_query_log_dir`
   - `profile_log_dir`
   - `feature_log_dir`

2. **Disk Usage Check**: For each directory, the cleaner checks the disk usage percentage. If multiple log directories point to the same path, they are automatically merged to avoid duplicate processing.

3. **Log File Collection**: When disk usage exceeds the threshold, the cleaner collects all log files in that directory matching the following patterns:
   - `fe.log`
   - `fe.warn.log`
   - `fe.audit.log`
   - `fe.internal.log`
   - `fe.dump.log`
   - `fe.big_query.log`
   - `fe.profile.log`
   - `fe.features.log`

4. **Cleaning Strategy**: 
   - Files are sorted by modification time (oldest first)
   - One oldest file is deleted per iteration
   - Audit log files are protected if they are newer than the minimum retention period
   - Cleaning continues until disk usage drops below the target threshold

5. **Safety Mechanisms**:
   - Maximum iteration limit (1000) to prevent infinite loops
   - File existence verification before deletion
   - Only runs on the FE leader node
   - Respects audit log retention policy

## Usage Example

### Enable Log Cleaner (Default)
```properties
# Log cleaner is enabled by default
log_cleaner_enable = true
```

### Customize Thresholds
```properties
# Trigger cleaning when disk usage exceeds 85%
log_cleaner_disk_usage_threshold = 85

# Stop cleaning when disk usage drops below 70%
log_cleaner_disk_usage_target = 70

# Keep audit logs for at least 7 days
log_cleaner_audit_log_min_retention_days = 7

# Check every 10 minutes
log_cleaner_check_interval_second = 600
```

### Disable Log Cleaner
```properties
log_cleaner_enable = false
```

## Log Output

The cleaner logs its activities at different levels:

- **INFO**: When cleaning starts, files deleted, and cleaning completion
- **DEBUG**: Current disk usage, iteration progress
- **WARN**: When no more files can be deleted, maximum iterations reached, or deletion failures

Example log output:
```
INFO: Disk usage 85.23% for directory /path/to/log exceeds threshold 80%, starting log cleanup
INFO: Deleted log file: /path/to/log/fe.log.20240101-1 (size: 1048576 bytes)
INFO: Log cleanup completed for directory /path/to/log. Deleted 5 files. Final disk usage: 58.45%
```

## Benefits

1. **Prevents Disk Space Exhaustion**: Automatically prevents log files from filling up disk space
2. **Configurable**: All parameters are runtime-mutable, allowing adjustments without restart
3. **Safe**: Protects audit logs with minimum retention period
4. **Efficient**: Only processes directories that exceed the threshold
5. **Comprehensive**: Handles all log types in each directory, even when multiple log directories point to the same path

## Implementation Details

- **Class**: `com.starrocks.common.LogCleaner`
- **Extends**: `FrontendDaemon`
- **Registration**: Automatically registered and started in `GlobalStateMgr`
- **Thread Safety**: Runs in a single daemon thread, only on the leader node

## Notes

- The cleaner only runs on the FE leader node
- All configuration parameters are mutable and can be changed at runtime
- The cleaner respects existing log rotation and deletion policies configured via other parameters (e.g., `sys_log_delete_age`, `audit_log_delete_age`)
- When multiple log directories are configured to the same path, they are automatically merged to avoid duplicate processing

