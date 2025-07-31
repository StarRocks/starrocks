When deploying and operating StarRocks, understanding and properly using the logging system is critical for troubleshooting, performance analysis, and system tuning. This article provides a detailed overview of the log file types, typical content, configuration methods, and log rolling and retention strategies for both the Frontend (FE) and Backend (BE or CN) components of StarRocks.

The information in this document is based on StarRocks Version 3.5.x.

## FE Logging in detail

### `fe.log`

The main FE logs include the startup process, cluster state changes, DML/DQL requests, and scheduling-related information. These logs primarily record the behavior of the FE during its runtime.

#### Configuration

- `sys_log_dir`: Log storage directory. Default is `${STARROCKS_HOME}/log`
- `sys_log_level`: Log level. Default is `INFO`
- `sys_log_roll_num`: Controls the number of retained log files to prevent unlimited growth from consuming too much disk space. Default is 10
- `sys_log_roll_interval`: Specifies the rotation frequency. Default is `DAY`, meaning logs are rotated daily
- `sys_log_delete_age`: Controls how long to keep old log files before deletion. Default is 7 day
- `sys_log_roll_mode`: Log rotation mode. Default is `SIZE-MB-1024`, meaning a new log file will be created when the current one reaches 1024 MB. Together with sys_log_roll_interval, this indicates that FE logs can be rotated either daily or based on file size
- `sys_log_enable_compress`: Controls whether log compression is enabled. Default is false, meaning compression is disabled

### `fe.warn.log`

`fe.warn.log` is an important log file for system monitoring and troubleshooting:
- Operations monitoring–monitors system health status
- Fault diagnosis–quickly locates critical issues
- Performance analysis–identifies system bottlenecks and anomalies
- Security auditing–records permission and access errors
Compared to `fe.log`, which records logs of all levels, `fe.warn.log` focuses on warnings and errors that require attention, helping operations personnel quickly identify and address system issues.

### `fe.gc.log`

`fe.gc.log` is the Java garbage collection log of the StarRocks FE, used to monitor and analyze JVM garbage collection behavior.

It’s important to note that this log file uses the native JVM log rotation mechanism. For example, you can enable automatic rotation based on file size and file count with the following configuration:

```bash
-Xlog:gc*:${LOG_DIR}/fe.gc.log:time,tags:filecount=7,filesize=100M
```

### `fe.out`

`fe.out` is the standard output log file of StarRocks FE. It records the content printed to standard output (stdout) and standard error (stderr) during the runtime of the FE process. The main content includes:

- Console output during FE startup
  - JVM startup information (e.g., heap settings, GC parameters)
  - FE module initialization order (Catalog, Scheduler, RPC, HTTP Server, etc.)
- Error stack traces from stderr
  - Java exceptions (Exception/StackTrace)
  - Uncaught errors like ClassNotFound, NullPointerException, etc.
- Output not captured by other logging systems
  - Some third-party libraries that use `System.out.println()` or `e.printStackTrace()`

You should check `fe.out` in the following scenarios:

- FE fails to start: Check `fe.out` for Java exceptions or invalid parameter messages.
- FE crashes unexpectedly: Look for uncaught exception stack traces.
- Logging system uncertainty: If `fe.out` is the only log file with output in the FE log directory, it’s likely that `log4j` failed to initialize, or the configuration is incorrect.
By default, `fe.out` does not support automatic log rotation.

### `fe.profile.log`

The purpose of `fe.profile.log` is to record detailed query execution information for performance analysis. Its main functions include:

- Query performance analysis: Logs detailed execution data for each query, including:
  - Query ID, user, database, SQL statement
  - Execution timing (startTime, endTime, latency)
  - Resource usage (CPU, memory, number/size of scanned rows)
  - Execution status (RUNNING, FINISHED, FAILED, CANCELLED)
- Runtime metrics tracking: Captures key indicators via the QueryDetail class:
  - `scanRows / scanBytes`: Amount of data scanned
  - `returnRows`: Number of result rows returned
  - `cpuCostNs`: CPU time consumed (in nanoseconds)
  - `memCostBytes`: Memory usage
  - `spillBytes`: Amount of data spilled to disk
- Error diagnosis: Records error messages and stack traces for failed queries
- Resource group monitoring: Tracks query execution metrics across different resource groups
`fe.profile.log` is stored in JSON format.

#### Configuration

- `enable_profile_log`: Whether to enable profile logging
- `profile_log_dir`: Directory for storing profile logs
- `profile_log_roll_size_mb`: Log rotation size (in MB)
- `profile_log_roll_num`: Controls the number of retained profile log files to prevent unlimited growth and excessive disk usage. Default is 5
- `profile_log_roll_interval`: Specifies the rotation frequency. Default is DAY, meaning daily rotation. When rotation conditions are met, the latest 5 files are retained, and older files are deleted
- `profile_log_delete_age`: Controls how long old files are kept before deletion. Default is 1 day
- `enable_profile_log_compress`: Controls whether profile log compression is enabled. Default is false, meaning compression is disabled

### `fe.internal.log`

The purpose of `fe.internal.log` is to record logs dedicated to internal operations of the FE (Frontend), primarily for system-level auditing and debugging. Its main functions include:
1. Internal operation auditing: Logs system-initiated internal SQL executions separately from user queries
2. Statistics tracking: Specifically records operations related to statistics collection
3. Debugging support: Provides detailed logs for troubleshooting internal operation issues
The log records include entries such as:
- Statistics module (internal.statistic)
- Core system module (internal.base)

This log is especially useful for analyzing StarRocks’ internal statistics collection process and diagnosing issues related to internal operations.

#### Configuration

- `internal_log_dir`: Controls the storage directory for this log
- `internal_log_modules`: An array configuring internal log modules, defining which internal operation modules need to be recorded in the `fe.internal.log` file. Default is `{"base", "statistic"}`
- `internal_log_roll_num`: Number of files to retain. Default is 90
- `internal_log_roll_interval`: Specifies the rotation frequency. Default is DAY, meaning daily rotation. When rotation conditions are met, the latest 90 files are retained, and older files are deleted
- `internal_log_delete_age`: Controls how long old files are kept before deletion. Default is 7 days

### `fe.audit.log`

This is StarRocks’ query audit log, which records detailed information about all user queries and connections. It is used for monitoring, analysis, and auditing. Its main purposes include:

1. Query monitoring: Logs the execution status and performance metrics of all SQL queries
2. User auditing: Tracks user behavior and database access
3. Performance analysis: Provides metrics such as query execution time and resource consumption
4. Issue diagnosis: Records error statuses and error codes to facilitate troubleshooting

#### Configuration

- `audit_log_dir`: Controls the storage directory for this log
- `audit_log_roll_num`: Number of files to retain. Default is 90
- `audit_log_roll_interval`: Specifies the rotation frequency. Default is DAY, meaning daily rotation. When rotation conditions are met, the latest 90 files are retained, and older files are deleted
- `audit_log_delete_age`: Controls how long old files are kept before deletion. Default is 7 days
- `audit_log_json_format`: Whether to log in JSON format. Default is false
- `audit_log_enable_compress`: Whether compression is enabled

### `fe.big_query.log`

This is StarRocks’ dedicated Big Query log file, used to monitor and analyze queries with high resource consumption. Its structure is similar to the audit log, but it includes three additional fields:
- `bigQueryLogCPUSecondThreshold`: CPU time threshold
- `bigQueryLogScanBytesThreshold`: Scan size (in bytes) threshold
- `bigQueryLogScanRowsThreshold`: Scan row count threshold

#### Configuration

- `big_query_log_dir`: Controls the storage directory for this log
- `big_query_log_roll_num`: Number of files to retain. Default is 10
- `big_query_log_modules`: Types of internal log modules. Default is query
- `big_query_log_roll_interval`: Specifies the rotation frequency. Default is DAY, meaning daily rotation. When rotation conditions are met, the latest 10 files are retained, and older files are deleted
- `big_query_log_delete_age`: Controls how long old files are kept before deletion. Default is 7 days

### `fe.dump.log`

This is StarRocks’ query dump log, specifically used for detailed query debugging and issue diagnosis. Its main purposes include:
- Exception debugging: Automatically records the complete query context when query execution encounters exceptions
- Issue reproduction: Provides sufficiently detailed information to reproduce query problems
- In-depth diagnosis: Contains debugging information such as metadata, statistics, execution plans, and more
- Technical support: Provides comprehensive data for the technical support team to analyze issues
It can be enabled using the following command:

```bash
SET enable_query_dump = true;
```

#### Configuration

- `dump_log_dir`: Controls the storage directory for this log
- `dump_log_roll_num`: Number of files to retain. Default is 10
- `dump_log_modules`: Types of internal log modules. Default is query
- `dump_log_roll_interval`: Specifies the rotation frequency. Default is DAY, meaning daily rotation. When rotation conditions are met, the latest 10 files are retained, and older files are deleted
- `dump_log_delete_age`: Controls how long old files are kept before deletion. Default is 7 days

### `fe.features.log`
This is StarRocks’ query plan feature log, used to collect and record feature information of query execution plans. It mainly serves machine learning and query optimization analysis. Key purposes include:
1. Query plan feature collection: Extracting various characteristics from query execution plans
2. Machine learning data source: Providing training data for query cost prediction models
3. Query pattern analysis: Analyzing execution patterns and feature distributions
4. Optimizer improvement: Supplying data to support enhancements in the cost-based optimizer (CBO)

It can be enabled via configuration.

```bash
// Enable plan feature collection  
enable_plan_feature_collection = false  // Disabled by default  

// Enable query cost prediction  
enable_query_cost_prediction = false  // Disabled by default  
```

#### Configuration

- `feature_log_dir`: Controls the storage directory for this log
- `feature_log_roll_num`: Number of files to retain. Default is 5
- `feature_log_roll_interval`: Specifies the rotation frequency. Default is DAY, meaning daily rotation. When rotation conditions are met, the latest 5 files are retained, and older files are deleted
- `feature_log_delete_age`: Controls how long old files are kept before deletion. Default is 3 days
- `feature_log_roll_size_mb`: Log rotation size. Default is 1024 MB, meaning a new file is created every 1 GB

## BE/CN Logging in detail

### `{be or cn}.INFO.log`

It primarily records various runtime behavior logs generated by BE/CN nodes, and these logs are at the `INFO` level. For example:

- System startup information:
  - BE process startup and initialization
  - Hardware resource detection (CPU, memory, disk)
  - Configuration parameter loading
- Query execution information:
  - Query reception and dispatch
  - Fragment execution status
- Storage-related information:
  - Tablet loading and unloading
  - Compaction execution process
  - Data import status
  - Storage space management

#### Configuration

- `sys_log_level`: Log level, default is `INFO`
- `sys_log_dir`: Log storage directory, default is `${STARROCKS_HOME}/log`
- `sys_log_roll_mode`: Log rotation mode, default is `SIZE-MB-1024`, meaning a new log file is created when the current one reaches 1024 MB
- `sys_log_roll_num`: Number of retained log files, default is 10

### `{be or cn}.WARN.log`

`be.WARN.log` stores log entries at WARNING level and above. Examples include 
Query execution warnings:
- Query execution time too long
- Memory allocation failure warning
- Operator execution exception
Storage-related warnings:
- Abnormal Tablet status
- Slow compaction execution
- Data file corruption warning
- Storage I/O exception

#### Configuration

- `sys_log_level`: Log level, default is INFO
- `sys_log_dir`: Log storage directory, default is `${STARROCKS_HOME}/log`
- `sys_log_roll_mode`: Log rotation mode, default is `SIZE-MB-1024`, meaning a new log file is created when the current one reaches 1024 MB
- `sys_log_roll_num`: Number of retained log files, default is 10

### `{be or cn}.ERROR.log`

`be.ERROR.log` stores log entries at `ERROR` level and above. Typical error log contents:
Query Execution Errors
- Query timeout or cancellation
- Query failure due to insufficient memory
Data Processing Errors
- Data load failures (e.g., format errors, constraint violations)
- Data write failures
- Data read errors (e.g., file corruption, I/O errors)
Storage System Errors
- Tablet load failures
- Compaction execution failures
- Corrupted data files
- Disk I/O errors

#### Configuration

- `sys_log_level`: Log level, default is `INFO`
- `sys_log_dir`: Log storage directory, default is `${STARROCKS_HOME}/log`
- `sys_log_roll_mode`: Log rotation mode, default is `SIZE-MB-1024`, meaning a new log file is created when the current one reaches 1024 MB
- `sys_log_roll_num`: Number of retained log files, default is 10

### `{be or cn}.FATAL.log`

It primarily records various runtime behavior logs generated by BE/CN nodes, and these logs are at the `FATAL` level. Once such a log is generated, the BE/CN node process will exit.

#### Configuration

- `sys_log_level`: Log level, default is `INFO`
- `sys_log_dir`: Log storage directory, default is `${STARROCKS_HOME}/log`
- `sys_log_roll_mode`: Log rotation mode, default is `SIZE-MB-1024`, meaning a new log file is created when the current one reaches 1024 MB
- `sys_log_roll_num`: Number of retained log files, default is 10

### `error_log`

This log primarily records various errors, rejected records, and ETL issues encountered by BE/CN nodes during data import. Users can obtain the main reasons for import errors via `http://be_ip:be_port/api/get_log_file`. The log files are stored in the `${STARROCKS_HOME}/storage/error_log` directory.

#### Configuration
- `load_error_log_reserve_hours`: How long error log files are retained. The default is 48 hours, meaning the log files will be deleted 48 hours after they are created.
