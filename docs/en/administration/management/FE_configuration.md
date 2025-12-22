---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

# FE Configuration

<FEConfigMethod />

## View FE configuration items

After your FE is started, you can run the ADMIN SHOW FRONTEND CONFIG command on your MySQL client to check the parameter configurations. If you want to query the configuration of a specific parameter, run the following command:

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

For detailed description of the returned fields, see [ADMIN SHOW CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md).

:::note
You must have administrator privileges to run cluster administration-related commands.
:::

## Configure FE parameters

### Configure FE dynamic parameters

You can configure or modify the settings of FE dynamic parameters using [ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md).

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### Configure FE static parameters

<StaticFEConfigNote />

## Understand FE parameters

### Logging

##### audit_log_delete_age

- Default: 30d
- Type: String
- Unit: -
- Is mutable: No
- Description: The retention period of audit log files. The default value `30d` specifies that each audit log file can be retained for 30 days. StarRocks checks each audit log file and deletes those that were generated 30 days ago.
- Introduced in: -

##### audit_log_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores audit log files.
- Introduced in: -

##### audit_log_enable_compress

- Default: false
- Type: Boolean
- Unit: N/A
- Is mutable: No
- Description: When true, the generated Log4j2 configuration appends a ".gz" postfix to rotated audit log filenames (fe.audit.log.*) so that Log4j2 will produce compressed (.gz) archived audit log files on rollover. The setting is read during FE startup in Log4jConfig.initLogging and is applied to the RollingFile appender for audit logs; it only affects rotated/archived files, not the active audit log. Because the value is initialized at startup, changing it requires restarting the FE to take effect. Use alongside audit log rotation settings (audit_log_dir, audit_log_roll_interval, audit_roll_maxsize, audit_log_roll_num).
- Introduced in: 3.2.12

##### audit_log_json_format

- Default: false
- Type: Boolean
- Unit: N/A
- Is mutable: Yes
- Description: When true, FE audit events are emitted as structured JSON (Jackson ObjectMapper serializing a Map of annotated AuditEvent fields) instead of the default pipe-separated "key=value" string. The setting affects all built-in audit sinks handled by AuditLogBuilder: connection audit, query audit, big-query audit (big-query threshold fields are added to the JSON when the event qualifies), and slow-audit output. Fields annotated for big-query thresholds and the "features" field are treated specially (excluded from normal audit entries; included in big-query or feature logs as applicable). Enable this to make logs machine-parsable for log collectors or SIEMs; note it changes the log format and may require updating any existing parsers that expect the legacy pipe-separated format.
- Introduced in: 3.2.7

##### audit_log_modules

- Default: slow_query, query
- Type: String[]
- Unit: -
- Is mutable: No
- Description: The modules for which StarRocks generates audit log entries. By default, StarRocks generates audit logs for the `slow_query` module and the `query` module. The `connection` module is supported from v3.0. Separate the module names with a comma (,) and a space.
- Introduced in: -

##### audit_log_roll_interval

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: The time interval at which StarRocks rotates audit log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of audit log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of audit log files.
- Introduced in: -

##### audit_log_roll_num

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of audit log files that can be retained within each retention period specified by the `audit_log_roll_interval` parameter.
- Introduced in: -

##### bdbje_log_level

- Default: INFO
- Type: String
- Unit: -
- Is mutable: No
- Description: Controls the logging level used by Berkeley DB Java Edition (BDB JE) in StarRocks. During BDB environment initialization BDBEnvironment.initConfigs() applies this value to the Java logger for the `com.sleepycat.je` package and to the BDB JE environment file logging level (EnvironmentConfig.FILE_LOGGING_LEVEL). Accepts standard java.util.logging.Level names such as SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST, ALL, OFF. Setting to ALL enables all log messages. Increasing verbosity will raise log volume and may impact disk I/O and performance; the value is read when the BDB environment is initialized, so it takes effect only after environment (re)initialization.
- Introduced in: v3.2.0

##### big_query_log_delete_age

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: Controls how long FE big query log files (`fe.big_query.log.*`) are retained before automatic deletion. The value is passed to Log4j's deletion policy as the IfLastModified age — any rotated big query log whose last-modified time is older than this value will be removed. Supports suffixes include `d` (day), `h` (hour), `m` (minute), and `s` (second). Example: `7d` (7 days), `10h` (10 hours), `60m` (60 minutes), and `120s` (120 seconds). This item works together with `big_query_log_roll_interval` and `big_query_log_roll_num` to determine which files are kept or purged.
- Introduced in: v3.2.0

##### big_query_log_dir

- Default: `Config.STARROCKS_HOME_DIR + "/log"`
- Type: String
- Unit: -
- Is mutable: No
- Description: Directory where the FE writes big query dump logs (`fe.big_query.log.*`). The Log4j configuration uses this path to create a RollingFile appender for `fe.big_query.log` and its rotated files. Rotation and retention are governed by `big_query_log_roll_interval` (time-based suffix), `log_roll_size_mb` (size trigger), `big_query_log_roll_num` (max files), and `big_query_log_delete_age` (age-based deletion). Big query records are logged for queries that exceed user-defined thresholds such as `big_query_log_cpu_second_threshold`, `big_query_log_scan_rows_threshold`, or `big_query_log_scan_bytes_threshold`. Use `big_query_log_modules` to control which modules log to this file.
- Introduced in: v3.2.0

##### big_query_log_modules

- Default: `{"query"}`
- Type: String[]
- Unit: -
- Is mutable: No
- Description: List of module name suffixes that enable per-module big query logging. Typical values are logical component names. For example, the default `query` produces `big_query.query`.
- Introduced in: v3.2.0

##### big_query_log_roll_interval

- Default: `"DAY"`
- Type: String
- Unit: -
- Is mutable: No
- Description: Specifies the time interval used to construct the date component of the rolling file name for the `big_query` log appender. Valid values (case-insensitive) are `DAY` (default) and `HOUR`. `DAY` produces a daily pattern (`"%d{yyyyMMdd}"`) and `HOUR` produces an hourly pattern (`"%d{yyyyMMddHH}"`). The value is combined with size-based rollover (`big_query_roll_maxsize`) and index-based rollover (`big_query_log_roll_num`) to form the RollingFile filePattern. An invalid value causes log configuration generation to fail (IOException) and may prevent log initialization or reconfiguration. Use alongside `big_query_log_dir`, `big_query_roll_maxsize`, `big_query_log_roll_num`, and `big_query_log_delete_age`.
- Introduced in: v3.2.0

##### big_query_log_roll_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: Maximum number of rotated FE big query log files to retain per `big_query_log_roll_interval`. This value is bound to the RollingFile appender's DefaultRolloverStrategy `max` attribute for `fe.big_query.log`; when logs roll (by time or by `log_roll_size_mb`), StarRocks keeps up to `big_query_log_roll_num` indexed files (filePattern uses a time suffix plus index). Files older than this count may be removed by rollover, and `big_query_log_delete_age` can additionally delete files by last-modified age.
- Introduced in: v3.2.0

##### dump_log_delete_age

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: The retention period of dump log files. The default value `7d` specifies that each dump log file can be retained for 7 days. StarRocks checks each dump log file and deletes those that were generated 7 days ago.
- Introduced in: -

##### dump_log_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores dump log files.
- Introduced in: -

##### dump_log_modules

- Default: query
- Type: String[]
- Unit: -
- Is mutable: No
- Description: The modules for which StarRocks generates dump log entries. By default, StarRocks generates dump logs for the query module. Separate the module names with a comma (,) and a space.
- Introduced in: -

##### dump_log_roll_interval

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: The time interval at which StarRocks rotates dump log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of dump log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of dump log files.
- Introduced in: -

##### dump_log_roll_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of dump log files that can be retained within each retention period specified by the `dump_log_roll_interval` parameter.
- Introduced in: -

##### edit_log_write_slow_log_threshold_ms

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Threshold (in ms) used by JournalWriter to detect and log slow edit-log batch writes. After a batch commit, if the batch duration exceeds this value, JournalWriter emits a WARN with batch size, duration and current journal queue size (rate-limited to once every ~2s). This setting only controls logging/alerts for potential IO or replication latency on the FE leader; it does not change commit or roll behavior (see `edit_log_roll_num` and commit-related settings). Metric updates still occur regardless of this threshold.
- Introduced in: v3.2.3

##### enable_audit_sql

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the FE audit subsystem records the SQL text of statements into FE audit logs (`fe.audit.log`) processed by ConnectProcessor. The stored statement respects other controls: encrypted statements are redacted (`AuditEncryptionChecker`), sensitive credentials may be redacted or desensitized if `enable_sql_desensitize_in_log` is set, and digest recording is controlled by `enable_sql_digest`. When it is set to `false`, ConnectProcessor replaces the statement text with "?" in audit events — other audit fields (user, host, duration, status, slow-query detection via `qe_slow_log_ms`, and metrics) are still recorded. Enabling SQL audit increases forensic and troubleshooting visibility but may expose sensitive SQL content and increase log volume and I/O; disabling it improves privacy at the cost of losing full-statement visibility in audit logs.
- Introduced in: -

##### enable_profile_log

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable profile logging. When this feature is enabled, the FE writes per-query profile logs (the serialized `queryDetail` JSON produced by `ProfileManager`) to the profile log sink. This logging is performed only if `enable_collect_query_detail_info` is also enabled; when `enable_profile_log_compress` is enabled, the JSON may be gzipped before logging. Profile log files are managed by `profile_log_dir`, `profile_log_roll_num`, `profile_log_roll_interval` and rotated/deleted according to `profile_log_delete_age` (supports formats like `7d`, `10h`, `60m`, `120s`). Disabling this feature stops writing profile logs (reducing disk I/O, compression CPU and storage usage).
- Introduced in: v3.2.5

##### enable_qe_slow_log

- Default: true
- Type: Boolean
- Unit: N/A
- Is mutable: Yes
- Description: When enabled, the FE builtin audit plugin (AuditLogBuilder) will write query events whose measured execution time ("Time" field) exceeds the threshold configured by qe_slow_log_ms into the slow-query audit log (AuditLog.getSlowAudit). If disabled, those slow-query entries are suppressed (regular query and connection audit logs are unaffected). The slow-audit entries follow the global audit_log_json_format setting (JSON vs. plain string). Use this flag to control generation of slow-query audit volume independently of regular audit logging; turning it off may reduce log I/O when qe_slow_log_ms is low or workloads produce many long-running queries.
- Introduced in: 3.2.11

##### enable_sql_desensitize_in_log

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the system replaces or hides sensitive SQL content before it is written to logs and query-detail records. Code paths that honor this configuration include ConnectProcessor.formatStmt (audit logs), StmtExecutor.addRunningQueryDetail (query details), and SimpleExecutor.formatSQL (internal executor logs). With the feature enabled, invalid SQLs may be replaced with a fixed desensitized message, credentials (user/password) are hidden, and the SQL formatter is required to produce a sanitized representation (it can also enable digest-style output). This reduces leakage of sensitive literals and credentials in audit/internal logs but also means logs and query details no longer contain the original full SQL text (which can affect replay or debugging).
- Introduced in: -

##### internal_log_delete_age

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: Specifies the retention period for FE internal log files (written to `internal_log_dir`). The value is a duration string. Supported suffixes: `d` (day), `h` (hour), `m` (minute), `s` (second). Examples: `7d` (7 days), `10h` (10 hours), `60m` (60 minutes), `120s` (120 seconds). This item is substituted into the log4j configuration as the `<IfLastModified age="..."/>` predicate used by the RollingFile Delete policy. Files whose last-modified time is earlier than this duration will be removed during log rollover. Increase this value to free disk space sooner, or decrease it to retain internal materialized view or statistics logs longer.
- Introduced in: v3.2.4

##### internal_log_dir

- Default: `Config.STARROCKS_HOME_DIR + "/log"`
- Type: String
- Unit: -
- Is mutable: No
- Description: Directory used by the FE logging subsystem for storing internal logs (`fe.internal.log`). This configuration is substituted into the Log4j configuration and determines where the InternalFile appender writes internal/materialized view/statistics logs and where per-module loggers under `internal.<module>` place their files. Ensure the directory exists, is writable, and has sufficient disk space. Log rotation and retention for files in this directory are controlled by `log_roll_size_mb`, `internal_log_roll_num`, `internal_log_delete_age`, and `internal_log_roll_interval`. If `sys_log_to_console` is enabled, internal logs may be written to console instead of this directory.
- Introduced in: v3.2.4

##### internal_log_modules

- Default: `{"base", "statistic"}`
- Type: String[]
- Unit: -
- Is mutable: No
- Description: A list of module identifiers that will receive dedicated internal logging. For each entry X, Log4j creates a logger named `internal.&lt;X&gt;` with level INFO and additivity="false". Those loggers are routed to the internal appender (written to `fe.internal.log`) or to console when `sys_log_to_console` is enabled. Use short names or package fragments as needed — the exact logger name becomes `internal.` + the configured string. Internal log file rotation and retention follow `internal_log_dir`, `internal_log_roll_num`, `internal_log_delete_age`, `internal_log_roll_interval`, and `log_roll_size_mb`. Adding a module causes its runtime messages to be separated into the internal logger stream for easier debugging and audit.
- Introduced in: v3.2.4

##### internal_log_roll_interval

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: Controls the time-based roll interval for the FE internal log appender. Accepted values (case-insensitive) are `HOUR` and `DAY`. `HOUR` produces an hourly file pattern (`"%d{yyyyMMddHH}"`) and `DAY` produces a daily file pattern (`"%d{yyyyMMdd}"`), which are used by the RollingFile TimeBasedTriggeringPolicy to name rotated `fe.internal.log` files. An invalid value causes initialization to fail (an IOException is thrown when building the active Log4j configuration). Roll behavior also depends on related settings such as `internal_log_dir`, `internal_roll_maxsize`, `internal_log_roll_num`, and `internal_log_delete_age`.
- Introduced in: v3.2.4

##### internal_log_roll_num

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: Maximum number of rolled internal FE log files to retain for the internal appender (`fe.internal.log`). This value is used as the Log4j DefaultRolloverStrategy `max` attribute; when rollovers occur, StarRocks keeps up to `internal_log_roll_num` archived files and removes older ones (also governed by `internal_log_delete_age`). A lower value reduces disk usage but shortens log history; a higher value preserves more historical internal logs. This item works together with `internal_log_dir`, `internal_log_roll_interval`, and `internal_roll_maxsize`.
- Introduced in: v3.2.4

##### log_cleaner_audit_log_min_retention_days

- Default: 3
- Type: Int
- Unit: Days
- Is mutable: Yes
- Description: Minimum retention days for audit log files. Audit log files newer than this will not be deleted even if disk usage is high. This ensures that audit logs are preserved for compliance and troubleshooting purposes.
- Introduced in: -

##### log_cleaner_check_interval_second

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval in seconds to check disk usage and clean logs. The cleaner periodically checks each log directory's disk usage and triggers cleaning when necessary. Default is 300 seconds (5 minutes).
- Introduced in: -

##### log_cleaner_disk_usage_target

- Default: 60
- Type: Int
- Unit: Percentage
- Is mutable: Yes
- Description: Target disk usage (percentage) after log cleaning. Log cleaning will continue until disk usage drops below this threshold. The cleaner deletes the oldest log files one by one until the target is reached.
- Introduced in: -

##### log_cleaner_disk_usage_threshold

- Default: 80
- Type: Int
- Unit: Percentage
- Is mutable: Yes
- Description: Disk usage threshold (percentage) to trigger log cleaning. When disk usage exceeds this threshold, log cleaning will start. The cleaner checks each configured log directory independently and processes directories that exceed this threshold.
- Introduced in: -

##### log_cleaner_disk_util_based_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Enable automatic log cleaning based on disk usage. When enabled, logs will be cleaned when disk usage exceeds the threshold. The log cleaner runs as a background daemon on the FE node and helps prevent disk space exhaustion from log file accumulation.
- Introduced in: -

##### log_plan_cancelled_by_crash_be

- Default: true
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the query execution plan logging when a query is cancelled due to BE crash or an RPC exception. When this feature is enabled, StarRocks logs the query execution plan (at `TExplainLevel.COSTS`) as a WARN entry when a query is cancelled due to BE crash or an `RpcException`. The log entry includes QueryId, SQL and the COSTS plan; in the ExecuteExceptionHandler path, the exception stacktrace is also logged. The logging is skipped when `enable_collect_query_detail_info` is enabled (the plan is then stored in the query detail) — in code paths, the check is performed by verifying the query detail is null. Note that, in ExecuteExceptionHandler, the plan is logged only on the first retry (`retryTime == 0`). Enabling this may increase log volume because full COSTS plans can be large.
- Introduced in: v3.2.0

##### log_register_and_unregister_query_id

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow FE to log query registration and deregistration messages (e.g., `"register query id = {}"` and `"deregister query id = {}"`) from QeProcessorImpl. The log is emitted only when the query has a non-null ConnectContext and either the command is not `COM_STMT_EXECUTE` or the session variable `isAuditExecuteStmt()` is true. Because these messages are written for every query lifecycle event, enabling this feature can produce high log volume and become a throughput bottleneck in high-concurrency environments. Enable it for debugging or auditing; and disable it to reduce logging overhead and improve performance.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### log_roll_size_mb

- Default: 1024
- Type: Int
- Unit: MB
- Is mutable: No
- Description: The maximum size of a system log file or an audit log file.
- Introduced in: -

##### profile_log_delete_age

- Default: 1d
- Type: String
- Unit: -
- Is mutable: No
- Description: Controls how long FE profile log files are retained before they are eligible for deletion. The value is injected into Log4j's `&lt;IfLastModified age="..."/&gt;` policy (via `Log4jConfig`) and is applied together with rotation settings such as `profile_log_roll_interval` and `profile_log_roll_num`. Supported suffixes: `d` (day), `h` (hour), `m` (minute), `s` (second). For example: `7d` (7 days), `10h` (10 hours), `60m` (60 minutes), `120s` (120 seconds).
- Introduced in: v3.2.5

##### profile_log_dir

- Default: `Config.STARROCKS_HOME_DIR + "/log"`
- Type: String
- Unit: -
- Is mutable: No
- Description: Directory where FE profile logs are written. Log4jConfig uses this value to place profile-related appenders (creates files like `fe.profile.log` and `fe.features.log` under this directory). Rotation and retention for these files are governed by `profile_log_roll_size_mb`, `profile_log_roll_num` and `profile_log_delete_age`; the timestamp suffix format is controlled by `profile_log_roll_interval` (supports DAY or HOUR). Because the default directory is under `STARROCKS_HOME_DIR`, ensure the FE process has write and rotation/delete permissions on this directory.
- Introduced in: v3.2.5

##### profile_log_roll_interval

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: Controls the time granularity used to generate the date part of profile log filenames. Valid values (case-insensitive) are `HOUR` and `DAY`. `HOUR` produces a pattern of `"%d{yyyyMMddHH}"` (hourly time bucket) and `DAY` produces `"%d{yyyyMMdd}"` (daily time bucket). This value is used when computing `profile_file_pattern` in the Log4j configuration and only affects the time-based component of rollover file names; size-based rollover is still controlled by `profile_log_roll_size_mb` and retention by `profile_log_roll_num` / `profile_log_delete_age`. Invalid values cause an IOException during logging initialization (error message: `"profile_log_roll_interval config error: <value>"`). Choose `HOUR` for high-volume profiling to limit per-file size per hour, or `DAY` for daily aggregation.
- Introduced in: v3.2.5

##### profile_log_roll_num

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: Specifies the maximum number of rotated profile log files retained by Log4j's DefaultRolloverStrategy for the profile logger. This value is injected into the logging XML as `${profile_log_roll_num}` (e.g. `&lt;DefaultRolloverStrategy max="${profile_log_roll_num}" fileIndex="min"&gt;`). Rotations are triggered by `profile_log_roll_size_mb` or `profile_log_roll_interval`; when rotation occurs, Log4j keeps at most these indexed files and older index files become eligible for removal. Actual retention on disk is also affected by `profile_log_delete_age` and the `profile_log_dir` location. Lower values reduce disk usage but limit retained history; higher values preserve more historical profile logs.
- Introduced in: v3.2.5

##### profile_log_roll_size_mb

- Default: 1024
- Type: Int
- Unit: MB
- Is mutable: No
- Description: Sets the size threshold (in megabytes) that triggers a size-based rollover of the FE profile log file. This value is used by the Log4j RollingFile SizeBasedTriggeringPolicy for the `ProfileFile` appender; when a profile log exceeds `profile_log_roll_size_mb` it will be rotated. Rotation can also occur by time when `profile_log_roll_interval` is reached — either condition will trigger rollover. Combined with `profile_log_roll_num` and `profile_log_delete_age`, this item controls how many historical profile files are retained and when old files are deleted. Compression of rotated files is controlled by `enable_profile_log_compress`.
- Introduced in: v3.2.5

##### qe_slow_log_ms

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The threshold used to determine whether a query is a slow query. If the response time of a query exceeds this threshold, it is recorded as a slow query in **fe.audit.log**.
- Introduced in: -

##### slow_lock_log_every_ms

- Default: 3000L
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: Minimum interval (in ms) to wait before emitting another "slow lock" warning for the same SlowLockLogStats instance. LockUtils checks this value after a lock wait exceeds slow_lock_threshold_ms and will suppress additional warnings until slow_lock_log_every_ms milliseconds have passed since the last logged slow-lock event. Use a larger value to reduce log volume during prolonged contention or a smaller value to get more frequent diagnostics. Changes take effect at runtime for subsequent checks.
- Introduced in: v3.2.0

##### slow_lock_print_stack

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow LockManager to include the owning thread's full stack trace in the JSON payload of slow-lock warnings emitted by `logSlowLockTrace` (the "stack" array is populated via `LogUtil.getStackTraceToJsonArray` with `start=0` and `max=Short.MAX_VALUE`). This configuration controls only the extra stack information for lock owners shown when a lock acquisition exceeds the threshold configured by `slow_lock_threshold_ms`. Enabling this feature helps debugging by giving precise thread stacks that hold the lock; disabling it reduces log volume and CPU/memory overhead caused by capturing and serializing stack traces in high-concurrency environments.
- Introduced in: v3.3.16, v3.4.5, v3.5.1

##### slow_lock_threshold_ms

- Default: 3000L
- Type: long
- Unit: Milliseconds
- Is mutable: Yes
- Description: Threshold (in ms) used to classify a lock operation or a held lock as "slow". When the elapsed wait or hold time for a lock exceeds this value, StarRocks will (depending on context) emit diagnostic logs, include stack traces or waiter/owner info, and—in LockManager—start deadlock detection after this delay. It's used by LockUtils (slow-lock logging), QueryableReentrantReadWriteLock (filtering slow readers), LockManager (deadlock-detection delay and slow-lock trace), LockChecker (periodic slow-lock detection), and other callers (e.g., DiskAndTabletLoadReBalancer logging). Lowering the value increases sensitivity and logging/diagnostic overhead; setting it to 0 or negative disables the initial wait-based deadlock-detection delay behavior. Tune together with slow_lock_log_every_ms, slow_lock_print_stack, and slow_lock_stack_trace_reserve_levels.
- Introduced in: 3.2.0

##### sys_log_delete_age

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: The retention period of system log files. The default value `7d` specifies that each system log file can be retained for 7 days. StarRocks checks each system log file and deletes those that were generated 7 days ago.
- Introduced in: -

##### sys_log_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores system log files.
- Introduced in: -

##### sys_log_enable_compress

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the system appends a ".gz" postfix to rotated system log filenames so Log4j will produce gzip-compressed rotated FE system logs (for example, fe.log.*). This value is read during Log4j configuration generation (Log4jConfig.initLogging / generateActiveLog4jXmlConfig) and controls the `sys_file_postfix` property used in the RollingFile filePattern. Enabling this feature reduces disk usage for retained logs but increases CPU and I/O during rollovers and changes log filenames, so that tools or scripts that read logs must be able to handle .gz files. Note that audit logs use a separate configuration for compression, that is, `audit_log_enable_compress`.
- Introduced in: v3.2.12

##### sys_log_format

- Default: "plaintext"
- Type: String
- Unit: -
- Is mutable: No
- Description: Selects the Log4j layout used for FE logs. Valid values: `"plaintext"` (Default) and `"json"`. The values are case-insensitive. `"plaintext"` configures PatternLayout with human-readable timestamps, level, thread, class.method:line and stack traces for WARN/ERROR. `"json"` configures JsonTemplateLayout and emits structured JSON events (UTC timestamps, level, thread id/name, source file/method/line, message, exception stackTrace) suitable for log aggregators (ELK, Splunk). JSON output abides by `sys_log_json_max_string_length` and `sys_log_json_profile_max_string_length` for maximum string lengths.
- Introduced in: v3.2.10

##### sys_log_json_max_string_length

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the JsonTemplateLayout "maxStringLength" value used for the JSON-formatted system logs. When `sys_log_format` is set to `"json"`, string-valued fields (for example "message" and stringified exception stack traces) are truncated if their length exceeds this limit. The value is injected into the generated Log4j XML in `Log4jConfig.generateActiveLog4jXmlConfig()`, and is applied to default, warning, audit, dump and bigquery layouts. The profile layout uses a separate configuration (`sys_log_json_profile_max_string_length`). Lowering this value reduces log size but can truncate useful information.
- Introduced in: 3.2.11

##### sys_log_json_profile_max_string_length

- Default: 104857600 (100 MB)
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the maxStringLength of JsonTemplateLayout for profile (and related feature) log appenders when `sys_log_format` is "json". String field values in JSON-formatted profile logs will be truncated to this byte length; non-string fields are unaffected. This item is applied in Log4jConfig `JsonTemplateLayout maxStringLength` and is ignored when `plaintext` logging is used. Keep the value large enough for full messages you need, but note larger values increase log size and I/O.
- Introduced in: v3.2.11

##### sys_log_level

- Default: INFO
- Type: String
- Unit: -
- Is mutable: No
- Description: The severity levels into which system log entries are classified. Valid values: `INFO`, `WARN`, `ERROR`, and `FATAL`.
- Introduced in: -

##### sys_log_roll_interval

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: The time interval at which StarRocks rotates system log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of system log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of system log files.
- Introduced in: -

##### sys_log_roll_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of system log files that can be retained within each retention period specified by the `sys_log_roll_interval` parameter.
- Introduced in: -

##### sys_log_to_console

- Default: false (unless the environment variable `SYS_LOG_TO_CONSOLE` is set to "1")
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the system configures Log4j to send all logs to the console (ConsoleErr appender) instead of the file-based appenders. This value is read when generating the active Log4j XML configuration (which affects the root logger and per-module logger appender selection). Its value is captured from the `SYS_LOG_TO_CONSOLE` environment variable at process startup. Changing it at runtime has no effect. This configuration is commonly used in containerized or CI environments where stdout/stderr log collection is preferred over writing log files.
- Introduced in: v3.2.0

##### sys_log_verbose_modules

- Default: Empty string
- Type: String[]
- Unit: -
- Is mutable: No
- Description: The modules for which StarRocks generates system logs. If this parameter is set to `org.apache.starrocks.catalog`, StarRocks generates system logs only for the catalog module. Separate the module names with a comma (,) and a space.
- Introduced in: -

##### sys_log_warn_modules

- Default: {}
- Type: String[]
- Unit: -
- Is mutable: No
- Description: A list of logger names or package prefixes that the system will configure at startup as WARN-level loggers and route to the warning appender (SysWF) — the `fe.warn.log` file. Entries are inserted into the generated Log4j configuration (alongside builtin warn modules such as org.apache.kafka, org.apache.hudi, and org.apache.hadoop.io.compress) and produce logger elements like `<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>`. Fully-qualified package and class prefixes (for example, "com.example.lib") are recommended to suppress noisy INFO/DEBUG output into the regular log and to allow warnings to be captured separately.
- Introduced in: v3.2.13

### Server

##### brpc_idle_wait_max_time

- Default: 10000
- Type: Int
- Unit: ms
- Is mutable: No
- Description: The maximum length of time for which bRPC clients wait as in the idle state.
- Introduced in: -

##### brpc_inner_reuse_pool

- Default: true
- Type: boolean
- Unit: -
- Is mutable: No
- Description: Controls whether the underlying BRPC client uses an internal shared reuse pool for connections/channels. StarRocks reads `brpc_inner_reuse_pool` in BrpcProxy when constructing RpcClientOptions (via `rpcOptions.setInnerResuePool(...)`). When enabled (true) the RPC client reuses internal pools to reduce per-call connection creation, lowering connection churn, memory and file-descriptor usage for FE-to-BE / LakeService RPCs. When disabled (false) the client may create more isolated pools (increasing concurrency isolation at the cost of higher resource usage). Changing this value requires restarting the process to take effect.
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### brpc_min_evictable_idle_time_ms

- Default: 120000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Time in milliseconds that an idle BRPC connection must remain in the connection pool before it becomes eligible for eviction. Applied to the RpcClientOptions used by `BrpcProxy` (via RpcClientOptions.setMinEvictableIdleTime). Raise this value to keep idle connections longer (reducing reconnect churn); lower it to free unused sockets faster (reducing resource usage). Tune together with `brpc_connection_pool_size` and `brpc_idle_wait_max_time` to balance connection reuse, pool growth, and eviction behavior.
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### brpc_reuse_addr

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When true, StarRocks sets the socket option to allow local address reuse for client sockets created by the brpc RpcClient (via RpcClientOptions.setReuseAddress). Enabling this reduces bind failures and allows faster rebinding of local ports after sockets are closed, which is helpful for high-rate connection churn or rapid restarts. When false, address/port reuse is disabled, which can reduce the chance of unintended port sharing but may increase transient bind errors. This option interacts with connection behavior configured by `brpc_connection_pool_size` and `brpc_short_connection` because it affects how rapidly client sockets can be rebound and reused.
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### cluster_name

- Default: StarRocks Cluster
- Type: String
- Unit: -
- Is mutable: No
- Description: The name of the StarRocks cluster to which the FE belongs. The cluster name is displayed for `Title` on the web page.
- Introduced in: -

##### dns_cache_ttl_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: DNS cache TTL (Time-To-Live) in seconds for successful DNS lookups. This sets the Java security property `networkaddress.cache.ttl` which controls how long the JVM caches successful DNS lookups. Set this item to `-1` to allow the system to always cache the infomration, or `0` to disable caching. This is particularly useful in environments where IP addresses change frequently, such as Kubernetes deployments or when dynamic DNS is used.
- Introduced in: v3.5.11, v4.0.4

##### enable_http_async_handler

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow the system to process HTTP requests asynchronously. If this feature is enabled, an HTTP request received by Netty worker threads will then be submitted to a separate thread pool for service logic handling to avoid blocking the HTTP server. If disabled, Netty workers will handle the service logic.
- Introduced in: 4.0.0

##### enable_http_validate_headers

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Controls whether Netty's HttpServerCodec performs strict HTTP header validation. The value is passed to HttpServerCodec when the HTTP pipeline is initialized in `HttpServer` (see UseLocations). Default is false for backward compatibility because newer netty versions enforce stricter header rules (https://github.com/netty/netty/pull/12760). Set to true to enforce RFC-compliant header checks; doing so may cause malformed or nonconforming requests from legacy clients or proxies to be rejected. Change requires a restart of the HTTP server to take effect.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### enable_https

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable HTTPS server alongside HTTP server in FE nodes.
- Introduced in: v4.0

##### frontend_address

- Default: 0.0.0.0
- Type: String
- Unit: -
- Is mutable: No
- Description: The IP address of the FE node.
- Introduced in: -

##### http_async_threads_num

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Size of the thread pool for asynchronous HTTP request processing. The alias is `max_http_sql_service_task_threads_num`.
- Introduced in: 4.0.0

##### http_backlog_num

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of the backlog queue held by the HTTP server in the FE node.
- Introduced in: -

##### http_max_chunk_size

- Default: 8192
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the maximum allowed size (in bytes) of a single HTTP chunk handled by Netty's HttpServerCodec in the FE HTTP server. It is passed as the third argument to HttpServerCodec and limits the length of chunks during chunked transfer or streaming requests/responses. If an incoming chunk exceeds this value, Netty will raise a frame-too-large error (e.g., TooLongFrameException) and the request may be rejected. Increase this for legitimate large chunked uploads; keep it small to reduce memory pressure and surface area for DoS attacks. This setting is used alongside `http_max_initial_line_length`, `http_max_header_size`, and `enable_http_validate_headers`.
- Introduced in: v3.2.0

##### http_max_header_size

- Default: 32768
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Maximum allowed size in bytes for the HTTP request header block parsed by Netty's `HttpServerCodec`. StarRocks passes this value to `HttpServerCodec` (as `Config.http_max_header_size`); if an incoming request's headers (names and values combined) exceed this limit, the codec will reject the request (decoder exception) and the connection/request will fail. Increase only when clients legitimately send very large headers (large cookies or many custom headers); larger values increase per-connection memory use. Tune in conjunction with `http_max_initial_line_length` and `http_max_chunk_size`. Changes require FE restart.
- Introduced in: v3.2.0

##### http_max_initial_line_length

- Default: 4096
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the maximum allowed length (in bytes) of the HTTP initial request line (method + request-target + HTTP version) accepted by the Netty `HttpServerCodec` used in HttpServer. The value is passed to Netty's decoder and requests with an initial line longer than this will be rejected (TooLongFrameException). Increase this only when you must support very long request URIs; larger values increase memory use and may raise exposure to malformed/request-abuse. Tune together with `http_max_header_size` and `http_max_chunk_size`.
- Introduced in: v3.2.0

##### http_port

- Default: 8030
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the HTTP server in the FE node listens.
- Introduced in: -

##### http_web_page_display_hardware

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When true, the HTTP index page (/index) will include a hardware information section populated via the oshi library (CPU, memory, processes, disks, filesystems, network, etc.). oshi may invoke system utilities or read system files indirectly (for example, it can execute commands such as `getent passwd`), which can surface sensitive system data. If you require stricter security or want to avoid executing those indirect commands on the host, set this configuration to false to disable collection and display of hardware details on the web UI.
- Introduced in: v3.2.0

##### http_worker_threads_num

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of worker threads for http server to deal with http requests. For a negative or 0 value, the number of threads will be twice the number of cpu cores.
- Introduced in: v2.5.18, v3.0.10, v3.1.7, v3.2.2

##### https_port

- Default: 8443
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the HTTPS server in the FE node listens.
- Introduced in: v4.0

##### max_mysql_service_task_threads_num

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads that can be run by the MySQL server in the FE node to process tasks.
- Introduced in: -

##### mysql_nio_backlog_num

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of the backlog queue held by the MySQL server in the FE node.
- Introduced in: -

##### mysql_server_version

- Default: 8.0.33
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The MySQL server version returned to the client. Modifying this parameter will affect the version information in the following situations:
  1. `select version();`
  2. Handshake packet version
  3. Value of the global variable `version` (`show variables like 'version';`)
- Introduced in: -

##### mysql_service_io_threads_num

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads that can be run by the MySQL server in the FE node to process I/O events.
- Introduced in: -

##### mysql_service_nio_enable_keep_alive

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Enable TCP Keep-Alive for MySQL connections. Useful for long-idled connections behind load balancers.
- Introduced in: -

##### net_use_ipv6_when_priority_networks_empty

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to use IPv6 addresses preferentially when `priority_networks` is not specified. `true` indicates to allow the system to use an IPv6 address preferentially when the server that hosts the node has both IPv4 and IPv6 addresses and `priority_networks` is not specified.
- Introduced in: v3.3.0

##### priority_networks

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Declares a selection strategy for servers that have multiple IP addresses. Note that at most one IP address must match the list specified by this parameter. The value of this parameter is a list that consists of entries, which are separated with semicolons (;) in CIDR notation, such as 10.10.10.0/24. If no IP address matches the entries in this list, an available IP address of the server will be randomly selected. From v3.3.0, StarRocks supports deployment based on IPv6. If the server has both IPv4 and IPv6 addresses, and this parameter is not specified, the system uses an IPv4 address by default. You can change this behavior by setting `net_use_ipv6_when_priority_networks_empty` to `true`.
- Introduced in: -

##### qe_max_connection

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of connections that can be established by all users to the FE node. From v3.1.12 and v3.2.7 onwards, the default value has been changed from `1024` to `4096`.
- Introduced in: -

##### query_port

- Default: 9030
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the MySQL server in the FE node listens.
- Introduced in: -

##### rpc_port

- Default: 9020
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the Thrift server in the FE node listens.
- Introduced in: -

##### slow_lock_stack_trace_reserve_levels

- Default: 15
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls how many stack-trace frames are captured and emitted when StarRocks dumps lock debug information for slow or held locks. This value is passed to `LogUtil.getStackTraceToJsonArray` by `QueryableReentrantReadWriteLock` when producing JSON for the exclusive lock owner, current thread, and oldest/shared readers. Increasing this value provides more context for diagnosing slow-lock or deadlock issues at the cost of larger JSON payloads and slightly higher CPU/memory for stack capture; decreasing it reduces overhead. Note: reader entries can be filtered by `slow_lock_threshold_ms` when only logging slow locks.
- Introduced in: v3.4.0, v3.5.0

##### ssl_cipher_blacklist

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Comma separated list, with regex support to blacklist ssl cipher suites by IANA names. If both whitelist and blacklist are set, blacklist takes precedence.
- Introduced in: v4.0

##### ssl_cipher_whitelist

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Comma separated list, with regex support to whitelist ssl cipher suites by IANA names. If both whitelist and blacklist are set, blacklist takes precedence.
- Introduced in: v4.0

##### thrift_backlog_num

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of the backlog queue held by the Thrift server in the FE node.
- Introduced in: -

##### thrift_client_timeout_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The length of time after which idle client connections time out.
- Introduced in: -

##### thrift_rpc_max_body_size

- Default: -1
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Controls the maximum allowed Thrift RPC message body size (in bytes) used when constructing the server's Thrift protocol (passed to TBinaryProtocol.Factory in `ThriftServer`). A value of `-1` disables the limit (unbounded). Setting a positive value enforces an upper bound so that messages larger than this are rejected by the Thrift layer, which helps limit memory usage and mitigate oversized-request or DoS risks. Set this to a size large enough for expected payloads (large structs or batched data) to avoid rejecting legitimate requests.
- Introduced in: v3.2.0

##### thrift_server_max_worker_threads

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of worker threads that are supported by the Thrift server in the FE node.
- Introduced in: -

##### thrift_server_queue_size

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of queue where requests are pending. If the number of threads that are being processed in the thrift server exceeds the value specified in `thrift_server_max_worker_threads`, new requests are added to the pending queue.
- Introduced in: -

### Metadata and cluster management

##### alter_max_worker_queue_size

- Default: 4096
- Type: Int
- Unit: Tasks
- Is mutable: No
- Description: Controls the capacity of the internal worker thread pool queue used by the alter subsystem. It is passed to `ThreadPoolManager.newDaemonCacheThreadPool` in `AlterHandler` together with `alter_max_worker_threads`. When the number of pending alter tasks exceeds `alter_max_worker_queue_size`, new submissions will be rejected and a `RejectedExecutionException` can be thrown (see `AlterHandler.handleFinishAlterTask`). Tune this value to balance memory usage and the amount of backlog you permit for concurrent alter tasks.
- Introduced in: v3.2.0

##### alter_max_worker_threads

- Default: 4
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Sets the maximum number of worker threads in the AlterHandler's thread pool. The AlterHandler constructs the executor with this value to run and finalize alter-related tasks (e.g., submitting `AlterReplicaTask` via handleFinishAlterTask). This value bounds concurrent execution of alter operations; raising it increases parallelism and resource usage, lowering it limits concurrent alters and may become a bottleneck. The executor is created together with `alter_max_worker_queue_size`, and the handler scheduling uses `alter_scheduler_interval_millisecond`.
- Introduced in: v3.2.0

##### automated_cluster_snapshot_interval_seconds

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which the Automated Cluster Snapshot tasks are triggered.
- Introduced in: v3.4.2

##### background_refresh_metadata_interval_millis

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The interval between two consecutive Hive metadata cache refreshes.
- Introduced in: v2.5.5

##### background_refresh_metadata_time_secs_since_last_access_secs

- Default: 3600 * 24
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of a Hive metadata cache refresh task. For the Hive catalog that has been accessed, if it has not been accessed for more than the specified time, StarRocks stops refreshing its cached metadata. For the Hive catalog that has not been accessed, StarRocks will not refresh its cached metadata.
- Introduced in: v2.5.5

##### bdbje_cleaner_threads

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of background cleaner threads for the Berkeley DB Java Edition (JE) environment used by StarRocks journal. This value is read during environment initialization in `BDBEnvironment.initConfigs` and applied to `EnvironmentConfig.CLEANER_THREADS` using `Config.bdbje_cleaner_threads`. It controls parallelism for JE log cleaning and space reclamation; increasing it can speed up cleaning at the cost of additional CPU and I/O interference with foreground operations. Changes take effect only when the BDB environment is (re)initialized, so a frontend restart is required to apply a new value.
- Introduced in: v3.2.0

##### bdbje_heartbeat_timeout_second

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The amount of time after which the heartbeats among the leader, follower, and observer FEs in the StarRocks cluster time out.
- Introduced in: -

##### bdbje_lock_timeout_second

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The amount of time after which a lock in the BDB JE-based FE times out.
- Introduced in: -

##### bdbje_replay_cost_percent

- Default: 150
- Type: Int
- Unit: Percent
- Is mutable: No
- Description: Sets the relative cost (as a percentage) of replaying transactions from a BDB JE log versus obtaining the same data via a network restore. The value is supplied to the underlying JE replication parameter REPLAY_COST_PERCENT and is typically >100 to indicate that replay is usually more expensive than a network restore. When deciding whether to retain cleaned log files for potential replay, the system compares replay cost multiplied by log size against the cost of a network restore; files will be removed if network restore is judged more efficient. A value of 0 disables retention based on this cost comparison. Log files required for replicas within `REP_STREAM_TIMEOUT` or for any active replication are always retained.
- Introduced in: v3.2.0

##### bdbje_replica_ack_timeout_second

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The maximum amount of time for which the leader FE can wait for ACK messages from a specified number of follower FEs when metadata is written from the leader FE to the follower FEs. Unit: second. If a large amount of metadata is being written, the follower FEs require a long time before they can return ACK messages to the leader FE, causing ACK timeout. In this situation, metadata writes fail, and the FE process exits. We recommend that you increase the value of this parameter to prevent this situation.
- Introduced in: -

##### bdbje_reserved_disk_size

- Default: 512 * 1024 * 1024 (536870912)
- Type: Long
- Unit: Bytes
- Is mutable: No
- Description: Limits the number of bytes Berkeley DB JE will reserve as "unprotected" (deletable) log/data files. StarRocks passes this value to JE via `EnvironmentConfig.RESERVED_DISK` in BDBEnvironment; JE's built-in default is 0 (unlimited). The StarRocks default (512 MiB) prevents JE from reserving excessive disk space for unprotected files while allowing safe cleanup of obsolete files. Tune this value on disk-constrained systems: decreasing it lets JE free more files sooner, increasing it lets JE retain more reserved space. Changes require restarting the process to take effect.
- Introduced in: v3.2.0

##### bdbje_reset_election_group

- Default: false
- Type: String
- Unit: -
- Is mutable: No
- Description: Whether to reset the BDBJE replication group. If this parameter is set to `TRUE`, the FE will reset the BDBJE replication group (that is, remove the information of all electable FE nodes) and start as the leader FE. After the reset, this FE will be the only member in the cluster, and other FEs can rejoin this cluster by using `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'`. Use this setting only when no leader FE can be elected because the data of most follower FEs have been damaged. `reset_election_group` is used to replace `metadata_failure_recovery`.
- Introduced in: -

##### black_host_connect_failures_within_time

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The threshold of connection failures allowed for a blacklisted BE node. If a BE node is added to the BE Blacklist automatically, StarRocks will assess its connectivity and judge whether it can be removed from the BE Blacklist. Within `black_host_history_sec`, only if a blacklisted BE node has fewer connection failures than the threshold set in `black_host_connect_failures_within_time`, it can be removed from the BE Blacklist.
- Introduced in: v3.3.0

##### black_host_history_sec

- Default: 2 * 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time duration for retaining historical connection failures of BE nodes in the BE Blacklist. If a BE node is added to the BE Blacklist automatically, StarRocks will assess its connectivity and judge whether it can be removed from the BE Blacklist. Within `black_host_history_sec`, only if a blacklisted BE node has fewer connection failures than the threshold set in `black_host_connect_failures_within_time`, it can be removed from the BE Blacklist.
- Introduced in: v3.3.0

##### brpc_connection_pool_size

- Default: 16
- Type: Int
- Unit: Connections
- Is mutable: No
- Description: The maximum number of pooled BRPC connections per endpoint used by the FE's BrpcProxy. This value is applied to RpcClientOptions via `setMaxTotoal` and `setMaxIdleSize`, so it directly limits concurrent outgoing BRPC requests because each request must borrow a connection from the pool. In high-concurrency scenarios increase this to avoid request queuing; increasing it raises socket and memory usage and may increase remote server load. When tuning, consider related settings such as `brpc_idle_wait_max_time`, `brpc_short_connection`, `brpc_inner_reuse_pool`, `brpc_reuse_addr`, and `brpc_min_evictable_idle_time_ms`. Changing this value is not hot-reloadable and requires a restart.
- Introduced in: v3.2.0

##### brpc_short_connection

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: Controls whether the underlying brpc RpcClient uses short-lived connections. When enabled (`true`), RpcClientOptions.setShortConnection is set and connections are closed after a request completes, reducing the number of long-lived sockets at the cost of higher connection setup overhead and increased latency. When disabled (`false`, the default) persistent connections and connection pooling are used. Enabling this option affects connection-pool behavior and should be considered together with `brpc_connection_pool_size`, `brpc_idle_wait_max_time`, `brpc_min_evictable_idle_time_ms`, `brpc_reuse_addr`, and `brpc_inner_reuse_pool`. Keep it disabled for typical high-throughput deployments; enable only to limit socket lifetime or when short connections are required by network policy.
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### catalog_try_lock_timeout_ms

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout duration to obtain the global lock.
- Introduced in: -

##### checkpoint_only_on_leader

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When `true`, the CheckpointController will only select the leader FE as the checkpoint worker; when `false`, the controller may pick any frontend and prefers nodes with lower heap usage. With `false`, workers are sorted by recent failure time and `heapUsedPercent` (the leader is treated as having infinite usage to avoid selecting it). For operations that require cluster snapshot metadata, the controller already forces leader selection regardless of this flag. Enabling `true` centralizes checkpoint work on the leader (simpler but increases leader CPU/memory and network load); keeping it `false` distributes checkpoint load to less-loaded FEs. This setting affects worker selection and interaction with timeouts such as `checkpoint_timeout_seconds` and RPC settings like `thrift_rpc_timeout_ms`.
- Introduced in: v3.4.0, v3.5.0

##### checkpoint_timeout_seconds

- Default: 24 * 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: Maximum time (in seconds) the leader's CheckpointController will wait for a checkpoint worker to complete a checkpoint. The controller converts this value to nanoseconds and polls the worker result queue; if no successful completion is received within this timeout the checkpoint is treated as failed and createImage returns failure. Increasing this value accommodates longer-running checkpoints but delays failure detection and subsequent image propagation; decreasing it causes faster failover/retries but can produce false timeouts for slow workers. This setting only controls the waiting period in `CheckpointController` during checkpoint creation and does not change the worker's internal checkpointing behavior.
- Introduced in: v3.4.0, v3.5.0

##### db_used_data_quota_update_interval_secs

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which the database used data quota is updated. StarRocks periodically updates the used data quota for all databases to track storage consumption. This value is used for quota enforcement and metrics collection. The minimum allowed interval is 30 seconds to prevent excessive system load. A value less than 30 will be rejected.
- Introduced in: -

##### drop_backend_after_decommission

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to delete a BE after the BE is decommissioned. `TRUE` indicates that the BE is deleted immediately after it is decommissioned. `FALSE` indicates that the BE is not deleted after it is decommissioned.
- Introduced in: -

##### edit_log_port

- Default: 9010
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port that is used for communication among the Leader, Follower, and Observer FEs in the cluster.
- Introduced in: -

##### edit_log_roll_num

- Default: 50000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of metadata log entries that can be written before a log file is created for these log entries. This parameter is used to control the size of log files. The new log file is written to the BDBJE database.
- Introduced in: -

##### edit_log_type

- Default: BDB
- Type: String
- Unit: -
- Is mutable: No
- Description: The type of edit log that can be generated. Set the value to `BDB`.
- Introduced in: -

##### enable_background_refresh_connector_metadata

- Default: true in v3.0 and later and false in v2.5
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the periodic Hive metadata cache refresh. After it is enabled, StarRocks polls the metastore (Hive Metastore or AWS Glue) of your Hive cluster, and refreshes the cached metadata of the frequently accessed Hive catalogs to perceive data changes. `true` indicates to enable the Hive metadata cache refresh, and `false` indicates to disable it.
- Introduced in: v2.5.5

##### enable_collect_query_detail_info

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect the profile of a query. If this parameter is set to `TRUE`, the system collects the profile of the query. If this parameter is set to `FALSE`, the system does not collect the profile of the query.
- Introduced in: -

##### enable_internal_sql

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, internal SQL statements executed by internal components (for example, SimpleExecutor) are preserved and written into internal audit or log messages (and can be further desensitized if `enable_sql_desensitize_in_log` is set). When it is set to `false`, internal SQL text is suppressed: formatting code (SimpleExecutor.formatSQL) returns "?" and the actual statement is not emitted to internal audit or log messages. This configuration does not change execution semantics of internal statements — it only controls logging and visibility of internal SQL for privacy or security.
- Introduced in: -

##### enable_legacy_compatibility_for_replication

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the Legacy Compatibility for Replication. StarRocks may behave differently between the old and new versions, causing problems during cross-cluster data migration. Therefore, you must enable Legacy Compatibility for the target cluster before data migration and disable it after data migration is completed. `true` indicates enabling this mode.
- Introduced in: v3.1.10, v3.2.6

##### enable_statistics_collect_profile

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to generate profiles for statistics queries. You can set this item to `true` to allow StarRocks to generate query profiles for queries on system statistics.
- Introduced in: v3.1.5

##### enable_table_name_case_insensitive

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable case-insensitive processing on catalog names, database names, table names, view names, and materialized view names. Currently, table names are case-sensitive by default.
  - After enabling this feature, all related names will be stored in lowercase, and all SQL commands containing these names will automatically convert them to lowercase.
  - You can enable this feature only when creating a cluster. **After the cluster is started, the value of this configuration cannot be modified by any means**. Any attempt to modify it will result in an error. FE will fail to start when it detects that the value of this configuration item is inconsistent with that when the cluster was first started.
  - Currently, this feature does not support JDBC catalog and table names. Do not enable this feature if you want to perform case-insensitive processing on JDBC or ODBC data sources.
- Introduced in: v4.0

##### enable_task_history_archive

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, finished task-run records are archived to the persistent task-run history table and recorded to the edit log so lookups (e.g., `lookupHistory`, `lookupHistoryByTaskNames`, `lookupLastJobOfTasks`) include archived results. Archiving is performed by the FE leader and is skipped during unit tests (`FeConstants.runningUnitTest`). When enabled, in-memory expiration and forced-GC paths are bypassed (the code returns early from `removeExpiredRuns` and `forceGC`), so retention/eviction is handled by the persistent archive instead of `task_runs_ttl_second` and `task_runs_max_history_number`. When disabled, history stays in memory and is pruned by those configurations.
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### enable_task_run_fe_evaluation

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, the FE will perform local evaluation for the system table `task_runs` in `TaskRunsSystemTable.supportFeEvaluation`. FE-side evaluation is only allowed for conjunctive equality predicates comparing a column to a constant and is limited to the columns `QUERY_ID` and `TASK_NAME`. Enabling this improves performance for targeted lookups by avoiding broader scans or additional remote processing; disabling it forces the planner to skip FE evaluation for `task_runs`, which may reduce predicate pruning and affect query latency for those filters.
- Introduced in: v3.3.13, v3.4.3, v3.5.0

##### heartbeat_mgr_blocking_queue_size

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The size of the blocking queue that stores heartbeat tasks run by the Heartbeat Manager.
- Introduced in: -

##### heartbeat_mgr_threads_num

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads that can be run by the Heartbeat Manager to run heartbeat tasks.
- Introduced in: -

##### ignore_materialized_view_error

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether FE ignores the metadata exception caused by materialized view errors. If FE fails to start due to the metadata exception caused by materialized view errors, you can set this parameter to `true` to allow FE to ignore the exception.
- Introduced in: v2.5.10

##### ignore_meta_check

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether non-Leader FEs ignore the metadata gap from the Leader FE. If the value is TRUE, non-Leader FEs ignore the metadata gap from the Leader FE and continue providing data reading services. This parameter ensures continuous data reading services even when you stop the Leader FE for a long period of time. If the value is FALSE, non-Leader FEs do not ignore the metadata gap from the Leader FE and stop providing data reading services.
- Introduced in: -

##### lock_checker_interval_second

- Default: 30
- Type: long
- Unit: Seconds
- Is mutable: Yes
- Description: Interval, in seconds, between executions of the LockChecker frontend daemon (named "deadlock-checker"). The daemon performs deadlock detection and slow-lock scanning; the configured value is multiplied by 1000 to set the timer in milliseconds. Decreasing this value reduces detection latency but increases scheduling and CPU overhead; increasing it reduces overhead but delays detection and slow-lock reporting. Changes take effect at runtime because the daemon resets its interval each run. This setting interacts with `lock_checker_enable_deadlock_check` (enables deadlock checks) and `slow_lock_threshold_ms` (defines what constitutes a slow lock).
- Introduced in: v3.2.0

##### master_sync_policy

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: The policy based on which the leader FE flushes logs to disk. This parameter is valid only when the current FE is a leader FE. Valid values:
  - `SYNC`: When a transaction is committed, a log entry is generated and flushed to disk simultaneously.
  - `NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.
  - `WRITE_NO_SYNC`: When a transaction is committed, a log entry is generated simultaneously but is not flushed to disk.

  If you have deployed only one follower FE, we recommend that you set this parameter to `SYNC`. If you have deployed three or more follower FEs, we recommend that you set this parameter and the `replica_sync_policy` both to `WRITE_NO_SYNC`.

- Introduced in: -

##### max_bdbje_clock_delta_ms

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: The maximum clock offset that is allowed between the leader FE and the follower or observer FEs in the StarRocks cluster.
- Introduced in: -

##### meta_delay_toleration_second

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration by which the metadata on the follower and observer FEs can lag behind that on the leader FE. Unit: seconds. If this duration is exceeded, the non-leader FEs stops providing services.
- Introduced in: -

##### meta_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/meta"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores metadata.
- Introduced in: -

##### metadata_ignore_unknown_operation_type

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to ignore an unknown log ID. When an FE is rolled back, the FEs of the earlier version may be unable to recognize some log IDs. If the value is `TRUE`, the FE ignores unknown log IDs. If the value is `FALSE`, the FE exits.
- Introduced in: -

##### profile_info_format

- Default: default
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The format of the Profile output by the system. Valid values: `default` and `json`. When set to `default`, Profile is of the default format. When set to `json`, the system outputs Profile in JSON format.
- Introduced in: v2.5

##### replica_ack_policy

- Default: SIMPLE_MAJORITY
- Type: String
- Unit: -
- Is mutable: No
- Description: The policy based on which a log entry is considered valid. The default value `SIMPLE_MAJORITY` specifies that a log entry is considered valid if a majority of follower FEs return ACK messages.
- Introduced in: -

##### replica_sync_policy

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: The policy based on which the follower FE flushes logs to disk. This parameter is valid only when the current FE is a follower FE. Valid values:
  - `SYNC`: When a transaction is committed, a log entry is generated and flushed to disk simultaneously.
  - `NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.
  - `WRITE_NO_SYNC`: When a transaction is committed, a log entry is generated simultaneously but is not flushed to disk.
- Introduced in: -

##### start_with_incomplete_meta

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: When true, the FE will allow startup when image data exists but Berkeley DB JE (BDB) log files are missing or corrupted. `MetaHelper.checkMetaDir()` uses this flag to bypass the safety check that otherwise prevents starting from an image without corresponding BDB logs; starting this way can produce stale or inconsistent metadata and should only be used for emergency recovery. `RestoreClusterSnapshotMgr` temporarily sets this flag to true while restoring a cluster snapshot and then rolls it back; that component also toggles `bdbje_reset_election_group` during restore. Do not enable in normal operation — enable only when recovering from corrupted BDB data or when explicitly restoring an image-based snapshot.
- Introduced in: v3.2.0

##### table_keeper_interval_second

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval, in seconds, between executions of the TableKeeper daemon. The TableKeeperDaemon uses this value (multiplied by 1000) to set its internal timer and periodically runs keeper tasks that ensure history tables exist, correct table properties (replication number) and update partition TTLs. The daemon only performs work on the leader node and updates its runtime interval via setInterval when `table_keeper_interval_second` changes. Increase to reduce scheduling frequency and load; decrease for faster reaction to missing or stale history tables.
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### task_runs_ttl_second

- Default: 7 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Controls the time-to-live (TTL) for task run history. Lowering this value shortens history retention and reduces memory/disk usage; raising it keeps histories longer but increases resource usage. Adjust together with `task_runs_max_history_number` and `enable_task_history_archive` for predictable retention and storage behavior.
- Introduced in: v3.2.0

##### task_ttl_second

- Default: 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Time-to-live (TTL) for tasks. For manual tasks (when no schedule is set), TaskBuilder uses this value to compute the task's `expireTime` (`expireTime = now + task_ttl_second * 1000L`). TaskRun also uses this value as an upper bound when computing a run's execute timeout — the effective execute timeout is `min(task_runs_timeout_second, task_runs_ttl_second, task_ttl_second)`. Adjusting this value changes how long manually created tasks remain valid and can indirectly limit the maximum allowed execution time of task runs.
- Introduced in: v3.2.0

##### thrift_rpc_retry_times

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls the total number of attempts a Thrift RPC call will make. This value is used by `ThriftRPCRequestExecutor` (and callers such as `NodeMgr` and `VariableMgr`) as the loop count for retries — i.e., a value of 3 allows up to three attempts including the initial try. On `TTransportException` the executor will try to reopen the connection and retry up to this count; it will not retry when the cause is a `SocketTimeoutException` or when reopen fails. Each attempt is subject to the per-attempt timeout configured by `thrift_rpc_timeout_ms`. Increasing this value improves resilience to transient connection failures but can increase overall RPC latency and resource usage.
- Introduced in: v3.2.0

##### thrift_rpc_strict_mode

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Controls the TBinaryProtocol "strict read" mode used by the Thrift server. This value is passed as the first argument to org.apache.thrift.protocol.TBinaryProtocol.Factory in the Thrift server stack and affects how incoming Thrift messages are parsed and validated. When `true` (default), the server enforces strict Thrift encoding/version checks and honors the configured `thrift_rpc_max_body_size` limit; when `false`, the server accepts non-strict (legacy/lenient) message formats, which can improve compatibility with older clients but may bypass some protocol validations. Use caution changing this on a running cluster because it is not mutable and affects interoperability and parsing safety.
- Introduced in: v3.2.0

##### thrift_rpc_timeout_ms

- Default: 10000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Timeout (in milliseconds) used as the default network/socket timeout for Thrift RPC calls. It is passed to TSocket when creating Thrift clients in `ThriftConnectionPool` (used by the frontend and backend pools) and is also added to an operation's execution timeout (e.g., ExecTimeout*1000 + `thrift_rpc_timeout_ms`) when computing RPC call timeouts in places such as `ConfigBase`, `LeaderOpExecutor`, `GlobalStateMgr`, `NodeMgr`, `VariableMgr`, and `CheckpointWorker`. Increasing this value makes RPC calls tolerate longer network or remote processing delays; decreasing it causes faster failover on slow networks. Changing this value affects connection creation and request deadlines across the FE code paths that perform Thrift RPCs.
- Introduced in: v3.2.0

##### txn_latency_metric_report_groups

- Default: An empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: A comma-separated list of transaction latency metric groups to report. Load types are categorized into logical groups for monitoring. When a group is enabled, its name is added as a 'type' label to transaction metrics. Valid values: `stream_load`, `routine_load`, `broker_load`, `insert`, and `compaction` (availabl only for shared-data clusters). Example: `"stream_load,routine_load"`.
- Introduced in: v4.0

##### txn_rollback_limit

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of transactions that can be rolled back.
- Introduced in: -

### User, role, and privilege

##### enable_task_info_mask_credential

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When true, StarRocks redacts credentials from task SQL definitions before returning them in information_schema.tasks and information_schema.task_runs by applying SqlCredentialRedactor.redact to the DEFINITION column. In `information_schema.task_runs` the same redaction is applied whether the definition comes from the task run status or, when empty, from the task definition lookup. When false, raw task definitions are returned (may expose credentials). Masking is CPU/string-processing work and can be time-consuming when the number of tasks or task_runs is large; disable only if you need unredacted definitions and accept the security risk.
- Introduced in: v3.5.6

##### privilege_max_role_depth

- Default: 16
- Type: Int
- Unit:
- Is mutable: Yes
- Description: The maximum role depth (level of inheritance) of a role.
- Introduced in: v3.0.0

##### privilege_max_total_roles_per_user

- Default: 64
- Type: Int
- Unit:
- Is mutable: Yes
- Description: The maximum number of roles a user can have.
- Introduced in: v3.0.0

### Query engine

##### brpc_send_plan_fragment_timeout_ms

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Timeout in milliseconds applied to the BRPC TalkTimeoutController before sending a plan fragment. `BackendServiceClient.sendPlanFragmentAsync` sets this value prior to calling the backend `execPlanFragmentAsync`. It governs how long BRPC will wait when borrowing an idle connection from the connection pool and while performing the send; if exceeded, the RPC will fail and may trigger the method's retry logic. Set this lower to fail fast under contention, or raise it to tolerate transient pool exhaustion or slow networks. Be cautious: very large values can delay failure detection and block request threads.
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### connector_table_query_trigger_analyze_large_table_interval

- Default: 12 * 3600
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: The interval for query-trigger ANALYZE tasks of large tables.
- Introduced in: v3.4.0

##### connector_table_query_trigger_analyze_max_pending_task_num

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of query-trigger ANALYZE tasks that are in Pending state on the FE.
- Introduced in: v3.4.0

##### connector_table_query_trigger_analyze_max_running_task_num

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of query-trigger ANALYZE tasks that are in Running state on the FE.
- Introduced in: v3.4.0

##### connector_table_query_trigger_analyze_small_table_interval

- Default: 2 * 3600
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: The interval for query-trigger ANALYZE tasks of small tables.
- Introduced in: v3.4.0

##### connector_table_query_trigger_analyze_small_table_rows

- Default: 10000000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The threshold for determining whether a table is a small table for query-trigger ANALYZE tasks.
- Introduced in: v3.4.0

##### connector_table_query_trigger_task_schedule_interval

- Default: 30
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: The interval at which the Scheduler thread schedules the query-trigger background tasks. This item is to replace `connector_table_query_trigger_analyze_schedule_interval` introduced in v3.4.0. Here, the background tasks refer `ANALYZE` tasks in v3.4，and the collection task of low-cardinality columns' dictionary in versions later than v3.4.  
- Introduced in: v3.4.2

##### create_table_max_serial_replicas

- Default: 128
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of replicas to create serially. If actual replica count exceeds this value, replicas will be created concurrently. Try to reduce this value if table creation is taking a long time to complete.
- Introduced in: -

##### default_mv_partition_refresh_number

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: When a materialized view refresh involves multiple partitions, this parameter controls how many partitions are refreshed in a single batch by default.
Starting from version 3.3.0, the system defaults to refreshing one partition at a time to avoid potential out-of-memory (OOM) issues. In earlier versions, all partitions were refreshed at once by default, which could lead to memory exhaustion and task failure. However, note that when a materialized view refresh involves a large number of partitions, refreshing only one partition at a time may lead to excessive scheduling overhead, longer overall refresh time, and a large number of refresh records. In such cases, it is recommended to adjust this parameter appropriately to improve refresh efficiency and reduce scheduling costs.
- Introduced in: v3.3.0

##### default_mv_refresh_immediate

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to refresh an asynchronous materialized view immediately after creation. When this item is set to `true`, newly created materialized view will be refreshed immediately.
- Introduced in: v3.2.3

##### dynamic_partition_check_interval_seconds

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which new data is checked. If new data is detected, StarRocks automatically creates partitions for the data.
- Introduced in: -

##### dynamic_partition_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the dynamic partitioning feature. When this feature is enabled, StarRocks dynamically creates partitions for new data and automatically deletes expired partitions to ensure the freshness of data.
- Introduced in: -

##### enable_active_materialized_view_schema_strict_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to strictly check the length consistency of data types when activating an inactive materialized view. When this item is set to `false`, the activation of the materialized view is not affected if the length of the data types has changed in the base table.
- Introduced in: v3.3.4

##### enable_auto_collect_array_ndv

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable automatic collection for the NDV information of the ARRAY type.
- Introduced in: v4.0

##### enable_backup_materialized_view

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the BACKUP and RESTORE of asynchronous materialized views when backing up or restoring a specific database. If this item is set to `false`, StarRocks will skip backing up asynchronous materialized views.
- Introduced in: v3.2.0

##### enable_collect_full_statistic

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable automatic full statistics collection. This feature is enabled by default.
- Introduced in: -

##### enable_colocate_mv_index

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to support colocating the synchronous materialized view index with the base table when creating a synchronous materialized view. If this item is set to `true`, tablet sink will speed up the write performance of synchronous materialized views.
- Introduced in: v3.2.0

##### enable_decimal_v3

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to support the DECIMAL V3 data type.
- Introduced in: -

##### enable_experimental_mv

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the asynchronous materialized view feature. TRUE indicates this feature is enabled. From v2.5.2 onwards, this feature is enabled by default. For versions earlier than v2.5.2, this feature is disabled by default.
- Introduced in: v2.4

##### enable_local_replica_selection

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to select local replicas for queries. Local replicas reduce the network transmission cost. If this parameter is set to TRUE, the CBO preferentially selects tablet replicas on BEs that have the same IP address as the current FE. If this parameter is set to `FALSE`, both local replicas and non-local replicas can be selected.
- Introduced in: -

##### enable_manual_collect_array_ndv

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable manual collection for the NDV information of the ARRAY type.
- Introduced in: v4.0

##### enable_materialized_view

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the creation of materialized views.
- Introduced in: -

##### enable_materialized_view_metrics_collect

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect monitoring metrics for asynchronous materialized views by default.
- Introduced in: v3.1.11, v3.2.5

##### enable_materialized_view_spill

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Intermediate Result Spilling for materialized view refresh tasks.
- Introduced in: v3.1.1

##### enable_materialized_view_text_based_rewrite

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable text-based query rewrite by default. If this item is set to `true`, the system builds the abstract syntax tree while creating an asynchronous materialized view.
- Introduced in: v3.2.5

##### enable_mv_automatic_active_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the system to automatically check and re-activate the asynchronous materialized views that are set inactive because their base tables (views) had undergone Schema Change or had been dropped and re-created. Please note that this feature will not re-activate the materialized views that are manually set inactive by users.
- Introduced in: v3.1.6

##### enable_predicate_columns_collection

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable predicate columns collection. If disabled, predicate columns will not be recorded during query optimization.
- Introduced in: -

##### enable_query_queue_v2

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: When true, switches the FE slot-based query scheduler to Query Queue V2. The flag is read by the slot manager and trackers (for example, `BaseSlotManager.isEnableQueryQueueV2` and `SlotTracker#createSlotSelectionStrategy`) to choose `SlotSelectionStrategyV2` instead of the legacy strategy. `query_queue_v2_xxx` configuration options and `QueryQueueOptions` take effect only when this flag is enabled. Because the value is static at runtime, enablement requires restarting the leader FE to take effect and may change query scheduling, concurrency limits, and queueing behavior cluster-wide.
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### enable_sql_blacklist

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable blacklist check for SQL queries. When this feature is enabled, queries in the blacklist cannot be executed.
- Introduced in: -

##### enable_statistic_collect

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect statistics for the CBO. This feature is enabled by default.
- Introduced in: -

##### enable_statistic_collect_on_first_load

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls automatic statistics collection and maintenance triggered by data loading operations. This includes:
  - Statistics collection when data is first loaded into a partition (partition version equals 2).
  - Statistics collection when data is loaded into empty partitions of multi-partition tables.
  - Statistics copying and updating for INSERT OVERWRITE operations.

  **Decision Policy for Statistics Collection Type:**
  
  - For INSERT OVERWRITE: `deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - If `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (Default: 0.1), statistics collection will not be performed. Only the existing statistics will be copied.
    - Else, if `targetRows > statistic_sample_collect_rows` (Default: 200000), SAMPLE statistics collection is used.
    - Else, FULL statistics collection is used.
  
  - For First Load: `deltaRatio = loadRows / (totalRows + 1)`
    - If `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (Default: 0.1), statistics collection will not be performed.
    - Else, if `loadRows > statistic_sample_collect_rows` (Default: 200000), SAMPLE statistics collection is used.
    - Else, FULL statistics collection is used.
  
  **Synchronization Behavior:**
  
  - For DML statements (INSERT INTO/INSERT OVERWRITE): Synchronous mode with table lock. The load operation waits for statistics collection to complete (up to `semi_sync_collect_statistic_await_seconds`).
  - For Stream Load and Broker Load: Asynchronous mode without lock. Statistics collection runs in background without blocking the load operation.
  
  :::note
  Disabling this configuration will prevent all loading-triggered statistics operations, including statistics maintenance for INSERT OVERWRITE, which may result in tables lacking statistics. If new tables are frequently created and data is frequently loaded, enabling this feature will increase memory and CPU overhead.
  :::

- Introduced in: v3.1

##### enable_statistic_collect_on_update

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls whether UPDATE statements can trigger automatic statistics collection. When enabled, UPDATE operations that modify table data may schedule statistics collection through the same ingestion-based statistics framework controlled by `enable_statistic_collect_on_first_load`. Disabling this configuration skips statistics collection for UPDATE statements while keeping load-triggered statistics collection behavior unchanged.
- Introduced in: v3.5.11, v4.0.4

##### enable_udf

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable UDF.
- Introduced in: -

##### expr_children_limit

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of child expressions allowed in an expression.
- Introduced in: -

##### histogram_buckets_size

- Default: 64
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The default bucket number for a histogram.
- Introduced in: -

##### histogram_max_sample_row_count

- Default: 10000000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rows to collect for a histogram.
- Introduced in: -

##### histogram_mcv_size

- Default: 100
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The number of most common values (MCV) for a histogram.
- Introduced in: -

##### histogram_sample_ratio

- Default: 0.1
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The sampling ratio for a histogram.
- Introduced in: -

##### http_slow_request_threshold_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: If the response time for an HTTP request exceeds the value specified by this parameter, a log is generated to track this request.
- Introduced in: v2.5.15, v3.1.5

##### lock_checker_enable_deadlock_check

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, the LockChecker thread performs JVM-level deadlock detection using ThreadMXBean.findDeadlockedThreads() and logs the offending threads' stack traces. The check runs inside the LockChecker daemon (whose frequency is controlled by `lock_checker_interval_second`) and writes detailed stack information to the log, which may be CPU- and I/O-intensive. Enable this option only for troubleshooting live or reproducible deadlock issues; leaving it enabled in normal operation can increase overhead and log volume.
- Introduced in: v3.2.0

##### low_cardinality_threshold

- Default: 255
- Type: Int
- Unit: -
- Is mutable: No
- Description: Threshold of low cardinality dictionary.
- Introduced in: v3.5.0

##### max_allowed_in_element_num_of_delete

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of elements allowed for the IN predicate in a DELETE statement.
- Introduced in: -

##### max_create_table_timeout_second

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum timeout duration for creating a table.
- Introduced in: -

##### max_distribution_pruner_recursion_depth

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:: The maximum recursion depth allowed by the partition pruner. Increasing the recursion depth can prune more elements but also increases CPU consumption.
- Introduced in: -

##### max_partitions_in_one_batch

- Default: 4096
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum number of partitions that can be created when you bulk create partitions.
- Introduced in: -

##### max_planner_scalar_rewrite_num

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum number of times that the optimizer can rewrite a scalar operator.
- Introduced in: -

##### max_query_queue_history_slots_number

- Default: 0
- Type: Int
- Unit: Slots
- Is mutable: Yes
- Description: Controls how many recently released (history) allocated slots are retained per query queue for monitoring and observability. When `max_query_queue_history_slots_number` is set to a value &gt; 0, BaseSlotTracker keeps up to that many most-recently released LogicalSlot entries in an in-memory queue, evicting the oldest when the limit is exceeded. Enabling this causes getSlots() to include these history entries (newest first), allows BaseSlotTracker to attempt registering slots with the ConnectContext for richer ExtraMessage data, and lets LogicalSlot.ConnectContextListener attach query finish metadata to history slots. When `max_query_queue_history_slots_number` &lt;= 0 the history mechanism is disabled (no extra memory used). Use a reasonable value to balance observability and memory overhead.
- Introduced in: v3.5.0

##### max_query_retry_time

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of query retries on an FE.
- Introduced in: -

##### max_running_rollup_job_num_per_table

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rollup jobs can run in parallel for a table.
- Introduced in: -

##### max_scalar_operator_flat_children

- Default：10000
- Type：Int
- Unit：-
- Is mutable: Yes
- Description：The maximum number of flat children for ScalarOperator. You can set this limit to prevent the optimizer from using too much memory.
- Introduced in: -

##### max_scalar_operator_optimize_depth

- Default：256
- Type：Int
- Unit：-
- Is mutable: Yes
- Description: The maximum depth that ScalarOperator optimization can be applied.
- Introduced in: -

##### mv_active_checker_interval_seconds

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: When the background active_checker thread is enabled, the system will periodically detect and automatically reactivate materialized views that became Inactive due to schema changes or rebuilds of their base tables (or views). This parameter controls the scheduling interval of the checker thread, in seconds. The default value is system-defined.
- Introduced in: v3.1.6

##### publish_version_interval_ms

- Default: 10
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The time interval at which release validation tasks are issued.
- Introduced in: -

##### query_queue_slots_estimator_strategy

- Default: MAX
- Type: String
- Unit: -
- Is mutable: Yes
- Description: Selects the slot estimation strategy used for queue-based queries when `enable_query_queue_v2` is true. Valid values: MBE (memory-based), PBE (parallelism-based), MAX (take max of MBE and PBE) and MIN (take min of MBE and PBE). MBE estimates slots from predicted memory or plan costs divided by the per-slot memory target and is capped by `totalSlots`. PBE derives slots from fragment parallelism (scan range counts or cardinality / rows-per-slot) and a CPU-cost based calculation (using CPU costs per slot), then bounds the result within [numSlots/2, numSlots]. MAX and MIN combine MBE and PBE by taking their maximum or minimum respectively. If the configured value is invalid, the default (`MAX`) is used.
- Introduced in: v3.5.0

##### query_queue_v2_concurrency_level

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls how many logical concurrency "layers" are used when computing the system's total query slots. In shared-nothing mode the total slots = `query_queue_v2_concurrency_level` * number_of_BEs * cores_per_BE (derived from BackendResourceStat). In multi-warehouse mode the effective concurrency is scaled down to max(1, `query_queue_v2_concurrency_level` / 4). If the configured value is non-positive it is treated as `4`. Changing this value increases or decreases totalSlots (and therefore concurrent query capacity) and affects per-slot resources: memBytesPerSlot is derived by dividing per-worker memory by (cores_per_worker * concurrency), and CPU accounting uses `query_queue_v2_cpu_costs_per_slot`. Set it proportional to cluster size; very large values may reduce per-slot memory and cause resource fragmentation. 
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### query_queue_v2_cpu_costs_per_slot

- Default: 1000000000
- Type: Long
- Unit: planner CPU cost units
- Is mutable: Yes
- Description: Per-slot CPU cost threshold used to estimate how many slots a query needs from its planner CPU cost. The scheduler computes slots as integer(plan_cpu_costs / `query_queue_v2_cpu_costs_per_slot`) and then clamps the result to the range [1, totalSlots] (totalSlots is derived from the query queue V2 `V2` parameters). The V2 code normalizes non-positive settings to 1 (Math.max(1, value)), so a non-positive value effectively becomes `1`. Increasing this value reduces slots allocated per query (favoring fewer, larger-slot queries); decreasing it increases slots per query. Tune together with `query_queue_v2_num_rows_per_slot` and concurrency settings to control parallelism vs. resource granularity.
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### query_queue_v2_num_rows_per_slot

- Default: 4096
- Type: Int
- Unit: Rows
- Is mutable: Yes
- Description: The target number of source-row records assigned to a single scheduling slot when estimating per-query slot count. StarRocks computes estimated_slots = (cardinality of the Source Node) / `query_queue_v2_num_rows_per_slot`, then clamps the result to the range [1, totalSlots] and enforces a minimum of 1 if the computed value is non-positive. totalSlots is derived from available resources (roughly DOP * `query_queue_v2_concurrency_level` * number_of_workers/BE) and therefore depends on cluster/core counts. Increase this value to reduce slot count (each slot handles more rows) and lower scheduling overhead; decrease it to increase parallelism (more, smaller slots), up to the resource limit.
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### query_queue_v2_schedule_strategy

- Default: SWRR
- Type: String
- Unit: -
- Is mutable: Yes
- Description: Selects the scheduling policy used by Query Queue V2 to order pending queries. Supported values (case-insensitive) are `SWRR` (Smooth Weighted Round Robin) — the default, suitable for mixed/hybrid workloads that need fair weighted sharing — and `SJF` (Short Job First + Aging) — prioritizes short jobs while using aging to avoid starvation. The value is parsed with case-insensitive enum lookup; an unrecognized value is logged as an error and the default policy is used. This configuration only affects behavior when Query Queue V2 is enabled and interacts with V2 sizing settings such as `query_queue_v2_concurrency_level`.
- Introduced in: v3.3.12, v3.4.2, v3.5.0

##### semi_sync_collect_statistic_await_seconds

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Maximum wait time for semi-synchronous statistics collection during DML operations (INSERT INTO and INSERT OVERWRITE statements). Stream Load and Broker Load use asynchronous mode and are not affected by this configuration. If statistics collection time exceeds this value, the load operation continues without waiting for collection to complete. This configuration works in conjunction with `enable_statistic_collect_on_first_load`.
- Introduced in: v3.1

##### slow_query_analyze_threshold

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:: The execution time threshold for queries to trigger the analysis of Query Feedback.
- Introduced in: v3.4.0

##### statistic_analyze_status_keep_second

- Default: 3 * 24 * 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The duration to retain the history of collection tasks. The default value is 3 days.
- Introduced in: -

##### statistic_auto_analyze_end_time

- Default: 23:59:59
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The end time of automatic collection. Value range: `00:00:00` - `23:59:59`.
- Introduced in: -

##### statistic_auto_analyze_start_time

- Default: 00:00:00
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The start time of automatic collection. Value range: `00:00:00` - `23:59:59`.
- Introduced in: -

##### statistic_auto_collect_ratio

- Default: 0.8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The threshold for determining whether the statistics for automatic collection are healthy. If statistics health is below this threshold, automatic collection is triggered.
- Introduced in: -

##### statistic_auto_collect_small_table_rows

- Default: 10000000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: Threshold to determine whether a table in an external data source (Hive, Iceberg, Hudi) is a small table during automatic collection. If the table has rows less than this value, the table is considered a small table.
- Introduced in: v3.2

##### statistic_cache_columns

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: No
- Description: The number of rows that can be cached for the statistics table.
- Introduced in: -

##### statistic_cache_thread_pool_size

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The size of the thread-pool which will be used to refresh statistic caches.
- Introduced in: -

##### statistic_collect_interval_sec

- Default: 5 * 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval for checking data updates during automatic collection.
- Introduced in: -

##### statistic_max_full_collect_data_size

- Default: 100 * 1024 * 1024 * 1024
- Type: Long
- Unit: bytes
- Is mutable: Yes
- Description: The data size threshold for the automatic collection of statistics. If the total size exceeds this value, then sampled collection is performed instead of full.
- Introduced in: -

##### statistic_sample_collect_rows

- Default: 200000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The row count threshold for deciding between SAMPLE and FULL statistics collection during loading-triggered statistics operations. If the number of loaded or changed rows exceeds this threshold (default 200,000), SAMPLE statistics collection is used; otherwise, FULL statistics collection is used. This setting works in conjunction with `enable_statistic_collect_on_first_load` and `statistic_sample_collect_ratio_threshold_of_first_load`.
- Introduced in: -

##### statistic_update_interval_sec

- Default: 24 * 60 * 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which the cache of statistical information is updated.
- Introduced in: -

##### task_check_interval_second

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval between executions of task background jobs. GlobalStateMgr uses this value to schedule the TaskCleaner FrontendDaemon which invokes `doTaskBackgroundJob()`; the value is multiplied by 1000 to set the daemon interval in milliseconds. Decreasing the value makes background maintenance (task cleanup, checks) run more frequently and react faster but increases CPU/IO overhead; increasing it reduces overhead but delays cleanup and detection of stale tasks. Tune this value to balance maintenance responsiveness and resource usage.
- Introduced in: v3.2.0

##### task_min_schedule_interval_s

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Minimum allowed schedule interval (in seconds) for task schedules checked by the SQL layer. When a task is submitted, TaskAnalyzer converts the schedule period to seconds and rejects the submission with ERR_INVALID_PARAMETER if the period is smaller than `task_min_schedule_interval_s`. This prevents creating tasks that run too frequently and protects the scheduler from high-frequency tasks. If a schedule has no explicit start time, TaskAnalyzer sets the start time to the current epoch seconds.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### Loading and unloading

##### broker_load_default_timeout_second

- Default: 14400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for a Broker Load job.
- Introduced in: -

##### desired_max_waiting_jobs

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of pending jobs in an FE. The number refers to all jobs, such as table creation, loading, and schema change jobs. If the number of pending jobs in an FE reaches this value, the FE will reject new load requests. This parameter takes effect only for asynchronous loading. From v2.5 onwards, the default value is changed from 100 to 1024.
- Introduced in: -

##### disable_load_job

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to disable loading when the cluster encounters an error. This prevents any loss caused by cluster errors. The default value is `FALSE`, indicating that loading is not disabled. `TRUE` indicates loading is disabled and the cluster is in read-only state.
- Introduced in: -

##### empty_load_as_error

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to return an error message "all partitions have no load data" if no data is loaded. Valid values:
  - `true`: If no data is loaded, the system displays a failure message and returns an error "all partitions have no load data".
  - `false`: If no data is loaded, the system displays a success message and returns OK, instead of an error.
- Introduced in: -

##### enable_file_bundling

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the File Bundling optimization for the cloud-native table. When this feature is enabled (set to `true`), the system automatically bundles the data files generated by loading, Compaction, or Publish operations, thereby reducing the API cost caused by high-frequency access to the external storage system. You can also control this behavior on the table level using the CREATE TABLE property `file_bundling`. For detailed instructions, see [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md).
- Introduced in: v4.0

##### enable_routine_load_lag_metrics

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect Routine Load Kafka partition offset lag metrics. Please note that set this item to `true` will call the Kafka API to get the partition's latest offset.
- Introduced in: -

##### enable_sync_publish

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to synchronously execute the apply task at the publish phase of a load transaction. This parameter is applicable only to Primary Key tables. Valid values:
  - `TRUE` (default): The apply task is synchronously executed at the publish phase of a load transaction. It means that the load transaction is reported as successful only after the apply task is completed, and the loaded data can truly be queried. When a task loads a large volume of data at a time or loads data frequently, setting this parameter to `true` can improve query performance and stability, but may increase load latency.
  - `FALSE`: The apply task is asynchronously executed at the publish phase of a load transaction. It means that the load transaction is reported as successful after the apply task is submitted, but the loaded data cannot be immediately queried. In this case, concurrent queries need to wait for the apply task to complete or time out before they can continue. When a task loads a large volume of data at a time or loads data frequently, setting this parameter to `false` may affect query performance and stability.
- Introduced in: v3.2.0

##### export_checker_interval_second

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which load jobs are scheduled.
- Introduced in: -

##### export_max_bytes_per_be_per_task

- Default: 268435456
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum amount of data that can be exported from a single BE by a single data unload task.
- Introduced in: -

##### export_running_job_num_limit

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of data exporting tasks that can run in parallel.
- Introduced in: -

##### export_task_default_timeout_second

- Default: 2 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for a data exporting task.
- Introduced in: -

##### export_task_pool_size

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: The size of the unload task thread pool.
- Introduced in: -

##### external_table_commit_timeout_ms

- Default: 10000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout duration for committing (publishing) a write transaction to a StarRocks external table. The default value `10000` indicates a 10-second timeout duration.
- Introduced in: -

##### finish_transaction_default_lock_timeout_ms

- Default: 1000
- Type: Int
- Unit: MilliSeconds
- Is mutable: Yes
- Description: The default timeout for acquiring the db and table lock during finishing transaction.
- Introduced in: v4.0.0, v3.5.8

##### history_job_keep_max_second

- Default: 7 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration a historical job can be retained, such as schema change jobs.
- Introduced in: -

##### insert_load_default_timeout_second

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for the INSERT INTO statement that is used to load data.
- Introduced in: -

##### label_clean_interval_second

- Default: 4 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which labels are cleaned up. Unit: second. We recommend that you specify a short time interval to ensure that historical labels can be cleaned up in a timely manner.
- Introduced in: -

##### label_keep_max_num

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of load jobs that can be retained within a period of time. If this number is exceeded, the information of historical jobs will be deleted.
- Introduced in: -

##### label_keep_max_second

- Default: 3 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration in seconds to keep the labels of load jobs that have been completed and are in the FINISHED or CANCELLED state. The default value is 3 days. After this duration expires, the labels will be deleted. This parameter applies to all types of load jobs. A value too large consumes a lot of memory.
- Introduced in: -

##### load_checker_interval_second

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which load jobs are processed on a rolling basis.
- Introduced in: -

##### load_parallel_instance_num

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls the number of parallel load fragment instances created on a single host for broker and stream loads. LoadPlanner uses this value as the per-host degree of parallelism unless the session enables adaptive sink DOP; if the session variable `enable_adaptive_sink_dop` is true, the session`s `sink_degree_of_parallelism` overrides this configuration. When shuffle is required, this value is applied to fragment parallel execution (scan fragment and sink fragment parallel exec instances). When no shuffle is needed, it is used as the sink pipeline DOP. Note: loads from local files are forced to a single instance (pipeline DOP = 1, parallel exec = 1) to avoid local disk contention. Increasing this number raises per-host concurrency and throughput but may increase CPU, memory and I/O contention.
- Introduced in: v3.2.0

##### load_straggler_wait_second

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum loading lag that can be tolerated by a BE replica. If this value is exceeded, cloning is performed to clone data from other replicas.
- Introduced in: -

##### loads_history_retained_days

- Default: 30
- Type: Int
- Unit: Days
- Is mutable: Yes
- Description: Number of days to retain load history in the internal `_statistics_.loads_history` table. This value is used for table creation to set the table property `partition_live_number` and is passed to `TableKeeper` (clamped to a minimum of 1) to determine how many daily partitions to keep. Increasing or decreasing this value adjusts how long completed load jobs are retained in daily partitions; it affects new table creation and the keeper's pruning behavior but does not automatically recreate past partitions. The `LoadsHistorySyncer` relies on this retention when managing the loads history lifecycle; its sync cadence is controlled by `loads_history_sync_interval_second`.
- Introduced in: v3.3.6, v3.4.0, v3.5.0

##### loads_history_sync_interval_second

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval (in seconds) used by LoadsHistorySyncer to schedule periodic syncs of finished load jobs from `information_schema.loads` into the internal `_statistics_.loads_history` table. The value is multiplied by 1000 in the constructor to set the FrontendDaemon interval. The syncer skips the first run (to allow table creation) and only imports loads that finished more than one minute ago; small values increase DML and executor load, while larger values delay availability of historical load records. See `loads_history_retained_days` for retention/partitioning behavior of the target table.
- Introduced in: v3.3.6, v3.4.0, v3.5.0

##### max_broker_load_job_concurrency

- Default: 5
- Alias: async_load_task_pool_size
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent Broker Load jobs allowed within the StarRocks cluster. This parameter is valid only for Broker Load. The value of this parameter must be less than the value of `max_running_txn_num_per_db`. From v2.5 onwards, the default value is changed from `10` to `5`.
- Introduced in: -

##### max_load_timeout_second

- Default: 259200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum timeout duration allowed for a load job. The load job fails if this limit is exceeded. This limit applies to all types of load jobs.
- Introduced in: -

##### max_routine_load_batch_size

- Default: 4294967296
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum amount of data that can be loaded by a Routine Load task.
- Introduced in: -

##### max_routine_load_task_concurrent_num

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent tasks for each Routine Load job.
- Introduced in: -

##### max_routine_load_task_num_per_be

- Default: 16
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent Routine Load tasks on each BE. Since v3.1.0, the default value for this parameter is increased to 16 from 5, and no longer needs to be less than or equal to the value of BE static parameter `routine_load_thread_pool_size` (deprecated).
- Introduced in: -

##### max_running_txn_num_per_db

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of load transactions allowed to be running for each database within a StarRocks cluster. The default value is `1000`. From v3.1 onwards, the default value is changed to `1000` from `100`. When the actual number of load transactions running for a database exceeds the value of this parameter, new load requests will not be processed. New requests for synchronous load jobs will be denied, and new requests for asynchronous load jobs will be placed in queue. We do not recommend you increase the value of this parameter because this will increase system load.
- Introduced in: -

##### max_stream_load_timeout_second

- Default: 259200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum allowed timeout duration for a Stream Load job.
- Introduced in: -

##### max_tolerable_backend_down_num

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of faulty BE nodes allowed. If this number is exceeded, Routine Load jobs cannot be automatically recovered.
- Introduced in: -

##### min_bytes_per_broker_scanner

- Default: 67108864
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: The minimum allowed amount of data that can be processed by a Broker Load instance.
- Introduced in: -

##### min_load_timeout_second

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum timeout duration allowed for a load job. This limit applies to all types of load jobs.
- Introduced in: -

##### min_routine_load_lag_for_metrics

- Default: 10000
- Type: INT
- Unit: -
- Is mutable: Yes
- Description: The minimum offset lag of Routine Load jobs to be shown in monitoring metrics. Routine Load jobs whose offset lags are greater than this value will be displayed in the metrics.
- Introduced in: -

##### period_of_auto_resume_min

- Default: 5
- Type: Int
- Unit: Minutes
- Is mutable: Yes
- Description: The interval at which Routine Load jobs are automatically recovered.
- Introduced in: -

##### prepared_transaction_default_timeout_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The default timeout duration for a prepared transaction.
- Introduced in: -

##### routine_load_task_consume_second

- Default: 15
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum time for each Routine Load task within the cluster to consume data. Since v3.1.0, Routine Load job supports a new parameter `task_consume_second` in [job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties). This parameter applies to individual load tasks within a Routine Load job, which is more flexible.
- Introduced in: -

##### routine_load_task_timeout_second

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for each Routine Load task within the cluster. Since v3.1.0, Routine Load job supports a new parameter `task_timeout_second` in [job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties). This parameter applies to individual load tasks within a Routine Load job, which is more flexible.
- Introduced in: -

##### routine_load_unstable_threshold_second

- Default: 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: Routine Load job is set to the UNSTABLE state if any task within the Routine Load job lags. To be specific, the difference between the timestamp of the message being consumed and the current time exceeds this threshold, and unconsumed messages exist in the data source.
- Introduced in: -

##### spark_dpp_version

- Default: 1.0.0
- Type: String
- Unit: -
- Is mutable: No
- Description: The version of Spark Dynamic Partition Pruning (DPP) used.
- Introduced in: -

##### spark_home_default_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of a Spark client.
- Introduced in: -

##### spark_launcher_log_dir

- Default: sys_log_dir + "/spark_launcher_log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores Spark log files.
- Introduced in: -

##### spark_load_default_timeout_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for each Spark Load job.
- Introduced in: -

##### spark_load_submit_timeout_second

- Default: 300
- Type: long
- Unit: Seconds
- Is mutable: No
- Description: Maximum time in seconds to wait for a YARN response after submitting a Spark application. `SparkLauncherMonitor.LogMonitor` converts this value to milliseconds and will stop monitoring and forcibly kill the spark launcher process if the job remains in UNKNOWN/CONNECTED/SUBMITTED longer than this timeout. `SparkLoadJob` reads this configuration as the default and allows a per-load override via the `LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT` property. Set it high enough to accommodate YARN queueing delays; setting it too low may abort legitimately queued jobs, while setting it too high may delay failure handling and resource cleanup.
- Introduced in: v3.2.0

##### spark_resource_path

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of the Spark dependency package.
- Introduced in: -

##### stream_load_default_timeout_second

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The default timeout duration for each Stream Load job.
- Introduced in: -

##### stream_load_max_txn_num_per_be

- Default: -1
- Type: Int
- Unit: Transactions
- Is mutable: Yes
- Description: Limits the number of concurrent stream-load transactions accepted from a single BE (backend) host. When set to a non-negative integer, FrontendServiceImpl checks the current transaction count for the BE (by client IP) and rejects new stream-load begin requests if the count >= this limit. A value of &lt; 0 disables the limit (unlimited). This check occurs during stream load begin and may cause a `streamload txn num per be exceeds limit` error when exceeded. Related runtime behavior uses `stream_load_default_timeout_second` for request timeout fallback.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### stream_load_task_keep_max_num

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of Stream Load tasks that StreamLoadMgr keeps in memory (global across all databases). When the number of tracked tasks (`idToStreamLoadTask`) exceeds this threshold, StreamLoadMgr first calls `cleanSyncStreamLoadTasks()` to remove completed synchronous stream-load tasks; if the size still remains greater than half of this threshold, it invokes `cleanOldStreamLoadTasks(true)` to force removal of older or finished tasks. Increase this value to retain more task history in memory; decrease it to reduce memory usage and make cleanup more aggressive. This value controls in-memory retention only and does not affect persisted/replayed tasks.
- Introduced in: v3.2.0

##### stream_load_task_keep_max_second

- Default: 3 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Retention window for finished or cancelled Stream Load tasks. After a task reaches a final state and its end timestamp is ealier than this threshold (`currentMs - endTimeMs > stream_load_task_keep_max_second * 1000`), it becomes eligible for removal by `StreamLoadMgr.cleanOldStreamLoadTasks` and is discarded when loading persisted state. Applies to both `StreamLoadTask` and `StreamLoadMultiStmtTask`. If total task count exceeds `stream_load_task_keep_max_num`, cleanup may be triggered earlier (synchronous tasks are prioritized by `cleanSyncStreamLoadTasks`). Set this to balance history/debugability against memory usage.
- Introduced in: v3.2.0

##### transaction_clean_interval_second

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which finished transactions are cleaned up. Unit: second. We recommend that you specify a short time interval to ensure that finished transactions can be cleaned up in a timely manner.
- Introduced in: -

##### transaction_stream_load_coordinator_cache_capacity

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The capacity of the cache that stores the mapping from transaction label to coordinator node.
- Introduced in: -

##### transaction_stream_load_coordinator_cache_expire_seconds

- Default: 900
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time to keep the coordinator mapping in the cache before it's evicted(TTL).
- Introduced in: -

##### yarn_client_path

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of the Yarn client package.
- Introduced in: -

##### yarn_config_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores the Yarn configuration file.
- Introduced in: -

### Statistic report

##### enable_http_detail_metrics

- Default: false
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: When true, the HTTP server computes and exposes detailed HTTP worker metrics (notably the `HTTP_WORKER_PENDING_TASKS_NUM` gauge). Enabling this causes the server to iterate over Netty worker executors and call `pendingTasks()` on each `NioEventLoop` to sum pending task counts; when disabled the gauge returns 0 to avoid that cost. This extra collection can be CPU- and latency-sensitive — enable only for debugging or detailed investigation.
- Introduced in: v3.2.3

### Storage

##### alter_table_timeout_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for the schema change operation (ALTER TABLE).
- Introduced in: -

##### capacity_used_percent_high_water

- Default: 0.75
- Type: double
- Unit: Fraction (0.0–1.0)
- Is mutable: Yes
- Description: The high-water threshold of disk capacity used percent (fraction of total capacity) used when computing backend load scores. `BackendLoadStatistic.calcSore` uses `capacity_used_percent_high_water` to set `LoadScore.capacityCoefficient`: if a backend's used percent less than 0.5 the coefficient equal to 0.5; if used percent > `capacity_used_percent_high_water` the coefficient = 1.0; otherwise the coefficient transitions linearly with used percent via (2 * usedPercent - 0.5). When the coefficient is 1.0, the load score is driven entirely by capacity proportion; lower values increase the weight of replica count. Adjusting this value changes how aggressively the balancer penalizes backends with high disk utilization.
- Introduced in: v3.2.0

##### catalog_trash_expire_second

- Default: 86400
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The longest duration the metadata can be retained after a database, table, or partition is dropped. If this duration expires, the data will be deleted and cannot be recovered through the [RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) command.
- Introduced in: -

##### check_consistency_default_timeout_second

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for a replica consistency check. You can set this parameter based on the size of your tablet.
- Introduced in: -

##### default_replication_num

- Default: 3
- Type: Short
- Unit: -
- Is mutable: Yes
- Description: Sets the default number of replicas for each data partition when creating a table in StarRocks. This setting can be overridden when creating a table by specifying `replication_num=x` in the CREATE TABLE DDL.
- Introduced in: -

##### enable_auto_tablet_distribution

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to automatically set the number of buckets.
  - If this parameter is set to `TRUE`, you don't need to specify the number of buckets when you create a table or add a partition. StarRocks automatically determines the number of buckets.
  - If this parameter is set to `FALSE`, you need to manually specify the number of buckets when you create a table or add a partition. If you do not specify the bucket count when adding a new partition to a table, the new partition inherits the bucket count set at the creation of the table. However, you can also manually specify the number of buckets for the new partition.
- Introduced in: v2.5.7

##### enable_experimental_rowstore

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the [hybrid row-column storage](../../table_design/hybrid_table.md) feature.
- Introduced in: v3.2.3

##### enable_fast_schema_evolution

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable fast schema evolution for all tables within the StarRocks cluster. Valid values are `TRUE` and `FALSE` (default). Enabling fast schema evolution can increase the speed of schema changes and reduce resource usage when columns are added or dropped.
- Introduced in: v3.2.0

> **NOTE**
>
> - StarRocks shared-data clusters supports this parameter from v3.3.0.
> - If you need to configure the fast schema evolution for a specific table, such as disabling fast schema evolution for a specific table, you can set the table property [`fast_schema_evolution`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution) at table creation.

##### enable_online_optimize_table

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls whether StarRocks will use the non-blocking online optimization path when creating an optimize job. When `enable_online_optimize_table` is true and the target table meets compatibility checks (no partition/keys/sort specification, distribution is not `RandomDistributionDesc`, storage type is not `COLUMN_WITH_ROW`, replicated storage enabled, and the table is not a cloud-native table or materialized view), the planner creates an `OnlineOptimizeJobV2` to perform optimization without blocking writes. If false or any compatibility condition fails, StarRocks falls back to `OptimizeJobV2`, which may block write operations during optimization.
- Introduced in: v3.3.3, v3.4.0, v3.5.0

##### enable_strict_storage_medium_check

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether the FE strictly checks the storage medium of BEs when users create tables. If this parameter is set to `TRUE`, the FE checks the storage medium of BEs when users create tables and returns an error if the storage medium of the BE is different from the `storage_medium` parameter specified in the CREATE TABLE statement. For example, the storage medium specified in the CREATE TABLE statement is SSD but the actual storage medium of BEs is HDD. As a result, the table creation fails. If this parameter is `FALSE`, the FE does not check the storage medium of BEs when users create a table.
- Introduced in: -

##### max_bucket_number_per_partition

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of buckets can be created in a partition.
- Introduced in: v3.3.2

##### max_column_number_per_table

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of columns can be created in a table.
- Introduced in: v3.3.2

##### max_dynamic_partition_num

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Limits the maximum number of partitions that can be created at once when analyzing or creating a dynamic-partitioned table. During dynamic partition property validation, the systemtask_runs_max_history_number computes expected partitions (end offset + history partition number) and throws a DDL error if that total exceeds `max_dynamic_partition_num`. Raise this value only when you expect legitimately large partition ranges; increasing it allows more partitions to be created but can increase metadata size, scheduling work, and operational complexity.
- Introduced in: v3.2.0

##### max_partition_number_per_table

- Default: 100000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of partitions can be created in a table.
- Introduced in: v3.3.2

##### partition_recycle_retention_period_secs

- Default: 1800
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The metadata retention time for the partition that is dropped by INSERT OVERWRITE or materialized view refresh operations. Note that such metadata cannot be recovered by executing [RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md).
- Introduced in: v3.5.9

##### recover_with_empty_tablet

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to replace a lost or corrupted tablet replica with an empty one. If a tablet replica is lost or corrupted, data queries on this tablet or other healthy tablets may fail. Replacing the lost or corrupted tablet replica with an empty tablet ensures that the query can still be executed. However, the result may be incorrect because data is lost. The default value is `FALSE`, which means lost or corrupted tablet replicas are not replaced with empty ones, and the query fails.
- Introduced in: -

##### storage_usage_hard_limit_percent

- Default: 95
- Alias: storage_flood_stage_usage_percent
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Hard limit of the storage usage percentage in a BE directory. If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_usage_hard_limit_reserve_bytes`, Load and Restore jobs are rejected. You need to set this item together with the BE configuration item `storage_flood_stage_usage_percent` to allow the configurations to take effect.
- Introduced in: -

##### storage_usage_hard_limit_reserve_bytes

- Default: 100 * 1024 * 1024 * 1024
- Alias: storage_flood_stage_left_capacity_bytes
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: Hard limit of the remaining storage space in a BE directory. If the remaining storage space in the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_usage_hard_limit_percent`, Load and Restore jobs are rejected. You need to set this item together with the BE configuration item `storage_flood_stage_left_capacity_bytes` to allow the configurations to take effect.
- Introduced in: -

##### storage_usage_soft_limit_percent

- Default: 90
- Alias: storage_high_watermark_usage_percent
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Soft limit of the storage usage percentage in a BE directory. If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_usage_soft_limit_reserve_bytes`, tablets cannot be cloned into this directory.
- Introduced in: -

##### storage_usage_soft_limit_reserve_bytes

- Default: 200 * 1024 * 1024 * 1024
- Alias: storage_min_left_capacity_bytes
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: Soft limit of the remaining storage space in a BE directory. If the remaining storage space in the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_usage_soft_limit_percent`, tablets cannot be cloned into this directory.
- Introduced in: -

##### tablet_checker_lock_time_per_cycle_ms

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The maximum lock hold time per cycle for tablet checker before releasing and reacquiring the table lock. Values less than 100 will be treated as 100.
- Introduced in: v3.5.9, v4.0.2

##### tablet_create_timeout_second

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for creating a tablet. The default value is changed from 1 to 10 from v3.1 onwards.
- Introduced in: -

##### tablet_delete_timeout_second

- Default: 2
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for deleting a tablet.
- Introduced in: -

##### tablet_sched_balance_load_disk_safe_threshold

- Default: 0.5
- Alias: balance_load_disk_safe_threshold
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The percentage threshold for determining whether the disk usage of BEs is balanced. If the disk usage of all BEs is lower than this value, it is considered balanced. If the disk usage is greater than this value and the difference between the highest and lowest BE disk usage is greater than 10%, the disk usage is considered unbalanced and a tablet re-balancing is triggered.
- Introduced in: -

##### tablet_sched_balance_load_score_threshold

- Default: 0.1
- Alias: balance_load_score_threshold
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The percentage threshold for determining whether the load of a BE is balanced. If a BE has a lower load than the average load of all BEs and the difference is greater than this value, this BE is in a low load state. On the contrary, if a BE has a higher load than the average load and the difference is greater than this value, this BE is in a high load state.
- Introduced in: -

##### tablet_sched_be_down_tolerate_time_s

- Default: 900
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration the scheduler allows for a BE node to remain inactive. After the time threshold is reached, tablets on that BE node will be migrated to other active BE nodes.
- Introduced in: v2.5.7

##### tablet_sched_disable_balance

- Default: false
- Alias: disable_balance
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to disable tablet balancing. `TRUE` indicates that tablet balancing is disabled. `FALSE` indicates that tablet balancing is enabled.
- Introduced in: -

##### tablet_sched_disable_colocate_balance

- Default: false
- Alias: disable_colocate_balance
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to disable replica balancing for Colocate Table. `TRUE` indicates replica balancing is disabled. `FALSE` indicates replica balancing is enabled.
- Introduced in: -

##### tablet_sched_max_balancing_tablets

- Default: 500
- Alias: max_balancing_tablets
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablets that can be balanced at the same time. If this value is exceeded, tablet re-balancing will be skipped.
- Introduced in: -

##### tablet_sched_max_clone_task_timeout_sec

- Default: 2 * 60 * 60
- Alias: max_clone_task_timeout_sec
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:The maximum timeout duration for cloning a tablet.
- Introduced in: -

##### tablet_sched_max_not_being_scheduled_interval_ms

- Default: 15 * 60 * 1000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: When the tablet clone tasks are being scheduled, if a tablet has not been scheduled for the specified time in this parameter, StarRocks gives it a higher priority to schedule it as soon as possible.
- Introduced in: -

##### tablet_sched_max_scheduling_tablets

- Default: 10000
- Alias: max_scheduling_tablets
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablets that can be scheduled at the same time. If the value is exceeded, tablet balancing and repair checks will be skipped.
- Introduced in: -

##### tablet_sched_min_clone_task_timeout_sec

- Default: 3 * 60
- Alias: min_clone_task_timeout_sec
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum timeout duration for cloning a tablet.
- Introduced in: -

##### tablet_sched_num_based_balance_threshold_ratio

- Default: 0.5
- Alias: -
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: Doing num based balance may break the disk size balance, but the maximum gap between disks cannot exceed tablet_sched_num_based_balance_threshold_ratio * tablet_sched_balance_load_score_threshold. If there are tablets in the cluster that are constantly balancing from A to B and B to A, reduce this value. If you want the tablet distribution to be more balanced, increase this value.
- Introduced in: - 3.1

##### tablet_sched_repair_delay_factor_second

- Default: 60
- Alias: tablet_repair_delay_factor_second
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which replicas are repaired, in seconds.
- Introduced in: -

##### tablet_sched_slot_num_per_path

- Default: 8
- Alias: schedule_slot_num_per_path
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablet-related tasks that can run concurrently in a BE storage directory. From v2.5 onwards, the default value of this parameter is changed from `4` to `8`.
- Introduced in: -

##### tablet_sched_storage_cooldown_second

- Default: -1
- Alias: storage_cooldown_second
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The latency of automatic cooling starting from the time of table creation. The default value `-1` specifies that automatic cooling is disabled. If you want to enable automatic cooling, set this parameter to a value greater than `-1`.
- Introduced in: -

##### tablet_stat_update_interval_second

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which the FE retrieves tablet statistics from each BE.
- Introduced in: -

### Shared-data

##### aws_s3_access_key

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Access Key ID used to access your S3 bucket.
- Introduced in: v3.0

##### aws_s3_endpoint

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The endpoint used to access your S3 bucket, for example, `https://s3.us-west-2.amazonaws.com`.
- Introduced in: v3.0

##### aws_s3_external_id

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The external ID of the AWS account that is used for cross-account access to your S3 bucket.
- Introduced in: v3.0

##### aws_s3_iam_role_arn

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The ARN of the IAM role that has privileges on your S3 bucket in which your data files are stored.
- Introduced in: v3.0

##### aws_s3_path

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The S3 path used to store data. It consists of the name of your S3 bucket and the sub-path (if any) under it, for example, `testbucket/subpath`.
- Introduced in: v3.0

##### aws_s3_region

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The region in which your S3 bucket resides, for example, `us-west-2`.
- Introduced in: v3.0

##### aws_s3_secret_key

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Secret Access Key used to access your S3 bucket.
- Introduced in: v3.0

##### aws_s3_use_aws_sdk_default_behavior

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use the default authentication credential of AWS SDK. Valid values: true and false (Default).
- Introduced in: v3.0

##### aws_s3_use_instance_profile

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use Instance Profile and Assumed Role as credential methods for accessing S3. Valid values: true and false (Default).
  - If you use IAM user-based credential (Access Key and Secret Key) to access S3, you must specify this item as `false`, and specify `aws_s3_access_key` and `aws_s3_secret_key`.
  - If you use Instance Profile to access S3, you must specify this item as `true`.
  - If you use Assumed Role to access S3, you must specify this item as `true`, and specify `aws_s3_iam_role_arn`.
  - And if you use an external AWS account, you must also specify `aws_s3_external_id`.
- Introduced in: v3.0

##### azure_adls2_endpoint

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The endpoint of your Azure Data Lake Storage Gen2 Account, for example, `https://test.dfs.core.windows.net`.
- Introduced in: v3.4.1

##### azure_adls2_oauth2_client_id

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Client ID of the Managed Identity used to authorize requests for your Azure Data Lake Storage Gen2.
- Introduced in: v3.4.4

##### azure_adls2_oauth2_tenant_id

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Tenant ID of the Managed Identity used to authorize requests for your Azure Data Lake Storage Gen2.
- Introduced in: v3.4.4

##### azure_adls2_oauth2_use_managed_identity

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use Managed Identity to authorize requests for your Azure Data Lake Storage Gen2.
- Introduced in: v3.4.4

##### azure_adls2_path

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Azure Data Lake Storage Gen2 path used to store data. It consists of the file system name and the directory name, for example, `testfilesystem/starrocks`.
- Introduced in: v3.4.1

##### azure_adls2_sas_token

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The shared access signatures (SAS) used to authorize requests for your Azure Data Lake Storage Gen2.
- Introduced in: v3.4.1

##### azure_adls2_shared_key

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Shared Key used to authorize requests for your Azure Data Lake Storage Gen2.
- Introduced in: v3.4.1

##### azure_blob_endpoint

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The endpoint of your Azure Blob Storage Account, for example, `https://test.blob.core.windows.net`.
- Introduced in: v3.1

##### azure_blob_path

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Azure Blob Storage path used to store data. It consists of the name of the container within your storage account and the sub-path (if any) under the container, for example, `testcontainer/subpath`.
- Introduced in: v3.1

##### azure_blob_sas_token

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The shared access signatures (SAS) used to authorize requests for your Azure Blob Storage.
- Introduced in: v3.1

##### azure_blob_shared_key

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Shared Key used to authorize requests for your Azure Blob Storage.
- Introduced in: v3.1

##### azure_use_native_sdk

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to use the native SDK to access Azure Blob Storage, thus allowing authentication with Managed Identities and Service Principals. If this item is set to `false`, only authentication with Shared Key and SAS Token is allowed.
- Introduced in: v3.4.4

##### cloud_native_hdfs_url

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The URL of the HDFS storage, for example, `hdfs://127.0.0.1:9000/user/xxx/starrocks/`.
- Introduced in: -

##### cloud_native_meta_port

- Default: 6090
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE cloud-native metadata server RPC listen port.
- Introduced in: -

##### cloud_native_storage_type

- Default: S3
- Type: String
- Unit: -
- Is mutable: No
- Description: The type of object storage you use. In shared-data mode, StarRocks supports storing data in HDFS, Azure Blob (supported from v3.1.1 onwards), Azure Data Lake Storage Gen2 (supported from v3.4.1 onwards), Google Storage (with native SDK, supported from v3.5.1 onwards), and object storage systems that are compatible with the S3 protocol (such as AWS S3, and MinIO). Valid value: `S3` (Default), `HDFS`, `AZBLOB`, `ADLS2`, and `GS`. If you specify this parameter as `S3`, you must add the parameters prefixed by `aws_s3`. If you specify this parameter as `AZBLOB`, you must add the parameters prefixed by `azure_blob`. If you specify this parameter as `ADLS2`, you must add the parameters prefixed by `azure_adls2`. If you specify this parameter as `GS`, you must add the parameters prefixed by `gcp_gcs`. If you specify this parameter as `HDFS`, you only need to specify `cloud_native_hdfs_url`.
- Introduced in: -

##### enable_load_volume_from_conf

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to allow StarRocks to create the built-in storage volume by using the object storage-related properties specified in the FE configuration file. The default value is changed from `true` to `false` from v3.4.1 onwards.
- Introduced in: v3.1.0

##### gcp_gcs_impersonation_service_account

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Service Account that you want to impersonate if you use the impersonation-based authentication to access Google Storage.
- Introduced in: v3.5.1

##### gcp_gcs_path

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Google Cloud path used to store data. It consists of the name of your Google Cloud bucket and the sub-path (if any) under it, for example, `testbucket/subpath`.
- Introduced in: v3.5.1

##### gcp_gcs_service_account_email

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The email address in the JSON file generated at the creation of the Service Account, for example, `user@hello.iam.gserviceaccount.com`.
- Introduced in: v3.5.1

##### gcp_gcs_service_account_private_key

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Private Key in the JSON file generated at the creation of the Service Account, for example, `-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`.
- Introduced in: v3.5.1

##### gcp_gcs_service_account_private_key_id

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Private Key ID in the JSON file generated at the creation of the Service Account.
- Introduced in: v3.5.1

##### gcp_gcs_use_compute_engine_service_account

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use the Service Account that is bound to your Compute Engine.
- Introduced in: v3.5.1

##### hdfs_file_system_expire_seconds

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Time-to-live in seconds for an unused cached HDFS/ObjectStore FileSystem managed by HdfsFsManager. The FileSystemExpirationChecker (runs every 60s) calls each HdfsFs.isExpired(...) using this value; when expired the manager closes the underlying FileSystem and removes it from the cache. Accessor methods (for example `HdfsFs.getDFSFileSystem`, `getUserName`, `getConfiguration`) update the last-access timestamp, so expiry is based on inactivity. Lower values reduce idle resource holding but increase reopen overhead; higher values keep handles longer and may consume more resources.
- Introduced in: v3.2.0

##### lake_autovacuum_grace_period_minutes

- Default: 30
- Type: Long
- Unit: Minutes
- Is mutable: Yes
- Description: The time range for retaining historical data versions in a shared-data cluster. Historical data versions within this time range are not automatically cleaned via AutoVacuum after Compactions. You need to set this value greater than the maximum query time to avoid that the data accessed by running queries get deleted before the queries finish. The default value has been changed from `5` to `30` since v3.3.0, v3.2.5, and v3.1.10.
- Introduced in: v3.1.0

##### lake_autovacuum_parallel_partitions

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of partitions that can undergo AutoVacuum simultaneously in a shared-data cluster. AutoVacuum is the Garbage Collection after Compactions.
- Introduced in: v3.1.0

##### lake_autovacuum_partition_naptime_seconds

- Default: 180
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum interval between AutoVacuum operations on the same partition in a shared-data cluster.
- Introduced in: v3.1.0

##### lake_autovacuum_stale_partition_threshold

- Default: 12
- Type: Long
- Unit: Hours
- Is mutable: Yes
- Description: If a partition has no updates (loading, DELETE, or Compactions) within this time range, the system will not perform AutoVacuum on this partition.
- Introduced in: v3.1.0

##### lake_compaction_allow_partial_success

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: If this item is set to `true`, the system will consider the Compaction operation in a shared-data cluster as successful when one of the sub-tasks succeeds.
- Introduced in: v3.5.2

##### lake_compaction_disable_ids

- Default: ""
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The table or partition list of which compaction is disabled in shared-data mode. The format is `tableId1;partitionId2`, seperated by semicolon, for example, `12345;98765`.
- Introduced in: v3.4.4

##### lake_compaction_history_size

- Default: 20
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of recent successful Compaction task records to keep in the memory of the Leader FE node in a shared-data cluster. You can view recent successful Compaction task records using the `SHOW PROC '/compactions'` command. Note that the Compaction history is stored in the FE process memory, and it will be lost if the FE process is restarted.
- Introduced in: v3.1.0

##### lake_compaction_max_tasks

- Default: -1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent Compaction tasks allowed in a shared-data cluster. Setting this item to `-1` indicates to calculate the concurrent task number in an adaptive manner. Setting this value to `0` will disable compaction.
- Introduced in: v3.1.0

##### lake_compaction_score_selector_min_score

- Default: 10.0
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The Compaction Score threshold that triggers Compaction operations in a shared-data cluster. When the Compaction Score of a partition is greater than or equal to this value, the system performs Compaction on that partition.
- Introduced in: v3.1.0

##### lake_compaction_score_upper_bound

- Default: 2000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The upper limit of the Compaction Score for a partition in a shared-data cluster. `0` indicates no upper limit. This item only takes effect when `lake_enable_ingest_slowdown` is set to `true`. When the Compaction Score of a partition reaches or exceeds this upper limit, incoming loading tasks will be rejected. From v3.3.6 onwards, the default value is changed from `0` to `2000`.
- Introduced in: v3.2.0

##### lake_enable_balance_tablets_between_workers

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to balance the number of tablets among Compute Nodes during the tablet migration of cloud-native tables in a shared-data cluster. `true` indicates to balance the tablets among Compute Nodes, and `false` indicates to disabling this feature.
- Introduced in: v3.3.4

##### lake_enable_ingest_slowdown

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Data Ingestion Slowdown in a shared-data cluster. When Data Ingestion Slowdown is enabled, if the Compaction Score of a partition exceeds `lake_ingest_slowdown_threshold`, loading tasks on that partition will be throttled down. This configuration only takes effect when `run_mode` is set to `shared_data`. From v3.3.6 onwards, the default value is chenged from `false` to `true`.
- Introduced in: v3.2.0

##### lake_ingest_slowdown_threshold

- Default: 100
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The Compaction Score threshold that triggers Data Ingestion Slowdown in a shared-data cluster. This configuration only takes effect when `lake_enable_ingest_slowdown` is set to `true`.
- Introduced in: v3.2.0

##### lake_publish_version_max_threads

- Default: 512
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for Version Publish tasks in a shared-data cluster.
- Introduced in: v3.2.0

##### meta_sync_force_delete_shard_meta

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow deleting the metadata of the shared-data cluster directly, bypassing cleaning the remote storage files. It is recommended to set this item to `true` only when there is an excessive number of shards to be cleaned, which leads to extreme memory pressure on the FE JVM. Note that the data files belonging to the shards or tablets cannot be automatically cleaned after this feature is enabled.
- Introduced in: v3.2.10, v3.3.3

##### run_mode

- Default: shared_nothing
- Type: String
- Unit: -
- Is mutable: No
- Description: The running mode of the StarRocks cluster. Valid values: `shared_data` and `shared_nothing` (Default).
  - `shared_data` indicates running StarRocks in shared-data mode.
  - `shared_nothing` indicates running StarRocks in shared-nothing mode.

  > **CAUTION**
  >
  > - You cannot adopt the `shared_data` and `shared_nothing` modes simultaneously for a StarRocks cluster. Mixed deployment is not supported.
  > - DO NOT change `run_mode` after the cluster is deployed. Otherwise, the cluster fails to restart. The transformation from a shared-nothing cluster to a shared-data cluster or vice versa is not supported.

- Introduced in: -

##### shard_group_clean_threshold_sec

- Default: 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The time before FE cleans the unused tablet and shard groups in a shared-data cluster. Tablets and shard groups created within this threshold will not be cleaned.
- Introduced in: -

##### star_mgr_meta_sync_interval_sec

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The interval at which FE runs the periodical metadata synchronization with StarMgr in a shared-data cluster.
- Introduced in: -

##### starmgr_grpc_server_max_worker_threads

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of worker threads that are used by the grpc server in the FE starmgr module.
- Introduced in: v4.0.0, v3.5.8

##### starmgr_grpc_timeout_seconds

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -

### Data Lake

##### files_enable_insert_push_down_schema

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, the analyzer will attempt to push the target table schema into the `files()` table function for INSERT ... FROM files() operations. This only applies when the source is a FileTableFunctionRelation, the target is a native table, and the SELECT list contains corresponding slot-ref columns (or *). The analyzer will match select columns to target columns (counts must match), lock the target table briefly, and replace file-column types with deep-copied target column types for non-complex types (complex types such as parquet json -> array&lt;varchar&gt; are skipped). Column names from the original files table are preserved. This reduces type-mismatch and looseness from file-based type inference during ingestion.
- Introduced in: v3.4.0, v3.5.0

##### hdfs_read_buffer_size_kb

- Default: 8192
- Type: Int
- Unit: Kilobytes
- Is mutable: Yes
- Description: Size of the HDFS read buffer in kilobytes. StarRocks converts this value to bytes (`<< 10`) and uses it to initialize HDFS read buffers in `HdfsFsManager` and to populate the thrift field `hdfs_read_buffer_size_kb` sent to BE tasks (e.g., `TBrokerScanRangeParams`, `TDownloadReq`) when broker access is not used. Increasing `hdfs_read_buffer_size_kb` can improve sequential read throughput and reduce syscall overhead at the cost of higher per-stream memory usage; decreasing it reduces memory footprint but may lower IO efficiency. Consider workload (many small streams vs. few large sequential reads) when tuning.
- Introduced in: v3.2.0

##### hdfs_write_buffer_size_kb

- Default: 1024
- Type: Int
- Unit: Kilobytes
- Is mutable: Yes
- Description: Sets the HDFS write buffer size (in KB) used for direct writes to HDFS or object stores when not using a broker. The FE converts this value to bytes (`<< 10`) and initializes the local write buffer in HdfsFsManager, and it is propagated in Thrift requests (e.g., TUploadReq, TExportSink, sink options) so backends/agents use the same buffer size. Increasing this value can improve throughput for large sequential writes at the cost of more memory per writer; decreasing it reduces per-stream memory usage and may lower latency for small writes. Tune alongside `hdfs_read_buffer_size_kb` and consider available memory and concurrent writers.
- Introduced in: v3.2.0

##### lake_batch_publish_max_version_num

- Default: 10
- Type: Int
- Unit: Count
- Is mutable: Yes
- Description: Sets the upper bound on how many consecutive transaction versions may be grouped together when building a publish batch for lake (cloud‑native) tables. The value is passed to the transaction graph batching routine (see getReadyToPublishTxnListBatch) and works together with `lake_batch_publish_min_version_num` to determine the candidate range size for a TransactionStateBatch. Larger values can increase publish throughput by batching more commits, but increase the scope of an atomic publish (longer visibility latency and larger rollback surface) and may be limited at runtime when versions are not consecutive. Tune according to workload and visibility/latency requirements.
- Introduced in: v3.2.0

##### lake_batch_publish_min_version_num

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Sets the minimum number of consecutive transaction versions required to form a publish batch for lake tables. DatabaseTransactionMgr.getReadyToPublishTxnListBatch passes this value to transactionGraph.getTxnsWithTxnDependencyBatch together with `lake_batch_publish_max_version_num` to select dependent transactions. A value of `1` allows single-transaction publishes (no batching). Values &gt;1 require at least that many consecutively-versioned, single-table, non-replication transactions to be available; batching is aborted if versions are non-consecutive, a replication transaction appears, or a schema change consumes a version. Increasing this value can improve publish throughput by grouping commits but may delay publishing while waiting for enough consecutive transactions.
- Introduced in: v3.2.0

##### lake_enable_batch_publish_version

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, PublishVersionDaemon batches ready transactions for the same Lake (shared-data) table/partition and publishes their versions together instead of issuing per-transaction publishes. In RunMode shared-data, the daemon calls getReadyPublishTransactionsBatch() and uses publishVersionForLakeTableBatch(...) to perform grouped publish operations (reducing RPCs and improving throughput). When disabled, the daemon falls back to per-transaction publishing via publishVersionForLakeTable(...). The implementation coordinates in-flight work using internal sets to avoid duplicate publishes when the switch is toggled and is affected by the thread pool sizing via `lake_publish_version_max_threads`.
- Introduced in: v3.2.0

##### lake_enable_tablet_creation_optimization

- Default: false
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, StarRocks optimizes tablet creation for cloud-native tables and materialized views in shared-data mode by creating a single shared tablet metadata for all tablets under a physical partition instead of distinct metadata per tablet. This reduces the number of tablet creation tasks and metadata/files produced during table creation, rollup, and schema-change jobs. The optimization is applied only for cloud-native tables/materialized views and is combined with `file_bundling` (the latter reuses the same optimization logic). Note: schema-change and rollup jobs explicitly disable the optimization for tables using `file_bundling` to avoid overwriting files with identical names. Enable cautiously — it changes the granularity of created tablet metadata and can affect how replica creation and file naming behave.
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### lake_use_combined_txn_log

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When this item is set to `true`, the system allows Lake tables to use the combined transaction log path for relevant transactions. Available for shared-data clusters only.
- Introduced in: v3.3.7, v3.4.0, v3.5.0

### Other

##### agent_task_resend_wait_time_ms

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The duration the FE must wait before it can resend an agent task. An agent task can be resent only when the gap between the task creation time and the current time exceeds the value of this parameter. This parameter is used to prevent repetitive sending of agent tasks.
- Introduced in: -

##### allow_system_reserved_names

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow users to create columns whose names are initiated with `__op` and `__row`. To enable this feature, set this parameter to `TRUE`. Please note that these name formats are reserved for special purposes in StarRocks and creating such columns may result in undefined behavior. Therefore this feature is disabled by default.
- Introduced in: v3.2.0

##### auth_token

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The token that is used for identity authentication within the StarRocks cluster to which the FE belongs. If this parameter is left unspecified, StarRocks generates a random token for the cluster at the time when the leader FE of the cluster is started for the first time.
- Introduced in: -

##### authentication_ldap_simple_bind_base_dn

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The base DN, which is the point from which the LDAP server starts to search for users' authentication information.
- Introduced in: -

##### authentication_ldap_simple_bind_root_dn

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The administrator DN used to search for users' authentication information.
- Introduced in: -

##### authentication_ldap_simple_bind_root_pwd

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The password of the administrator used to search for users' authentication information.
- Introduced in: -

##### authentication_ldap_simple_server_host

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The host on which the LDAP server runs.
- Introduced in: -

##### authentication_ldap_simple_server_port

- Default: 389
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The port of the LDAP server.
- Introduced in: -

##### authentication_ldap_simple_user_search_attr

- Default: uid
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The name of the attribute that identifies users in LDAP objects.
- Introduced in: -

##### backup_job_default_timeout_ms

- Default: 86400 * 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout duration of a backup job. If this value is exceeded, the backup job fails.
- Introduced in: -

##### enable_collect_tablet_num_in_show_proc_backend_disk_path

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the collection of tablet numbers for each disk in the `SHOW PROC /BACKENDS/{id}` command
- Introduced in: v4.0.1, v3.5.8

##### enable_colocate_restore

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Backup and Restore for Colocate Tables. `true` indicates enabling Backup and Restore for Colocate Tables and `false` indicates disabling it.
- Introduced in: v3.2.10, v3.3.3

##### enable_materialized_view_concurrent_prepare

- Default: true
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to prepare materialized view concurrently to improve performance.
- Introduced in: v3.4.4

##### enable_metric_calculator

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Specifies whether to enable the feature that is used to periodically collect metrics. Valid values: `TRUE` and `FALSE`. `TRUE` specifies to enable this feature, and `FALSE` specifies to disable this feature.
- Introduced in: -

##### enable_mv_post_image_reload_cache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to perform reload flag check after FE loaded an image. If the check is performed for a base materialized view, it is not needed for other materialized views that related to it.
- Introduced in: v3.5.0

##### enable_mv_query_context_cache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable query-level materialized view rewrite cache to improve query rewrite performance.
- Introduced in: v3.3

##### enable_mv_refresh_collect_profile

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable profile in refreshing materialized view by default for all materialized views.
- Introduced in: v3.3.0

##### enable_mv_refresh_extra_prefix_logging

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable prefixes with materialized view names in logs for better debug.
- Introduced in: v3.4.0

##### enable_mv_refresh_query_rewrite

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable rewrite query during materialized view refresh so that the query can use the rewritten mv directly rather than the base table to improve query performance.
- Introduced in: v3.3

##### enable_trace_historical_node

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow the system to trace the historical nodes. By setting this item to `true`, you can enable the Cache Sharing feature and allow the system to choose the right cache nodes during elastic scaling.
- Introduced in: v3.5.1

##### es_state_sync_interval_second

- Default: 10
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which the FE obtains Elasticsearch indexes and synchronizes the metadata of StarRocks external tables.
- Introduced in: -

##### hive_meta_cache_refresh_interval_s

- Default: 3600 * 2
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which the cached metadata of Hive external tables is updated.
- Introduced in: -

##### hive_meta_store_timeout_s

- Default: 10
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The amount of time after which a connection to a Hive metastore times out.
- Introduced in: -

##### jdbc_connection_idle_timeout_ms

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The maximum amount of time after which a connection for accessing a JDBC catalog times out. Timed-out connections are considered idle.
- Introduced in: -

##### jdbc_connection_pool_size

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum capacity of the JDBC connection pool for accessing JDBC catalogs.
- Introduced in: -

##### jdbc_meta_default_cache_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: The default value for whether the JDBC Catalog metadata cache is enabled. When set to True, newly created JDBC Catalogs will default to metadata caching enabled.
- Introduced in: -

##### jdbc_meta_default_cache_expire_sec

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The default expiration time for the JDBC Catalog metadata cache. When `jdbc_meta_default_cache_enable` is set to true, newly created JDBC Catalogs will default to setting the expiration time of the metadata cache.
- Introduced in: -

##### jdbc_minimum_idle_connections

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum number of idle connections in the JDBC connection pool for accessing JDBC catalogs.
- Introduced in: -

##### jwt_jwks_url

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The URL to the JSON Web Key Set (JWKS) service or the path to the public key local file under the `fe/conf` directory.
- Introduced in: v3.5.0

##### jwt_principal_field

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks.
- Introduced in: v3.5.0

##### jwt_required_audience

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.
- Introduced in: v3.5.0

##### jwt_required_issuer

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.
- Introduced in: v3.5.0

##### locale

- Default: zh_CN.UTF-8
- Type: String
- Unit: -
- Is mutable: No
- Description: The character set that is used by the FE.
- Introduced in: -

##### max_agent_task_threads_num

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads that are allowed in the agent task thread pool.
- Introduced in: -

##### max_download_task_per_be

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: In each RESTORE operation, the maximum number of download tasks StarRocks assigned to a BE node. When this item is set to less than or equal to 0, no limit is imposed on the task number.
- Introduced in: v3.1.0

##### max_mv_check_base_table_change_retry_times

- Default: 10
- Type: -
- Unit: -
- Is mutable: Yes
- Description: The maximum retry times for detecting base table change when refreshing materialized views.
- Introduced in: v3.3.0

##### max_mv_refresh_failure_retry_times

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum retry times when materialized view fails to refresh.
- Introduced in: v3.3.0

##### max_mv_refresh_try_lock_failure_retry_times

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum retry times of try lock when materialized view fails to refresh.
- Introduced in: v3.3.0

##### max_small_file_number

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of small files that can be stored on an FE directory.
- Introduced in: -

##### max_small_file_size_bytes

- Default: 1024 * 1024
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum size of a small file.
- Introduced in: -

##### max_upload_task_per_be

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: In each BACKUP operation, the maximum number of upload tasks StarRocks assigned to a BE node. When this item is set to less than or equal to 0, no limit is imposed on the task number.
- Introduced in: v3.1.0

##### mv_create_partition_batch_interval_ms

- Default: 1000
- Type: Int
- Unit: ms
- Is mutable: Yes
- Description: During materialized view refresh, if multiple partitions need to be created in bulk, the system divides them into batches of 64 partitions each. To reduce the risk of failures caused by frequent partition creation, a default interval (in milliseconds) is set between each batch to control the creation frequency.
- Introduced in: v3.3

##### mv_plan_cache_max_size

- Default: 1000
- Type: Long
- Unit:
- Is mutable: Yes
- Description: The maximum size of materialized view plan cache (which is used for materialized view rewrite). If there are many materialized views used for transparent query rewrite, you may increase this value.
- Introduced in: v3.2

##### mv_plan_cache_thread_pool_size

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The default thread pool size of materialized view plan cache (which is used for materialized view rewrite).
- Introduced in: v3.2

##### mv_refresh_default_planner_optimize_timeout

- Default: 30000
- Type: -
- Unit: -
- Is mutable: Yes
- Description: The default timeout for the planning phase of the optimizer when refresh materialized views.
- Introduced in: v3.3.0

##### mv_refresh_fail_on_filter_data

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Mv refresh fails if there is filtered data in refreshing, true by default, otherwise return success by ignoring the filtered data.
- Introduced in: -

##### mv_refresh_try_lock_timeout_ms

- Default: 30000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The default try lock timeout for materialized view refresh to try the DB lock of its base table/materialized view.
- Introduced in: v3.3.0

##### oauth2_auth_server_url

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The authorization URL. The URL to which the users’ browser will be redirected in order to begin the OAuth 2.0 authorization process.
- Introduced in: v3.5.0

##### oauth2_client_id

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The public identifier of the StarRocks client.
- Introduced in: v3.5.0

##### oauth2_client_secret

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The secret used to authorize StarRocks client with the authorization server.
- Introduced in: v3.5.0

##### oauth2_jwks_url

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The URL to the JSON Web Key Set (JWKS) service or the path to the local file under the `conf` directory.
- Introduced in: v3.5.0

##### oauth2_principal_field

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks.
- Introduced in: v3.5.0

##### oauth2_redirect_url

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The URL to which the users’ browser will be redirected after the OAuth 2.0 authentication succeeds. The authorization code will be sent to this URL. In most cases, it need to be configured as `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`.
- Introduced in: v3.5.0

##### oauth2_required_audience

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.
- Introduced in: v3.5.0

##### oauth2_required_issuer

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.
- Introduced in: v3.5.0

##### oauth2_token_server_url

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The URL of the endpoint on the authorization server from which StarRocks obtains the access token.
- Introduced in: v3.5.0

##### plugin_dir

- Default: System.getenv("STARROCKS_HOME") + "/plugins"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores plugin installation packages.
- Introduced in: -

##### plugin_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether plugins can be installed on FEs. Plugins can be installed or uninstalled only on the Leader FE.
- Introduced in: -

##### query_detail_explain_level

- Default: COSTS
- Type: String
- Unit: -
- Is mutable: true
- Description: The detail level of query plan returned by the EXPLAIN statement. Valid values: COSTS, NORMAL, VERBOSE.
- Introduced in: v3.2.12, v3.3.5

##### replication_interval_ms

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum time interval at which the replication tasks are scheduled.
- Introduced in: v3.3.5

##### replication_max_parallel_data_size_mb

- Default: 1048576
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description: The maximum size of data allowed for concurrent synchronization.
- Introduced in: v3.3.5

##### replication_max_parallel_replica_count

- Default: 10240
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablet replicas allowed for concurrent synchronization.
- Introduced in: v3.3.5

##### replication_max_parallel_table_count

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent data synchronization tasks allowed. StarRocks creates one synchronization task for each table.
- Introduced in: v3.3.5

##### replication_transaction_timeout_sec

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for synchronization tasks.
- Introduced in: v3.3.5

##### small_file_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/small_files"
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of small files.
- Introduced in: -

##### task_runs_max_history_number

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of task run records to retain in memory and to use as a default LIMIT when querying archived task-run history. When `enable_task_history_archive` is false, this value bounds in-memory history: Force GC trims older entries so only the newest `task_runs_max_history_number` remain. When archive history is queried (and no explicit LIMIT is provided), `TaskRunHistoryTable.lookup` uses `"ORDER BY create_time DESC LIMIT <value>"` if this value is greater than 0. Note: setting this to 0 disables the query-side LIMIT (no cap) but will cause in-memory history to be truncated to zero (unless archiving is enabled).
- Introduced in: v3.2.0

##### tmp_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores temporary files such as files generated during backup and restore procedures. After these procedures finish, the generated temporary files are deleted.
- Introduced in: -

##### transform_type_prefer_string_for_varchar

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to prefer string type for fixed length varchar columns in materialized view creation and CTAS operations.
- Introduced in: v4.0.0



<EditionSpecificFEItem />
