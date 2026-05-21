---
displayed_sidebar: docs
sidebar_label: "Logging, Server, and Metadata"
---

# FE Configuration - Logging, Server, and Metadata

import FEConfigMethod from '../../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

<FEConfigMethod />

## View FE configuration items

After your FE is started, you can run the ADMIN SHOW FRONTEND CONFIG command on your MySQL client to check the parameter configurations. If you want to query the configuration of a specific parameter, run the following command:

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

For detailed description of the returned fields, see [`ADMIN SHOW CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md).

:::note
You must have administrator privileges to run cluster administration-related commands.
:::

## Configure FE parameters

### Configure FE dynamic parameters

You can configure or modify the settings of FE dynamic parameters using [`ADMIN SET FRONTEND CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md).

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### Configure FE static parameters

<StaticFEConfigNote />

---

This topic introduces the following types of FE configurations:
- [Logging](#logging)
- [Server](#server)
- [Metadata and Cluster Management](#metadata-and-cluster-management)

## Logging

### `audit_log_delete_age`

- Default: 30d
- Type: String
- Unit: -
- Is mutable: No
- Description: The retention period of audit log files. The default value `30d` specifies that each audit log file can be retained for 30 days. StarRocks checks each audit log file and deletes those that were generated 30 days ago.
- Introduced in: -

### `audit_log_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores audit log files.
- Introduced in: -

### `audit_log_enable_compress`

- Default: false
- Type: Boolean
- Unit: N/A
- Is mutable: No
- Description: When true, the generated Log4j2 configuration appends a ".gz" postfix to rotated audit log filenames (fe.audit.log.*) so that Log4j2 will produce compressed (.gz) archived audit log files on rollover. The setting is read during FE startup in Log4jConfig.initLogging and is applied to the RollingFile appender for audit logs; it only affects rotated/archived files, not the active audit log. Because the value is initialized at startup, changing it requires restarting the FE to take effect. Use alongside audit log rotation settings (`audit_log_dir`, `audit_log_roll_interval`, `audit_roll_maxsize`, `audit_log_roll_num`).
- Introduced in: 3.2.12

### `audit_log_json_format`

- Default: false
- Type: Boolean
- Unit: N/A
- Is mutable: Yes
- Description: When true, FE audit events are emitted as structured JSON (Jackson ObjectMapper serializing a Map of annotated AuditEvent fields) instead of the default pipe-separated "key=value" string. The setting affects all built-in audit sinks handled by AuditLogBuilder: connection audit, query audit, big-query audit (big-query threshold fields are added to the JSON when the event qualifies), and slow-audit output. Fields annotated for big-query thresholds and the "features" field are treated specially (excluded from normal audit entries; included in big-query or feature logs as applicable). Enable this to make logs machine-parsable for log collectors or SIEMs; note it changes the log format and may require updating any existing parsers that expect the legacy pipe-separated format.
- Introduced in: 3.2.7

### `audit_log_modules`

- Default: `slow_query`, query
- Type: String[]
- Unit: -
- Is mutable: No
- Description: The modules for which StarRocks generates audit log entries. By default, StarRocks generates audit logs for the `slow_query` module and the `query` module. The `connection` module is supported from v3.0. Separate the module names with a comma (,) and a space.
- Introduced in: -

### `audit_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: The time interval at which StarRocks rotates audit log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of audit log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of audit log files.
- Introduced in: -

### `audit_log_roll_num`

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of audit log files that can be retained within each retention period specified by the `audit_log_roll_interval` parameter.
- Introduced in: -

### `audit_stmt_before_execute`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls whether FE emits a `BEFORE_QUERY` audit event before each statement executes. When enabled, ConnectProcessor writes one audit record before statement execution and still writes the normal `AFTER_QUERY` audit record after the statement finishes. For multi-statement requests, this happens per executed statement: statements executed before a failure each emit a before/after pair, the failed statement also emits both records, and statements after the failure emit nothing because they are not executed. Parse failures are unchanged and still only produce the existing failed after-audit for the original SQL text. This is an FE-wide switch, so changing it affects all sessions on the FE.
- Introduced in: v4.1

### `bdbje_log_level`

- Default: INFO
- Type: String
- Unit: -
- Is mutable: No
- Description: Controls the logging level used by Berkeley DB Java Edition (BDB JE) in StarRocks. During BDB environment initialization BDBEnvironment.initConfigs() applies this value to the Java logger for the `com.sleepycat.je` package and to the BDB JE environment file logging level (`EnvironmentConfig.FILE_LOGGING_LEVEL`). Accepts standard java.util.logging.Level names such as SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST, ALL, OFF. Setting to ALL enables all log messages. Increasing verbosity will raise log volume and may impact disk I/O and performance; the value is read when the BDB environment is initialized, so it takes effect only after environment (re)initialization.
- Introduced in: v3.2.0

### `big_query_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: Controls how long FE big query log files (`fe.big_query.log.*`) are retained before automatic deletion. The value is passed to Log4j's deletion policy as the IfLastModified age — any rotated big query log whose last-modified time is older than this value will be removed. Supports suffixes include `d` (day), `h` (hour), `m` (minute), and `s` (second). Example: `7d` (7 days), `10h` (10 hours), `60m` (60 minutes), and `120s` (120 seconds). This item works together with `big_query_log_roll_interval` and `big_query_log_roll_num` to determine which files are kept or purged.
- Introduced in: v3.2.0

### `big_query_log_dir`

- Default: `Config.STARROCKS_HOME_DIR + "/log"`
- Type: String
- Unit: -
- Is mutable: No
- Description: Directory where the FE writes big query dump logs (`fe.big_query.log.*`). The Log4j configuration uses this path to create a RollingFile appender for `fe.big_query.log` and its rotated files. Rotation and retention are governed by `big_query_log_roll_interval` (time-based suffix), `log_roll_size_mb` (size trigger), `big_query_log_roll_num` (max files), and `big_query_log_delete_age` (age-based deletion). Big query records are logged for queries that exceed user-defined thresholds such as `big_query_log_cpu_second_threshold`, `big_query_log_scan_rows_threshold`, or `big_query_log_scan_bytes_threshold`. Use `big_query_log_modules` to control which modules log to this file.
- Introduced in: v3.2.0

### `big_query_log_modules`

- Default: `{"query"}`
- Type: String[]
- Unit: -
- Is mutable: No
- Description: List of module name suffixes that enable per-module big query logging. Typical values are logical component names. For example, the default `query` produces `big_query.query`.
- Introduced in: v3.2.0

### `big_query_log_roll_interval`

- Default: `"DAY"`
- Type: String
- Unit: -
- Is mutable: No
- Description: Specifies the time interval used to construct the date component of the rolling file name for the `big_query` log appender. Valid values (case-insensitive) are `DAY` (default) and `HOUR`. `DAY` produces a daily pattern (`"%d{yyyyMMdd}"`) and `HOUR` produces an hourly pattern (`"%d{yyyyMMddHH}"`). The value is combined with size-based rollover (`big_query_roll_maxsize`) and index-based rollover (`big_query_log_roll_num`) to form the RollingFile filePattern. An invalid value causes log configuration generation to fail (IOException) and may prevent log initialization or reconfiguration. Use alongside `big_query_log_dir`, `big_query_roll_maxsize`, `big_query_log_roll_num`, and `big_query_log_delete_age`.
- Introduced in: v3.2.0

### `big_query_log_roll_num`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: Maximum number of rotated FE big query log files to retain per `big_query_log_roll_interval`. This value is bound to the RollingFile appender's DefaultRolloverStrategy `max` attribute for `fe.big_query.log`; when logs roll (by time or by `log_roll_size_mb`), StarRocks keeps up to `big_query_log_roll_num` indexed files (filePattern uses a time suffix plus index). Files older than this count may be removed by rollover, and `big_query_log_delete_age` can additionally delete files by last-modified age.
- Introduced in: v3.2.0

### `dump_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: The retention period of dump log files. The default value `7d` specifies that each dump log file can be retained for 7 days. StarRocks checks each dump log file and deletes those that were generated 7 days ago.
- Introduced in: -

### `dump_log_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores dump log files.
- Introduced in: -

### `dump_log_modules`

- Default: query
- Type: String[]
- Unit: -
- Is mutable: No
- Description: The modules for which StarRocks generates dump log entries. By default, StarRocks generates dump logs for the query module. Separate the module names with a comma (,) and a space.
- Introduced in: -

### `dump_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: The time interval at which StarRocks rotates dump log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of dump log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of dump log files.
- Introduced in: -

### `dump_log_roll_num`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of dump log files that can be retained within each retention period specified by the `dump_log_roll_interval` parameter.
- Introduced in: -

### `edit_log_write_slow_log_threshold_ms`

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Threshold (in ms) used by JournalWriter to detect and log slow edit-log batch writes. After a batch commit, if the batch duration exceeds this value, JournalWriter emits a WARN with batch size, duration and current journal queue size (rate-limited to once every ~2s). This setting only controls logging/alerts for potential IO or replication latency on the FE leader; it does not change commit or roll behavior (see `edit_log_roll_num` and commit-related settings). Metric updates still occur regardless of this threshold.
- Introduced in: v3.2.3

### `enable_audit_sql`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the FE audit subsystem records the SQL text of statements into FE audit logs (`fe.audit.log`) processed by ConnectProcessor. The stored statement respects other controls: encrypted statements are redacted (`AuditEncryptionChecker`), sensitive credentials may be redacted or desensitized if `enable_sql_desensitize_in_log` is set, and digest recording is controlled by `enable_sql_digest`. When it is set to `false`, ConnectProcessor replaces the statement text with "?" in audit events — other audit fields (user, host, duration, status, slow-query detection via `qe_slow_log_ms`, and metrics) are still recorded. Enabling SQL audit increases forensic and troubleshooting visibility but may expose sensitive SQL content and increase log volume and I/O; disabling it improves privacy at the cost of losing full-statement visibility in audit logs.
- Introduced in: -

### `enable_profile_log`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable profile logging. When this feature is enabled, the FE writes per-query profile logs (the serialized `queryDetail` JSON produced by `ProfileManager`) to the profile log sink. This logging is performed only if `enable_collect_query_detail_info` is also enabled; when `enable_profile_log_compress` is enabled, the JSON may be gzipped before logging. Profile log files are managed by `profile_log_dir`, `profile_log_roll_num`, `profile_log_roll_interval` and rotated/deleted according to `profile_log_delete_age` (supports formats like `7d`, `10h`, `60m`, `120s`). Disabling this feature stops writing profile logs (reducing disk I/O, compression CPU and storage usage). Which queries are logged can be further filtered by `profile_log_latency_threshold_ms`.
- Introduced in: v3.2.5

### `enable_qe_slow_log`

- Default: true
- Type: Boolean
- Unit: N/A
- Is mutable: Yes
- Description: When enabled, the FE builtin audit plugin (AuditLogBuilder) will write query events whose measured execution time ("Time" field) exceeds the threshold configured by `qe_slow_log_ms` into the slow-query audit log (AuditLog.getSlowAudit). If disabled, those slow-query entries are suppressed (regular query and connection audit logs are unaffected). The slow-audit entries follow the global `audit_log_json_format` setting (JSON vs. plain string). Use this flag to control generation of slow-query audit volume independently of regular audit logging; turning it off may reduce log I/O when `qe_slow_log_ms` is low or workloads produce many long-running queries.
- Introduced in: 3.2.11

### `enable_sql_desensitize_in_log`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the system replaces or hides sensitive SQL content before it is written to logs, query-detail records, and query profiles. Code paths that honor this configuration include ConnectProcessor.formatStmt (audit logs), StmtExecutor.addRunningQueryDetail (query details), SimpleExecutor.formatSQL (internal executor logs), and StmtExecutor.buildTopLevelProfile / processProfileAsync (the `Sql Statement` and `ExplainPlan` info-strings stored in a profile's `Summary` section). With the feature enabled, invalid SQLs may be replaced with a fixed desensitized message, credentials (user/password) are hidden, and the SQL formatter is required to produce a sanitized representation (it can also enable digest-style output). For the `ExplainPlan` field added by the `enable_explain_in_profile` session variable, this config also forces literal-digest rendering of the embedded `EXPLAIN COSTS` text, so the profile does not leak the literals that the persisted `Sql Statement` would have hidden. This reduces leakage of sensitive literals and credentials in audit/internal logs and profiles, but also means logs, query details, and profiles no longer contain the original full SQL text (which can affect replay or debugging).
- Introduced in: -

### `internal_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: Specifies the retention period for FE internal log files (written to `internal_log_dir`). The value is a duration string. Supported suffixes: `d` (day), `h` (hour), `m` (minute), `s` (second). Examples: `7d` (7 days), `10h` (10 hours), `60m` (60 minutes), `120s` (120 seconds). This item is substituted into the log4j configuration as the `<IfLastModified age="..."/>` predicate used by the RollingFile Delete policy. Files whose last-modified time is earlier than this duration will be removed during log rollover. Increase this value to free disk space sooner, or decrease it to retain internal materialized view or statistics logs longer.
- Introduced in: v3.2.4

### `internal_log_dir`

- Default: `Config.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: Directory used by the FE logging subsystem for storing internal logs (`fe.internal.log`). This configuration is substituted into the Log4j configuration and determines where the InternalFile appender writes internal/materialized view/statistics logs and where per-module loggers under `internal.<module>` place their files. Ensure the directory exists, is writable, and has sufficient disk space. Log rotation and retention for files in this directory are controlled by `log_roll_size_mb`, `internal_log_roll_num`, `internal_log_delete_age`, and `internal_log_roll_interval`. If `sys_log_to_console` is enabled, internal logs may be written to console instead of this directory.
- Introduced in: v3.2.4

### `internal_log_json_format`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When this item is set to `true`, internal statistic/audit entries are written as compact JSON objects to the statistic audit logger. The JSON contains keys "executeType" (InternalType: QUERY or DML), "queryId", "sql", and "time" (elapsed milliseconds). When it is set to `false`, the same information is logged as a single formatted text line ("statistic execute: ... | QueryId: [...] | SQL: ..."). Enabling JSON improves machine parsing and integration with log processors but also causes raw SQL text to be included in logs, which may expose sensitive information and increase log size.
- Introduced in: -

### `internal_log_modules`

- Default: `{"base", "statistic"}`
- Type: String[]
- Unit: -
- Is mutable: No
- Description: A list of module identifiers that will receive dedicated internal logging. For each entry X, Log4j creates a logger named `internal.<X>` with level INFO and additivity="false". Those loggers are routed to the internal appender (written to `fe.internal.log`) or to console when `sys_log_to_console` is enabled. Use short names or package fragments as needed — the exact logger name becomes `internal.` + the configured string. Internal log file rotation and retention follow `internal_log_dir`, `internal_log_roll_num`, `internal_log_delete_age`, `internal_log_roll_interval`, and `log_roll_size_mb`. Adding a module causes its runtime messages to be separated into the internal logger stream for easier debugging and audit.
- Introduced in: v3.2.4

### `internal_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: Controls the time-based roll interval for the FE internal log appender. Accepted values (case-insensitive) are `HOUR` and `DAY`. `HOUR` produces an hourly file pattern (`"%d{yyyyMMddHH}"`) and `DAY` produces a daily file pattern (`"%d{yyyyMMdd}"`), which are used by the RollingFile TimeBasedTriggeringPolicy to name rotated `fe.internal.log` files. An invalid value causes initialization to fail (an IOException is thrown when building the active Log4j configuration). Roll behavior also depends on related settings such as `internal_log_dir`, `internal_roll_maxsize`, `internal_log_roll_num`, and `internal_log_delete_age`.
- Introduced in: v3.2.4

### `internal_log_roll_num`

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: Maximum number of rolled internal FE log files to retain for the internal appender (`fe.internal.log`). This value is used as the Log4j DefaultRolloverStrategy `max` attribute; when rollovers occur, StarRocks keeps up to `internal_log_roll_num` archived files and removes older ones (also governed by `internal_log_delete_age`). A lower value reduces disk usage but shortens log history; a higher value preserves more historical internal logs. This item works together with `internal_log_dir`, `internal_log_roll_interval`, and `internal_roll_maxsize`.
- Introduced in: v3.2.4

### `log_cleaner_audit_log_min_retention_days`

- Default: 3
- Type: Int
- Unit: Days
- Is mutable: Yes
- Description: Minimum retention days for audit log files. Audit log files newer than this will not be deleted even if disk usage is high. This ensures that audit logs are preserved for compliance and troubleshooting purposes.
- Introduced in: -

### `log_cleaner_check_interval_second`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval in seconds to check disk usage and clean logs. The cleaner periodically checks each log directory's disk usage and triggers cleaning when necessary. Default is 300 seconds (5 minutes).
- Introduced in: -

### `log_cleaner_disk_usage_target`

- Default: 60
- Type: Int
- Unit: Percentage
- Is mutable: Yes
- Description: Target disk usage (percentage) after log cleaning. Log cleaning will continue until disk usage drops below this threshold. The cleaner deletes the oldest log files one by one until the target is reached.
- Introduced in: -

### `log_cleaner_disk_usage_threshold`

- Default: 80
- Type: Int
- Unit: Percentage
- Is mutable: Yes
- Description: Disk usage threshold (percentage) to trigger log cleaning. When disk usage exceeds this threshold, log cleaning will start. The cleaner checks each configured log directory independently and processes directories that exceed this threshold.
- Introduced in: -

### `log_cleaner_disk_util_based_enable`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Enable automatic log cleaning based on disk usage. When enabled, logs will be cleaned when disk usage exceeds the threshold. The log cleaner runs as a background daemon on the FE node and helps prevent disk space exhaustion from log file accumulation.
- Introduced in: -

### `log_plan_cancelled_by_crash_be`

- Default: true
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the query execution plan logging when a query is cancelled due to BE crash or an RPC exception. When this feature is enabled, StarRocks logs the query execution plan (at `TExplainLevel.COSTS`) as a WARN entry when a query is cancelled due to BE crash or an `RpcException`. The log entry includes QueryId, SQL and the COSTS plan; in the ExecuteExceptionHandler path, the exception stacktrace is also logged. The logging is skipped when `enable_collect_query_detail_info` is enabled (the plan is then stored in the query detail) — in code paths, the check is performed by verifying the query detail is null. Note that, in ExecuteExceptionHandler, the plan is logged only on the first retry (`retryTime == 0`). Enabling this may increase log volume because full COSTS plans can be large.
- Introduced in: v3.2.0

### `log_register_and_unregister_query_id`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow FE to log query registration and deregistration messages (e.g., `"register query id = {}"` and `"deregister query id = {}"`) from QeProcessorImpl. The log is emitted only when the query has a non-null ConnectContext and either the command is not `COM_STMT_EXECUTE` or the session variable `isAuditExecuteStmt()` is true. Because these messages are written for every query lifecycle event, enabling this feature can produce high log volume and become a throughput bottleneck in high concurrency environments. Enable it for debugging or auditing; and disable it to reduce logging overhead and improve performance.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### `log_roll_size_mb`

- Default: 1024
- Type: Int
- Unit: MB
- Is mutable: No
- Description: The maximum size of a system log file or an audit log file.
- Introduced in: -

### `proc_profile_file_retained_days`

- Default: 1
- Type: Int
- Unit: Days
- Is mutable: Yes
- Description: Number of days to retain process profiling files (CPU and memory) generated under `sys_log_dir/proc_profile`. The ProcProfileCollector computes a cutoff by subtracting `proc_profile_file_retained_days` days from the current time (formatted as yyyyMMdd-HHmmss) and deletes profile files whose timestamp portion is lexicographically earlier than that cutoff (that is, `timePart.compareTo(timeToDelete) < 0`). File deletion also respects the size-based cutoff controlled by `proc_profile_file_retained_size_bytes`. Profile files use the prefixes `cpu-profile-` and `mem-profile-` and are compressed after collection.
- Introduced in: v3.2.12

### `proc_profile_file_retained_size_bytes`

- Default: 2L * 1024 * 1024 * 1024 (2147483648)
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: Maximum total bytes of collected CPU and memory profile files (files named with prefixes `cpu-profile-` and `mem-profile-`) to keep under the profile directory. When the sum of valid profile files exceeds `proc_profile_file_retained_size_bytes`, the collector deletes the oldest profile files until the remaining total size is less than or equal to `proc_profile_file_retained_size_bytes`. Files older than `proc_profile_file_retained_days` are also removed regardless of size. This setting controls disk usage for profile archives and interacts with `proc_profile_file_retained_days` to determine deletion order and retention.
- Introduced in: v3.2.12

### `profile_log_delete_age`

- Default: 1d
- Type: String
- Unit: -
- Is mutable: No
- Description: Controls how long FE profile log files are retained before they are eligible for deletion. The value is injected into Log4j's `<IfLastModified age="..."/>` policy (via `Log4jConfig`) and is applied together with rotation settings such as `profile_log_roll_interval` and `profile_log_roll_num`. Supported suffixes: `d` (day), `h` (hour), `m` (minute), `s` (second). For example: `7d` (7 days), `10h` (10 hours), `60m` (60 minutes), `120s` (120 seconds).
- Introduced in: v3.2.5

### `profile_log_dir`

- Default: `Config.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: Directory where FE profile logs are written. Log4jConfig uses this value to place profile-related appenders (creates files like `fe.profile.log` and `fe.features.log` under this directory). Rotation and retention for these files are governed by `profile_log_roll_size_mb`, `profile_log_roll_num` and `profile_log_delete_age`; the timestamp suffix format is controlled by `profile_log_roll_interval` (supports DAY or HOUR). Because the default directory is under `STARROCKS_HOME_DIR`, ensure the FE process has write and rotation/delete permissions on this directory.
- Introduced in: v3.2.5

### `profile_log_latency_threshold_ms`

- Default: 0
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: Minimum query latency (in milliseconds) for a profile to be written to `fe.profile.log`. Only queries whose execution time is greater than or equal to this value are logged. Set to 0 to log all profiles (no threshold). Use a positive value to reduce log volume by logging only slower queries.
- Introduced in: -


### `profile_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: Controls the time granularity used to generate the date part of profile log filenames. Valid values (case-insensitive) are `HOUR` and `DAY`. `HOUR` produces a pattern of `"%d{yyyyMMddHH}"` (hourly time bucket) and `DAY` produces `"%d{yyyyMMdd}"` (daily time bucket). This value is used when computing `profile_file_pattern` in the Log4j configuration and only affects the time-based component of rollover file names; size-based rollover is still controlled by `profile_log_roll_size_mb` and retention by `profile_log_roll_num` / `profile_log_delete_age`. Invalid values cause an IOException during logging initialization (error message: `"profile_log_roll_interval config error: <value>"`). Choose `HOUR` for high-volume profiling to limit per-file size per hour, or `DAY` for daily aggregation.
- Introduced in: v3.2.5

### `profile_log_roll_num`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: Specifies the maximum number of rotated profile log files retained by Log4j's DefaultRolloverStrategy for the profile logger. This value is injected into the logging XML as `${profile_log_roll_num}` (e.g. `<DefaultRolloverStrategy max="${profile_log_roll_num}" fileIndex="min">`). Rotations are triggered by `profile_log_roll_size_mb` or `profile_log_roll_interval`; when rotation occurs, Log4j keeps at most these indexed files and older index files become eligible for removal. Actual retention on disk is also affected by `profile_log_delete_age` and the `profile_log_dir` location. Lower values reduce disk usage but limit retained history; higher values preserve more historical profile logs.
- Introduced in: v3.2.5

### `profile_log_roll_size_mb`

- Default: 1024
- Type: Int
- Unit: MB
- Is mutable: No
- Description: Sets the size threshold (in megabytes) that triggers a size-based rollover of the FE profile log file. This value is used by the Log4j RollingFile SizeBasedTriggeringPolicy for the `ProfileFile` appender; when a profile log exceeds `profile_log_roll_size_mb` it will be rotated. Rotation can also occur by time when `profile_log_roll_interval` is reached — either condition will trigger rollover. Combined with `profile_log_roll_num` and `profile_log_delete_age`, this item controls how many historical profile files are retained and when old files are deleted. Compression of rotated files is controlled by `enable_profile_log_compress`.
- Introduced in: v3.2.5

### `qe_slow_log_ms`

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The threshold used to determine whether a query is a slow query. If the response time of a query exceeds this threshold, it is recorded as a slow query in **fe.audit.log**.
- Introduced in: -

### `slow_lock_log_every_ms`

- Default: 3000L
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: Minimum interval (in ms) to wait before emitting another "slow lock" warning for the same SlowLockLogStats instance. LockUtils checks this value after a lock wait exceeds `slow_lock_threshold_ms` and will suppress additional warnings until `slow_lock_log_every_ms` milliseconds have passed since the last logged slow-lock event. Use a larger value to reduce log volume during prolonged contention or a smaller value to get more frequent diagnostics. Changes take effect at runtime for subsequent checks.
- Introduced in: v3.2.0

### `slow_lock_print_stack`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow LockManager to include the owning thread's full stack trace in the JSON payload of slow-lock warnings emitted by `logSlowLockTrace` (the "stack" array is populated via `LogUtil.getStackTraceToJsonArray` with `start=0` and `max=Short.MAX_VALUE`). This configuration controls only the extra stack information for lock owners shown when a lock acquisition exceeds the threshold configured by `slow_lock_threshold_ms`. Enabling this feature helps debugging by giving precise thread stacks that hold the lock; disabling it reduces log volume and CPU/memory overhead caused by capturing and serializing stack traces in high concurrency environments.
- Introduced in: v3.3.16, v3.4.5, v3.5.1

### `slow_lock_threshold_ms`

- Default: 3000L
- Type: long
- Unit: Milliseconds
- Is mutable: Yes
- Description: Threshold (in ms) used to classify a lock operation or a held lock as "slow". When the elapsed wait or hold time for a lock exceeds this value, StarRocks will (depending on context) emit diagnostic logs, include stack traces or waiter/owner info, and—in LockManager—start deadlock detection after this delay. It's used by LockUtils (slow-lock logging), QueryableReentrantReadWriteLock (filtering slow readers), LockManager (deadlock-detection delay and slow-lock trace), LockChecker (periodic slow-lock detection), and other callers (e.g., DiskAndTabletLoadReBalancer logging). Lowering the value increases sensitivity and logging/diagnostic overhead; setting it to 0 or negative disables the initial wait-based deadlock-detection delay behavior. Tune together with `slow_lock_log_every_ms`, `slow_lock_print_stack`, and `slow_lock_stack_trace_reserve_levels`.
- Introduced in: 3.2.0

### `sys_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: The retention period of system log files. The default value `7d` specifies that each system log file can be retained for 7 days. StarRocks checks each system log file and deletes those that were generated 7 days ago.
- Introduced in: -

### `sys_log_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores system log files.
- Introduced in: -

### `sys_log_enable_compress`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the system appends a ".gz" postfix to rotated system log filenames so Log4j will produce gzip-compressed rotated FE system logs (for example, fe.log.*). This value is read during Log4j configuration generation (Log4jConfig.initLogging / generateActiveLog4jXmlConfig) and controls the `sys_file_postfix` property used in the RollingFile filePattern. Enabling this feature reduces disk usage for retained logs but increases CPU and I/O during rollovers and changes log filenames, so that tools or scripts that read logs must be able to handle .gz files. Note that audit logs use a separate configuration for compression, that is, `audit_log_enable_compress`.
- Introduced in: v3.2.12

### `sys_log_format`

- Default: "plaintext"
- Type: String
- Unit: -
- Is mutable: No
- Description: Selects the Log4j layout used for FE logs. Valid values: `"plaintext"` (Default) and `"json"`. The values are case-insensitive. `"plaintext"` configures PatternLayout with human-readable timestamps, level, thread, class.method:line and stack traces for WARN/ERROR. `"json"` configures JsonTemplateLayout and emits structured JSON events (UTC timestamps, level, thread id/name, source file/method/line, message, exception stackTrace) suitable for log aggregators (ELK, Splunk). JSON output abides by `sys_log_json_max_string_length` and `sys_log_json_profile_max_string_length` for maximum string lengths.
- Introduced in: v3.2.10

### `sys_log_json_max_string_length`

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the JsonTemplateLayout "maxStringLength" value used for the JSON-formatted system logs. When `sys_log_format` is set to `"json"`, string-valued fields (for example "message" and stringified exception stack traces) are truncated if their length exceeds this limit. The value is injected into the generated Log4j XML in `Log4jConfig.generateActiveLog4jXmlConfig()`, and is applied to default, warning, audit, dump and bigquery layouts. The profile layout uses a separate configuration (`sys_log_json_profile_max_string_length`). Lowering this value reduces log size but can truncate useful information.
- Introduced in: 3.2.11

### `sys_log_json_profile_max_string_length`

- Default: 104857600 (100 MB)
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the maxStringLength of JsonTemplateLayout for profile (and related feature) log appenders when `sys_log_format` is "json". String field values in JSON-formatted profile logs will be truncated to this byte length; non-string fields are unaffected. This item is applied in Log4jConfig `JsonTemplateLayout maxStringLength` and is ignored when `plaintext` logging is used. Keep the value large enough for full messages you need, but note larger values increase log size and I/O.
- Introduced in: v3.2.11

### `sys_log_level`

- Default: INFO
- Type: String
- Unit: -
- Is mutable: No
- Description: The severity levels into which system log entries are classified. Valid values: `INFO`, `WARN`, `ERROR`, and `FATAL`.
- Introduced in: -

### `sys_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: The time interval at which StarRocks rotates system log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of system log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of system log files.
- Introduced in: -

### `sys_log_roll_num`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of system log files that can be retained within each retention period specified by the `sys_log_roll_interval` parameter.
- Introduced in: -

### `sys_log_to_console`

- Default: false (unless the environment variable `SYS_LOG_TO_CONSOLE` is set to "1")
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, the system configures Log4j to send all logs to the console (ConsoleErr appender) instead of the file-based appenders. This value is read when generating the active Log4j XML configuration (which affects the root logger and per-module logger appender selection). Its value is captured from the `SYS_LOG_TO_CONSOLE` environment variable at process startup. Changing it at runtime has no effect. This configuration is commonly used in containerized or CI environments where stdout/stderr log collection is preferred over writing log files.
- Introduced in: v3.2.0

### `sys_log_verbose_modules`

- Default: Empty string
- Type: String[]
- Unit: -
- Is mutable: No
- Description: The modules for which StarRocks generates system logs. If this parameter is set to `org.apache.starrocks.catalog`, StarRocks generates system logs only for the catalog module. Separate the module names with a comma (,) and a space.
- Introduced in: -

### `sys_log_warn_modules`

- Default: {}
- Type: String[]
- Unit: -
- Is mutable: No
- Description: A list of logger names or package prefixes that the system will configure at startup as WARN-level loggers and route to the warning appender (SysWF) — the `fe.warn.log` file. Entries are inserted into the generated Log4j configuration (alongside builtin warn modules such as org.apache.kafka, org.apache.hudi, and org.apache.hadoop.io.compress) and produce logger elements like `<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>`. Fully-qualified package and class prefixes (for example, "com.example.lib") are recommended to suppress noisy INFO/DEBUG output into the regular log and to allow warnings to be captured separately.
- Introduced in: v3.2.13

## Server

<EditionSpecificFEItem />

### `brpc_idle_wait_max_time`

- Default: 10000
- Type: Int
- Unit: ms
- Is mutable: No
- Description: The maximum length of time for which bRPC clients wait as in the idle state.
- Introduced in: -

### `brpc_inner_reuse_pool`

- Default: true
- Type: boolean
- Unit: -
- Is mutable: No
- Description: Controls whether the underlying BRPC client uses an internal shared reuse pool for connections/channels. StarRocks reads `brpc_inner_reuse_pool` in BrpcProxy when constructing RpcClientOptions (via `rpcOptions.setInnerResuePool(...)`). When enabled (true) the RPC client reuses internal pools to reduce per-call connection creation, lowering connection churn, memory and file-descriptor usage for FE-to-BE / LakeService RPCs. When disabled (false) the client may create more isolated pools (increasing concurrency isolation at the cost of higher resource usage). Changing this value requires restarting the process to take effect.
- Introduced in: v3.3.11, v3.4.1, v3.5.0

### `brpc_min_evictable_idle_time_ms`

- Default: 120000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Time in milliseconds that an idle BRPC connection must remain in the connection pool before it becomes eligible for eviction. Applied to the RpcClientOptions used by `BrpcProxy` (via RpcClientOptions.setMinEvictableIdleTime). Raise this value to keep idle connections longer (reducing reconnect churn); lower it to free unused sockets faster (reducing resource usage). Tune together with `brpc_connection_pool_size` and `brpc_idle_wait_max_time` to balance connection reuse, pool growth, and eviction behavior.
- Introduced in: v3.3.11, v3.4.1, v3.5.0

### `brpc_reuse_addr`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When true, StarRocks sets the socket option to allow local address reuse for client sockets created by the brpc RpcClient (via RpcClientOptions.setReuseAddress). Enabling this reduces bind failures and allows faster rebinding of local ports after sockets are closed, which is helpful for high-rate connection churn or rapid restarts. When false, address/port reuse is disabled, which can reduce the chance of unintended port sharing but may increase transient bind errors. This option interacts with connection behavior configured by `brpc_connection_pool_size` and `brpc_short_connection` because it affects how rapidly client sockets can be rebound and reused.
- Introduced in: v3.3.11, v3.4.1, v3.5.0

### `cluster_name`

- Default: StarRocks Cluster
- Type: String
- Unit: -
- Is mutable: No
- Description: The name of the StarRocks cluster to which the FE belongs. The cluster name is displayed for `Title` on the web page.
- Introduced in: -

### `dns_cache_ttl_seconds`

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: DNS cache TTL (Time-To-Live) in seconds for successful DNS lookups. This sets the Java security property `networkaddress.cache.ttl` which controls how long the JVM caches successful DNS lookups. Set this item to `-1` to allow the system to always cache the infomration, or `0` to disable caching. This is particularly useful in environments where IP addresses change frequently, such as Kubernetes deployments or when dynamic DNS is used.
- Introduced in: v3.5.11, v4.0.4

### `enable_http_async_handler`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow the system to process HTTP requests asynchronously. If this feature is enabled, an HTTP request received by Netty worker threads will then be submitted to a separate thread pool for service logic handling to avoid blocking the HTTP server. If disabled, Netty workers will handle the service logic.
- Introduced in: 4.0.0

### `enable_http_validate_headers`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Controls whether Netty's HttpServerCodec performs strict HTTP header validation. The value is passed to HttpServerCodec when the HTTP pipeline is initialized in `HttpServer` (see UseLocations). Default is false for backward compatibility because newer netty versions enforce stricter header rules (https://github.com/netty/netty/pull/12760). Set to true to enforce RFC-compliant header checks; doing so may cause malformed or nonconforming requests from legacy clients or proxies to be rejected. Change requires a restart of the HTTP server to take effect.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### `enable_https`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable HTTPS server alongside HTTP server in FE nodes.
- Introduced in: v4.0

### `frontend_address`

- Default: 0.0.0.0
- Type: String
- Unit: -
- Is mutable: No
- Description: The IP address of the FE node.
- Introduced in: -

### `http_async_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Size of the thread pool for asynchronous HTTP request processing. The alias is `max_http_sql_service_task_threads_num`.
- Introduced in: 4.0.0

### `http_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of the backlog queue held by the HTTP server in the FE node.
- Introduced in: -

### `http_max_chunk_size`

- Default: 8192
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the maximum allowed size (in bytes) of a single HTTP chunk handled by Netty's HttpServerCodec in the FE HTTP server. It is passed as the third argument to HttpServerCodec and limits the length of chunks during chunked transfer or streaming requests/responses. If an incoming chunk exceeds this value, Netty will raise a frame-too-large error (e.g., TooLongFrameException) and the request may be rejected. Increase this for legitimate large chunked uploads; keep it small to reduce memory pressure and surface area for DoS attacks. This setting is used alongside `http_max_initial_line_length`, `http_max_header_size`, and `enable_http_validate_headers`.
- Introduced in: v3.2.0

### `http_max_header_size`

- Default: 32768
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Maximum allowed size in bytes for the HTTP request header block parsed by Netty's `HttpServerCodec`. StarRocks passes this value to `HttpServerCodec` (as `Config.http_max_header_size`); if an incoming request's headers (names and values combined) exceed this limit, the codec will reject the request (decoder exception) and the connection/request will fail. Increase only when clients legitimately send very large headers (large cookies or many custom headers); larger values increase per-connection memory use. Tune in conjunction with `http_max_initial_line_length` and `http_max_chunk_size`. Changes require FE restart.
- Introduced in: v3.2.0

### `http_max_initial_line_length`

- Default: 4096
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Sets the maximum allowed length (in bytes) of the HTTP initial request line (method + request-target + HTTP version) accepted by the Netty `HttpServerCodec` used in HttpServer. The value is passed to Netty's decoder and requests with an initial line longer than this will be rejected (TooLongFrameException). Increase this only when you must support very long request URIs; larger values increase memory use and may raise exposure to malformed/request-abuse. Tune together with `http_max_header_size` and `http_max_chunk_size`.
- Introduced in: v3.2.0

### `http_port`

- Default: 8030
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the HTTP server in the FE node listens.
- Introduced in: -

### `http_web_page_display_hardware`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When true, the HTTP index page (/index) will include a hardware information section populated via the oshi library (CPU, memory, processes, disks, filesystems, network, etc.). oshi may invoke system utilities or read system files indirectly (for example, it can execute commands such as `getent passwd`), which can surface sensitive system data. If you require stricter security or want to avoid executing those indirect commands on the host, set this configuration to false to disable collection and display of hardware details on the web UI.
- Introduced in: v3.2.0

### `http_worker_threads_num`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of worker threads for http server to deal with http requests. For a negative or 0 value, the number of threads will be twice the number of cpu cores.
- Introduced in: v2.5.18, v3.0.10, v3.1.7, v3.2.2

### `https_port`

- Default: 8443
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the HTTPS server in the FE node listens.
- Introduced in: v4.0

### `max_mysql_service_task_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads that can be run by the MySQL server in the FE node to process tasks.
- Introduced in: -

### `max_task_runs_threads_num`

- Default: 512
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Controls the maximum number of threads in the task-run executor thread pool. This value is the upper bound of concurrent task-run executions; increasing it raises parallelism but also increases CPU, memory, and network usage, while reducing it can cause task-run backlog and higher latency. Tune this value according to expected concurrent scheduled jobs and available system resources.
- Introduced in: v3.2.0

### `memory_tracker_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Enables the FE memory tracker subsystem. When `memory_tracker_enable` is set to `true`, `MemoryUsageTracker` periodically scans registered metadata modules, updates the in-memory `MemoryUsageTracker.MEMORY_USAGE` map, logs totals, and causes `MetricRepo` to expose memory usage and object-count gauges in metrics output. Use `memory_tracker_interval_seconds` to control the sampling interval. Enabling this feature helps monitoring and debugging memory consumption but introduces CPU and I/O overhead and additional metric cardinality.
- Introduced in: v3.2.4

### `memory_tracker_interval_seconds`

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval in seconds for the FE `MemoryUsageTracker` daemon to poll and record memory usage of the FE process and registered `MemoryTrackable` modules. When `memory_tracker_enable` is set to `true`, the tracker runs on this cadence, updates `MEMORY_USAGE`, and logs aggregated JVM and tracked-module usage.
- Introduced in: v3.2.4

### `mysql_nio_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of the backlog queue held by the MySQL server in the FE node.
- Introduced in: -

### `mysql_server_version`

- Default: 8.0.33
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The MySQL server version returned to the client. Modifying this parameter will affect the version information in the following situations:
  1. `select version();`
  2. Handshake packet version
  3. Value of the global variable `version` (`show variables like 'version';`)
- Introduced in: -

### `mysql_service_io_threads_num`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads that can be run by the MySQL server in the FE node to process I/O events.
- Introduced in: -

### `mysql_service_kill_after_disconnect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Controls how the server handles the session when the MySQL TCP connection is detected closed (EOF on read). If it is set to `true`, the server immediately kills any running query for that connection and performs immediate cleanup. If it is `false`, the server does not kill running queries on disconnection and only performs cleanup when there are no pending request tasks, allowing long-running queries to continue after client disconnects. Note: despite a brief comment suggesting TCP keep‑alive, this parameter specifically governs post-disconnection killing behavior and should be set according to whether you want orphaned queries terminated (recommended behind unreliable/load‑balanced clients) or allowed to finish.
- Introduced in: -

### `mysql_service_nio_enable_keep_alive`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Enable TCP Keep-Alive for MySQL connections. Useful for long-idled connections behind load balancers.
- Introduced in: -

### `net_use_ipv6_when_priority_networks_empty`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to use IPv6 addresses preferentially when `priority_networks` is not specified. `true` indicates to allow the system to use an IPv6 address preferentially when the server that hosts the node has both IPv4 and IPv6 addresses and `priority_networks` is not specified.
- Introduced in: v3.3.0

### `priority_networks`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Declares a selection strategy for servers that have multiple IP addresses. Note that at most one IP address must match the list specified by this parameter. The value of this parameter is a list that consists of entries, which are separated with semicolons (;) in CIDR notation, such as 10.10.10.0/24. If no IP address matches the entries in this list, an available IP address of the server will be randomly selected. From v3.3.0, StarRocks supports deployment based on IPv6. If the server has both IPv4 and IPv6 addresses, and this parameter is not specified, the system uses an IPv4 address by default. You can change this behavior by setting `net_use_ipv6_when_priority_networks_empty` to `true`.
- Introduced in: -

### `proc_profile_cpu_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When this item is set to `true`, the background `ProcProfileCollector` will collect CPU profiles using `AsyncProfiler` and write HTML reports under `sys_log_dir/proc_profile`. Each collection run records CPU stacks for the duration configured by `proc_profile_collect_time_s` and uses `proc_profile_jstack_depth` for Java stack depth. Generated profiles are compressed and old files are pruned according to `proc_profile_file_retained_days` and `proc_profile_file_retained_size_bytes`. `AsyncProfiler` requires the native library (`libasyncProfiler.so`); `one.profiler.extractPath` is set to `STARROCKS_HOME_DIR/bin` to avoid noexec issues on `/tmp`.
- Introduced in: v3.2.12

### `qe_max_connection`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of connections that can be established by all users to the FE node. From v3.1.12 and v3.2.7 onwards, the default value has been changed from `1024` to `4096`.
- Introduced in: -

### `query_port`

- Default: 9030
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the MySQL server in the FE node listens.
- Introduced in: -

### `rpc_port`

- Default: 9020
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the Thrift server in the FE node listens.
- Introduced in: -

### `slow_lock_stack_trace_reserve_levels`

- Default: 15
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls how many stack-trace frames are captured and emitted when StarRocks dumps lock debug information for slow or held locks. This value is passed to `LogUtil.getStackTraceToJsonArray` by `QueryableReentrantReadWriteLock` when producing JSON for the exclusive lock owner, current thread, and oldest/shared readers. Increasing this value provides more context for diagnosing slow-lock or deadlock issues at the cost of larger JSON payloads and slightly higher CPU/memory for stack capture; decreasing it reduces overhead. Note: reader entries can be filtered by `slow_lock_threshold_ms` when only logging slow locks.
- Introduced in: v3.4.0, v3.5.0

### `ssl_cipher_blacklist`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Comma separated list, with regex support to blacklist ssl cipher suites by IANA names. If both whitelist and blacklist are set, blacklist takes precedence.
- Introduced in: v4.0

### `ssl_cipher_whitelist`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Comma separated list, with regex support to whitelist ssl cipher suites by IANA names. If both whitelist and blacklist are set, blacklist takes precedence.
- Introduced in: v4.0

### `task_runs_concurrency`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Global limit of concurrently running TaskRun instances. `TaskRunScheduler` stops scheduling new runs when current running count is greater than or equal to `task_runs_concurrency`, so this value caps parallel TaskRun execution across the scheduler. It is also used by `MVPCTRefreshPartitioner` to compute per-TaskRun partition refresh granularity. Increasing the value raises parallelism and resource usage; decreasing it reduces concurrency and makes partition refreshes larger per run. Do not set to 0 or negative unless intentionally disabling scheduling: 0 (or negative) will effectively prevent new TaskRuns from being scheduled by `TaskRunScheduler`.
- Introduced in: v3.2.0

### `task_runs_queue_length`

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Limits the maximum number of pending TaskRun items kept in the pending queue. `TaskRunManager` checks the current pending count and rejects new submissions when valid pending TaskRun count is greater than or equal to `task_runs_queue_length`. The same limit is rechecked before merged/accepted TaskRuns are added. Tune this value to balance memory and scheduling backlog: set higher for large bursty workloads to avoid rejects, or lower to bound memory and reduce pending backlog.
- Introduced in: v3.2.0

### `thrift_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of the backlog queue held by the Thrift server in the FE node.
- Introduced in: -

### `thrift_client_timeout_ms`

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The length of time after which idle client connections time out.
- Introduced in: -

### `thrift_rpc_max_body_size`

- Default: -1
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Controls the maximum allowed Thrift RPC message body size (in bytes) used when constructing the server's Thrift protocol (passed to TBinaryProtocol.Factory in `ThriftServer`). A value of `-1` disables the limit (unbounded). Setting a positive value enforces an upper bound so that messages larger than this are rejected by the Thrift layer, which helps limit memory usage and mitigate oversized-request or DoS risks. Set this to a size large enough for expected payloads (large structs or batched data) to avoid rejecting legitimate requests.
- Introduced in: v3.2.0

### `thrift_server_max_worker_threads`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of worker threads that are supported by the Thrift server in the FE node.
- Introduced in: -

### `thrift_server_queue_size`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of queue where requests are pending. If the number of threads that are being processed in the thrift server exceeds the value specified in `thrift_server_max_worker_threads`, new requests are added to the pending queue.
- Introduced in: -

## Metadata and cluster management

### `alter_max_worker_queue_size`

- Default: 4096
- Type: Int
- Unit: Tasks
- Is mutable: No
- Description: Controls the capacity of the internal worker thread pool queue used by the alter subsystem. It is passed to `ThreadPoolManager.newDaemonCacheThreadPool` in `AlterHandler` together with `alter_max_worker_threads`. When the number of pending alter tasks exceeds `alter_max_worker_queue_size`, new submissions will be rejected and a `RejectedExecutionException` can be thrown (see `AlterHandler.handleFinishAlterTask`). Tune this value to balance memory usage and the amount of backlog you permit for concurrent alter tasks.
- Introduced in: v3.2.0

### `alter_max_worker_threads`

- Default: 4
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: Sets the maximum number of worker threads in the AlterHandler's thread pool. The AlterHandler constructs the executor with this value to run and finalize alter-related tasks (e.g., submitting `AlterReplicaTask` via handleFinishAlterTask). This value bounds concurrent execution of alter operations; raising it increases parallelism and resource usage, lowering it limits concurrent alters and may become a bottleneck. The executor is created together with `alter_max_worker_queue_size`, and the handler scheduling uses `alter_scheduler_interval_millisecond`.
- Introduced in: v3.2.0

### `automated_cluster_snapshot_interval_seconds`

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which the Automated Cluster Snapshot tasks are triggered.
- Introduced in: v3.4.2

### `background_refresh_metadata_interval_millis`

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The interval between two consecutive Hive metadata cache refreshes.
- Introduced in: v2.5.5

### `background_refresh_metadata_time_secs_since_last_access_secs`

- Default: 3600 * 24
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of a Hive metadata cache refresh task. For the Hive catalog that has been accessed, if it has not been accessed for more than the specified time, StarRocks stops refreshing its cached metadata. For the Hive catalog that has not been accessed, StarRocks will not refresh its cached metadata.
- Introduced in: v2.5.5

### `bdbje_cleaner_threads`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of background cleaner threads for the Berkeley DB Java Edition (JE) environment used by StarRocks journal. This value is read during environment initialization in `BDBEnvironment.initConfigs` and applied to `EnvironmentConfig.CLEANER_THREADS` using `Config.bdbje_cleaner_threads`. It controls parallelism for JE log cleaning and space reclamation; increasing it can speed up cleaning at the cost of additional CPU and I/O interference with foreground operations. Changes take effect only when the BDB environment is (re)initialized, so a frontend restart is required to apply a new value.
- Introduced in: v3.2.0

### `bdbje_heartbeat_timeout_second`

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The amount of time after which the heartbeats among the leader, follower, and observer FEs in the StarRocks cluster time out.
- Introduced in: -

### `bdbje_lock_timeout_second`

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The amount of time after which a lock in the BDB JE-based FE times out.
- Introduced in: -

### `bdbje_replay_cost_percent`

- Default: 150
- Type: Int
- Unit: Percent
- Is mutable: No
- Description: Sets the relative cost (as a percentage) of replaying transactions from a BDB JE log versus obtaining the same data via a network restore. The value is supplied to the underlying JE replication parameter `REPLAY_COST_PERCENT` and is typically `>100` to indicate that replay is usually more expensive than a network restore. When deciding whether to retain cleaned log files for potential replay, the system compares replay cost multiplied by log size against the cost of a network restore; files will be removed if network restore is judged more efficient. A value of 0 disables retention based on this cost comparison. Log files required for replicas within `REP_STREAM_TIMEOUT` or for any active replication are always retained.
- Introduced in: v3.2.0

### `bdbje_replica_ack_timeout_second`

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The maximum amount of time for which the leader FE can wait for ACK messages from a specified number of follower FEs when metadata is written from the leader FE to the follower FEs. Unit: second. If a large amount of metadata is being written, the follower FEs require a long time before they can return ACK messages to the leader FE, causing ACK timeout. In this situation, metadata writes fail, and the FE process exits. We recommend that you increase the value of this parameter to prevent this situation.
- Introduced in: -

### `bdbje_reserved_disk_size`

- Default: 512 * 1024 * 1024 (536870912)
- Type: Long
- Unit: Bytes
- Is mutable: No
- Description: Limits the number of bytes Berkeley DB JE will reserve as "unprotected" (deletable) log/data files. StarRocks passes this value to JE via `EnvironmentConfig.RESERVED_DISK` in BDBEnvironment; JE's built-in default is 0 (unlimited). The StarRocks default (512 MiB) prevents JE from reserving excessive disk space for unprotected files while allowing safe cleanup of obsolete files. Tune this value on disk-constrained systems: decreasing it lets JE free more files sooner, increasing it lets JE retain more reserved space. Changes require restarting the process to take effect.
- Introduced in: v3.2.0

### `bdbje_reset_election_group`

- Default: false
- Type: String
- Unit: -
- Is mutable: No
- Description: Whether to reset the BDBJE replication group. If this parameter is set to `TRUE`, the FE will reset the BDBJE replication group (that is, remove the information of all electable FE nodes) and start as the leader FE. After the reset, this FE will be the only member in the cluster, and other FEs can rejoin this cluster by using `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'`. Use this setting only when no leader FE can be elected because the data of most follower FEs have been damaged. `reset_election_group` is used to replace `metadata_failure_recovery`.
- Introduced in: -

### `black_host_connect_failures_within_time`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The threshold of connection failures allowed for a blacklisted BE node. If a BE node is added to the BE Blacklist automatically, StarRocks will assess its connectivity and judge whether it can be removed from the BE Blacklist. Within `black_host_history_sec`, only if a blacklisted BE node has fewer connection failures than the threshold set in `black_host_connect_failures_within_time`, it can be removed from the BE Blacklist.
- Introduced in: v3.3.0

### `black_host_history_sec`

- Default: 2 * 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time duration for retaining historical connection failures of BE nodes in the BE Blacklist. If a BE node is added to the BE Blacklist automatically, StarRocks will assess its connectivity and judge whether it can be removed from the BE Blacklist. Within `black_host_history_sec`, only if a blacklisted BE node has fewer connection failures than the threshold set in `black_host_connect_failures_within_time`, it can be removed from the BE Blacklist.
- Introduced in: v3.3.0

### `brpc_connection_pool_size`

- Default: 16
- Type: Int
- Unit: Connections
- Is mutable: No
- Description: The maximum number of pooled BRPC connections per endpoint used by the FE's BrpcProxy. This value is applied to RpcClientOptions via `setMaxTotoal` and `setMaxIdleSize`, so it directly limits concurrent outgoing BRPC requests because each request must borrow a connection from the pool. In high concurrency scenarios increase this to avoid request queuing; increasing it raises socket and memory usage and may increase remote server load. When tuning, consider related settings such as `brpc_idle_wait_max_time`, `brpc_short_connection`, `brpc_inner_reuse_pool`, `brpc_reuse_addr`, and `brpc_min_evictable_idle_time_ms`. Changing this value is not hot-reloadable and requires a restart.
- Introduced in: v3.2.0

### `brpc_short_connection`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: Controls whether the underlying brpc RpcClient uses short-lived connections. When enabled (`true`), RpcClientOptions.setShortConnection is set and connections are closed after a request completes, reducing the number of long-lived sockets at the cost of higher connection setup overhead and increased latency. When disabled (`false`, the default) persistent connections and connection pooling are used. Enabling this option affects connection-pool behavior and should be considered together with `brpc_connection_pool_size`, `brpc_idle_wait_max_time`, `brpc_min_evictable_idle_time_ms`, `brpc_reuse_addr`, and `brpc_inner_reuse_pool`. Keep it disabled for typical high-throughput deployments; enable only to limit socket lifetime or when short connections are required by network policy.
- Introduced in: v3.3.11, v3.4.1, v3.5.0

### `catalog_try_lock_timeout_ms`

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout duration to obtain the global lock.
- Introduced in: -

### `checkpoint_only_on_leader`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When `true`, the CheckpointController will only select the leader FE as the checkpoint worker; when `false`, the controller may pick any frontend and prefers nodes with lower heap usage. With `false`, workers are sorted by recent failure time and `heapUsedPercent` (the leader is treated as having infinite usage to avoid selecting it). For operations that require cluster snapshot metadata, the controller already forces leader selection regardless of this flag. Enabling `true` centralizes checkpoint work on the leader (simpler but increases leader CPU/memory and network load); keeping it `false` distributes checkpoint load to less-loaded FEs. This setting affects worker selection and interaction with timeouts such as `checkpoint_timeout_seconds` and RPC settings like `thrift_rpc_timeout_ms`.
- Introduced in: v3.4.0, v3.5.0

### `checkpoint_timeout_seconds`

- Default: 24 * 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: Maximum time (in seconds) the leader's CheckpointController will wait for a checkpoint worker to complete a checkpoint. The controller converts this value to nanoseconds and polls the worker result queue; if no successful completion is received within this timeout the checkpoint is treated as failed and createImage returns failure. Increasing this value accommodates longer-running checkpoints but delays failure detection and subsequent image propagation; decreasing it causes faster failover/retries but can produce false timeouts for slow workers. This setting only controls the waiting period in `CheckpointController` during checkpoint creation and does not change the worker's internal checkpointing behavior.
- Introduced in: v3.4.0, v3.5.0

### `db_used_data_quota_update_interval_secs`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which the database used data quota is updated. StarRocks periodically updates the used data quota for all databases to track storage consumption. This value is used for quota enforcement and metrics collection. The minimum allowed interval is 30 seconds to prevent excessive system load. A value less than 30 will be rejected.
- Introduced in: -

### `drop_backend_after_decommission`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to delete a BE after the BE is decommissioned. `TRUE` indicates that the BE is deleted immediately after it is decommissioned. `FALSE` indicates that the BE is not deleted after it is decommissioned.
- Introduced in: -

### `edit_log_port`

- Default: 9010
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port that is used for communication among the Leader, Follower, and Observer FEs in the cluster.
- Introduced in: -

### `edit_log_roll_num`

- Default: 50000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of metadata log entries that can be written before a log file is created for these log entries. This parameter is used to control the size of log files. The new log file is written to the BDBJE database.
- Introduced in: -

### `edit_log_type`

- Default: BDB
- Type: String
- Unit: -
- Is mutable: No
- Description: The type of edit log that can be generated. Set the value to `BDB`.
- Introduced in: -

### `enable_background_refresh_connector_metadata`

- Default: true in v3.0 and later and false in v2.5
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the periodic Hive metadata cache refresh. After it is enabled, StarRocks polls the metastore (Hive Metastore or AWS Glue) of your Hive cluster, and refreshes the cached metadata of the frequently accessed Hive catalogs to perceive data changes. `true` indicates to enable the Hive metadata cache refresh, and `false` indicates to disable it.
- Introduced in: v2.5.5

### `refresh_other_fe_dispatch_executor_thread_num`

- Default: 4
- Type: Integer
- Unit: -
- Is mutable: Yes
- Description: The number of threads in the FE-global dispatch executor for asynchronous "refresh other FE" jobs. These threads only schedule background refresh tasks from connector write paths. They do not send peer FE refresh RPCs directly. Changes take effect on running FEs without restart.
- Introduced in: -

### `refresh_other_fe_rpc_executor_thread_num`

- Default: 4
- Type: Integer
- Unit: -
- Is mutable: Yes
- Description: The number of threads in the FE-global RPC executor for "refresh other FE" fan-out. This executor bounds the number of concurrent refresh RPCs sent to peer FEs for both synchronous and asynchronous external table refresh flows. Changes take effect on running FEs without restart.
- Introduced in: -

### `enable_collect_query_detail_info`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect the profile of a query. If this parameter is set to `TRUE`, the system collects the profile of the query. If this parameter is set to `FALSE`, the system does not collect the profile of the query.
- Introduced in: -

### `enable_create_partial_partition_in_batch`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: When this item is set to `false` (default), StarRocks enforces that batch-created range partitions align to the standard time unit boundaries. It will reject non‑aligned ranges to avoid creating holes. Setting this item to `true` disables that alignment check and allows creating partial (non‑standard) partitions in batch, which can produce gaps or misaligned partition ranges. You should only set it to `true` when you intentionally need partial batch partitions and accept the associated risks.
- Introduced in: v3.2.0

### `enable_internal_sql`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: When this item is set to `true`, internal SQL statements executed by internal components (for example, SimpleExecutor) are preserved and written into internal audit or log messages (and can be further desensitized if `enable_sql_desensitize_in_log` is set). When it is set to `false`, internal SQL text is suppressed: formatting code (SimpleExecutor.formatSQL) returns "?" and the actual statement is not emitted to internal audit or log messages. This configuration does not change execution semantics of internal statements — it only controls logging and visibility of internal SQL for privacy or security.
- Introduced in: -

### `enable_legacy_compatibility_for_replication`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the Legacy Compatibility for Replication. StarRocks may behave differently between the old and new versions, causing problems during cross-cluster data migration. Therefore, you must enable Legacy Compatibility for the target cluster before data migration and disable it after data migration is completed. `true` indicates enabling this mode.
- Introduced in: v3.1.10, v3.2.6

### `enable_show_materialized_views_include_all_task_runs`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls how TaskRuns are returned to the SHOW MATERIALIZED VIEWS command. When this item is set to `false`, StarRocks returns only the newest TaskRun per task (legacy behavior for compatibility). When it is set to `true` (default), `TaskManager` may include additional TaskRuns for the same task only when they share the same start TaskRun ID (for example, belong to the same job), preventing unrelated duplicate runs from appearing while allowing multiple statuses tied to one job to be shown. Set this item to `false` to restore single-run output or to surface multi-run job history for debugging and monitoring.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### `enable_statistics_collect_profile`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to generate profiles for statistics queries. You can set this item to `true` to allow StarRocks to generate query profiles for queries on system statistics.
- Introduced in: v3.1.5

### `enable_table_name_case_insensitive`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable case-insensitive processing on catalog names, database names, table names, view names, and materialized view names. Currently, table names are case-sensitive by default.
  - After enabling this feature, all related names will be stored in lowercase, and all SQL commands containing these names will automatically convert them to lowercase.
  - You can enable this feature only when creating a cluster. **After the cluster is started, the value of this configuration cannot be modified by any means**. Any attempt to modify it will result in an error. FE will fail to start when it detects that the value of this configuration item is inconsistent with that when the cluster was first started.
  - Currently, this feature does not support JDBC catalog and table names. Do not enable this feature if you want to perform case-insensitive processing on JDBC or ODBC data sources.
- Introduced in: v4.0

### `enable_task_history_archive`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, finished task-run records are archived to the persistent task-run history table and recorded to the edit log so lookups (e.g., `lookupHistory`, `lookupHistoryByTaskNames`, `lookupLastJobOfTasks`) include archived results. Archiving is performed by the FE leader and is skipped during unit tests (`FeConstants.runningUnitTest`). When enabled, in-memory expiration and forced-GC paths are bypassed (the code returns early from `removeExpiredRuns` and `forceGC`), so retention/eviction is handled by the persistent archive instead of `task_runs_ttl_second` and `task_runs_max_history_number`. When disabled, history stays in memory and is pruned by those configurations.
- Introduced in: v3.3.1, v3.4.0, v3.5.0

### `enable_task_run_fe_evaluation`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, the FE will perform local evaluation for the system table `task_runs` in `TaskRunsSystemTable.supportFeEvaluation`. FE-side evaluation is only allowed for conjunctive equality predicates comparing a column to a constant and is limited to the columns `QUERY_ID` and `TASK_NAME`. Enabling this improves performance for targeted lookups by avoiding broader scans or additional remote processing; disabling it forces the planner to skip FE evaluation for `task_runs`, which may reduce predicate pruning and affect query latency for those filters.
- Introduced in: v3.3.13, v3.4.3, v3.5.0

### `heartbeat_mgr_blocking_queue_size`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The size of the blocking queue that stores heartbeat tasks run by the Heartbeat Manager.
- Introduced in: -

### `heartbeat_mgr_threads_num`

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads that can be run by the Heartbeat Manager to run heartbeat tasks.
- Introduced in: -

### `ignore_materialized_view_error`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether FE ignores the metadata exception caused by materialized view errors. If FE fails to start due to the metadata exception caused by materialized view errors, you can set this parameter to `true` to allow FE to ignore the exception.
- Introduced in: v2.5.10

### `ignore_meta_check`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether non-Leader FEs ignore the metadata gap from the Leader FE. If the value is TRUE, non-Leader FEs ignore the metadata gap from the Leader FE and continue providing data reading services. This parameter ensures continuous data reading services even when you stop the Leader FE for a long period of time. If the value is FALSE, non-Leader FEs do not ignore the metadata gap from the Leader FE and stop providing data reading services.
- Introduced in: -

### `ignore_task_run_history_replay_error`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When StarRocks deserializes TaskRun history rows for `information_schema.task_runs`, a corrupted or invalid JSON row will normally cause deserialization to log a warning and throw a RuntimeException. If this item is set to `true`, the system will catch deserialization errors, skip the malformed record, and continue processing remaining rows instead of failing the query. This will make `information_schema.task_runs` queries tolerant of bad entries in the `_statistics_.task_run_history` table. Note that enabling it will silently drop corrupted history records (potential data loss) instead of surfacing an explicit error.
- Introduced in: v3.3.3, v3.4.0, v3.5.0

### `leader_demotion_drain_timeout_sec`

- Default: 180
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Per-daemon timeout used during leader demotion to wait for each leader-only daemon worker thread to exit after being interrupted. If a worker is still alive when the timeout elapses, the FE process is terminated, because a stuck worker combined with a later re-election would run two workers against the same singleton state - strictly worse than a process restart. Increase this if you routinely see heartbeat/report/publish/tablet-scheduler daemons needing more than the default to drain.
- Introduced in: v4.1

### `lock_checker_interval_second`

- Default: 30
- Type: long
- Unit: Seconds
- Is mutable: Yes
- Description: Interval, in seconds, between executions of the LockChecker frontend daemon (named "deadlock-checker"). The daemon performs deadlock detection and slow-lock scanning; the configured value is multiplied by 1000 to set the timer in milliseconds. Decreasing this value reduces detection latency but increases scheduling and CPU overhead; increasing it reduces overhead but delays detection and slow-lock reporting. Changes take effect at runtime because the daemon resets its interval each run. This setting interacts with `lock_checker_enable_deadlock_check` (enables deadlock checks) and `slow_lock_threshold_ms` (defines what constitutes a slow lock).
- Introduced in: v3.2.0

### `master_sync_policy`

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

### `max_bdbje_clock_delta_ms`

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: The maximum clock offset that is allowed between the leader FE and the follower or observer FEs in the StarRocks cluster.
- Introduced in: -

### `meta_delay_toleration_second`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration by which the metadata on the follower and observer FEs can lag behind that on the leader FE. Unit: seconds. If this duration is exceeded, the non-leader FEs stops providing services.
- Introduced in: -

### `meta_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/meta"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores metadata.
- Introduced in: -

### `metadata_ignore_unknown_operation_type`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to ignore an unknown log ID. When an FE is rolled back, the FEs of the earlier version may be unable to recognize some log IDs. If the value is `TRUE`, the FE ignores unknown log IDs. If the value is `FALSE`, the FE exits.
- Introduced in: -

### `profile_info_format`

- Default: default
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The format of the Profile output by the system. Valid values: `default` and `json`. When set to `default`, Profile is of the default format. When set to `json`, the system outputs Profile in JSON format.
- Introduced in: v2.5

### `replica_ack_policy`

- Default: `SIMPLE_MAJORITY`
- Type: String
- Unit: -
- Is mutable: No
- Description: The policy based on which a log entry is considered valid. The default value `SIMPLE_MAJORITY` specifies that a log entry is considered valid if a majority of follower FEs return ACK messages.
- Introduced in: -

### `replica_sync_policy`

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: The policy based on which the follower FE flushes logs to disk. This parameter is valid only when the current FE is a follower FE. Valid values:
  - `SYNC`: When a transaction is committed, a log entry is generated and flushed to disk simultaneously.
  - `NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.
  - `WRITE_NO_SYNC`: When a transaction is committed, a log entry is generated simultaneously but is not flushed to disk.
- Introduced in: -

### `start_with_incomplete_meta`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: When true, the FE will allow startup when image data exists but Berkeley DB JE (BDB) log files are missing or corrupted. `MetaHelper.checkMetaDir()` uses this flag to bypass the safety check that otherwise prevents starting from an image without corresponding BDB logs; starting this way can produce stale or inconsistent metadata and should only be used for emergency recovery. `RestoreClusterSnapshotMgr` temporarily sets this flag to true while restoring a cluster snapshot and then rolls it back; that component also toggles `bdbje_reset_election_group` during restore. Do not enable in normal operation — enable only when recovering from corrupted BDB data or when explicitly restoring an image-based snapshot.
- Introduced in: v3.2.0

### `table_keeper_interval_second`

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval, in seconds, between executions of the TableKeeper daemon. The TableKeeperDaemon uses this value (multiplied by 1000) to set its internal timer and periodically runs keeper tasks that ensure history tables exist, correct table properties (replication number) and update partition TTLs. The daemon only performs work on the leader node and updates its runtime interval via setInterval when `table_keeper_interval_second` changes. Increase to reduce scheduling frequency and load; decrease for faster reaction to missing or stale history tables.
- Introduced in: v3.3.1, v3.4.0, v3.5.0

### `task_runs_ttl_second`

- Default: 7 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Controls the time-to-live (TTL) for task run history. Lowering this value shortens history retention and reduces memory/disk usage; raising it keeps histories longer but increases resource usage. Adjust together with `task_runs_max_history_number` and `enable_task_history_archive` for predictable retention and storage behavior.
- Introduced in: v3.2.0

### `task_ttl_second`

- Default: 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Time-to-live (TTL) for tasks. For manual tasks (when no schedule is set), TaskBuilder uses this value to compute the task's `expireTime` (`expireTime = now + task_ttl_second * 1000L`). TaskRun also uses this value as an upper bound when computing a run's execute timeout — the effective execute timeout is `min(task_runs_timeout_second, task_runs_ttl_second, task_ttl_second)`. Adjusting this value changes how long manually created tasks remain valid and can indirectly limit the maximum allowed execution time of task runs.
- Introduced in: v3.2.0

### `thrift_rpc_retry_times`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls the total number of attempts a Thrift RPC call will make. This value is used by `ThriftRPCRequestExecutor` (and callers such as `NodeMgr` and `VariableMgr`) as the loop count for retries — i.e., a value of 3 allows up to three attempts including the initial try. On `TTransportException` the executor will try to reopen the connection and retry up to this count; it will not retry when the cause is a `SocketTimeoutException` or when reopen fails. Each attempt is subject to the per-attempt timeout configured by `thrift_rpc_timeout_ms`. Increasing this value improves resilience to transient connection failures but can increase overall RPC latency and resource usage.
- Introduced in: v3.2.0

### `thrift_rpc_strict_mode`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Controls the TBinaryProtocol "strict read" mode used by the Thrift server. This value is passed as the first argument to org.apache.thrift.protocol.TBinaryProtocol.Factory in the Thrift server stack and affects how incoming Thrift messages are parsed and validated. When `true` (default), the server enforces strict Thrift encoding/version checks and honors the configured `thrift_rpc_max_body_size` limit; when `false`, the server accepts non-strict (legacy/lenient) message formats, which can improve compatibility with older clients but may bypass some protocol validations. Use caution changing this on a running cluster because it is not mutable and affects interoperability and parsing safety.
- Introduced in: v3.2.0

### `thrift_rpc_timeout_ms`

- Default: 10000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Timeout (in milliseconds) used as the default network/socket timeout for Thrift RPC calls. It is passed to TSocket when creating Thrift clients in `ThriftConnectionPool` (used by the frontend and backend pools) and is also added to an operation's execution timeout (e.g., ExecTimeout*1000 + `thrift_rpc_timeout_ms`) when computing RPC call timeouts in places such as `ConfigBase`, `LeaderOpExecutor`, `GlobalStateMgr`, `NodeMgr`, `VariableMgr`, and `CheckpointWorker`. Increasing this value makes RPC calls tolerate longer network or remote processing delays; decreasing it causes faster failover on slow networks. Changing this value affects connection creation and request deadlines across the FE code paths that perform Thrift RPCs.
- Introduced in: v3.2.0

### `txn_latency_metric_report_groups`

- Default: An empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: A comma-separated list of transaction latency metric groups to report. Load types are categorized into logical groups for monitoring. When a group is enabled, its name is added as a 'type' label to transaction metrics. Valid values: `stream_load`, `routine_load`, `broker_load`, `insert`, and `compaction` (available only for shared-data clusters). Example: `"stream_load,routine_load"`.
- Introduced in: v4.0

### `txn_rollback_limit`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of transactions that can be rolled back.
- Introduced in: -
