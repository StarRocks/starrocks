---
displayed_sidebar: docs
sidebar_label: "Session, Protocol, and Governance"
sidebar_position: 8
description: "Session variables for MySQL protocol compatibility, query queue, timeouts, resource groups, and profiling."
---

# System Variables - Session, Protocol, and Governance

For how to view and set variables, see the [System variables overview](../System_variable.md).

import Restricted from '../../_assets/commonMarkdown/_restricted.mdx'

### activate_all_roles_on_login (global)

* **Description**: Whether to enable all roles (including default roles and granted roles) for a StarRocks user when the user connects to the StarRocks cluster.
  * If enabled (`true`), all roles of the user are activated at user login. This takes precedence over the roles set by [SET DEFAULT ROLE](sql-statements/account-management/SET_DEFAULT_ROLE.md).
  * If disabled (`false`), the roles set by SET DEFAULT ROLE are activated.
* **Default**: false
* **Introduced in**: v3.0

If you want to activate the roles assigned to you in a session, use the [SET ROLE](sql-statements/account-management/SET_DEFAULT_ROLE.md) command.

### allow_default_partition

<Restricted />

* **Description**: Enables the creation of tables with a default partition when no explicit partitioning is defined.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### always_collect_low_card_dict

<Restricted />

* **Description**: only used for test case
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### ann_params

* **Description**: To set ANN tuning parameters for user. Since the session variables does not support map variables, it needs to be passed in the form of a JSON string.
* **Scope**: Session
* **Default**: `""`
* **Data type**: `String`
* **Mutable**: Yes

### arrow_flight_proxy

* **Description**: Arrow Flight SQL proxy endpoint. Format: "hostname:port" or "grpcs://hostname:port" for TLS.
* **Scope**: Global
* **Default**: `-`
* **Mutable**: No

### arrow_flight_proxy_enabled

* **Description**: Enable Arrow Flight SQL proxy mode.
* **Scope**: Global
* **Default**: `64`
* **Data type**: `int`
* **Mutable**: No

### audit_execute_stmt

* **Description**: Enables logging of executed SQL statements to the audit log.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### authentication_policy

* **Scope**: Session
* **Description**: Session-level variable that holds the raw authentication policy string (kept for MySQL 8.0 compatibility). It is declared and stored on the FE as part of the session state and exposed under the name `authentication_policy`. In the current codebase snapshot the value is stored as an opaque comma-separated string and there is no other usage or internal parsing of this field in FE (it is not referenced elsewhere). It is related to the `default_authentication_plugin` session variable: `authentication_policy` represents per-session authentication policy data while `default_authentication_plugin` indicates the default authentication plugin. Administrators or clients can set this variable per session (for example with `SET authentication_policy = '...'`) to preserve compatibility with MySQL clients or tooling.
* **Default**: `*,,`
* **Data Type**: String
* **Introduced in**: -

### auto_increment_increment

Used for MySQL client compatibility. No practical usage.

### autocommit

* **Description**: this is used to make mysql client happy
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### big_query_log_cpu_second_threshold

* **Description**: the value is set for testing, if a query needs to perform 10s for computing tasks at full load on three 16-core machines, we treat it as a big query, so set this value to 480(10 * 16 * 3). Users need to set up according to their own scenario.
* **Scope**: Session
* **Default**: `480`
* **Data type**: `long`
* **Mutable**: Yes

### big_query_profile_threshold

* **Description**: Used to set the threshold for big queries. When the session variable `enable_profile` is set to `false` and the amount of time taken by a query exceeds the threshold specified by the variable `big_query_profile_threshold`, a profile is generated for that query.

  Note: In versions v3.1.5 to v3.1.7, as well as v3.2.0 to v3.2.2, we introduced the `big_query_profile_second_threshold` for setting the threshold for big queries. In versions v3.1.8, v3.2.3, and subsequent releases, this parameter has been replaced by `big_query_profile_threshold` to offer more flexible configuration options.
* **Default**: `30s`
* **Unit**: Second
* **Data type**: String
* **Introduced in**: v3.1

### binary_encoding_format

* **Scope**: Session
* **Description**: Controls how `BINARY` / `VARBINARY` values are encoded when StarRocks serializes MySQL text results. Valid values are `raw`, `hex`, and `base64`. The default is `hex`. This variable works together with `binary_encoding_level`. MySQL clients can already handle top-level binary values, but nested binary values inside `ARRAY`, `MAP`, or `STRUCT` are returned through JSON-like strings, so they may need extra encoding to stay printable and well-formed. Set this variable to `base64` if you prefer a denser printable representation, or `raw` to disable extra encoding entirely.
* **Default**: `hex`
* **Data Type**: String
* **Introduced in**: v4.1

### binary_encoding_level

* **Scope**: Session
* **Description**: Controls which binary values are encoded for MySQL text results. Valid values are `nested` and `all`. The default is `nested`, which preserves historical behavior for top-level binary columns while still encoding nested binary values inside `ARRAY`, `MAP`, or `STRUCT`, where the result is rendered as a JSON-like string. Set this variable to `all` if your team wants a uniform convention and prefers top-level binary values to be encoded as well. If `binary_encoding_format = raw`, no additional binary encoding is applied even when this variable is set to `nested` or `all`, which may make nested output less readable.
* **Default**: `nested`
* **Data Type**: String
* **Introduced in**: v4.1

### broadcast_right_table_scale_factor

<Restricted />

* **Description**: Controls the size threshold multiplier for determining whether the right table in a join should be broadcast to all nodes.
* **Scope**: Session
* **Default**: `10.0`
* **Data type**: `double`
* **Mutable**: Yes

### catalog

* **Description**: Used to specify the catalog to which the session belongs.
* **Default**: default_catalog
* **Data type**: String
* **Introduced in**: v3.2.4

### character_set_database (global)

* **Data type**: StringThe character set supported by StarRocks. Only UTF8 (`utf8`) is supported.
* **Default**: utf8
* **Data type**: String

### choose_execute_instances_mode

* **Description**: Specifies the strategy for selecting execution instances when distributing query tasks across nodes.
* **Scope**: Session
* **Default**: `LOCALITY.name()`
* **Data type**: `String`
* **Mutable**: Yes

### cngroup_low_watermark_cpu_used_permille

* **Description**: Sets the CPU usage threshold in permille (per thousand) below which a compute node group is considered to have low resource utilization.
* **Scope**: Global
* **Default**: `600`
* **Data type**: `long`
* **Mutable**: No

### cngroup_low_watermark_running_query_count

* **Description**: Specifies the minimum number of concurrent running queries on a compute node group before triggering low watermark resource scheduling.
* **Scope**: Global
* **Default**: `8`
* **Data type**: `long`
* **Mutable**: No

### cngroup_resource_usage_fresh_ratio

* **Description**: Determines the freshness ratio for CN group resource usage metrics in scheduling decisions.
* **Scope**: Global
* **Default**: `0.5`
* **Data type**: `double`
* **Mutable**: No

### collation_connection

* **Description**: Session-scoped variable that stores the connection collation name for the current client session. It is declared in `SessionVariable` as `collationConnection` and exposed with the show-name `collation_connection`. The variable is surfaced in metadata and SHOW outputs (for example, used when building `SHOW CREATE VIEW` rows in `ShowExecutor` and returned as a constant for `information_schema.views` via `ViewsSystemTable.CONSTANT_MAP`). It represents the collation reported by the server for the connection (paired with `character_set_client` and related character-set variables) but does not by itself indicate runtime collation enforcement beyond what other components implement.
* **Scope**: Session
* **Default**: `utf8_general_ci`
* **Data Type**: String
* **Introduced in**: v3.2.0

### collation_database

* **Description**: Session-level variable that holds the default database collation name for the current session. It is declared in SessionVariable (annotated with `@VariableMgr.VarAttr`) and lives alongside other charset/collation session variables such as `character_set_client`, `collation_connection` and `collation_server`. The value is serialized when session variables are exported (e.g., included in the JSON produced by SessionVariable#getJsonString and in the session variable machinery), and is used to report the session's database collation. Changing this variable updates the session's reported database collation name; engine-level or object-level collation settings (server/table/column) may still take precedence for actual comparison/ordering behavior depending on context.
* **Scope**: Session
* **Default**: `utf8_general_ci`
* **Data Type**: String
* **Introduced in**: v3.2.0

### collation_server

* **Scope**: Session
* **Description**: Session-level server collation name used by the FE to present MySQL-compatible collation behavior for this session. This variable sets the default collation identifier (for example `utf8_general_ci`) that FE reports to clients and that is associated with `character_set_server` / `collation_connection` / `collation_database`. It is persisted in the session variable JSON (see SessionVariable#getJsonString / replayFromJson) and is exposed via the variable manager (`@VarAttr(name = COLLATION_SERVER)`), so it appears in SHOW VARIABLES and can be changed per-session. The value is stored as a plain String in SessionVariable and typically holds a standard MySQL collation name (e.g. `utf8_general_ci`, `utf8mb4_unicode_ci`); the code does not enforce a fixed enum or perform additional validation here, so the effective behavior depends on downstream components that interpret the collation name for comparisons, ordering and other collation-sensitive operations.
* **Default Value**: `utf8_general_ci`
* **Data Type**: String
* **Introduced in**: `v3.2.0`

### column_view_concat_bytes_limit

* **Description**: Limits the maximum total byte size of data when concatenating column view results.
* **Scope**: Session
* **Default**: `4294967296L`
* **Data type**: `long`
* **Mutable**: Yes

### column_view_concat_rows_limit

* **Description**: when rows_num of ColumnView is less than column_view_concat_rows_limit or bytes size of ColumnView is less than column_view concat_bytes_limit, underlying columns of column view is concatenated together. 1. when both these variables are -1, ColumnView is disabled; 2. when either of these variables are 0, ColumnView is always enabled; 3. otherwise, ColumnView concatenation depends on its rows_num and bytes_size
* **Scope**: Session
* **Default**: `-1`
* **Data type**: `long`
* **Mutable**: Yes

### consistent_hash_virtual_number

<Restricted />

* **Description**: Specifies the number of virtual nodes per physical node in consistent hashing for data distribution.
* **Scope**: Session
* **Default**: `256`
* **Data type**: `int`
* **Mutable**: Yes

### custom_query_id (session)

* **Description**: Used to bind some external identifier to a current query. Can be set using `SET SESSION custom_query_id = 'my-query-id';` before executing a query. The value is reset after query is finished. This value can be passed to `KILL QUERY 'my-query-id'`. Value can be found in audit logs as a `customQueryId` field.
* **Default**: ""
* **Data type**: String
* **Introduced in**: v3.4.0

### custom_session_name (session)

* **Description**: Used to specify custom name of current session, analog of `applicationName` or `program_name` in DMBS like MySQL or PostgreSQL. Can be set using `SET SESSION custom_session_name = 'my session name';`. Value can be found in audit logs in `customSessionName` field.
* **Default**: ""
* **Data type**: String
* **Introduced in**: v4.1.0

### data_skew_row_percentage_threshold

* **Description**: Specifies the row percentage threshold for detecting data skew in query operations.
* **Scope**: Session
* **Default**: `0.2`
* **Data type**: `double`
* **Mutable**: Yes

### decimal_overflow_to_double

* **Scope**: Session
* **Description**: When enabled, the analyzer converts decimal arithmetic results that would overflow the maximum decimal precision into 64-bit floating point (`DOUBLE`) instead of widening to larger decimal types or failing. Concretely, in DecimalV3 arithmetic (see DecimalV3FunctionAnalyzer), if a multiplication's computed precision exceeds the engine's max decimal precision but its return scale is within the maximum, the session flag `decimal_overflow_to_double = true` causes the return type and operand target types to be set to `DOUBLE`. This yields an approximate (lossy) numeric result but avoids decimal precision overflow errors or forced use of larger decimal types. When false (default), the planner will keep decimal semantics (attempt decimal128/256 or throw on unrepresentable scale/precision).
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: -

### default_authentication_plugin

* **Scope**: Session
* **Description**: Session-scoped variable that specifies the default MySQL authentication plugin name for this session. It is stored as SessionVariable.defaultAuthenticationPlugin and is used by StarRocks' MySQL-protocol compatibility layers when the server needs to advertise or use a default authentication plugin (for example during handshake or when a plugin is not specified). Accepts standard MySQL authentication plugin identifiers (e.g. `mysql_native_password`, `caching_sha2_password`) supported by the server. This variable affects session behavior only; persistent user account authentication configuration is managed separately. See related session variable `authentication_policy`.
* **Default**: `mysql_native_password`
* **Data Type**: String
* **Introduced in**: -

### default_rowset_type (global)

Used to set the default storage format used by the storage engine of the computing node. The currently supported storage formats are `alpha` and `beta`.

### default_storage_engine

* **Scope**: Session
* **Description**: Session system variable exposed as `default_storage_engine` (see SessionVariable VarAttr). It exists for MySQL 8.0 compatibility and to satisfy MySQL clients/libraries that query the session default storage engine. The variable is stored per-session in the SessionVariable object and is returned to clients (e.g., via SHOW VARIABLES). It is informational for compatibility; changing it adjusts the session-reported value but does not imply StarRocks will change internal storage implementation.
* **Default**: `InnoDB`
* **Data Type**: String
* **Introduced in**: v3.4.2, v3.5.0

### default_table_compression

* **Description**: The default compression algorithm for table storage. Supported compression algorithms are `snappy, lz4, zlib, zstd`.

  Note that if you specified the `compression` property in a CREATE TABLE statement, the compression algorithm specified by `compression` takes effect.

* **Default**: lz4_frame
* **Introduced in**: v3.0

### default_tmp_storage_engine

* **Description**: Session variable that controls the default storage engine used for temporary tables (both explicit `CREATE TEMPORARY TABLE` and internal/implicit temporary tables created by the engine). Declared in `SessionVariable.java` with a `@VariableMgr.VarAttr` annotation, it exists primarily for MySQL 8.0 compatibility so clients and tools expecting MySQL-like behavior can observe or change the temporary-table engine per session. Changing this value affects how temporary table data is stored/managed on storage layers that honor different engines (for example, choosing between memory-backed vs. disk-backed engines).
* **Scope**: Session
* **Default**: `InnoDB`
* **Data Type**: String
* **Introduced in**: v3.4.2, v3.5.0

### default_view_sql_security

* **Description**: The default SQL SECURITY characteristic applied when a `CREATE VIEW` statement does not specify a `SECURITY` clause. `NONE` (equivalent to an explicit `SECURITY NONE` clause) means querying the view only requires the invoker to have the `SELECT` privilege on the view itself; the tables the view references are not checked against the invoker. `INVOKER` (equivalent to `SECURITY INVOKER`) means the invoker must additionally have the `SELECT` privilege on the tables the view references. An explicit `SECURITY NONE` or `SECURITY INVOKER` clause in the statement always overrides this variable. This variable only affects `CREATE VIEW`; `ALTER VIEW` is unaffected.
* **Scope**: Session
* **Default**: `NONE`
* **Data Type**: String
* **Valid values**: `NONE`, `INVOKER`
* **Introduced in**: v4.1.1

### disable_function_fold_constants

<Restricted />

* **Description**: Disables constant folding optimization for function expressions during query planning.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### disable_table_stats_from_metadata_for_single_table

* **Description**: Controls whether table statistics from metadata are excluded when optimizing queries on single tables.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### div_precision_increment

Used for MySQL client compatibility. No practical usage.

### enable_async_profile

<Restricted />

* **Description**: Enables asynchronous collection and reporting of query execution profiles.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_big_query_log

* **Description**: if enable_big_query_log = true and cpu/io cost of a query exceeds the related threshold, the information will be written to the big query log
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_color_explain_output

* **Scope**: Session
* **Description**: Controls whether ANSI color escape sequences are included in textual EXPLAIN / PROFILE outputs. When enabled (`true`), StmtExecutor passes the session setting into the explain/profile pipeline (via calls to ExplainAnalyzer) so explain, EXPLAIN ANALYZE and analyze-profile outputs contain colored highlighting for readability in ANSI-capable terminals. When disabled (`false`), the output is produced without ANSI sequences (plain text), which is appropriate for logging, clients that do not support ANSI, or when piping output to files. This is a per-session toggle and does not change execution semantics—only the presentation of explain/profile text.
* **Default**: `true`
* **Data type**: boolean
* **Introduced in**: v3.5.0

### enable_connector_async_list_partitions

* **Description**: Enables asynchronous partition listing for external connectors during query planning.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_connector_sink_global_shuffle

<Restricted />

* **Description**: Add a global shuffle between the connector sink fragment and its child fragment.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_connector_sink_writer_scaling

* **Description**: Enables dynamic scaling of connector sink writers to optimize write parallelism based on workload.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_constant_execute_in_fe

* **Description**: Enables the frontend to execute constant expressions directly during query planning rather than sending them to backends.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_desensitize_explain

* **Description**: Enables the inclusion of extended sections such as query queue information in EXPLAIN output at COSTS/VERBOSE level.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_dialect_downgrade

* **Description**: Controls whether Trino dialect syntax is automatically downgraded to StarRocks dialect.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_execution_only

<Restricted />

* **Description**: execute sql don't return result, for performance test
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_explain_in_profile

* **Scope**: Session
* **Description**: When set to `true` and a profile is built for the query, the `EXPLAIN COSTS` text of the executed plan is embedded in the profile's `Summary` section under the `ExplainPlan` key. This lets the optimizer's cardinality estimates, column statistics, predicates, runtime-filter declarations, and overall plan cost be inspected offline alongside the runtime metrics, which is useful when triaging slow queries from a saved profile artifact without access to the live cluster.

  The embedded plan honors the same desensitization controls as other persisted SQL artifacts: credential literals (e.g. in `FILES(...)`) are always redacted, and predicate / projection literals are rendered as digests when either the cluster-wide FE config `enable_sql_desensitize_in_log` or the session variable `enable_desensitize_explain` is enabled.
* **Default**: false
* **Data type**: boolean

### enable_extended_explain

* **Description**: When true, EXPLAIN may include extended sections (e.g. query queue info). Extensible for other features.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_file_metacache

* **Description**: Enables caching of file metadata to improve query performance on external data sources.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_file_pagecache

* **Description**: Enables caching of file pages in memory during query execution.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_full_sort_use_german_string

* **Description**: Enables German collation rules for string sorting in full sort operations.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_global_late_materialization

* **Description**: Enables late materialization optimization to defer fetching non-essential columns until after filtering and limiting operations.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_global_late_materialization_cost_based

* **Description**: When enabled, GLM is only applied to a scan if the estimated byte-cost of the columns that would be deferred exceeds the byte-cost of the row-id locator columns that GLM adds to the scan output. This prevents GLM from being applied when the deferred columns are small (e.g. a single INT) and the row-id overhead would cost more than simply reading those columns eagerly.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_group_level_query_queue (global)

* **Description**: Whether to enable resource group-level [query queue](../administration/management/resource_management/query_queues.md).
* **Default**: false, which means this feature is disabled.
* **Introduced in**: v3.1.4

### enable_groupby_use_output_alias

* **Description**: In order to be compatible with the logic of the old planner, When the column name is the same as the alias name, the alias will be used as the groupby column if set to true.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_insert_select_external_auto_refresh

* **Description**: Enables automatic refresh of external table metadata during INSERT INTO ... SELECT operations.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_ivm_refresh

<Restricted />

* **Description**: Enables incremental materialized view refresh during query execution.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_metadata_profile

* **Description**: Whether to enabled Profile for Iceberg Catalog metadata.
* **Default**: `false`
* **Introduced in**: v3.3.3

### enable_multi_cast_limit_push_down

<Restricted />

* **Description**: When this variable is enabled, the limits of consumers a CTE are pushed down to the producer of the CTE. The limits can then be applied before the exchange. For example: Fragment-2 Fragment-3 Fragment-2 Fragment-3 \ / \ / limit-1 limit-2 shuffle by v1 shuffle by v2 \ / ==> \ / shuffle by v1 shuffle by v2 limit-1 limit-2 \ / \ / Fragment-1 Fragment-1
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_prepare_stmt

* **Description**: Enables or disables the use of prepared statements in the current session.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_profile

* **Description**: Specifies whether to send the profile of a query for analysis. The default value is `false`, which means no profile is required.

  By default, a profile is sent to the FE only when a query error occurs in the BE. Profile sending causes network overhead and therefore affects high concurrency.

  If you need to analyze the profile of a query, you can set this variable to `true`. After the query is completed, the profile can be viewed on the web page of the currently connected FE (address: `fe_host:fe_http_port/query`). This page displays the profiles of the latest 100 queries with `enable_profile` turned on.

* **Default**: false

### enable_query_debug_trace

<Restricted />

* **Description**: Enables detailed debug tracing output for query execution.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_query_dump

* **Description**: Controls per-session query dumping. When this variable is set to true and HTTP Query Dump mode is not active, the server will collect and persist the session's Dump informatation. For non-HTTP queries, the system will serialize the session Dump information and write it to the Query Dump Log when an exception occurs. For HTTP-triggered dumps, the system uses a separate path (to add exception stack traces into the connection's Dump information). Use this variable to opt a session into FE-side query dump collection for post-mortem analysis and debugging; it is evaluated at runtime per session and does not affect global behavior.
* **Default**: false
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_query_history

* **Description**: Enables recording of query execution history for audit and analysis purposes.
* **Scope**: Global
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: No

### enable_query_queue

<Restricted />

* **Description**: Enables or disables the query queue mechanism for managing concurrent query execution.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_query_queue_load (global)

* **Description**: Boolean value to enable query queues for loading tasks.
* **Default**: false

### enable_query_queue_select (global)

* **Description**: Whether to enable query queues for SELECT queries.
* **Default**: false

### enable_query_queue_statistic (global)

* **Description**: Whether to enable query queues for statistics queries.
* **Default**: false

### enable_range_distribution

<Restricted />

* **Description**: Enables the use of range distribution strategy for table data partitioning.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_result_sink_accumulate

* **Description**: Enables accumulation of query results in the result sink before returning them to the client.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_show_all_variables

<Restricted />

* **Description**: Enables displaying all session and global variables in SHOW VARIABLES output.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_simplify_case_when

<Restricted />

* **Description**: Enables optimization of CASE WHEN expressions during query planning.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_skew_detect_with_inaccurate_stats

<Restricted />

* **Description**: When enabled, skew detection proceeds even when table row count is marked as potentially inaccurate (isTableRowCountMayInaccurate). This allows rules consuming skew info (joins, aggregations, window functions) to fire based on histogram/MCV data regardless of row count reliability.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_sql_digest

<Restricted />

* **Description**: Enables generation of SQL digests for query tracking and analysis.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_strict_order_by

* **Description**: Used to check whether the column name referenced in ORDER BY is ambiguous. When this variable is set to the default value `TRUE`, an error is reported for such a query pattern: Duplicate alias is used in different expressions of the query and this alias is also a sorting field in ORDER BY, for example, `select distinct t1.* from tbl1 t1 order by t1.k1;`. The logic is the same as that in v2.3 and earlier. When this variable is set to `FALSE`, a loose deduplication mechanism is used, which processes such queries as valid SQL queries.
* **Default**: true
* **Introduced in**: v2.5.18 and v3.1.7

### enable_strict_type

<Restricted />

* **Description**: Enables strict type checking for implicit type conversions during query execution.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_subfield_no_copy

* **Description**: Enables optimized subfield extraction from complex types without copying the entire parent structure.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_table_name_case_insensitive

* **Description**: This configuration controls case sensitivity for SQL catalog/database/table names. When enabled, these database object names are treated as case-insensitive. IMPORTANT NOTES: - This setting can ONLY be configured during the initial cluster setup via `Config#enable_table_name_case_insensitive` on the FE leader node - Once set during first initialization, this value is IMMUTABLE and will NOT be modified by any subsequent operations including: * Cluster upgrades/downgrades * Fe node restarts * Any other maintenance operations - During FE restart or leader failover, if the leader node's `Config#enable_table_name_case_insensitive` differs from the cluster's initially recorded enableTableNameCaseInsensitive value, the leader node will FAIL to start - Existing clusters CANNOT modify this value. it can only be configured in NEW clusters during initial setup
* **Scope**: Global
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: No

### enable_tde

* **Description**: Controls whether transparent data encryption is enabled for the database.
* **Scope**: Global
* **Default**: `KeyMgr.isEncrypted()`
* **Data type**: `boolean`
* **Mutable**: No

### enable_vector_index_refine

* **Description**: When on, a quantized vector index (IVFPQ, or HNSW with a non-flat quantizer) refines its ANN result by recomputing the exact distance on the full-precision vectors. Off = trust the (lossy) index distance. No effect on a non-quantized index, whose distance is already exact.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### etl_exec_enable_queue_all_workloads

* **Description**: Enables queuing of all workloads in ETL execution mode.
* **Scope**: Global
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: No

### event_scheduler

Used for MySQL client compatibility. No practical usage.

### exec_mode

* **Description**: DEFAULT/ETL
* **Scope**: Session
* **Default**: `SessionVariableConstants.ExecMode.DEFAULT.name()`
* **Data type**: `String`
* **Mutable**: Yes

### follower_query_forward_mode

<Restricted />

* **Description**: Specifies the mode for forwarding queries from follower nodes to leader nodes.
* **Scope**: Session
* **Default**: `""`
* **Data type**: `String`
* **Mutable**: Yes

### forward_to_leader

Used to specify whether some commands will be forwarded to the leader FE for execution. Alias: `forward_to_master`. The default value is `false`, meaning not forwarding to the leader FE. There are multiple FEs in a StarRocks cluster, one of which is the leader FE. Normally, users can connect to any FE for full-featured operations. However, some information is only available on the leader FE.

For example, if the SHOW BACKENDS command is not forwarded to the leader FE, only basic information (for example, whether the node is alive) can be viewed. Forwarding to the leader FE can get more detailed information including the node start time and last heartbeat time.

The commands affected by this variable are as follows:

* SHOW FRONTENDS: Forwarding to the leader FE allows users to view the last heartbeat message.

* SHOW BACKENDS: Forwarding to the leader FE allows users to view the boot time, last heartbeat information, and disk capacity information.

* SHOW BROKER: Forwarding to the leader FE allows users to view the boot time and last heartbeat information.

* SHOW TABLET

* ADMIN SHOW REPLICA DISTRIBUTION

* ADMIN SHOW REPLICA STATUS: Forwarding to the leader FE allows users to view the tablet information stored in the metadata of the leader FE. Normally, the tablet information should be the same in the metadata of different FEs. If an error occurs, you can use this method to compare the metadata of the current FE and the leader FE.

* Show PROC: Forwarding to the leader FE allows users to view the PROC information stored in the metadata. This is mainly used for metadata comparison.

### full_sort_late_materialization

* **Description**: Enables late materialization optimization for full sort operations to improve performance on high-cardinality columns.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### full_sort_max_buffered_bytes

<Restricted />

* **Description**: Limits the maximum amount of memory buffered during full sort operations before initiating cascading merge phases.
* **Scope**: Session
* **Default**: `256L * 1024 * 1024`
* **Data type**: `long`
* **Mutable**: Yes

### full_sort_max_buffered_rows

<Restricted />

* **Description**: Limits the maximum number of rows buffered in memory during a full sort operation.
* **Scope**: Session
* **Default**: `1 * 1024 * 1024 * 1024`
* **Data type**: `long`
* **Mutable**: Yes

### global_late_materialization_max_fetch_ops

* **Description**: Limits the maximum number of fetch operations allowed in global late materialization optimization.
* **Scope**: Session
* **Default**: `4`
* **Data type**: `int`
* **Mutable**: Yes

### global_late_materialization_max_limit

* **Description**: Limits the maximum number of rows processed during global late materialization optimization.
* **Scope**: Session
* **Default**: `4096`
* **Data type**: `int`
* **Mutable**: Yes

### group_concat_max_len

* **Description**: The maximum length of string returned by the [group_concat](sql-functions/string-functions/group_concat.md) function.
* **Default**: 1024
* **Min value**: 4
* **Unit**: Characters
* **Data type**: Long

### historical_nodes_min_update_interval

* **Description**: The minimum interval between two updates of historical node records. If the nodes of a cluster change frequently in a short period of time (that is, less than the value set in this variable), some intermediate states will not be recorded as valid historical node snapshots. The historical nodes are the main basis for the Cache Sharing feature to choose the right cache nodes during cluster scaling.
* **Default**: 600
* **Unit**: Seconds
* **Introduced in**: v3.5.1

### init_connect (global)

Used for MySQL client compatibility. No practical usage.

### innodb_read_only

* **Description**: Session-level flag (MySQL-compatible) that indicates the session's InnoDB read-only mode. The variable is declared and stored on the session as the Java field `innodbReadOnly` in `SessionVariable.java` and is accessible via `isInnodbReadOnly()` and `setInnodbReadOnly(boolean)`. The SessionVariable class only holds the flag; any enforcement (preventing write/DDL to InnoDB tables or altering transaction behavior) must be implemented by the transaction/storage/authorization layers which should read this session flag. Use this variable to convey client intent for read-only behavior within the current session for components that respect it.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### interactive_timeout

Used for MySQL client compatibility. No practical usage.

### interleaving_group_size

* **Description**: Specifies the group size threshold for interleaving query execution, where negative values force interleaving.
* **Scope**: Session
* **Default**: `10`
* **Data type**: `int`
* **Mutable**: Yes

### interpolate_passthrough

<Restricted />

* **Description**: Enables optimization to pass through interpolation predicates directly in query execution without intermediate processing.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### k_factor

* **Description**: INSUFFICIENT_CONTEXT
* **Scope**: Session
* **Default**: `1`
* **Data type**: `double`
* **Mutable**: Yes

### language (global)

Used for MySQL client compatibility. No practical usage.

### large_decimal_underlying_type

* **Description**: Specifies the underlying data type representation for large decimal numbers in queries.
* **Scope**: Session
* **Default**: `SessionVariableConstants.PANIC`
* **Data type**: `String`
* **Mutable**: Yes

### license (global)

* **Description**: Displays the license of StarRocks.
* **Default**: Apache License 2.0

### lower_case_table_names (global)

Used for MySQL client compatibility. No practical usage. Table names in StarRocks are case-sensitive.

### lower_upper_support_utf8

* **Description**: Determines whether the upper/lower function supports utf8, introduced by https://github.com/StarRocks/starrocks/pull/56192 Before this, the upper/lower function only supports ascii characters, and SR has made special optimizations in performance. After this change, the upper/lower function is able to handle utf8 characters, but the performance will be slightly reduced in the scenario of only ascii characters. This variable is added to give the user the right to choose. If the user does not use utf8 characters, turning off the switch can avoid performance degradation. In order to be compatible with the previous behavior, the default value is false.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### max_allowed_packet

* **Description**: Used for compatibility with the JDBC connection pool C3P0. This variable specifies the maximum size of packets that can be transmitted between the client and server.
* **Default**: 33554432 (32 MB). You can raise this value if the client reports "PacketTooBigException".
* **Unit**: Byte
* **Data type**: Int

### max_buckets_per_be_to_use_balancer_assignment

<Restricted />

* **Description**: The threshold for determining whether to use a more evenly assignment bucket sequences to backend algorithm for query execution. This algorithm is only used when `numBucketsPerBe` is smaller than this threshold, because the time complexity of it is `numBucketsPerBe` times than the previous algorithm.
* **Scope**: Session
* **Default**: `6`
* **Data type**: `int`
* **Mutable**: Yes

### max_unknown_string_meta_length (global)

* **Description**: Fallback length for string columns in query result metadata when the max length is unknown. Clients that rely on the metadata may return empty values or truncation if the reported length is smaller than actual values. Valid range is `1` to `1048576`.
* **Default**: 64
* **Data Type**: int
* **Introduced in**: v3.5.16, v4.0.9

### mcv_row_percentage_propagation_threshold

<Restricted />

* **Description**: The percentage of rows that need to be contained in MCVs so that MCV propagation through operators is enabled while losing bucket information. Lowering this threshold can lead to worse plans.
* **Scope**: Session
* **Default**: `0.5`
* **Data type**: `double`
* **Mutable**: Yes

### net_buffer_length

Used for MySQL client compatibility. No practical usage.

### net_read_timeout

Used for MySQL client compatibility. No practical usage.

### net_write_timeout

Used for MySQL client compatibility. No practical usage.

### ngram_search_support_utf8

* **Description**: Enable UTF-8 support for ngram_search and ngram_search_case_insensitive functions. When enabled, n-grams are computed based on UTF-8 characters instead of bytes. This allows proper similarity computation for non-ASCII text (e.g., Cyrillic, Chinese). Default is false for backward compatibility.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### parse_tokens_limit

* **Description**: Limits the maximum number of tokens allowed during SQL query parsing.
* **Scope**: Session
* **Default**: `3500000`
* **Data type**: `int`
* **Mutable**: Yes

### performance_schema (global)

Used for compatibility with MySQL JDBC versions 8.0.16 and above. No practical usage.

### pipeline_profile_level

* **Description**: Controls the level of the query profile. A query profile often has five layers: Fragment, FragmentInstance, Pipeline, PipelineDriver, and Operator. Different levels provide different details of the profile:

  * 0: StarRocks combines metrics of the profile and shows only a few core metrics.
  * 1: default value. StarRocks simplifies the profile and combines metrics of the profile to reduce profile layers.
  * 2: StarRocks retains all the layers of the profile. The profile size is large in this scenario, especially when the SQL query is complex. This value is not recommended.

* **Default**: 1
* **Data type**: Int

### pq_refine_factor

* **Description**: _Description pending._
* **Scope**: Session
* **Default**: `1`
* **Data type**: `double`
* **Mutable**: Yes

### prefer_compute_node

* **Description**: Enables preferential use of compute nodes over backend nodes for query execution.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### profile_format_version

* **Description**: Profile output format version: 1 = legacy (separate MIN/MAX counters), 2 = compact (inline min/max)
* **Scope**: Session
* **Default**: `1`
* **Data type**: `int`
* **Mutable**: Yes

### profile_log_latency_threshold_ms

* **Scope**: Session
* **Description**: Minimum query latency (milliseconds) for the FE to write a profile to `fe.profile.log`. Only queries with execution time greater than or equal to this value are logged. When set to `-1` (default), the FE config `profile_log_latency_threshold_ms` is used. When set to `0`, all profiles are logged. When set to a positive value (e.g. `1000`), only queries with latency ≥ that value (in ms) are logged. Use this session variable to override the deployment-wide config per connection.
* **Default**: -1
* **Data type**: long
* **Unit**: Milliseconds

### profile_timeout

<Restricted />

* **Description**: Sets the maximum time in seconds to wait for query profile collection before timing out.
* **Scope**: Session
* **Default**: `10`
* **Data type**: `int`
* **Mutable**: Yes

### push_down_heavy_exprs

* **Description**: Enables pushing down computationally expensive expressions to the storage layer during query execution.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### query_debug_options

<Restricted />

* **Description**: _Description pending._
* **Scope**: Session
* **Default**: `""`
* **Data type**: `String`
* **Mutable**: Yes

### query_delivery_timeout

* **Scope**: Session
* **Description**: Timeout (in seconds) for phase 1 of query execution — the delivery of all plan fragment instances from the coordinator (FE) to backend executors (BEs). StarRocks executes queries in two phases: (1) deliver fragments to BEs, and (2) pull results after fragments are prepared. `query_delivery_timeout` controls how long the coordinator waits for fragment delivery to complete before timing out. When building the query options sent to the backend, the value is assigned to the `query_delivery_timeout` field (subject to an internal cap to avoid integer overflow). If `enablePhasedScheduler` is enabled, the system uses the `query_timeout` value instead for delivery timeout.
* **Default**: `300`
* **Data Type**: int (seconds)
* **Introduced in**: v3.2.0

### query_history_keep_seconds

* **Description**: Specifies the retention period for query history records in seconds.
* **Scope**: Global
* **Default**: `86400 * 3`
* **Data type**: `long`
* **Mutable**: No

### query_history_load_interval_seconds

* **Description**: Specifies the interval in seconds between loading query history entries into memory.
* **Scope**: Global
* **Default**: `60 * 15`
* **Data type**: `long`
* **Mutable**: No

### query_queue_concurrency_limit (global)

* **Description**: The upper limit of concurrent queries on a BE. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed.
* **Default**: 0
* **Data type**: Int

### query_queue_cpu_used_permille_limit (global)

* **Description**: The upper limit of CPU usage permille (CPU usage * 1000) on a BE. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed.
* **Value range**: [0, 1000]
* **Default**: `0`

### query_queue_driver_high_water

* **Description**: Effective iff it is non-negative.
* **Scope**: Global
* **Default**: `-1`
* **Data type**: `int`
* **Mutable**: No

### query_queue_driver_low_water

* **Description**: Effective iff it is non-negative.
* **Scope**: Global
* **Default**: `-1`
* **Data type**: `int`
* **Mutable**: No

### query_queue_fresh_resource_usage_interval_ms

* **Description**: Use the resource usage, only when the duration from the last report is within this interval.
* **Scope**: Global
* **Default**: `5000`
* **Data type**: `long`
* **Mutable**: No

### query_queue_max_queued_queries (global)

* **Description**: The upper limit of queries in a queue. When this threshold is reached, incoming queries are rejected. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed.
* **Default**: `1024`.

### query_queue_mem_used_pct_limit (global)

* **Description**: The upper limit of memory usage percentage on a BE. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed.
* **Value range**: [0, 1]
* **Default**: 0

### query_queue_pending_timeout_second (global)

* **Description**: The maximum timeout of a pending query in a queue. When this threshold is reached, the corresponding query is rejected.
* **Default**: 300
* **Unit**: Second

### query_timeout

* **Description**: Used to set the query timeout in "seconds". This variable will act on all query statements in the current connection. The default value is 300 seconds. From v3.4.0 onwards, `query_timeout` does not apply to operations involved INSERT (for example, UPDATE, DELETE, CTAS, materialized view refresh, statistics collection, and PIPE).
* **Value range**: [1, 259200]
* **Default**: 300
* **Data type**: Int
* **Unit**: Second

### resource_group

        * **Description**: The specified resource group of this session
        * **Default**: ""
        * **Data Type**: String
        * **Introduced in**: 3.2.0

### resource_group_id

<Restricted />

* **Description**: Specifies the resource group to which queries in the session are assigned for resource management.
* **Scope**: Session
* **Default**: `0`
* **Data type**: `int`
* **Mutable**: Yes

### rpc_http_min_size

<Restricted />

* **Description**: if a packet's size is larger than RPC_HTTP_MIN_SIZE, it will use RPC via http, as the std rpc has 2GB size limit. the setting size is a bit smaller than 2GB, as the pre-computed serialization size of packets may not accurate. no need to change it in general.
* **Scope**: Session
* **Default**: `((1L << 31) - (1L << 10))`
* **Data type**: `long`
* **Mutable**: Yes

### run_mode

* **Description**: _Description pending._
* **Scope**: Global
* **Default**: `Config.run_mode`
* **Data type**: `String`
* **Mutable**: No

### runtime_profile_report_interval

* **Description**: The time interval at which runtime profiles are reported.
* **Default**: 10
* **Unit**: Second
* **Data type**: Int
* **Introduced in**: v3.1.0

### select_ratio_threshold

<Restricted />

* **Description**: Controls the minimum ratio threshold for determining whether to use a SELECT operation in query optimization.
* **Scope**: Session
* **Default**: `0.15`
* **Data type**: `double`
* **Mutable**: Yes

### skip_black_list

* **Description**: Controls whether to bypass the blacklist when selecting backup compute nodes for tablets in shared-data mode.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### sql_auto_is_null

* **Description**: this is used to make c3p0 library happy
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### sql_dialect

* **Description**: The SQL dialect that is used. For example, you can run the `set sql_dialect = 'trino';` command to set the SQL dialect to Trino, so you can use Trino-specific SQL syntax and functions in your queries.

  > **NOTICE**
  >
  > After you configure StarRocks to use the Trino dialect, identifiers in queries are not case-sensitive by default. Therefore, you must specify names in lowercase for your databases and tables at database and table creation. If you specify database and table names in uppercase, queries against these databases and tables will fail.

* **Data type**: StarRocks
* **Introduced in**: v3.0

### sql_mode

Used to specify the SQL mode to accommodate certain SQL dialects. Valid values include:

* `PIPES_AS_CONCAT`: The pipe symbol `|` is used to concatenate strings, for example, `select 'hello ' || 'world'`.
* `ONLY_FULL_GROUP_BY` (Default): The SELECT LIST can only contain GROUP BY columns or aggregate functions.
* `ALLOW_THROW_EXCEPTION`: returns an error instead of NULL when type conversion fails.
* `FORBID_INVALID_DATE`: prohibits invalid dates.
* `MODE_DOUBLE_LITERAL`: interprets floating-point types as DOUBLE rather than DECIMAL.
* `SORT_NULLS_LAST`: places NULL values at the end after sorting.
* `ERROR_IF_OVERFLOW`: returns an error instead of NULL in the case of arithmetic overflow. Currently, only the DECIMAL data type supports this option.
* `GROUP_CONCAT_LEGACY`: uses the `group_concat` syntax of v2.5 and earlier. This option is supported from v3.0.9 and v3.1.6.
* `FORBID_INVALID_IMPLICIT_CAST`: enforces Trino-style strict type checking at plan time. Only widening coercions within the same type family are allowed implicitly (for example, `TINYINT`→`INT`→`BIGINT`→`DECIMAL`→`DOUBLE`, `DATE`→`DATETIME`). Casts within the `VARCHAR`/`CHAR` family are allowed regardless of declared length. Cross-family casts (such as `string`↔`numeric`, `string`↔`date`, `numeric`↔`date`, `boolean`↔other types) and narrowing numeric casts (such as `BIGINT`→`INT` or `DOUBLE`→`FLOAT`) are rejected with a semantic error. Use an explicit `CAST` to perform those conversions.
* `STRUCT_CAST_BY_NAME`: enables name-based field matching when casting between STRUCT types, rather than the default position-based matching. When this mode is enabled, fields in the source struct are matched to fields in the target struct by field name (case-insensitively), regardless of the order in which they are declared. Fields present in the source but absent in the target are ignored; fields present in the target but absent in the source are filled with NULL. This mode affects both the FE type resolution (common supertype computation for UNION ALL and castability checks) and the BE cast evaluation (runtime field reordering in CastStructExpr). This is particularly useful when performing UNION ALL on STRUCT columns whose fields are defined in different orders across branches.

You can set only one SQL mode, for example:

```SQL
set sql_mode = 'PIPES_AS_CONCAT';
```

Or, you can set multiple modes at a time, for example:

```SQL
set sql_mode = 'PIPES_AS_CONCAT,ERROR_IF_OVERFLOW,GROUP_CONCAT_LEGACY';
```

### sql_quote_show_create

* **Description**: Controls whether identifiers in SHOW CREATE TABLE output are quoted with backticks.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### sql_safe_updates

Used for MySQL client compatibility. No practical usage.

### sql_select_limit

* **Description**: Used to limit the maximum number of rows returned by a query, which can prevent issues such as insufficient memory or network congestion caused by the query returning too much data.
* **Default**: Unlimited
* **Data type**: Long

### storage_engine

The types of engines supported by StarRocks:

* olap (default): StarRocks system-owned engine.
* mysql: MySQL external tables.
* broker: Access external tables through a broker program.
* elasticsearch or es: Elasticsearch external tables.
* hive: Hive external tables.
* iceberg: Iceberg external tables, supported from v2.1.
* hudi: Hudi external tables, supported from v2.2.
* jdbc: external table for JDBC-compatible databases, supported from v2.3.

### system_time_zone

Used to display the time zone of the current system. Cannot be changed.

### time_zone

Used to set the time zone of the current session. The time zone can affect the results of certain time functions.

### trace_log_level

<Restricted />

* **Description**: 0, 1, 2... : more detail logs will be printed with higher level
* **Scope**: Session
* **Default**: `0`
* **Data type**: `int`
* **Mutable**: Yes

### trace_log_mode

<Restricted />

* **Description**: command, file
* **Scope**: Session
* **Default**: `"command"`
* **Data type**: `String`
* **Mutable**: Yes

### tvr_target_mvid

<Restricted />

* **Description**: Specifies the target materialized view ID for time-validity rule operations.
* **Scope**: Session
* **Default**: `""`
* **Data type**: `String`
* **Mutable**: Yes

### use_compute_nodes

* **Description**: Controls whether to route queries to compute nodes instead of backend nodes.
* **Scope**: Session
* **Default**: `-1`
* **Data type**: `int`
* **Mutable**: Yes

### version (global)

The MySQL server version returned to the client. The value is the same as FE parameter `mysql_server_version`.

### version_comment (global)

The StarRocks version. Cannot be changed.

### wait_timeout

* **Description**: The number of seconds the server waits for activity on a non-interactive connection before closing it. If a client does not interact with StarRocks for this length of time, StarRocks will actively close the connection.
* **Default**: 28800 (8 hours).
* **Unit**: Second
* **Data type**: Int

### warehouse

* **Description**: Specifies the warehouse to which queries and operations are directed.
* **Scope**: Session
* **Default**: `WarehouseManager.DEFAULT_WAREHOUSE_NAME`
* **Data type**: `String`
* **Mutable**: Yes

