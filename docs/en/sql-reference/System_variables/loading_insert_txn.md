---
displayed_sidebar: docs
sidebar_label: "Loading, Insert, and Transactions"
sidebar_position: 7
description: "Session variables for data loading, INSERT behavior, and transaction handling."
---

# System Variables - Loading, Insert, and Transactions

For how to view and set variables, see the [System variables overview](../System_variable.md).

### dynamic_overwrite

* **Description**: Whether to enable the [Dynamic Overwrite](../sql-statements/loading_unloading/INSERT.md#dynamic-overwrite) semantic for INSERT OVERWRITE with partitioned tables. Valid values:
  * `true`: Enables Dynamic Overwrite.
  * `false`: Disables Dynamic Overwrite and uses the default semantic.
* **Default**: false
* **Introduced in**: v3.4.0

### enable_insert_partial_update

* **Description**: Whether to enable Partial Update for INSERT statements on Primary Key tables. When this item is set to `true` (default), if an INSERT statement specifies only a subset of columns (fewer than the number of all non-generated columns in the table), the system performs a Partial Update to update only the specified columns while preserving existing values in other columns. When set to `false`, the system uses default values for unspecified columns instead of preserving existing values. This feature is particularly useful for updating specific columns in Primary Key tables without affecting other column values.
* **Default**: true
* **Introduced in**: v3.3.20, v3.4.9, v3.5.8, v4.0.2

### enable_insert_strict

* **Description**: Whether to enable strict mode while loading data using INSERT from files(). Valid values: `true` and `false` (Default). When strict mode is enabled, the system loads only qualified rows. It filters out unqualified rows and returns details about the unqualified rows. For more information, see [Strict mode](../../loading/load_concept/strict_mode.md). In versions earlier than v3.4.0, when `enable_insert_strict` is set to `true`, the INSERT jobs fails when there is an unqualified rows.
* **Default**: true

### enable_load_profile

* **Scope**: Session
* **Description**: When enabled, the FE requests collection of the runtime profile for load jobs and the load coordinator will collect/export the profile after a load completes. For stream load, FE sets `TQueryOptions.enable_profile = true` and passes `load_profile_collect_second` (from `stream_load_profile_collect_threshold_second`) to backends; the coordinator then conditionally calls profile collection (see StreamLoadTask.collectProfile()). The effective behavior is the logical OR of this session variable and the table-level property `enable_load_profile` on the destination table; collection is further gated by `load_profile_collect_interval_second` (FE-side sampling interval) to avoid frequent collection. The session flag is read via `SessionVariable.isEnableLoadProfile()` and can be set per-connection with `setEnableLoadProfile(...)`.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_sql_transaction

* **Description**: Enables SQL transaction capability for the current session.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### insert_local_shuffle_for_window_pre_agg

* **Description**: Enables local data shuffling during window function pre-aggregation to optimize query performance.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### insert_max_filter_ratio

* **Description**: The maximum error tolerance of INSERT from files(). It's the maximum ratio of data records that can be filtered out due to inadequate data quality. When the ratio of unqualified data records reaches this threshold, the job fails. Range: [0, 1].
* **Default**: 0
* **Introduced in**: v3.4.0

### insert_timeout

* **Description**: The timeout duration of the INSERT job. Unit: Seconds. From v3.4.0 onwards, `insert_timeout` applies to operations involved INSERT (for example, UPDATE, DELETE, CTAS, materialized view refresh, statistics collection, and PIPE), replacing `query_timeout`.
* **Default**: 14400
* **Introduced in**: v3.4.0

### load_mem_limit

Specifies the memory limit for the import operation. The default value is 0, meaning that this variable is not used and `query_mem_limit` is used instead.

This variable is only used for the `INSERT` operation which involves both query and import. If the user does not set this variable, the memory limit for both query and import will be set as `exec_mem_limit`. Otherwise, the memory limit for query will be set as `exec_mem_limit` and the memory limit for import will be as `load_mem_limit`.

Other import methods such as `BROKER LOAD`, `STREAM LOAD` still use `exec_mem_limit` for memory limit.

### load_transmission_compression_type

* **Description**: Specifies the compression algorithm used for transmitting data during load operations.
* **Scope**: Session
* **Default**: `"NO_COMPRESSION"`
* **Data type**: `String`
* **Mutable**: Yes

### log_rejected_record_num (v3.1 and later)

Specifies the maximum number of unqualified data rows that can be logged. Valid values: `0`, `-1`, and any non-zero positive integer. Default value: `0`.

* The value `0` specifies that data rows that are filtered out will not be logged.
* The value `-1` specifies that all data rows that are filtered out will be logged.
* A non-zero positive integer such as `n` specifies that up to `n` data rows that are filtered out can be logged on each BE.

### partial_update_mode

* **Description**: Used to control the mode of partial updates. Valid values:

  * `auto` (default): The system automatically determines the mode of partial updates by analyzing the UPDATE statement and the columns involved.
  * `column`: The column mode is used for the partial updates, which is particularly suitable for the partial updates which involve a small number of columns and a large number of rows.

  For more information, see [UPDATE](../sql-statements/table_bucket_part_index/UPDATE.md#partial-updates-in-column-mode-since-v31).
* **Default**: auto
* **Introduced in**: v3.1

### transaction_isolation

* **Description**: Session-scoped compatibility variable that records the client-requested transaction isolation level using MySQL-style names. Declared in SessionVariable as `transactionIsolation` and annotated with `@VariableMgr.VarAttr(name = TRANSACTION_ISOLATION)`; its presence ensures compatibility with MySQL 5.8 clients. There is a related `tx_isolation` variable (kept for c3p0 client compatibility). The value is stored per-session to satisfy client/libraries, e.g., `REPEATABLE-READ`. Transaction isolation semantics in the engine are managed by StarRocks' transaction subsystem and may not be changed solely by modifying this session variable.
* **Scope**: Session
* **Default**: `REPEATABLE-READ`
* **Data Type**: String
* **Introduced in**: v3.2.0

### transaction_read_only

* **Description**: Used for MySQL 5.8 compatibility. The alias is `tx_read_only`. This variable specifies the transaction access mode. `ON` indicates read only and `OFF` indicates readable and writable.
* **Default**: OFF
* **Introduced in**: v2.5.18, v3.0.9, v3.1.7

### tx_isolation

Used for MySQL client compatibility. No practical usage. The alias is `transaction_isolation`.

### tx_visible_wait_timeout

* **Description**: Session-scoped timeout (in seconds) that controls how long the server waits for a committed transaction to become visible (published) before proceeding. If the visible wait expires, the transaction is treated as COMMITTED but not yet VISIBLE. Materialized view refresh logic (`MVTaskRunProcessor`) temporarily sets this variable to `Long.MAX_VALUE / 1000` to wait effectively indefinitely for visibility and restores the original value after refresh. When `enable_sync_publish` is set to `true`, this variable is ignored because the publish wait is derived from the job deadline instead.
* **Scope**: Session
* **Default**: `10`
* **Data Type**: long
* **Introduced in**: v3.2.0

