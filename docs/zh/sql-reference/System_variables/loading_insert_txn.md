---
displayed_sidebar: docs
sidebar_label: "导入、Insert 与事务"
sidebar_position: 7
description: "用于数据导入、INSERT 行为以及事务处理的会话变量。"
---

# 系统变量 - 导入、Insert 与事务

如需了解如何查看和设置变量，请参见 [系统变量概述](../System_variable.md)。

### dynamic_overwrite

* 描述：是否为 INSERT OVERWRITE 语句覆盖写分区表时启用 [Dynamic Overwrite](./sql-statements/loading_unloading/INSERT.md#dynamic-overwrite) 语义。有效值：
  * `true`：启用 Dynamic Overwrite。
  * `false`：禁用 Dynamic Overwrite 并使用默认语义。
* 默认值：false
* 引入版本：v3.4.0

### enable_insert_partial_update

* **描述**：是否为主键表的 INSERT 语句启用部分更新（Partial Update）。当设置为 `true`（默认）时，如果 INSERT 语句只指定了部分列（少于表中所有非生成列），系统会执行部分更新，即仅更新指定列，并保留其他列的现有值。当设置为 `false` 时，系统会对未指定的列使用默认值，而不是保留已有值。此功能特别适用于对主键表的特定列进行更新，而不影响其他列的值。
* **默认值**：true
* **引入版本**：v3.3.20、v3.4.9、v3.5.8、v4.0.2

### enable_insert_strict

* 描述：是否在使用 INSERT from FILES() 导入数据时启用严格模式。有效值：`true` 和 `false`（默认值）。启用严格模式时，系统仅导入合格的数据行，过滤掉不合格的行，并返回不合格行的详细信息。更多信息请参见 [严格模式](../loading/load_concept/strict_mode.md)。在早于 v3.4.0 的版本中，当 `enable_insert_strict` 设置为 `true` 时，INSERT 作业会在出现不合格行时失败。
* 默认值：true

### enable_load_profile

* **作用域**: Session
* **描述**: 启用后，FE 会请求收集 load 作业的运行时 profile，load 协调器将在 load 完成后收集/导出该 profile。对于 stream load，FE 会设置 `TQueryOptions.enable_profile = true` 并将 `load_profile_collect_second`（来自 `stream_load_profile_collect_threshold_second`）传递给 backends；协调器随后有条件地调用 profile 收集（参见 StreamLoadTask.collectProfile()）。实际行为是此会话变量与目标表上的表级属性 `enable_load_profile` 的逻辑 OR；采集还受 `load_profile_collect_interval_second`（FE 端采样间隔）的限制以避免频繁采集。会话标志通过 `SessionVariable.isEnableLoadProfile()` 读取，并且可以通过 `setEnableLoadProfile(...)` 按连接设置。
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### insert_max_filter_ratio

* 描述：INSERT 导入作业的最大容忍率，即导入作业能够容忍的因数据质量不合格而过滤掉的数据行所占的最大比例。当不合格行数比例超过该限制时，导入作业失败。默认值：`0`。范围：[0, 1]。
* 默认值：0
* 引入版本：v3.4.0

### insert_timeout

* 描述：INSERT 作业的超时时间。单位：秒。从 v3.4.0 版本开始，`insert_timeout` 作用于所有涉及 INSERT 的操作（例如，UPDATE、DELETE、CTAS、物化视图刷新、统计信息收集和 PIPE），替代原本的 `query_timeout`。
* 默认值：14400
* 引入版本：v3.4.0

### load_mem_limit

* 描述：用于指定导入操作的内存限制，单位为 Byte。默认值为 0，即表示不使用该变量，而采用 `query_mem_limit` 作为导入操作的内存限制。

  这个变量仅用于 INSERT 操作。因为 INSERT 操作涉及查询和导入两个部分，如果用户不设置此变量，则查询和导入操作各自的内存限制均为 `query_mem_limit`。否则，INSERT 的查询部分内存限制为 `query_mem_limit`，而导入部分限制为 `load_mem_limit`。

  其他导入方式，如 Broker Load，STREAM LOAD 的内存限制依然使用 `query_mem_limit`。

* 默认值：0
* 单位：Byte

### log_rejected_record_num（3.1 及以后）

* 描述：指定最多允许记录多少条因数据质量不合格而过滤掉的数据行数。取值范围：`0`、`-1`、大于 0 的正整数。
* 取值为 `0` 表示不记录过滤掉的数据行。
* 取值为 `-1` 表示记录所有过滤掉的数据行。
* 取值为大于 0 的正整数（比如 `n`）表示每个 BE 节点上最多可以记录 `n` 条过滤掉的数据行。

### partial_update_mode

* 描述：控制部分更新的模式，有效值为：

  * `auto`（默认值），表示由系统通过分析更新语句以及其涉及的列，自动判断执行部分更新时使用的模式。
  * `column`，指定使用列模式执行部分更新，比较适用于涉及少数列并且大量行的部分列更新场景。

  详细信息，请参见[UPDATE](sql-statements/table_bucket_part_index/UPDATE.md#列模式的部分更新自-31)。
* 默认值：auto
* 引入版本：v3.1

### transaction_read_only

* 描述：用于兼容 MySQL 5.8 以上客户端，无实际作用。别名 `tx_read_only`。该变量用于指定事务访问模式。取值 `ON` 表示只读。取值 `OFF` 表示可读可写。
* 默认值：OFF
* 引入版本：v2.5.18, v3.0.9, v3.1.7

### tx_isolation

用于兼容 MySQL 客户端，无实际作用。别名 `transaction_isolation`。

### tx_visible_wait_timeout

* **描述**: 会话范围的超时时间（秒），控制服务器在提交事务后等待该事务变为可见（published）再继续处理的最长时间。如果可见等待超时，事务将被视为已 COMMITTED 但尚未 VISIBLE。物化视图刷新逻辑（`MVTaskRunProcessor`）会临时将此变量设置为 `Long.MAX_VALUE / 1000` 以有效地无限期等待可见性，并在刷新后恢复原值。当启用 `enable_sync_publish` 时，该变量被忽略，因为 publish 等待时间将由作业截止时间推导。
* **范围**: Session
* **默认值**: `10`
* **类型**: long
* **引入版本**: v3.2.0

