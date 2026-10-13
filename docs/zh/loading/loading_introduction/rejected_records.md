---
sidebar_position: 60
displayed_sidebar: docs
sidebar_label: "拒绝行"
keywords: ['rejected', 'records', 'max_filter_ratio', 'replay']
description: "介绍当 max_filter_ratio > 0 时 StarRocks 如何将被过滤的行持久化到 _statistics_.rejected_records，以及如何查询和回放被拒绝的记录。"
---

# 拒绝行（Rejected Records）

对于 `max_filter_ratio` 不为 0 的导入作业，系统可以配置为将每一行被过滤的数据持久化到系统表 **`_statistics_.rejected_records`** 中，方便您排查异常数据，并在无需重新执行整个作业的情况下将其回放至目标表。本文介绍如何启用拒绝行捕获功能、查询该表，以及如何使用 SQL 回放被拒绝的行。

`_statistics_.rejected_records` 提供以下来源的被拒绝数据信息：

- Stream Load、Routine Load、Broker Load 以及 `INSERT`（包括 `INSERT INTO ... SELECT ... FROM FILES()`）
- Scanner 解析失败（CSV 列数不匹配、类型转换失败、严格模式过滤）
- Sink 约束冲突（NOT NULL、分区范围未命中、VARCHAR 长度、DECIMAL 精度）
- ORC reader 行拒绝（列存格式，发生在应用过滤条件之前）

## 启用拒绝行捕获

拒绝行捕获功能默认关闭，以避免新建集群在您显式启用之前就向该系统表写入数据。

要启用该功能，请按照以下步骤操作：

1. 将 BE 配置项 `enable_rejected_record_sync` 设置为 `true`，以启用拒绝行同步（写入系统表）功能。

   - 要动态修改该配置，请执行以下 SQL：

     ```SQL
     UPDATE information_schema.be_configs SET VALUE = "true" WHERE name = "enable_rejected_record_sync";
     ```

   - 要永久修改该配置，请在 `be.conf` 中添加以下配置项，并重启 BE 服务：

     ```Properties
     enable_rejected_record_sync=true
     ```

2. 将 `log_rejected_record_num` 设置为一个正数（表示上限）或 `-1`（表示不限制记录数量），可在会话级别或导入作业级别进行设置。

   ```sql
   -- 会话级别设置，适用于 INSERT / INSERT ... SELECT
   SET log_rejected_record_num = -1;

   -- Broker Load 属性
   LOAD LABEL mydb.my_label ( ... )
   PROPERTIES (
       "log_rejected_record_num" = "10000"
   );

   -- Routine Load 属性
   CREATE ROUTINE LOAD mydb.my_job ON my_table ...
   PROPERTIES (
       "log_rejected_record_num" = "10000",
       ...
   );

   -- Stream Load Header
   curl -H "log_rejected_record_num: 10000" ...
   ```

被拒绝的记录将同步至 `_statistics_.rejected_records`，供后续操作使用。

## 查询 `_statistics_.rejected_records`

您可以先执行以下 SQL 查看 `_statistics_.rejected_records` 的表结构：

```SQL
DESC _statistics_.rejected_records;
```

您可以重点关注以下列，用于日常排查：

| 列           | 说明                             |
| ------------ | --------------------------------------- |
| `raw_record` | 被拒绝的行，JSON 格式，以列名为键。 |
| `error_code`<br />`error_message`<br />`error_column` | 该行被拒绝的原因。 |
| `load_label`<br />`load_type`<br />`txn_id` | 产生该被拒绝行的导入作业的标签、类型和事务 ID。对于 Broker Load、Routine Load 和 INSERT 作业，`user_name` 为 NULL。您可以基于 `load_label` 将 `_statistics_.rejected_records` 与 `_statistics_.loads_history` 关联查询，以获取 `user_name`。 |
| `source_info` | 导入数据源的相关信息。对于文件导入为 `file` 和 `line`，对于 Routine Load 为 `topic`、`partition` 和 `offset`。 |
| `created_at` | 该记录的生成时间，是该系统表的分区键。建议优先基于该列进行过滤。 |

以下示例展示了该表的一些基本用法：

- 查看某个指定导入作业被拒绝的行，并按最新记录优先排序。

  ```SQL
  SELECT created_at, error_code, error_column, error_message, raw_record
  FROM _statistics_.rejected_records
  WHERE load_label = 'load_orders_20260327'
  ORDER BY created_at DESC
  LIMIT 100;
  ```

- 查看某个目标表在过去 24 小时内的错误分布情况。

```SQL
SELECT error_code, error_column, COUNT(*) AS cnt
FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table = 'orders'
  AND created_at >= NOW() - INTERVAL 1 DAY
GROUP BY error_code, error_column
ORDER BY cnt DESC;
```

- 基于与 `information_schema.loads` 的关联查询，查看某个导入作业的所有被拒绝行。由于 `information_schema.loads` 未提供 `txn_id`，因此必须基于导入标签进行关联。

```SQL
SELECT r.created_at, r.error_code, r.raw_record, l.state, l.scan_rows
FROM _statistics_.rejected_records AS r
JOIN information_schema.loads AS l
  ON r.load_label = l.label
WHERE r.load_label = 'my_load_label_2026_04_28';
```

## 回放被拒绝的行

`raw_record` 列是一个 JSON 字符串，包含被拒绝行的各列值，并以列名为键。您可以使用 `->>` 运算符将某个值提取为字符串，再通过 `CAST(... AS <类型>)` 将其恢复为目标数据类型。

以下示例通过截断的方式修复 VARCHAR 长度超限的问题，并将修复后的值 INSERT 至目标表。

```SQL
INSERT INTO mydb.orders (order_id, customer_name, amount, created_at)
SELECT
    CAST(raw_record->>'order_id'      AS BIGINT),
    LEFT(raw_record->>'customer_name', 64),
    CAST(raw_record->>'amount'        AS DECIMAL(10,2)),
    CAST(raw_record->>'created_at'    AS DATETIME)
FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table = 'orders'
  AND error_code    = 'VALUE_OUT_OF_RANGE'
  AND created_at    > '2026-03-27';
```

当 Scanner 完全无法拆分该行时（例如 CSV 文件出现列数不匹配的情况），`raw_record` 会仅包含一个键 `_raw`，其值为原始行内容。

以下示例展示了用于诊断的前 20 行无法解析的数据。

```SQL
SELECT raw_record->>'_raw' AS raw_line
FROM _statistics_.rejected_records
WHERE error_code = 'PARSE_ERROR'
ORDER BY created_at DESC
LIMIT 20;
```

## 数据保留与清理

`_statistics_.rejected_records` 按天分区，并根据表属性 `partition_live_number = 7` 自动清理过期分区。您可以通过修改 FE 配置项 `rejected_records_retained_days`（默认值为 `7`）来调整保留时长。table-keeper
守护线程会在下一次巡检时同步该表属性。

以下示例删除某个指定目标表中，创建时间早于指定日期的记录：

```sql
DELETE FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table    = 'orders'
  AND created_at      < '2026-03-01';
```

## 权限

对 `_statistics_.rejected_records` 的访问由内置的行级访问策略控制：

- 内置的 **`root`** 用户可以查看该表中的所有行（该策略不会对 root 应用任何过滤条件）。
- **其他所有用户**——包括拥有 `db_admin`、`cluster_admin`、`user_admin` 和 `security_admin` 角色的用户——仅能查看其拥有 `SELECT` 权限的 `target_database.target_table` 所对应的行。用户没有 `SELECT` 权限的表所对应的行会从结果集中被过滤掉。
- 如果该策略无法解析或校验某一行的目标对象，则该行会被隐藏（即失败时默认拒绝访问）。

对于运维人员使用的仪表盘，如果需要完整可见性，请使用管理员账号；否则，请为报表角色授予相关目标表的 `SELECT` 权限。

## 限制

- **Parquet 被拒绝记录仅包含定位信息，而非完整行数据。**

  Parquet 导入会将出错列的原始值记录在单列的 `raw_record` 片段中，**同时**在 `source_info` 中附加来源定位信息（anchor）：

  ```json
  {
    "format": "parquet",
    "file": "gs://bucket/orders.parquet",
    "row_in_file": 1817542,
    "file_size": 12345678,
    "file_mtime_ms": 1711531331000
  }
  ```

  仅凭 `raw_record` 就足以逐列诊断问题所在。若要回放完整行数据，后续提交将提供 `parquet_read_rows(file, anchors)` TVF，该函数会使用 anchor 重新读取源文件以还原完整行。在该 TVF 上线之前，anchor 仍然可用于帮助用户定位到原始 Parquet 文件中的具体行（`row_in_file` 从 0 开始计数），以及校验源文件是否发生变化（`file_size` 和 `file_mtime_ms` 会在扫描开始时被记录为快照，在尝试手动还原前应先进行比对）。

- **`information_schema.loads.rejected_record_path` 已废弃。**

  该字段过去指向的 BE 本地 Tab 分隔的拒绝记录文件已被移除；为保证升级兼容性，该列仍然保留，但其值始终为 `NULL`。请直接通过 `load_label` 或 `txn_id` 查询 `_statistics_.rejected_records`。

- **秒级延迟。**

  导入作业完成后，被拒绝的行会在 `rejected_record_sync_interval_sec`（默认 30 秒）内变为可查询状态，而非立即可查询。

## 相关配置

| 作用域            | 参数                                     | 默认值 | 说明                                                |
| ---------------- | --------------------------------------- | ------- | ---------------------------------------------------------- |
| 会话变量 | `log_rejected_record_num`               | `0`     | 记录的被拒绝记录数量。`0` 表示禁用该功能，`-1` 表示不限制记录数量。 |
| FE 配置项 | `rejected_records_retained_days`        | `7`     | `_statistics_.rejected_records` 中保留的每日分区数。 |
| BE 配置项 | `enable_rejected_record_sync`           | `false` | 用于控制将被拒绝的行同步至 `_statistics_.rejected_records` 的守护线程的总开关。 |
| BE 配置项 | `rejected_record_sync_interval_sec`     | `30`    | 同步任务的调度间隔。                 |
| BE 配置项 | `rejected_record_sync_max_batch_rows`   | `10000` | 每个 Merge Commit 批次的行数软上限。               |
| BE 配置项 | `rejected_record_local_retention_hours` | `24`    | 无法同步的记录在本地文件中的 GC（清理）保留时长。 |
| BE 配置项 | `rejected_record_sync_post_timeout_sec` | `60`    | 每次请求的 Stream Load 超时时间。                           |
