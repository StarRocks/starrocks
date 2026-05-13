---
displayed_sidebar: docs
sidebar_label: "拒绝行"
keywords: ['rejected', 'records', 'max_filter_ratio', '回放']
---

# 拒绝行（Rejected records）

当导入允许非零 `max_filter_ratio` 时，StarRocks 将每一条被过滤的行写入
系统表 **`_statistics_.rejected_records`**，使运维可以在不重跑整次导入的
前提下检视脏数据并用 SQL 回放到目标表。本文说明如何启用、查询并回放这些记录。

系统表覆盖的行级拒绝场景：

- Stream Load、Routine Load、Broker Load、`INSERT`（含
  `INSERT INTO ... SELECT ... FROM FILES()`）
- Scanner 解析失败（CSV 列数不匹配、类型转换失败、strict mode 过滤）
- Sink 约束校验失败（NOT NULL、分区范围外、VARCHAR 超长、DECIMAL 精度）
- ORC 读取器的行级拒绝（列式格式，在 filter 应用前捕获）

## 启用拒绝行捕获

默认关闭，新集群在显式开启前不会写入系统表。需要同时设置两个开关：

1. **单次导入**：把 `log_rejected_record_num` 设为正数（上限）或 `-1`（不限）：

   ```sql
   -- Session 级（INSERT / INSERT ... SELECT）
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

   -- Stream Load 头部
   curl -H "log_rejected_record_num: 10000" ...
   ```

2. **集群级**：把 BE 配置 `enable_rejected_record_sync` 设为 `true`。
   不设置时拒绝行仍然会落到 BE 本地 JSON Lines 文件，但同步守护线程
   不会把它们回写到系统表：

   ```bash
   curl -X POST "http://<be>:<be_http_port>/api/update_config?enable_rejected_record_sync=true"
   ```

   或者写入 `be.conf` 以持久化：

   ```
   enable_rejected_record_sync=true
   ```

## 查询系统表

使用 `DESCRIBE _statistics_.rejected_records;` 查看完整 schema。日常排障
最常用的列：

- `raw_record`（JSON）— 被拒绝的行，以列名为键
- `error_code`、`error_message`、`error_column` — 拒绝原因
- `load_label`、`load_type`、`txn_id` — 谁产生的。`user_name` 在
  Broker / Routine / INSERT 导入时为 NULL（BE 执行分片拿不到提交者
  身份）；需要用户信息请按 `load_label` JOIN
  `_statistics_.loads_history` 恢复
- `source_info`（JSON）— 文件导入是 `file`+`line`，Routine Load 是
  `topic`/`partition`/`offset`
- `created_at` — 分区键；查询时优先用它过滤

```sql
-- 某次导入的全部拒绝行，按最新排序
SELECT created_at, error_code, error_column, error_message, raw_record
FROM _statistics_.rejected_records
WHERE load_label = 'load_orders_20260327'
ORDER BY created_at DESC
LIMIT 100;

-- 最近 24 小时某张目标表的错误分布
SELECT error_code, error_column, COUNT(*) AS cnt
FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table = 'orders'
  AND created_at >= NOW() - INTERVAL 1 DAY
GROUP BY error_code, error_column
ORDER BY cnt DESC;

-- 与 information_schema.loads JOIN 查看一次导入的整体信息。
-- 注意：information_schema.loads 没有 txn_id 列，应当用 label 关联
-- （两侧都把 label 作为单次导入的稳定句柄）。
SELECT r.created_at, r.error_code, r.raw_record, l.state, l.scan_rows
FROM _statistics_.rejected_records AS r
JOIN information_schema.loads AS l
  ON r.load_label = l.label
WHERE r.load_label = 'my_load_label_2026_04_28';
```

## 回放拒绝行

`raw_record` 列是 JSON，键为列名、值为原始字符串。用 `->>` 取出字符串，
再 `CAST(... AS <type>)` 恢复目标类型。

```sql
-- 修复 VARCHAR 超长后回放
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

当 Scanner 完全无法按列拆分（例如 CSV 列数不匹配）时，`raw_record`
只含单键 `_raw`，保存原始整行：

```sql
-- 排查无法解析的前 20 行
SELECT raw_record->>'_raw' AS raw_line
FROM _statistics_.rejected_records
WHERE error_code = 'PARSE_ERROR'
ORDER BY created_at DESC
LIMIT 20;
```

## 保留与清理

表按日分区，`partition_live_number = 7` 自动过期。调整 FE 配置
`rejected_records_retained_days`（默认 `7`）后，TableKeeper 守护线程
会在下一次 tick 时把表属性同步为新值。一次性清理：

```sql
DELETE FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table    = 'orders'
  AND created_at      < '2026-03-01';
```

## 权限

`_statistics_.rejected_records` 的查询受内建行访问策略保护：

- 内置的 **`root`** 用户能看到表里的全部数据（策略不附加过滤条件）。
- **其他所有用户** —— 包括持有 `db_admin`、`cluster_admin`、
  `user_admin`、`security_admin` 等内置 admin 角色的用户 —— 只能看到
  自己对 `target_database.target_table` 拥有 `SELECT` 权限的那些行。
  没有 SELECT 权限的目标表对应的行会被过滤掉。
- 如果策略无法解析或校验目标，则该行会被隐藏（fail-closed）。

运维看板可按需选择：全量可见时使用 `root` 账号；按权限查看时，把相关
目标表的 `SELECT` 权限授予看板使用的角色即可。

## 当前限制

- **Parquet 拒绝行携带锚点，不是完整行。** Parquet 导入在
  `raw_record` 里写失败列的单列片段，并在 `source_info` 里写回放锚点：

  ```json
  {
    "format": "parquet",
    "file": "gs://bucket/orders.parquet",
    "row_in_file": 1817542,
    "file_size": 12345678,
    "file_mtime_ms": 1711531331000
  }
  ```

  `raw_record` 本身已足以逐列排查原因。完整行回放使用
  `parquet_read_rows(source_info)` 表值函数，它把锚点作为输入并以
  lateral 形式接在 `_statistics_.rejected_records` 右侧：

  ```sql
  -- 把 db.t 表中所有 parquet 拒绝行还原后修复重导
  INSERT INTO db.t
  SELECT cast(json_extract_string(p.raw_record, '$.id')   AS INT),
         cast(case when json_extract_string(p.raw_record, '$.val') = 'bad_val'
                   then '0'
                   else json_extract_string(p.raw_record, '$.val')
              end AS INT),
         json_extract_string(p.raw_record, '$.name')
  FROM _statistics_.rejected_records r,
       TABLE(parquet_read_rows(r.source_info)) p
  WHERE r.target_database = 'db' AND r.target_table = 't'
    AND r.format = 'parquet';
  ```

  `parquet_read_rows` 每个锚点输出一行，列为
  `(file VARCHAR, row_in_file BIGINT, raw_record JSON)`。
  `raw_record` 是按 Parquet 列名为键的 JSON 对象，值保留 Parquet 中
  的原始内容（例如 `STRING` 列里的 `"bad_val"` 仍以字符串形式回来，
  你在 SQL 里再施加修复后写回）。当 `source_info` 携带 `file_size` /
  `file_mtime_ms` 时，BE 会在打开文件后做 fail-closed 校验，大小或
  修改时间漂移则整条查询失败；若底层文件系统不暴露修改时间
  （S3 / OSS），mtime 校验自动跳过。单个 chunk 处理的锚点数受 BE
  配置 `parquet_read_rows_max_anchors` 限制（默认 10000）。
- **`information_schema.loads.rejected_record_path` 已弃用。** 它此前
  指向的 BE 本地 tab 分隔拒绝文件已被移除；列本身保留用于升级兼容，取值
  恒为 `NULL`。改用 `_statistics_.rejected_records`，按 `load_label`
  或 `txn_id` 直接查询。
- **秒级延迟。** 导入结束后，拒绝行在 `rejected_record_sync_interval_sec`
  （默认 30 秒）内才在系统表中可查询，并非即时可见。

## 相关配置一览

| 作用域 | 名称 | 默认值 | 说明 |
| --- | --- | --- | --- |
| Session | `log_rejected_record_num` | `0` | `0` = 关闭、`-1` = 不限、`N` = 限额 `N` 条 |
| FE | `rejected_records_retained_days` | `7` | 系统表每日分区保留天数 |
| BE | `enable_rejected_record_sync` | `false` | 同步守护线程总开关 |
| BE | `rejected_record_sync_interval_sec` | `30` | 同步调度周期 |
| BE | `rejected_record_sync_max_batch_rows` | `10000` | 单次 merge-commit 批次行数软上限 |
| BE | `rejected_record_local_retention_hours` | `24` | 本地文件 GC 保留上限 |
| BE | `rejected_record_sync_post_timeout_sec` | `60` | 单次 Stream Load 请求超时 |
