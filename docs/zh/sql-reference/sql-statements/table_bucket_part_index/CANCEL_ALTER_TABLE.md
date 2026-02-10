---
displayed_sidebar: docs
---

# CANCEL ALTER TABLE

CANCEL ALTER TABLE 语句可以取消正在执行的 ALTER TABLE 操作，包括：

- 修改列。
- 优化表结构（从 v3.2 版本开始），包括修改分桶方式和分桶数量。
- 创建和删除 rollup 索引。

:::note
- 此语句为同步操作。
- 执行此语句需要您拥有表的 `ALTER_PRIV` 权限。
- 此语句仅支持取消使用 ALTER TABLE 执行的异步操作（如上所述），不支持取消使用 ALTER TABLE 执行的同步操作，例如重命名。
:::

## 语法

```SQL
CANCEL ALTER TABLE { COLUMN | OPTIMIZE | ROLLUP } FROM [db_name.]table_name [ (rollup_job_id [, rollup_job_id]) ]
```

## 参数

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`

  - 如果指定 `COLUMN`，则此语句会取消修改列的操作。
  - 如果指定 `OPTIMIZE`，则此语句会取消优化表结构的操作。
  - 如果指定 `ROLLUP`，则此语句会取消添加或删除 Rollup 索引的操作。您可以进一步指定 `rollup_job_id` 来取消特定的 Rollup 作业。

- `db_name`：可选。表所属的数据库的名称。如果未指定此参数，则默认使用您当前的数据库。
- `table_name`：必需。表名。
- `rollup_job_id`：可选。Rollup 作业的 ID。可通过 [SHOW ALTER MATERIALIZED VIEW](../materialized_view/SHOW_ALTER_MATERIALIZED_VIEW.md) 获取 Rollup 作业 ID。

## 示例

1. 取消数据库 `example_db` 中 `example_table` 的修改列操作。

   ```SQL
   CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
   ```

2.  取消数据库 `example_db` 中 `example_table` 的表结构优化操作。

   ```SQL
   CANCEL ALTER TABLE OPTIMIZE FROM example_db.example_table;
   ```

3. 取消当前数据库中 `example_table` 的添加或删除 rollup index 的操作。

   ```SQL
   CANCEL ALTER TABLE ROLLUP FROM example_table;
   ```

4. 使用作业 ID 取消当前数据库中 `example_table` 的特定 rollup 更改。

   ```SQL
   CANCEL ALTER TABLE ROLLUP FROM example_table (12345, 12346);
   ```
