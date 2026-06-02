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
CANCEL ALTER TABLE { COLUMN | OPTIMIZE | ROLLUP } FROM [db_name.]table_name [ (rollup_job_id [, rollup_job_id]) ] [ FORCE ]
```

## 参数

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`

  - 如果指定 `COLUMN`，则此语句会取消修改列的操作。
  - 如果指定 `OPTIMIZE`，则此语句会取消优化表结构的操作。
  - 如果指定 `ROLLUP`，则此语句会取消添加或删除 Rollup 索引的操作。您可以进一步指定 `rollup_job_id` 来取消特定的 Rollup 作业。

- `db_name`：可选。表所属的数据库的名称。如果未指定此参数，则默认使用您当前的数据库。
- `table_name`：必需。表名。
- `rollup_job_id`：可选。Rollup 作业的 ID。可通过 [SHOW ALTER MATERIALIZED VIEW](../materialized_view/SHOW_ALTER_MATERIALIZED_VIEW.md) 获取 Rollup 作业 ID。
- `FORCE`：可选。**仅供运维人员使用的应急手段**，用于强制取消存算分离（lake）表上 `publish_version` 长时间卡在 `FINISHED_REWRITING` 状态的 `COLUMN` alter 作业（例如由于 `txnlog` 缺失、segment/SST 文件丢失或持久化存储故障导致）。普通的 `CANCEL ALTER TABLE` 会拒绝取消处于 `FINISHED_REWRITING` 的作业，`FORCE` 会绕过该限制。其内部会执行一次 no-op publish（在不应用该 alter 变更的前提下推进分区的可见版本），然后取消作业，从而解除该表上后续导入的阻塞。

  :::warning

  - `FORCE` 受 FE 配置项 `enable_admin_skip_committed_txn`（默认 `false`）控制，仅应在恢复期间开启，恢复后立即关闭。该配置项以及事务级别的同类应急手段详见 [ADMIN SKIP COMMITTED TRANSACTION](../cluster-management/tablet_replica/ADMIN_SKIP_COMMITTED_TRANSACTION.md)。
  - `FORCE` **仅支持存算分离（lake）表上的 `COLUMN` alter 作业**（schema change 以及 `enable_persistent_index` / `file_bundling` 等元数据 alter）。**不支持** `OPTIMIZE`、`ROLLUP` 以及物化视图 alter；对这些类型使用 `FORCE` 会被拒绝。
  - 被强制取消的 alter 将被丢弃，变更不会生效。

  :::

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

5. 强制取消存算分离（lake）表上 publish 卡住的 `COLUMN` alter 操作（仅供运维人员使用，需要 `enable_admin_skip_committed_txn=true`）。

   ```SQL
   -- 仅在恢复期间开启该应急开关。
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "true");

   CANCEL ALTER TABLE COLUMN FROM example_db.example_table FORCE;

   -- 恢复后立即关闭。
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "false");
   ```
