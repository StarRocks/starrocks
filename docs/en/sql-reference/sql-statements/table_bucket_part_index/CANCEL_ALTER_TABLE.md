---
displayed_sidebar: docs
description: "Cancels an in-progress ALTER TABLE operation such as column modification, schema optimization, or rollup index creation."
---

# CANCEL ALTER TABLE

CANCEL ALTER TABLE cancels the execution of the ongoing ALTER TABLE operation, including:

- Modify columns.
- Optimize table schema (from v3.2), including modifying the bucketing method and the number of buckets.
- Create and delete the rollup index.

:::note
- This statement is a synchronous operation.
- This statement requires you to have the `ALTER_PRIV` privilege on the table.
- This statement only supports canceling asynchronous operations using ALTER TABLE (as mentioned above) and does not support canceling synchronous operations using ALTER TABLE, such as rename.
:::

## Syntax

```SQL
CANCEL ALTER TABLE { COLUMN | OPTIMIZE | ROLLUP } FROM [db_name.]table_name [ (rollup_job_id [, rollup_job_id]) ] [ FORCE ]
```

## Parameters

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`

  - If `COLUMN` is specified, this statement cancels operations of modifying columns.
  - If `OPTIMIZE` is specified, this statement cancels operations of optimizing table schema.
  - If `ROLLUP` is specified, this statement cancels operations of adding or deleting the rollup index. You can further specify `rollup_job_id` to cancel specific rollup jobs.

- `db_name`: optional. The name of the database to which the table belongs. If this parameter is not specified, your current database is used by default.
- `table_name`: required. The table name.
- `rollup_job_id`: optional. The ID of the rollup job. You can get the rollup job ID using [SHOW ALTER MATERIALIZED VIEW](../materialized_view/SHOW_ALTER_MATERIALIZED_VIEW.md).
- `FORCE`: optional. An **operator-only escape hatch** for force-cancelling a `COLUMN` alter job on a shared-data (lake) table whose `publish_version` is permanently stuck in the `FINISHED_REWRITING` state (for example by a missing `txnlog`, a lost segment / SST file, or a persistent storage outage). A normal `CANCEL ALTER TABLE` refuses to cancel a job in `FINISHED_REWRITING`; `FORCE` bypasses that guard. Internally it performs a no-op publish (advancing the partition's visible version without applying the alter's changes) and then cancels the job, which unblocks subsequent loads on the table.

  :::warning

  - `FORCE` is gated by the FE config `enable_admin_skip_committed_txn` (default `false`); enable it only during recovery and disable it again immediately afterwards. See [ADMIN SKIP COMMITTED TRANSACTION](../cluster-management/tablet_replica/ADMIN_SKIP_COMMITTED_TRANSACTION.md) for the shared config and the related transaction-level escape hatch.
  - `FORCE` is supported **only for `COLUMN` alter jobs** (schema change and metadata alter, such as `enable_persistent_index` / `file_bundling`) **on shared-data (lake) tables**. It is **not** supported for `OPTIMIZE`, `ROLLUP`, or materialized-view alters; using `FORCE` with those is rejected.
  - The alter being force-cancelled is discarded; the change does not take effect.

  :::

## Examples

1. Cancel the operation of modifying columns for `example_table` in the database `example_db`.

   ```SQL
   CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
   ```

2. Cancel the operation of optimizing table schema for `example_table` in the database `example_db`.

   ```SQL
   CANCEL ALTER TABLE OPTIMIZE FROM example_db.example_table;
   ```

3. Cancel the operation of adding or deleting the rollup index for `example_table` in the current database.

   ```SQL
   CANCEL ALTER TABLE ROLLUP FROM example_table;
   ```

4. Cancel specific rollup alterations for `example_table` in the current database using their job IDs.

   ```SQL
   CANCEL ALTER TABLE ROLLUP FROM example_table (12345, 12346);
   ```

5. Force-cancel a publish-stuck `COLUMN` alter on a shared-data (lake) table (operator-only; requires `enable_admin_skip_committed_txn=true`).

   ```SQL
   -- Enable the escape hatch only for the duration of the recovery.
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "true");

   CANCEL ALTER TABLE COLUMN FROM example_db.example_table FORCE;

   -- Disable it again immediately afterwards.
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "false");
   ```