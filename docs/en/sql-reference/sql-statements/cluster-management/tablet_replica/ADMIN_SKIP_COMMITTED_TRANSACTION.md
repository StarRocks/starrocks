---
displayed_sidebar: docs
---

# ADMIN SKIP COMMITTED TRANSACTION

Force-abort a transaction that is stuck in the `COMMITTED` state on a shared-data (lake) table by performing a no-op publish: the transaction's data contribution is discarded, while the partition's visible version still advances by writing a new tablet metadata file that carries no data changes from this transaction.

This is an **operator-only escape hatch** for unblocking a publish queue that is permanently stalled (for example by a missing `txnlog`, a lost segment / SST file, or a persistent storage outage). It will **discard the affected transaction's data**. Use only when normal retry cannot recover, and prefer it over destructive recovery paths such as dropping the partition.

:::warning

Aborting a `COMMITTED` transaction is **irreversible** and causes data loss for that transaction. The transaction record is still finalized as `VISIBLE` to keep the partition's version sequence intact, but it is internally marked as a no-op publish.

:::

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SKIP COMMITTED TRANSACTION <txn_id> [REASON '<text>']
```

## Parameters

| Parameter | Required | Description |
|---|---|---|
| `txn_id` | Yes | The numeric transaction ID. Find it with [SHOW TRANSACTION](../../loading_unloading/SHOW_TRANSACTION.md) or in the FE log. |
| `REASON '<text>'` | No | A free-form reason for the abort, recorded in the FE audit log. Strongly recommended so the action can be tracked back during incident review. |

## Constraints (phase 1)

1. **Feature is gated by FE config**. The statement is rejected unless the FE config `enable_admin_skip_committed_txn` is set to `true`. The default is `false` to prevent accidental data loss. Set it via:

   ```sql
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "true");
   ```

   Set it back to `false` after the recovery is finished.

2. **The transaction must be in the `COMMITTED` state**. Transactions in `PREPARED` time out on their own (driven by the txn `timeout` setting) or can be cancelled via the loader-specific path — for example `CANCEL LOAD` for broker / spark / routine load, or `POST /api/transaction/rollback` for stream load. Transactions already in `VISIBLE` cannot be undone.

3. **Only shared-data (cloud-native) tables with `file_bundling=true` are supported**. With `file_bundling`, the partition's metadata is written atomically by the aggregator, so the no-op publish either takes effect for the whole partition or not at all. Tables without `file_bundling` may end up in a partial-publish state and are rejected at the entrypoint.

4. **Only load and lake-compaction transactions are supported in this release**. Alter / schema-change transactions are not supported yet and return an error if attempted.

## Behavior

1. FE validates the state, source type, and table properties.
2. FE persists a no-op-publish marker on the transaction in the edit log.
3. The next tick of the publish daemon picks up the marker and sends the publish RPC to BE with the no-op-publish flag set.
4. BE writes a tablet metadata file at the transaction's target version whose content equals the base version (no data changes from this transaction). The aggregator under `file_bundling` makes this write atomic across the whole partition.
5. The partition's visible version advances to the transaction's target version. Subsequent transactions on the table can now publish normally.
6. The original transaction is recorded as `VISIBLE`, but its no-op-publish marker is preserved for audit.

Race resolution: if the in-flight publish RPC happened to succeed first, the transaction becomes `VISIBLE` normally and the marker is a no-op (the data lands safely instead of being discarded). This is recorded in the FE log.

## Auditing

`SHOW PROC '/transactions/<db_name>/finished'` exposes two trailing columns for inspecting whether the abort actually took effect:

- `NoOpPublish` — `true`/`false`, whether the transaction was finalized via the no-op publish path
- `NoOpPublishReason` — the free-form `REASON` text supplied to the `ADMIN SKIP COMMITTED TRANSACTION` statement

Use these to retroactively confirm whether a recovery action succeeded and what justification was recorded.

## Examples

1. Find the stuck transaction:

   ```sql
   SHOW PROC '/transactions/<db_name>/running';
   ```

2. Enable the feature switch:

   ```sql
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "true");
   ```

3. Abort the stuck transaction:

   ```sql
   ADMIN SKIP COMMITTED TRANSACTION 12345 REASON 'txnlog file lost on OSS, manual recovery';
   ```

4. Disable the feature switch again to prevent accidental use:

   ```sql
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "false");
   ```

## Related

- [`SHOW TRANSACTION`](../../loading_unloading/SHOW_TRANSACTION.md) — inspect transaction state.
- [`ADMIN REPAIR`](./ADMIN_REPAIR.md) — rollback a lake partition to a historical version when metadata is unrecoverable; a more destructive option used when this command is not applicable.
